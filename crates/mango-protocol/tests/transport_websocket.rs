//! Runs the conformance suite over a real WebSocket connection: a TCP
//! listener that upgrades with `accept_websocket`, and a dialler that offers
//! `mango.v1` and presents a bearer token.
//!
//! Two message ceilings are exercised. The 2 KiB one makes the suite's bulk
//! results many chunks, which is what the interleaving case needs to mean
//! anything.
#![cfg(all(feature = "testing", feature = "websocket"))]

use std::time::Duration;

use mango_protocol::close::close_codes;
use mango_protocol::session::{Session, SessionClosure, SessionOptions};
use mango_protocol::testing::{ConformancePair, Fixture, NoRawConnection, run_conformance_suite};
use mango_protocol::transports::deadline::{ConnectDeadline, ConnectError};
use mango_protocol::transports::websocket::client::{WebSocketConnectOptions, connect_websocket};
use mango_protocol::transports::websocket::server::{AcceptError, accept_websocket};
use mango_protocol::transports::websocket::{WebSocketOptions, WebSocketPort};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// The credential the acceptor in these tests knows.
const TOKEN: &str = "conformance-token";

/// One accepted connection, upgraded and authorised, plus the address it came
/// in on.
struct Acceptor {
    listener: TcpListener,
    options: WebSocketOptions,
}

impl Acceptor {
    async fn bind(options: WebSocketOptions) -> Self {
        Self {
            listener: TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a loopback port"),
            options,
        }
    }

    fn url(&self) -> String {
        let address = self.listener.local_addr().expect("a bound address");
        format!("ws://{address}/conformance")
    }

    async fn accept(&self) -> Result<WebSocketPort<TcpStream>, AcceptError> {
        let (socket, _address) = self.listener.accept().await.expect("a dialler arrives");
        accept_websocket(socket, self.options, |token| match token {
            Some(TOKEN) => Ok(()),
            _ => Err(close_codes::UNAUTHORIZED),
        })
        .await
    }
}

async fn dial(
    url: &str,
    options: WebSocketOptions,
    token: Option<&str>,
) -> Result<mango_protocol::transports::websocket::client::DialledWebSocketPort, ConnectError> {
    let mut connect = WebSocketConnectOptions::default().with_websocket(options);
    if let Some(token) = token {
        connect = connect.with_bearer(token);
    }
    connect_websocket(
        url,
        &connect,
        &ConnectDeadline::default().with_timeout(Duration::from_secs(10)),
    )
    .await
}

struct SocketPair {
    a: Session,
    b: Session,
    driver_a: Option<JoinHandle<SessionClosure>>,
    driver_b: Option<JoinHandle<SessionClosure>>,
}

impl ConformancePair for SocketPair {
    fn a(&self) -> &Session {
        &self.a
    }

    fn b(&self) -> &Session {
        &self.b
    }

    async fn sever(&mut self) {
        // Aborting b's driver drops its half of the socket without a close
        // frame: a sees the connection vanish, which is what a crash leaves.
        if let Some(driver_b) = self.driver_b.take() {
            driver_b.abort();
        }
    }

    async fn close(&mut self) {
        self.a.close_now(close_codes::RELEASED, None);
        self.b.close_now(close_codes::RELEASED, None);
        if let Some(driver_a) = self.driver_a.take() {
            let _ = driver_a.await;
        }
        if let Some(driver_b) = self.driver_b.take() {
            let _ = driver_b.await;
        }
    }
}

struct WebSocketFixture {
    options: WebSocketOptions,
}

impl Fixture for WebSocketFixture {
    type Pair = SocketPair;
    type Raw = NoRawConnection;

    async fn connect(&self, a: SessionOptions, b: SessionOptions) -> SocketPair {
        let acceptor = Acceptor::bind(self.options).await;
        let url = acceptor.url();
        let dialling = tokio::spawn({
            let options = self.options;
            async move { dial(&url, options, Some(TOKEN)).await }
        });
        let accepted = acceptor.accept().await.expect("the credential is known");
        let dialled = dialling
            .await
            .expect("the dial task runs")
            .expect("the acceptor selects mango.v1");

        let (session_a, driver_a) = Session::spawn(dialled, a);
        let (session_b, driver_b) = Session::spawn(accepted, b);
        SocketPair {
            a: session_a,
            b: session_b,
            driver_a: Some(driver_a),
            driver_b: Some(driver_b),
        }
    }

    /// Frames are split across messages, so two concurrent oversized results
    /// are an interleaving test the suite knows how to run.
    fn chunked(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn the_websocket_behaves_like_a_mango_transport() {
    run_conformance_suite(&WebSocketFixture {
        options: WebSocketOptions::default(),
    })
    .await;
}

#[tokio::test]
async fn the_websocket_behaves_like_a_mango_transport_at_a_small_message_ceiling() {
    run_conformance_suite(&WebSocketFixture {
        options: WebSocketOptions::default().with_max_message_bytes(2048),
    })
    .await;
}

#[tokio::test]
async fn a_credential_the_acceptor_does_not_know_is_refused_before_hello() {
    let acceptor = Acceptor::bind(WebSocketOptions::default()).await;
    let url = acceptor.url();
    let dialling =
        tokio::spawn(async move { dial(&url, WebSocketOptions::default(), Some("wrong")).await });

    let refusal = acceptor
        .accept()
        .await
        .expect_err("the credential is not the one this acceptor knows");
    assert!(
        matches!(
            refusal,
            AcceptError::Unauthorized {
                code: close_codes::UNAUTHORIZED
            }
        ),
        "{refusal}"
    );

    // The upgrade itself completed, which is what lets the dialler read a
    // code at all: a refused upgrade would reach it as a socket that never
    // opened.
    let port = dialling
        .await
        .expect("the dial task runs")
        .expect("the upgrade completed before the credential was judged");
    let peer = mango_protocol::frame::PeerInfo {
        name: "dialler".into(),
        version: "0".into(),
        role: "runtime".into(),
    };
    let (session, driver) = Session::spawn(port, SessionOptions::new(peer));
    let closure = session.closed().await;
    assert_eq!(closure.code, close_codes::UNAUTHORIZED);
    assert!(
        closure.fatal,
        "4401 is a code redialling cannot recover from"
    );
    let _ = driver.await;
}

#[tokio::test]
async fn a_dialler_that_does_not_offer_the_subprotocol_is_closed_with_4400() {
    let acceptor = Acceptor::bind(WebSocketOptions::default()).await;
    let address = acceptor.listener.local_addr().expect("a bound address");
    // A bare tungstenite dial offers no subprotocol at all.
    let dialling = tokio::spawn(async move {
        tokio_tungstenite::connect_async(format!("ws://{address}/conformance")).await
    });

    let refusal = acceptor
        .accept()
        .await
        .expect_err("a connection without mango.v1 is not a session");
    assert!(matches!(refusal, AcceptError::Subprotocol), "{refusal}");
    let _ = dialling.await;
}

#[tokio::test]
async fn an_acceptor_that_selects_nothing_refuses_the_dial() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let address = listener.local_addr().expect("a bound address");
    // An acceptor that upgrades without echoing the subprotocol: a WebSocket,
    // but not a Mango Protocol one.
    tokio::spawn(async move {
        let (socket, _address) = listener.accept().await.expect("a dialler");
        let _ = tokio_tungstenite::accept_async(socket).await;
    });

    let error = dial(
        &format!("ws://{address}/conformance"),
        WebSocketOptions::default(),
        None,
    )
    .await
    .expect_err("an acceptor that selected nothing is not speaking mango.v1");
    // Whether the refusal comes from the handshake itself or from this
    // crate's own check of the selected subprotocol, what matters is that no
    // port is handed back for a connection neither side agreed the protocol
    // on.
    match error {
        ConnectError::Refused { detail, .. } => {
            assert!(detail.to_lowercase().contains("subprotocol"), "{detail}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}
