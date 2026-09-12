//! Runs the conformance suite over a real local socket: a Unix domain socket
//! on POSIX, a named pipe on Windows. One listener, one dialled connection,
//! one session on each end.
#![cfg(feature = "testing")]

use std::sync::atomic::{AtomicU64, Ordering};

use mango_protocol::close::close_codes;
use mango_protocol::session::{Session, SessionClosure, SessionOptions};
use mango_protocol::testing::{ConformancePair, Fixture, NoRawConnection, run_conformance_suite};
use mango_protocol::transports::deadline::ConnectDeadline;
use mango_protocol::transports::ipc::{IpcListener, connect_ipc, listen_ipc};
use tokio::task::JoinHandle;

static NEXT_ADDRESS: AtomicU64 = AtomicU64::new(0);

/// An address no other case in this run is using. A conformance run opens one
/// connection per case, and a Windows pipe name is global to the machine.
fn address() -> String {
    let unique = NEXT_ADDRESS.fetch_add(1, Ordering::Relaxed);
    let name = format!("mango-conformance-{}-{unique}", std::process::id());
    if cfg!(windows) {
        format!(r"\\.\pipe\{name}")
    } else {
        std::env::temp_dir()
            .join(format!("{name}.sock"))
            .display()
            .to_string()
    }
}

struct SocketPair {
    a: Session,
    b: Session,
    driver_a: Option<JoinHandle<SessionClosure>>,
    driver_b: Option<JoinHandle<SessionClosure>>,
    listener: Option<IpcListener>,
}

impl ConformancePair for SocketPair {
    fn a(&self) -> &Session {
        &self.a
    }

    fn b(&self) -> &Session {
        &self.b
    }

    async fn sever(&mut self) {
        // Aborting b's driver drops its end of the connection without a
        // farewell: a sees the socket close, which is what a crashed peer
        // leaves behind.
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
        if let Some(listener) = self.listener.take() {
            listener.close().await;
        }
    }
}

struct IpcFixture;

impl Fixture for IpcFixture {
    type Pair = SocketPair;
    type Raw = NoRawConnection;

    async fn connect(&self, a: SessionOptions, b: SessionOptions) -> SocketPair {
        let path = address();
        let mut listener = listen_ipc(&path).await.expect("the address is free");

        // The dial has to be in flight before `accept` is awaited: a named
        // pipe server waits for a client, and a Unix listener has nothing to
        // accept until one arrives.
        let dial = tokio::spawn({
            let path = path.clone();
            async move { connect_ipc(path, &ConnectDeadline::default()).await }
        });
        let (accepted, _identity) = listener.accept().await.expect("a connection arrives");
        let dialled = dial
            .await
            .expect("the dial task runs")
            .expect("the address accepts");

        let (session_a, driver_a) = Session::spawn(dialled, a);
        let (session_b, driver_b) = Session::spawn(accepted, b);
        SocketPair {
            a: session_a,
            b: session_b,
            driver_a: Some(driver_a),
            driver_b: Some(driver_b),
            listener: Some(listener),
        }
    }

    /// Frames are split across reads on a byte stream, exactly as they are on
    /// stdio.
    fn chunked(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn the_local_socket_behaves_like_a_mango_transport() {
    run_conformance_suite(&IpcFixture).await;
}
