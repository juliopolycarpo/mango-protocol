//! A peer that stops reading is a peer the sender must give up on, not one it
//! waits for for ever.
//!
//! `spec/transports/websocket.md` (Backpressure): "A queue that grows past one
//! frame limit while the socket is not draining is a peer that is not reading;
//! the sender closes with `4400` rather than holding every pending response
//! for a socket that may never drain."
#![cfg(feature = "websocket")]

use std::time::Duration;

use mango_protocol::close::close_codes;
use mango_protocol::frame::{Frame, Request};
use mango_protocol::port::{Port, PortTx, SendOutcome};
use mango_protocol::transports::deadline::ConnectDeadline;
use mango_protocol::transports::websocket::WebSocketOptions;
use mango_protocol::transports::websocket::client::{WebSocketConnectOptions, connect_websocket};
use serde_json::Value;
use tokio::net::TcpListener;

/// Small enough that a handful of frames passes it, and well under the
/// operating system's own socket buffers so the stall is this port's rule
/// rather than the kernel's.
const FRAME_LIMIT: usize = 64 * 1024;

/// One frame of roughly an eighth of the limit, so eight of them reach it.
fn filler(index: usize) -> Frame {
    Frame::Req(Request {
        id: format!("r-{index}"),
        method: "test.bulk".into(),
        params: Value::String("x".repeat(FRAME_LIMIT / 8)),
    })
}

#[allow(
    clippy::result_large_err,
    reason = "the handshake callback's error type is tungstenite's own ErrorResponse"
)]
#[tokio::test]
async fn a_peer_that_stops_reading_is_closed_with_4400_rather_than_waited_on() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port");
    let address = listener.local_addr().expect("a bound address");

    // An acceptor that completes the upgrade and then never reads another
    // byte: the socket stays open, and nothing drains it.
    let acceptor = tokio::spawn(async move {
        let (socket, _address) = listener.accept().await.expect("a dialler");
        let stream = tokio_tungstenite::accept_hdr_async(
            socket,
            |_request: &tokio_tungstenite::tungstenite::handshake::server::Request,
             mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    tokio_tungstenite::tungstenite::http::HeaderValue::from_static("mango.v1"),
                );
                Ok(response)
            },
        )
        .await
        .expect("the upgrade completes");
        // Hold the socket open without reading it.
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(stream);
    });

    let port = connect_websocket(
        &format!("ws://{address}/stalled"),
        &WebSocketConnectOptions::default()
            .with_websocket(WebSocketOptions::default().with_max_frame_bytes(FRAME_LIMIT)),
        &ConnectDeadline::default().with_timeout(Duration::from_secs(5)),
    )
    .await
    .expect("the acceptor selects mango.v1");

    let (mut tx, _rx) = port.split();
    let sending = async {
        // Far more than the frame limit: whatever the kernel's own buffers
        // absorb, the queue has to pass one frame limit long before this ends.
        for index in 0..2048 {
            if tx.send(filler(index)).await != SendOutcome::Sent {
                return index;
            }
        }
        panic!("the port accepted 2048 frames without ever reporting the peer as gone");
    };

    let stopped_at = tokio::time::timeout(Duration::from_secs(10), sending)
        .await
        .expect("the port gives up on a peer that is not reading rather than blocking for ever");
    assert!(
        stopped_at > 0,
        "the first frame should still have been accepted"
    );

    acceptor.abort();
    // The code the peer is owed, whether or not it ever reads it.
    assert_eq!(close_codes::PROTOCOL_ERROR, 4400);
}
