//! Mirrors the handshake, frame-ceiling and teardown cases of
//! `packages/protocol/src/session.test.ts`. Request/response, events and
//! liveness are covered once the driver grows those match arms.
#![cfg(feature = "tokio")]

mod support;

use std::time::Duration;

use mango_protocol::close::close_codes;
use mango_protocol::error::CodecErrorKind;
use mango_protocol::frame::{Close, Hello, Limits, PeerInfo};
use mango_protocol::port::{Inbound, PortClosure, port_pair};
use mango_protocol::session::{Session, SessionOptions, SessionState};
use mango_protocol::{CodecError, Frame};

use support::{RawPeer, ScriptedPort, within};

fn peer(role: &str) -> PeerInfo {
    PeerInfo {
        name: "example".into(),
        version: "0.1.0".into(),
        role: role.into(),
    }
}

fn hello_frame(role: &str) -> Frame {
    Frame::Hello(Hello {
        protocol: mango_protocol::PROTOCOL_VERSION,
        peer: peer(role),
        capabilities: Default::default(),
        limits: None,
    })
}

#[tokio::test]
async fn resolves_ready_on_both_sides_with_the_peer_announcement() {
    let (a, b) = port_pair();
    let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
    let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));

    let remote_a = within("a's ready()", session_a.ready())
        .await
        .expect("a's handshake succeeds");
    let remote_b = within("b's ready()", session_b.ready())
        .await
        .expect("b's handshake succeeds");

    assert_eq!(remote_a.peer.role, "b");
    assert_eq!(remote_b.peer.role, "a");
    assert_eq!(session_a.state(), SessionState::Ready);
    assert_eq!(session_b.state(), SessionState::Ready);
}

#[tokio::test]
async fn throws_from_remote_before_the_handshake_completes() {
    let (a, _b) = port_pair();
    let (session, _driver) = Session::open(a, SessionOptions::new(peer("a")));

    let error = session.remote().expect_err("not ready yet");
    assert_eq!(error.code, "UNAVAILABLE");
}

#[tokio::test(start_paused = true)]
async fn times_out_when_the_peer_never_says_hello() {
    let (a, _b) = port_pair();
    // Shorter than within()'s 5s guard: under a paused clock, every timer
    // fires by auto-advancing to its virtual deadline, so the guard and the
    // handshake timeout would otherwise race — and whichever names the
    // earlier deadline wins, guard included.
    let options = SessionOptions::new(peer("a")).with_handshake_timeout(Duration::from_millis(50));
    let (session, driver) = Session::open(a, options);
    tokio::spawn(driver.run());

    let error = within("ready()", session.ready())
        .await
        .expect_err("no hello ever arrives");
    assert_eq!(error.code, "UNAVAILABLE");

    let closure = within("closed()", session.closed()).await;
    assert_eq!(closure.code, close_codes::PROTOCOL_ERROR);
    assert_eq!(closure.reason.as_deref(), Some("handshake timeout"));
}

#[tokio::test]
async fn closes_with_4400_on_a_duplicate_hello() {
    let (a, b) = port_pair();
    let (session, _driver) = Session::spawn(a, SessionOptions::new(peer("a")));
    let mut raw = RawPeer::new(b);

    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    raw.send(hello_frame("b")).await;

    let closure = within("closed()", session.closed()).await;
    assert_eq!(closure.code, close_codes::PROTOCOL_ERROR);
    assert_eq!(closure.reason.as_deref(), Some("duplicate hello"));
}

#[tokio::test]
async fn announces_its_frame_ceiling_and_honours_the_lower_one() {
    let (a, b) = port_pair();
    let options = SessionOptions::new(peer("a")).with_max_frame_bytes(8192);
    let (session, _driver) = Session::spawn(a, options);
    let mut raw = RawPeer::new(b);

    let sent_hello = within(
        "the session's own hello",
        raw.until(|frame| matches!(frame, Frame::Hello(_))),
    )
    .await;
    let Frame::Hello(sent_hello) = sent_hello else {
        unreachable!("until() only returns a frame matching the predicate")
    };
    assert_eq!(
        sent_hello.limits,
        Some(Limits {
            max_frame_bytes: Some(8192)
        })
    );

    raw.send(Frame::Hello(Hello {
        protocol: mango_protocol::PROTOCOL_VERSION,
        peer: peer("b"),
        capabilities: Default::default(),
        limits: Some(Limits {
            max_frame_bytes: Some(4096),
        }),
    }))
    .await;

    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");
    assert_eq!(session.send_limit_bytes(), 4096);
}

#[tokio::test]
async fn tears_down_on_a_received_close_frame_with_its_code() {
    let (a, b) = port_pair();
    let (session, _driver) = Session::spawn(a, SessionOptions::new(peer("a")));
    let mut raw = RawPeer::new(b);

    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    raw.send(Frame::Close(Close {
        code: 4409,
        reason: Some("superseded by a newer connection".into()),
    }))
    .await;

    let closure = within("closed()", session.closed()).await;
    assert_eq!(closure.code, 4409);
    assert_eq!(
        closure.reason.as_deref(),
        Some("superseded by a newer connection")
    );
    assert!(closure.fatal);
}

#[tokio::test]
async fn fires_on_close_once_and_immediately_for_a_late_subscriber() {
    let (a, _b) = port_pair();
    let (session, _driver) = Session::spawn(a, SessionOptions::new(peer("a")));

    session.close_now(close_codes::RELEASED, Some("bye"));
    let first = within("the first closed()", session.closed()).await;
    let second = within("the late closed()", session.closed()).await;

    assert_eq!(first, second);
    assert_eq!(first.code, close_codes::RELEASED);
    assert_eq!(first.reason.as_deref(), Some("bye"));
}

#[tokio::test]
async fn rejects_ready_with_protocol_mismatch_when_the_port_refuses_the_peer_hello_with_4426() {
    let error = CodecError::new(CodecErrorKind::Schema, "peer speaks an unreadable hello")
        .with_frame_type("hello");
    let port = ScriptedPort::new([Inbound::Closed(PortClosure::ProtocolError {
        error,
        code: 4426,
    })]);
    let (session, _driver) = Session::spawn(port, SessionOptions::new(peer("a")));

    let error = within("ready()", session.ready())
        .await
        .expect_err("the hello could not be read");
    assert_eq!(error.code, "PROTOCOL_MISMATCH");

    let closure = within("closed()", session.closed()).await;
    assert_eq!(closure.code, 4426);
}

#[tokio::test]
async fn carries_the_codec_error_and_rejects_ready_with_unavailable_on_a_4400_refusal() {
    let codec_error = CodecError::new(CodecErrorKind::Schema, "bad method name");
    let port = ScriptedPort::new([Inbound::Closed(PortClosure::ProtocolError {
        error: codec_error.clone(),
        code: 4400,
    })]);
    let (session, _driver) = Session::spawn(port, SessionOptions::new(peer("a")));

    let error = within("ready()", session.ready())
        .await
        .expect_err("the record was refused");
    assert_eq!(error.code, "UNAVAILABLE");

    let closure = within("closed()", session.closed()).await;
    assert_eq!(closure.code, 4400);
    assert_eq!(closure.error, Some(codec_error));
}

#[tokio::test]
async fn treats_a_link_that_vanished_as_a_4000_release() {
    let port = ScriptedPort::new([]);
    let (session, _driver) = Session::spawn(port, SessionOptions::new(peer("a")));

    let closure = within("closed()", session.closed()).await;
    assert_eq!(closure.code, close_codes::RELEASED);
    assert_eq!(closure.reason, None);
}
