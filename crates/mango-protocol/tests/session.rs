//! Mirrors the handshake, frame-ceiling, dispatch, events/liveness and
//! teardown cases of `packages/protocol/src/session.test.ts`.
#![cfg(feature = "tokio")]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use mango_protocol::close::close_codes;
use mango_protocol::error::{CodecErrorKind, RemoteError, codes};
use mango_protocol::frame::{
    Cancel, Close, ErrorPayload, ErrorResponse, Hello, Limits, PeerInfo, Request, Response,
};
use mango_protocol::port::{Inbound, PortClosure, port_pair};
use mango_protocol::session::{
    CallContext, EventInput, RequestOptions, Session, SessionOptions, SessionState,
};
use mango_protocol::{CodecError, Frame};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

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

/// The driver's `select!` is `biased`, so whichever branch comes first wins
/// every poll it is ready for. A peer that floods frames faster than the
/// driver drains them must not be able to hold the control plane off — here,
/// the handshake deadline it never intends to satisfy.
///
/// Real time, not `start_paused`: a paused clock only auto-advances while the
/// runtime is idle, and a flood never lets it be.
#[tokio::test]
async fn fires_the_handshake_timeout_under_a_flood_of_inbound_frames() {
    let (a, b) = port_pair();
    let options = SessionOptions::new(peer("a"))
        .with_handshake_timeout(Duration::from_millis(50))
        .with_liveness_interval(None);
    let (session, driver) = Session::open(a, options);
    tokio::spawn(driver.run());

    // `pong` is the one frame the driver answers with nothing at all, so the
    // flood stays one-way and this test measures the driver's own fairness
    // rather than how fast it can fill the peer's queue.
    let mut raw = RawPeer::new(b);
    let flood = tokio::spawn(async move {
        loop {
            for _ in 0..512 {
                raw.send(Frame::Pong).await;
            }
            tokio::task::yield_now().await;
        }
    });

    let started = tokio::time::Instant::now();
    let closure = within("closed() under an inbound flood", session.closed()).await;
    let elapsed = started.elapsed();
    flood.abort();

    assert_eq!(closure.code, close_codes::PROTOCOL_ERROR);
    assert_eq!(closure.reason.as_deref(), Some("handshake timeout"));
    assert!(
        elapsed < Duration::from_secs(1),
        "expected the 50ms handshake timeout to fire near its deadline while the flood ran | \
         received: {elapsed:?}"
    );
}

/// The same fairness question for the command branch: a flooding peer must not
/// be able to hold off this side's own `close()`.
#[tokio::test]
async fn closes_under_a_flood_of_inbound_frames() {
    let (a, b) = port_pair();
    let options = SessionOptions::new(peer("a")).with_liveness_interval(None);
    let (session, _driver) = Session::spawn(a, options);
    let mut raw = RawPeer::new(b);
    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    let flood = tokio::spawn(async move {
        loop {
            for _ in 0..512 {
                raw.send(Frame::Pong).await;
            }
            tokio::task::yield_now().await;
        }
    });

    let started = tokio::time::Instant::now();
    session.close_now(close_codes::RELEASED, Some("bye"));
    let closure = within("closed() under an inbound flood", session.closed()).await;
    let elapsed = started.elapsed();
    flood.abort();

    assert_eq!(closure.code, close_codes::RELEASED);
    assert_eq!(closure.reason.as_deref(), Some("bye"));
    assert!(
        elapsed < Duration::from_secs(1),
        "expected close() to land while the flood ran | received: {elapsed:?}"
    );
}

#[tokio::test]
async fn answers_a_request_that_arrives_before_the_handshake_with_unavailable() {
    let (a, b) = port_pair();
    let (_session, driver) = Session::open(a, SessionOptions::new(peer("a")));
    tokio::spawn(driver.run());
    let mut raw = RawPeer::new(b);

    // The peer's own hello is still in flight; send a request ahead of ours
    // so the driver answers it while still Handshaking.
    raw.send(Frame::Req(Request {
        id: "req-1".into(),
        method: "test.echo".into(),
        params: Value::Null,
    }))
    .await;

    let response = within(
        "the UNAVAILABLE error response",
        raw.until(|frame| matches!(frame, Frame::Err(_))),
    )
    .await;
    let Frame::Err(error_response) = response else {
        unreachable!("until() only returns a frame matching the predicate")
    };
    assert_eq!(error_response.id, "req-1");
    assert_eq!(error_response.error.code, codes::UNAVAILABLE);
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

#[tokio::test]
async fn rejects_an_invalid_or_reserved_method_name_locally() {
    let (a, _b) = port_pair();
    // The driver is never spawned: this check must be synchronous, resolved
    // before ever awaiting readiness — if it regressed to await first, this
    // request would hang forever, and within() turns that into a clear panic
    // instead of a stuck test run.
    let (session, _driver) = Session::open(a, SessionOptions::new(peer("a")));

    let invalid = within(
        "an invalid method name",
        session.request("not-a-method", Value::Null),
    )
    .await
    .expect_err("refused locally");
    assert_eq!(invalid.code, "INVALID_REQUEST");

    let reserved = within(
        "a reserved method name",
        session.request("rpc.discover", Value::Null),
    )
    .await
    .expect_err("refused locally");
    assert_eq!(reserved.code, "INVALID_REQUEST");
}

#[tokio::test]
async fn maps_a_handler_that_throws_remote_error_error_and_abort_error_onto_the_wire() {
    let (a, b) = port_pair();
    let options_b = SessionOptions::new(peer("b"))
        .handle("test.denied", |_params, _context| async move {
            Err(RemoteError::new("DENIED", "fsRead was not granted"))
        })
        .handle("test.panics", |_params, _context| async move {
            panic!("the handler misbehaved")
        })
        .handle(
            "test.cancelled",
            |_params, context: CallContext| async move {
                context.cancel().cancelled().await;
                Err(RemoteError::new(
                    codes::CANCELLED,
                    "the request was cancelled",
                ))
            },
        );
    let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
    let (session_b, _driver_b) = Session::spawn(b, options_b);
    within("a's ready()", session_a.ready())
        .await
        .expect("handshake succeeds");
    within("b's ready()", session_b.ready())
        .await
        .expect("handshake succeeds");

    let denied = within(
        "the RemoteError case",
        session_a.request("test.denied", Value::Null),
    )
    .await
    .expect_err("denied");
    assert_eq!(denied.code, "DENIED");

    let panicked = within(
        "the Error case",
        session_a.request("test.panics", Value::Null),
    )
    .await
    .expect_err("a panic becomes INTERNAL");
    assert_eq!(panicked.code, "INTERNAL");

    let cancel = CancellationToken::new();
    cancel.cancel();
    let cancelled = within(
        "the AbortError case",
        session_a.request_with(
            "test.cancelled",
            Value::Null,
            RequestOptions {
                cancel: Some(cancel),
                ..Default::default()
            },
        ),
    )
    .await
    .expect_err("cancelled");
    assert_eq!(cancelled.code, "CANCELLED");
}

#[tokio::test]
async fn answers_a_duplicate_in_flight_id_with_invalid_request_and_keeps_the_first_running() {
    let (a, b) = port_pair();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let options_a =
        SessionOptions::new(peer("a")).handle("test.parked", move |_params, _context| {
            let release_rx = Arc::clone(&release_rx);
            async move {
                let released = release_rx.lock().unwrap().take();
                if let Some(rx) = released {
                    let _ = rx.await;
                }
                Ok(Value::Null)
            }
        });
    let (session, _driver) = Session::spawn(a, options_a);
    let mut raw = RawPeer::new(b);
    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    raw.send(Frame::Req(Request {
        id: "dup-1".into(),
        method: "test.parked".into(),
        params: Value::Null,
    }))
    .await;
    raw.send(Frame::Req(Request {
        id: "dup-1".into(),
        method: "test.parked".into(),
        params: Value::Null,
    }))
    .await;

    let duplicate = within(
        "the duplicate's immediate refusal",
        raw.until(|frame| matches!(frame, Frame::Err(response) if response.id == "dup-1")),
    )
    .await;
    let Frame::Err(error_response) = duplicate else {
        unreachable!("until() only returns a frame matching the predicate")
    };
    assert_eq!(error_response.error.code, "INVALID_REQUEST");

    let _ = release_tx.send(());
    let settled = within(
        "the first request's real answer",
        raw.until(|frame| matches!(frame, Frame::Res(response) if response.id == "dup-1")),
    )
    .await;
    assert!(matches!(settled, Frame::Res(_)));
}

#[tokio::test]
async fn ignores_a_response_for_an_unknown_id() {
    let (a, b) = port_pair();
    let (session, _driver) = Session::spawn(a, SessionOptions::new(peer("a")));
    let mut raw = RawPeer::new(b);
    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    raw.send(Frame::Res(Response {
        id: "no-such-id".into(),
        result: Value::Null,
    }))
    .await;
    raw.send(Frame::Err(ErrorResponse {
        id: "no-such-id".into(),
        error: ErrorPayload {
            code: "DENIED".into(),
            message: "irrelevant".into(),
            details: None,
        },
    }))
    .await;

    // Neither frame panicked or tore the session down.
    assert_eq!(session.state(), SessionState::Ready);
}

/// A caller that walks away from a request future — a losing `tokio::select!`
/// branch, an aborted task — still owes the peer a `cancel`, so the handler it
/// started does not run on unwatched.
#[tokio::test]
async fn sends_cancel_when_a_request_future_is_dropped_before_it_settles() {
    let (a, b) = port_pair();
    let (session, _driver) = Session::spawn(a, SessionOptions::new(peer("a")));
    let mut raw = RawPeer::new(b);
    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    let requester = tokio::spawn(async move {
        let _ = session.request("test.slow", Value::Null).await;
    });
    let sent = within(
        "the req frame",
        raw.until(|frame| matches!(frame, Frame::Req(_))),
    )
    .await;
    let Frame::Req(sent) = sent else {
        unreachable!("until() matched a req frame")
    };

    requester.abort();

    let cancel = within(
        "the cancel frame",
        raw.until(|frame| matches!(frame, Frame::Cancel(_))),
    )
    .await;
    assert_eq!(cancel, Frame::Cancel(Cancel { id: sent.id }));
}

#[tokio::test]
async fn fails_a_request_sent_after_close_with_unavailable() {
    let (a, b) = port_pair();
    let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
    let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));
    within("a's ready()", session_a.ready())
        .await
        .expect("handshake succeeds");
    within("b's ready()", session_b.ready())
        .await
        .expect("handshake succeeds");

    session_a.close_now(close_codes::RELEASED, Some("bye"));
    within("closed()", session_a.closed()).await;

    let error = within(
        "request() after close",
        session_a.request("test.echo", Value::Null),
    )
    .await
    .expect_err("the session is closed");
    assert_eq!(error.code, "UNAVAILABLE");
}

#[tokio::test]
async fn aborts_the_handler_signal_when_the_session_closes() {
    let (a, b) = port_pair();
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (cancelled_tx, cancelled_rx) = oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let cancelled_tx = Arc::new(Mutex::new(Some(cancelled_tx)));
    let options_a = SessionOptions::new(peer("a"))
        .with_handler_grace(Duration::from_millis(500))
        .handle(
            "test.wait-for-cancel",
            move |_params, context: CallContext| {
                let started_tx = Arc::clone(&started_tx);
                let cancelled_tx = Arc::clone(&cancelled_tx);
                async move {
                    if let Some(tx) = started_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    context.cancel().cancelled().await;
                    if let Some(tx) = cancelled_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    Err(RemoteError::new(
                        codes::CANCELLED,
                        "cancelled by session close",
                    ))
                }
            },
        );
    let (session_a, _driver_a) = Session::spawn(a, options_a);
    let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));
    within("a's ready()", session_a.ready())
        .await
        .expect("handshake succeeds");
    within("b's ready()", session_b.ready())
        .await
        .expect("handshake succeeds");

    let requester = tokio::spawn(async move {
        let _ = session_b.request("test.wait-for-cancel", Value::Null).await;
    });
    within("the handler starting", started_rx)
        .await
        .expect("the handler ran");

    session_a.close_now(close_codes::RELEASED, Some("bye"));
    let closure = within("closed()", session_a.closed()).await;

    within("the handler observing cancellation", cancelled_rx)
        .await
        .expect("the token fired");
    assert_eq!(closure.unfinished_handlers, 0);

    let _ = requester.await;
}

fn tick_event(topic: &str) -> EventInput {
    EventInput {
        topic: topic.into(),
        payload: Value::from(1),
        stream_id: None,
        end: false,
    }
}

#[tokio::test]
async fn drops_events_emitted_before_the_handshake_and_after_close() {
    let (a, b) = port_pair();
    let (session, driver) = Session::open(a, SessionOptions::new(peer("a")));
    tokio::spawn(driver.run());
    let mut raw = RawPeer::new(b);

    let early = session
        .emit(tick_event("test.early"))
        .expect("emit does not fail before the handshake");
    assert!(
        !early,
        "expected the event before the handshake to be dropped"
    );

    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    let ok = session
        .emit(tick_event("test.ok"))
        .expect("emit does not fail once ready");
    assert!(ok, "expected the event to be sent once ready");

    session.close(close_codes::RELEASED, None).await;

    let late = session
        .emit(tick_event("test.late"))
        .expect("emit does not fail after close");
    assert!(!late, "expected the event after close to be dropped");
}

/// A zero period is no cadence at all, and `tokio::time::interval_at` panics
/// on one. That panic used to unwind the driver the moment the handshake
/// completed, skipping teardown entirely: the handle stayed visibly `Ready`
/// and `closed()` waited for ever.
#[tokio::test]
async fn treats_a_zero_liveness_interval_as_disabled() {
    let (a, b) = port_pair();
    let options = SessionOptions::new(peer("a")).with_liveness_interval(Some(Duration::ZERO));
    let (session, driver) = Session::spawn(a, options);
    let mut raw = RawPeer::new(b);
    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    session.close_now(close_codes::RELEASED, Some("bye"));
    let closure = within("the driver's own closure", driver)
        .await
        .expect("the driver ran its teardown instead of panicking on a zero interval");

    assert_eq!(closure.code, close_codes::RELEASED);
    assert_eq!(closure.reason.as_deref(), Some("bye"));
}

#[tokio::test(start_paused = true)]
async fn closes_with_a_liveness_timeout_when_pongs_stop() {
    let (a, b) = port_pair();
    let options =
        SessionOptions::new(peer("a")).with_liveness_interval(Some(Duration::from_millis(15)));
    let (session, driver) = Session::open(a, options);
    tokio::spawn(driver.run());
    let mut raw = RawPeer::new(b);

    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    // The peer never answers a ping, so the second tick (one missed round
    // trip) closes the session.
    let closure = within("closed()", session.closed()).await;
    assert_eq!(closure.code, close_codes::RELEASED);
    assert_eq!(closure.reason.as_deref(), Some("liveness timeout"));
}

#[tokio::test(start_paused = true)]
async fn stays_open_while_the_peer_answers_pings() {
    let (a, b) = port_pair();
    let options =
        SessionOptions::new(peer("a")).with_liveness_interval(Some(Duration::from_millis(10)));
    let (session, driver) = Session::open(a, options);
    tokio::spawn(driver.run());
    let mut raw = RawPeer::new(b);

    raw.send(hello_frame("b")).await;
    within("ready()", session.ready())
        .await
        .expect("handshake succeeds");

    tokio::spawn(async move {
        loop {
            match raw.next().await {
                Inbound::Frame(Frame::Ping) => raw.send(Frame::Pong).await,
                Inbound::Closed(_) => break,
                _ => {}
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(session.state(), SessionState::Ready);
    session.close_now(close_codes::RELEASED, None);
}

// The remaining two cases are Rust-only: nothing in the TS table names them,
// but "every new function gets a test" covers `emit`'s per-stream sequencing
// and `ping`/`pongs` regardless.

#[tokio::test]
async fn sequences_events_per_stream_key_and_releases_the_counter_on_end() {
    let (a, b) = port_pair();
    let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
    let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));
    within("a's ready()", session_a.ready())
        .await
        .expect("handshake succeeds");
    within("b's ready()", session_b.ready())
        .await
        .expect("handshake succeeds");

    let mut events = session_b.events();

    for (payload, end) in [(1, false), (2, false), (3, true)] {
        session_a
            .emit(EventInput {
                topic: "fs.changed".into(),
                payload: Value::from(payload),
                stream_id: Some("s1".into()),
                end,
            })
            .expect("emit does not fail once ready");
    }
    // `end` released the "s1" counter; a new stream on the same key restarts
    // at 0. A different key (no stream_id) is independent, starting at its
    // own 0 regardless of how many events "s1" has already seen.
    session_a
        .emit(EventInput {
            topic: "fs.changed".into(),
            payload: Value::from(4),
            stream_id: Some("s1".into()),
            end: false,
        })
        .expect("emit does not fail once ready");
    session_a
        .emit(EventInput {
            topic: "fs.tick".into(),
            payload: Value::from(5),
            stream_id: None,
            end: false,
        })
        .expect("emit does not fail once ready");

    let mut seen = Vec::new();
    for _ in 0..5 {
        let event = within("the next event", events.recv())
            .await
            .expect("the stream stays open for every emitted event");
        seen.push((event.topic, event.stream_id, event.seq, event.end.is_some()));
    }
    assert_eq!(
        seen,
        vec![
            ("fs.changed".to_string(), Some("s1".to_string()), 0, false),
            ("fs.changed".to_string(), Some("s1".to_string()), 1, false),
            ("fs.changed".to_string(), Some("s1".to_string()), 2, true),
            ("fs.changed".to_string(), Some("s1".to_string()), 0, false),
            ("fs.tick".to_string(), None, 0, false),
        ]
    );
}

#[tokio::test]
async fn answers_a_ping_with_a_pong_the_pinger_can_observe() {
    let (a, b) = port_pair();
    let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
    let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));
    within("a's ready()", session_a.ready())
        .await
        .expect("handshake succeeds");
    within("b's ready()", session_b.ready())
        .await
        .expect("handshake succeeds");

    let mut pongs = session_a.pongs();
    session_a.ping();

    within("the pong", pongs.recv())
        .await
        .expect("the peer answers the ping");
}
