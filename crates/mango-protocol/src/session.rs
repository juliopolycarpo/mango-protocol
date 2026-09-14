//! A tokio session over any [`crate::port::Port`]: the request/response
//! multiplexing, cancel, event streams, liveness and close semantics of
//! `packages/protocol/src/session.ts`, without a transport of its own.
//!
//! Build one with [`Session::open`] (you drive the returned [`SessionDriver`])
//! or [`Session::spawn`] (the driver is spawned for you). Either way, the
//! handshake and everything after it only happens while that driver future is
//! being polled — a [`Session`] handle alone is inert.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::codec::limits::check_at_least;
use crate::codec::ndjson::{DEFAULT_MAX_FRAME_BYTES, MIN_MAX_FRAME_BYTES};
use crate::port::Port;

mod command;
mod dispatch;
mod driver;
mod handle;
mod handler;
mod options;
mod shared;
mod teardown;

pub use driver::SessionDriver;
pub use handle::{
    EventInput, EventStream, PongStream, RemotePeer, RequestOptions, Session, SessionState,
};
pub use handler::{CallContext, Handler, HandlerFuture, HandlerGuard};
pub use options::{
    DEFAULT_HANDLER_GRACE, DEFAULT_HANDSHAKE_TIMEOUT, DEFAULT_LIVENESS_INTERVAL,
    DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_STREAM_KEYS, DEFAULT_REQUEST_ID_PREFIX,
    HANDSHAKE_TIMEOUT_REASON, IN_FLIGHT_LIMIT_KIND, STREAM_KEY_LIMIT_KIND, SessionOptions,
};
pub use teardown::SessionClosure;

use shared::{Inner, Shared};

impl Session {
    /// Opens a session over `port`, returning the handle and its not-yet-run
    /// driver. Nothing progresses — not even the handshake — until the driver
    /// is polled; [`Session::spawn`] is the default that avoids that footgun.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::frame::PeerInfo;
    /// use mango_protocol::port::port_pair;
    /// use mango_protocol::session::{Session, SessionOptions, SessionState};
    ///
    /// let (a, _b) = port_pair();
    /// let peer = PeerInfo { name: "example".into(), version: "0.1.0".into(), role: "runtime".into() };
    /// let (session, _driver) = Session::open(a, SessionOptions::new(peer));
    /// assert_eq!(session.state(), SessionState::Handshaking);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics when `options.max_frame_bytes` is `Some` value below
    /// [`crate::codec::ndjson::MIN_MAX_FRAME_BYTES`], naming both. Likewise
    /// for `options.max_in_flight` or `options.max_stream_keys` at `0`.
    /// `SessionOptions`'s builders already refuse these on the builder path,
    /// but every field involved is `pub`, so this is the check for a caller
    /// that assigned one directly.
    #[must_use]
    pub fn open<P: Port>(
        port: P,
        options: SessionOptions,
    ) -> (Session, SessionDriver<P::Tx, P::Rx>) {
        let port_max_frame_bytes = port.max_frame_bytes();
        let (tx, rx) = port.split();
        // A session option *narrows* the port's ceiling, it never replaces
        // it: the port is what actually decodes and encodes, so announcing
        // more than it accepts would make the peer send frames this side
        // then refuses. Unset defers to the port, then to the default —
        // unchanged, and never clamped down to the default on its own.
        let local_max_frame_bytes = match (options.max_frame_bytes, port_max_frame_bytes) {
            (Some(session_ceiling), Some(port_ceiling)) => {
                check_at_least("max_frame_bytes", session_ceiling, MIN_MAX_FRAME_BYTES)
                    .min(port_ceiling)
            }
            (Some(session_ceiling), None) => {
                check_at_least("max_frame_bytes", session_ceiling, MIN_MAX_FRAME_BYTES)
            }
            (None, Some(port_ceiling)) => port_ceiling,
            (None, None) => DEFAULT_MAX_FRAME_BYTES,
        };
        let max_in_flight = check_at_least("max_in_flight", options.max_in_flight, 1);
        let max_stream_keys = check_at_least("max_stream_keys", options.max_stream_keys, 1);
        let (ready, _) = watch::channel(None);
        let (closure, _) = watch::channel(None);
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            local_peer: options.peer,
            local_protocol: options.protocol,
            local_capabilities: options.capabilities,
            local_max_frame_bytes,
            max_in_flight,
            max_stream_keys,
            inner: Mutex::new(Inner {
                state: SessionState::Handshaking,
                remote: None,
            }),
            ready,
            closure,
            commands: commands_tx,
            request_id_prefix: options.request_id_prefix,
            request_sequence: std::sync::atomic::AtomicU64::new(0),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            handlers: Mutex::new(HashMap::new()),
            next_generation: std::sync::atomic::AtomicU64::new(0),
            handler_grace: options.handler_grace,
            event_sequences: Mutex::new(HashMap::new()),
            event_subscribers: Mutex::new(Vec::new()),
            pong_subscribers: Mutex::new(Vec::new()),
        });
        for (method, handler) in options.handlers {
            shared.register_handler(method, handler);
        }
        let session = Session {
            shared: Arc::clone(&shared),
        };
        let driver = SessionDriver {
            shared,
            tx,
            rx,
            commands: commands_rx,
            handshake_timeout: options.handshake_timeout,
            pending: HashMap::new(),
            tracking: dispatch::RequestTracking::default(),
            // A zero period is no cadence at all, and `interval_at` panics on
            // one. Normalised here rather than in the setter because
            // `SessionOptions` exposes the field publicly, so a caller can
            // assign it without going through `with_liveness_interval`.
            liveness_interval: options.liveness_interval.filter(|period| !period.is_zero()),
            liveness: None,
            awaiting_pong: false,
        };
        (session, driver)
    }

    /// [`Session::open`] plus `tokio::spawn(driver.run())`.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// use mango_protocol::frame::PeerInfo;
    /// use mango_protocol::port::port_pair;
    /// use mango_protocol::session::{Session, SessionOptions};
    ///
    /// let (a, b) = port_pair();
    /// let peer = |role: &str| PeerInfo {
    ///     name: "example".into(),
    ///     version: "0.1.0".into(),
    ///     role: role.into(),
    /// };
    /// let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
    /// let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));
    /// let remote = session_a.ready().await.expect("handshake succeeds");
    /// assert_eq!(remote.peer.role, "b");
    /// # }
    /// ```
    #[must_use]
    pub fn spawn<P: Port>(
        port: P,
        options: SessionOptions,
    ) -> (Session, JoinHandle<SessionClosure>) {
        let (session, driver) = Self::open(port, options);
        let handle = tokio::spawn(driver.run());
        (session, handle)
    }
}
