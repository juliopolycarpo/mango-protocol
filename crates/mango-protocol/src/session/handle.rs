//! [`Session`] — the cheap `Clone` handle a caller holds.

use std::sync::Arc;

use serde_json::{Map, Value};

use crate::error::{RemoteError, codes};
use crate::frame::{Limits, PeerInfo};
use crate::version::ProtocolVersion;

use super::command::Command;
use super::shared::{Shared, lock};
use super::teardown::SessionClosure;

/// Where a [`Session`] is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Waiting for the peer's `hello`.
    Handshaking,
    /// Both hellos have crossed; requests and events flow.
    Ready,
    /// The transport is gone; every handle method now fails or reports it.
    Closed,
}

/// What the far peer announced in its `hello`, plus the negotiated minor.
///
/// # Example
///
/// ```
/// use mango_protocol::frame::PeerInfo;
/// use mango_protocol::session::RemotePeer;
///
/// let remote = RemotePeer {
///     peer: PeerInfo { name: "hub".into(), version: "1.0.0".into(), role: "hub".into() },
///     protocol: mango_protocol::PROTOCOL_VERSION,
///     capabilities: Default::default(),
///     limits: None,
///     effective_minor: 0,
/// };
/// assert_eq!(remote.peer.role, "hub");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RemotePeer {
    /// Who the peer is.
    pub peer: PeerInfo,
    /// The wire version the peer announced.
    pub protocol: ProtocolVersion,
    /// The peer's capability object.
    pub capabilities: Map<String, Value>,
    /// The peer's own frame ceiling, if it announced one.
    pub limits: Option<Limits>,
    /// The lower of both sides' announced minors.
    pub effective_minor: u32,
}

/// A symmetric Mango Protocol session over any [`crate::port::Port`].
///
/// Cloning a `Session` is cheap: every clone shares the same driver through an
/// `Arc`. The driver itself only progresses while its `SessionDriver` future
/// is polled — build one with [`Session::open`] or, for the common case,
/// spawn it directly with [`Session::spawn`].
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
/// let peer = |role: &str| PeerInfo { name: "example".into(), version: "0.1.0".into(), role: role.into() };
/// let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
/// let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));
/// let remote = session_a.ready().await.expect("handshake succeeds");
/// assert_eq!(remote.peer.role, "b");
/// # }
/// ```
#[derive(Clone)]
pub struct Session {
    pub(super) shared: Arc<Shared>,
}

impl Session {
    /// Settles once both hellos have crossed; fails once the handshake cannot
    /// complete (a timeout, a duplicate hello, a version mismatch, or the
    /// port going away first).
    pub async fn ready(&self) -> Result<RemotePeer, RemoteError> {
        let mut receiver = self.shared.ready.subscribe();
        loop {
            let current = receiver.borrow().clone();
            if let Some(outcome) = current {
                return outcome;
            }
            if receiver.changed().await.is_err() {
                // As with `closed()`: the sender lives in `Shared`, reachable
                // through this very `self`, so it cannot have dropped already.
                std::future::pending::<()>().await;
            }
        }
    }

    /// Where the session is in its lifecycle.
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
    #[must_use]
    pub fn state(&self) -> SessionState {
        lock(&self.shared.inner).state
    }

    /// The peer's announcement. Fails with `UNAVAILABLE` before the handshake
    /// completes.
    pub fn remote(&self) -> Result<RemotePeer, RemoteError> {
        lock(&self.shared.inner).remote.clone().ok_or_else(|| {
            RemoteError::new(
                codes::UNAVAILABLE,
                "The session handshake has not completed; expected a ready session, received \
                 one still handshaking.",
            )
        })
    }

    /// Why the session closed, once it has. `None` until teardown finishes.
    #[must_use]
    pub fn closure(&self) -> Option<SessionClosure> {
        self.shared.closure.borrow().clone()
    }

    /// Resolves once the session ends; resolves immediately if it already has.
    pub async fn closed(&self) -> SessionClosure {
        let mut receiver = self.shared.closure.subscribe();
        loop {
            let current = receiver.borrow().clone();
            if let Some(closure) = current {
                return closure;
            }
            if receiver.changed().await.is_err() {
                // Only reachable if every Session clone (and so every Arc that
                // could still hold the sender) were already gone, which can't
                // happen while this very call is running through one. Park
                // rather than fabricate a closure that never occurred.
                std::future::pending::<()>().await;
            }
        }
    }

    /// The frame ceiling this side may send: the lower of both announced
    /// limits.
    #[must_use]
    pub fn send_limit_bytes(&self) -> usize {
        self.shared.send_limit_bytes()
    }

    /// Closes the transport with a reason code and settles everything in
    /// flight, resolving once every handler has settled (bounded by
    /// `handler_grace`) and the port is shut.
    ///
    /// Called from inside a handler, this waits out the grace rather than the
    /// handler's own return — use [`Session::close_now`] there instead.
    pub async fn close(&self, code: u16, reason: Option<&str>) -> SessionClosure {
        if let Some(closure) = self.closure() {
            return closure;
        }
        let _ = self.shared.commands.send(Command::Close {
            code,
            reason: reason.map(str::to_string),
        });
        self.closed().await
    }

    /// Closes the transport without waiting for the teardown to finish.
    pub fn close_now(&self, code: u16, reason: Option<&str>) {
        let _ = self.shared.commands.send(Command::Close {
            code,
            reason: reason.map(str::to_string),
        });
    }
}
