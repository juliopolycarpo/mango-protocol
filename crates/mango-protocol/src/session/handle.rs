//! [`Session`] — the cheap `Clone` handle a caller holds.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::codec::ndjson::encode_frame_bytes;
use crate::error::{CodecErrorKind, RemoteError, codes};
use crate::frame::{Frame, Limits, PeerInfo, Request};
use crate::validate::{is_reserved_method_name, is_valid_method_name};
use crate::version::ProtocolVersion;

use super::command::Command;
use super::handler::{Handler, HandlerGuard};
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

/// Tunes one [`Session::request_with`] call.
///
/// # Example
///
/// ```
/// use mango_protocol::session::RequestOptions;
/// use std::time::Duration;
///
/// let options = RequestOptions { timeout: Some(Duration::from_secs(5)), ..Default::default() };
/// assert!(options.cancel.is_none());
/// ```
#[derive(Default)]
pub struct RequestOptions {
    /// Cancelling this sends `cancel` to the peer, but the call still waits
    /// for the peer's real answer: cancel is advisory, never a promise.
    pub cancel: Option<CancellationToken>,
    /// A local deadline: sends `cancel`, then rejects with `TIMEOUT` without
    /// waiting for the peer's answer.
    pub timeout: Option<Duration>,
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

    /// Registers a handler for `method`; dropping the returned guard
    /// unregisters it unless [`HandlerGuard::persist`] is called first.
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
    /// let (a, _b) = port_pair();
    /// let peer = PeerInfo { name: "e".into(), version: "0.1.0".into(), role: "runtime".into() };
    /// let (session, _driver) = Session::open(a, SessionOptions::new(peer));
    /// let guard = session.handle("text.echo", |params, _context| async move { Ok(params) });
    /// guard.persist();
    /// # }
    /// ```
    #[must_use = "dropping the guard unregisters the handler"]
    pub fn handle(&self, method: impl Into<String>, handler: impl Handler) -> HandlerGuard {
        let (method, generation) = self
            .shared
            .register_handler(method.into(), Arc::new(handler));
        HandlerGuard {
            method,
            generation,
            shared: Arc::clone(&self.shared),
        }
    }

    /// Sends a request and resolves with its `result`, or rejects with a
    /// [`RemoteError`]. Equivalent to `request_with` with the defaults.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, RemoteError> {
        self.request_with(method, params, RequestOptions::default())
            .await
    }

    /// Sends a request, tuned by `options`.
    pub async fn request_with(
        &self,
        method: &str,
        params: Value,
        options: RequestOptions,
    ) -> Result<Value, RemoteError> {
        if !is_valid_method_name(method) || is_reserved_method_name(method) {
            return Err(RemoteError::new(
                codes::INVALID_REQUEST,
                format!(
                    "Method \"{method}\" is not a valid, unreserved method name; expected two \
                     or more dot-separated lowercase segments outside rpc."
                ),
            ));
        }
        self.ready().await?;
        if self.state() == SessionState::Closed {
            return Err(self.unavailable(method));
        }

        let id = self.shared.next_request_id();
        let frame = Frame::Req(Request {
            id: id.clone(),
            method: method.to_string(),
            params,
        });
        assert_fits(&self.shared, &frame, &format!("Request \"{method}\""))?;

        let (reply_tx, mut reply_rx) = oneshot::channel();
        if self
            .shared
            .commands
            .send(Command::Request {
                frame,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(RemoteError::new(
                codes::UNAVAILABLE,
                format!("Request \"{method}\" could not be sent: the session driver is gone."),
            ));
        }

        let mut guard = CancelGuard {
            id: id.clone(),
            commands: self.shared.commands.clone(),
            armed: true,
        };
        let deadline = options
            .timeout
            .map(|timeout| tokio::time::Instant::now() + timeout);
        let mut cancel_sent = false;
        let result = loop {
            tokio::select! {
                biased;
                received = &mut reply_rx => {
                    break received.unwrap_or_else(|_| Err(self.unavailable(method)));
                }
                () = cancel_wait(options.cancel.as_ref()), if !cancel_sent => {
                    cancel_sent = true;
                    let _ = self.shared.commands.send(Command::Cancel { id: id.clone(), forget: true });
                }
                () = timeout_wait(deadline) => {
                    let _ = self.shared.commands.send(Command::Cancel { id: id.clone(), forget: false });
                    let timeout_ms = options.timeout.map(|value| value.as_millis()).unwrap_or_default();
                    break Err(RemoteError::new(
                        codes::TIMEOUT,
                        format!("Request \"{method}\" timed out after {timeout_ms}ms."),
                    )
                    .with_detail("method", method.to_string())
                    .with_detail("timeout_ms", u64::try_from(timeout_ms).unwrap_or(u64::MAX)));
                }
            }
        };
        guard.disarm();
        result
    }

    fn unavailable(&self, method: &str) -> RemoteError {
        let closure = self.closure();
        let why = closure
            .as_ref()
            .map(|closure| {
                let reason = closure
                    .reason
                    .as_deref()
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default();
                format!(" (closed with {}{reason})", closure.code)
            })
            .unwrap_or_default();
        let error = RemoteError::new(
            codes::UNAVAILABLE,
            format!("Request \"{method}\" cannot complete: the session is closed{why}."),
        )
        .with_detail("method", method.to_string());
        match closure {
            Some(closure) => error.with_detail("close_code", closure.code),
            None => error,
        }
    }
}

/// Waits for `cancel` to fire, or never resolves if there is none.
async fn cancel_wait(cancel: Option<&CancellationToken>) {
    match cancel {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}

/// Waits until `deadline`, or never resolves if there is none.
async fn timeout_wait(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(instant) => tokio::time::sleep_until(instant).await,
        None => std::future::pending().await,
    }
}

/// Validates `frame` and measures its encoded size against the session's
/// negotiated limit, mirroring what a port's own `send` would discover, but
/// synchronously and before the frame ever reaches the command channel.
fn assert_fits(shared: &Shared, frame: &Frame, what: &str) -> Result<(), RemoteError> {
    let limit = shared.send_limit_bytes();
    match encode_frame_bytes(frame, limit) {
        Ok(_) => Ok(()),
        Err(error) if error.kind == CodecErrorKind::TooLarge => {
            let bytes = serde_json::to_vec(frame).map_or(limit + 1, |encoded| encoded.len());
            Err(RemoteError::new(
                codes::FRAME_TOO_LARGE,
                format!("{what} encodes to {bytes} bytes; the session limit is {limit} bytes."),
            )
            .with_detail("bytes", u64::try_from(bytes).unwrap_or(u64::MAX))
            .with_detail("limit", u64::try_from(limit).unwrap_or(u64::MAX)))
        }
        Err(error) => Err(RemoteError::new(
            codes::INTERNAL,
            format!("{what} failed to encode: {error}"),
        )),
    }
}

/// Sends `cancel` for `id` when dropped before being [`CancelGuard::disarm`]ed
/// — covers a request future dropped before it settled, not just an explicit
/// user cancel.
struct CancelGuard {
    id: String,
    commands: mpsc::UnboundedSender<Command>,
    armed: bool,
}

impl CancelGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.commands.send(Command::Cancel {
                id: std::mem::take(&mut self.id),
                forget: true,
            });
        }
    }
}
