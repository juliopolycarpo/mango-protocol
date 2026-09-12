//! The [`Handler`] trait, [`CallContext`], and the guard registration returns.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::RemoteError;

use super::handle::{RemotePeer, Session};
use super::shared::lock;

/// The boxed future a [`Handler`] returns.
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, RemoteError>> + Send>>;

/// Answers one request.
///
/// Returning `Err(RemoteError)` chooses the wire `error.code`; a panic inside
/// `call` is caught by the `JoinSet` that runs it and becomes `INTERNAL`, and
/// a handler that observes [`CallContext::is_cancelled`] and stops early
/// should return `Err` with [`crate::error::codes::CANCELLED`].
///
/// # Example
///
/// ```
/// use mango_protocol::session::{CallContext, Handler};
/// use serde_json::Value;
///
/// let echo = |params: Value, _context: CallContext| async move { Ok(params) };
/// fn assert_is_handler(_: impl Handler) {}
/// assert_is_handler(echo);
/// ```
pub trait Handler: Send + Sync + 'static {
    /// Runs the handler.
    fn call(&self, params: Value, context: CallContext) -> HandlerFuture;
}

impl<F, Fut> Handler for F
where
    F: Fn(Value, CallContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, RemoteError>> + Send + 'static,
{
    fn call(&self, params: Value, context: CallContext) -> HandlerFuture {
        Box::pin(self(params, context))
    }
}

/// What a running handler needs: the request it is answering, a cooperative
/// cancellation signal, and a handle back onto the session that dispatched
/// it.
#[derive(Clone)]
pub struct CallContext {
    pub(super) id: String,
    pub(super) method: String,
    pub(super) cancel: CancellationToken,
    pub(super) remote: RemotePeer,
    pub(super) session: Session,
}

impl CallContext {
    /// The id of the request this handler is answering.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The method the peer requested.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The token a handler should watch: cancelled on an inbound `cancel`
    /// frame, or when the session closes.
    #[must_use]
    pub fn cancel(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Shorthand for `self.cancel().is_cancelled()`.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// How many requests this session is answering right now, this one
    /// included.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.session.shared.in_flight.load(Ordering::Relaxed)
    }

    /// The peer's handshake announcement.
    #[must_use]
    pub fn remote(&self) -> &RemotePeer {
        &self.remote
    }

    /// The session this request arrived on, for a handler that itself emits
    /// an event or makes a further request.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }
}

/// Returned by [`Session::handle`]; dropping it unregisters the handler.
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
pub struct HandlerGuard {
    pub(super) method: String,
    pub(super) generation: u64,
    pub(super) shared: Arc<super::shared::Shared>,
}

impl HandlerGuard {
    /// Keeps the registration for the session's life; the returned value has
    /// no further effect and dropping it does nothing.
    pub fn persist(self) {
        std::mem::forget(self);
    }
}

impl Drop for HandlerGuard {
    fn drop(&mut self) {
        let mut handlers = lock(&self.shared.handlers);
        if handlers
            .get(&self.method)
            .is_some_and(|(generation, _)| *generation == self.generation)
        {
            handlers.remove(&self.method);
        }
    }
}
