//! [`SessionClosure`] and the teardown every path funnels through.

use std::sync::Arc;

use crate::close::{close_codes, is_fatal_close_code};
use crate::error::{CodecError, RemoteError, codes};
use crate::port::{PortClosure, PortTx};

use super::driver::Writer;
use super::handle::SessionState;
use super::shared::{Shared, lock};

/// Why the driver's main loop ended: what closure to report, and whether the
/// transport still needs telling.
pub(super) enum Teardown {
    /// This side initiated the close: a local rule violation (a duplicate
    /// hello, a version mismatch), a handshake timeout, or an explicit
    /// `Session::close`/`close_now`. The port is told, via `PortTx::close`.
    Local { code: u16, reason: Option<String> },
    /// The peer sent a `close` frame naming its own code/reason. The port is
    /// already on its way out; nothing to tell it.
    PeerFrame { code: u16, reason: Option<String> },
    /// The port itself ended the stream, or reported a codec-level refusal.
    Port(PortClosure),
}

/// Why a [`super::handle::Session`] ended, in the vocabulary of the
/// close-code table.
///
/// # Example
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use mango_protocol::close::close_codes;
/// use mango_protocol::frame::PeerInfo;
/// use mango_protocol::port::port_pair;
/// use mango_protocol::session::{Session, SessionOptions};
///
/// let (a, _b) = port_pair();
/// let peer = PeerInfo { name: "example".into(), version: "0.1.0".into(), role: "runtime".into() };
/// let (session, driver) = Session::open(a, SessionOptions::new(peer));
/// tokio::spawn(driver.run());
/// let closure = session.close(close_codes::RELEASED, Some("done")).await;
/// assert_eq!(closure.code, close_codes::RELEASED);
/// # }
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct SessionClosure {
    /// The close code, from the `4000..=4999` table or an application's own.
    pub code: u16,
    /// The peer's or the local side's reason text, if any.
    pub reason: Option<String>,
    /// True for a code redialling the same connection cannot recover from.
    pub fatal: bool,
    /// Present when a refused record ended the session.
    pub error: Option<CodecError>,
    /// Handlers still running when the grace period expired. Rust-only; a
    /// TypeScript session aborts and forgets instead of waiting.
    pub unfinished_handlers: usize,
}

/// Runs the teardown sequence and returns the closure it produced.
///
/// A free function rather than a method on `SessionDriver`: by the time this
/// runs, `SessionDriver::tx` has already moved into `writer`, and a struct
/// that has had one field moved out of it can no longer be moved as a whole
/// — only its remaining individual fields (here, just `shared`) can be.
pub(super) async fn teardown<Tx: PortTx>(
    shared: Arc<Shared>,
    reason: Teardown,
    writer: Writer<Tx>,
) -> SessionClosure {
    let (code, reason_text, error, close_port) = match reason {
        Teardown::Local { code, reason } => (code, reason, None, true),
        Teardown::PeerFrame { code, reason } => (code, reason, None, false),
        Teardown::Port(PortClosure::Closed { code, reason }) => {
            (code.unwrap_or(close_codes::RELEASED), reason, None, false)
        }
        Teardown::Port(PortClosure::ProtocolError { error, code }) => {
            let message = error.to_string();
            (code, Some(message), Some(error), false)
        }
    };
    let fatal = is_fatal_close_code(code);

    // Step 2: flip state to Closed; every later read only observes it.
    lock(&shared.inner).state = SessionState::Closed;

    // Step 3: flush the writer, then close (or just drop) the port. Nothing
    // enqueued after this point is ever sent.
    if let Some(tx) = writer.shut_down().await
        && close_port
    {
        tx.close(code, reason_text.clone()).await;
    }

    // Step 4: fail `ready`, unless a specific failure already settled it (a
    // version mismatch, a handshake timeout) or it already succeeded.
    let ready_code = if code == close_codes::PROTOCOL_MISMATCH {
        codes::PROTOCOL_MISMATCH
    } else {
        codes::UNAVAILABLE
    };
    let why = reason_text
        .as_deref()
        .map(|text| format!(": {text}"))
        .unwrap_or_default();
    shared.fail_ready(
        RemoteError::new(
            ready_code,
            format!("The session closed before the handshake completed ({code}{why})."),
        )
        .with_detail("close_code", code),
    );

    // Steps 5 (fail pending requests), 6 (cancel active handlers' tokens) and
    // 8 (grace-drain outstanding handler tasks) have nothing to do until a
    // later commit gives the driver a handler registry and a JoinSet to
    // populate them. Step 7 (clear event/pong subscribers) is the same until
    // events exist.
    let unfinished_handlers = 0;

    let closure = SessionClosure {
        code,
        reason: reason_text,
        fatal,
        error,
        unfinished_handlers,
    };

    // Step 9: publish, unblocking every `closed()` waiter. `send_replace`,
    // not `send`: the latter is a no-op whenever there is not yet a live
    // receiver (nobody has called `closed()` yet), which would otherwise
    // strand every later caller on a value that was never actually written.
    shared.closure.send_replace(Some(closure.clone()));
    closure
}
