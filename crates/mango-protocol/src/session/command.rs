//! The internal command channel between a [`super::handle::Session`] handle
//! and its `SessionDriver`.
//!
//! Handler registration and event sequencing are not commands: both go
//! through [`super::shared::Shared`]'s own lock instead, since a handle needs
//! them to happen synchronously with respect to a concurrent dispatch.

use tokio::sync::oneshot;

use crate::error::RemoteError;
use crate::frame::Frame;
use serde_json::Value;

/// One request from a `Session` handle to its `SessionDriver`.
pub(super) enum Command {
    /// Ends the session with `code`/`reason`, from the vocabulary of the
    /// close-code table.
    Close { code: u16, reason: Option<String> },
    /// Sends an outbound `req` frame and remembers `reply`, so that when the
    /// matching `res`/`err` arrives, the driver can settle it.
    Request {
        frame: Frame,
        reply: oneshot::Sender<Result<Value, RemoteError>>,
    },
    /// Sends a `cancel` frame for `id`. `forget: false` on a local timeout
    /// (the pending entry is deleted, so a late real answer is silently
    /// ignored); `forget: true` on a user cancel or a dropped request future
    /// (the pending entry stays, so the peer's real `err CANCELLED` — cancel
    /// is advisory, not a promise — still settles it).
    Cancel { id: String, forget: bool },
}
