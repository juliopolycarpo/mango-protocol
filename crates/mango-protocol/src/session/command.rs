//! The internal command channel between a [`super::handle::Session`] handle
//! and its `SessionDriver`.
//!
//! Handler registration and event sequencing are not commands: both go
//! through [`super::shared::Shared`]'s own lock instead, since a handle needs
//! them to happen synchronously with respect to a concurrent dispatch.

/// One request from a `Session` handle to its `SessionDriver`.
pub(super) enum Command {
    /// Ends the session with `code`/`reason`, from the vocabulary of the
    /// close-code table.
    Close { code: u16, reason: Option<String> },
}
