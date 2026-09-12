//! The transports a [`crate::session::Session`] is opened over.
//!
//! Every module here produces a [`crate::port::Port`], and nothing below this
//! one knows which: the session's own loop is written against the trait. The
//! byte-oriented transports — stdio, the local socket, the spawn launcher —
//! share [`ndjson`], which owns the framing, the refusal handling and the
//! close sequence once, exactly as `packages/protocol/src/transports/
//! ndjson-port.ts` does for the TypeScript SDK.
//!
//! | Module | Carries frames over |
//! | --- | --- |
//! | [`ndjson`] | any pair of byte streams |
//! | [`stdio`] | standard input and standard output |
//! | [`ipc`] | a Unix domain socket, or a Windows named pipe |
//! | [`spawn`] | a child process's own standard streams |
//! | [`websocket`] | one WebSocket connection, dialled or accepted |
//!
//! [`deadline`] is not a transport but the bound every dialling one connects
//! under, and the one error type they all fail with; [`ssh`] is not one
//! either, but the argv that puts [`spawn`] on the far end of a network.

pub mod deadline;
pub mod ipc;
pub mod ndjson;
#[cfg(feature = "spawn")]
pub mod spawn;
#[cfg(feature = "spawn")]
pub mod ssh;
pub mod stdio;
#[cfg(feature = "websocket")]
pub mod websocket;

pub use deadline::{ConnectDeadline, ConnectError};
