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

pub mod ndjson;
pub mod stdio;
