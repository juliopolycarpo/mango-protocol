//! Framing: how whole frames are carved out of a transport's bytes.
//!
//! [`ndjson`] is the line framing of the stdio, local socket and spawn
//! transports.

pub mod ndjson;
