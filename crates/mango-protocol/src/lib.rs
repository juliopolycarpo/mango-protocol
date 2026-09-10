//! Mango Protocol: wire types and codec for MangoStudio hubs, runtimes and tools.
//!
//! This crate is the Rust half of one wire contract published three ways: the
//! normative specification under `spec/`, the TypeScript SDK
//! `@mangostudio/protocol`, and this crate. It carries the frame types and the
//! rules a decoder enforces; framing, the catalog document and JSON Schema
//! emission land in the commits that follow. Sessions, transports and any async
//! runtime are a later milestone and live outside this crate.
//!
//! # Example
//!
//! ```
//! use mango_protocol::{Frame, Request, validate};
//! use serde_json::json;
//!
//! let request = Frame::Req(Request {
//!     id: "r-42".into(),
//!     method: "fs.read-file".into(),
//!     params: json!({ "path": "/etc/hosts" }),
//! });
//! assert!(validate(&request).is_ok());
//! ```
//!
//! # Layout
//!
//! - [`frame`] — the eight frame types and their members.
//! - [`mod@validate`] — the lengths, grammars and ranges serde cannot express.
//! - [`version`] — the wire version and the negotiation rule.
//! - [`close`] and [`error`] — the reserved close codes and error codes.

pub mod close;
pub mod error;
pub mod frame;
pub mod validate;
pub mod version;

pub use close::{close_code_name, close_codes, is_fatal_close_code};
pub use error::{CodecError, CodecErrorKind, is_reserved_error_code};
pub use frame::{
    Cancel, Close, End, ErrorPayload, ErrorResponse, Event, Frame, Hello, Limits, PeerInfo,
    Request, Response,
};
pub use validate::{ValidationError, is_valid_method_name, validate};
pub use version::{Negotiation, PROTOCOL_VERSION, ProtocolVersion, negotiate};

/// Wire major version this crate speaks. Mirrors [`PROTOCOL_VERSION`].
pub const PROTOCOL_MAJOR: u16 = 1;
/// Highest wire minor version this crate speaks. Mirrors [`PROTOCOL_VERSION`].
pub const PROTOCOL_MINOR: u16 = 0;

#[cfg(test)]
mod tests {
    use super::{PROTOCOL_MAJOR, PROTOCOL_MINOR, PROTOCOL_VERSION};

    #[test]
    fn starts_at_wire_one_zero() {
        assert_eq!(PROTOCOL_MAJOR, 1);
        assert_eq!(PROTOCOL_MINOR, 0);
    }

    #[test]
    fn the_constants_mirror_the_version_struct() {
        assert_eq!(u32::from(PROTOCOL_MAJOR), PROTOCOL_VERSION.major);
        assert_eq!(u32::from(PROTOCOL_MINOR), PROTOCOL_VERSION.minor);
    }
}
