//! Reserved error codes (§6.3) and the codec's own refusal type.

use std::fmt;

/// The error codes this specification reserves.
///
/// Applications define any other code; an unknown code is preserved as
/// received and never refused.
///
/// # Example
///
/// ```
/// use mango_protocol::error::codes;
///
/// assert_eq!(codes::DENIED, "DENIED");
/// ```
pub mod codes {
    /// The session cannot serve requests: handshake not complete, or closing.
    pub const UNAVAILABLE: &str = "UNAVAILABLE";
    /// Schema-valid but against a protocol rule: duplicate in-flight id, reserved `rpc.` method.
    pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
    /// The responder has no handler for `method`.
    pub const METHOD_UNSUPPORTED: &str = "METHOD_UNSUPPORTED";
    /// `params` failed the contract's schema for this method.
    pub const INVALID_PARAMS: &str = "INVALID_PARAMS";
    /// The method exists but policy refuses it.
    pub const DENIED: &str = "DENIED";
    /// The handler stopped because a `cancel` arrived.
    pub const CANCELLED: &str = "CANCELLED";
    /// A deadline passed, locally or inside the responder.
    pub const TIMEOUT: &str = "TIMEOUT";
    /// The response would exceed the frame limit.
    pub const FRAME_TOO_LARGE: &str = "FRAME_TOO_LARGE";
    /// A request that cannot be served at the effective minor.
    pub const PROTOCOL_MISMATCH: &str = "PROTOCOL_MISMATCH";
    /// Anything else that failed inside the responder.
    pub const INTERNAL: &str = "INTERNAL";

    /// Every reserved code, in the order the specification lists them.
    pub const RESERVED: [&str; 10] = [
        UNAVAILABLE,
        INVALID_REQUEST,
        METHOD_UNSUPPORTED,
        INVALID_PARAMS,
        DENIED,
        CANCELLED,
        TIMEOUT,
        FRAME_TOO_LARGE,
        PROTOCOL_MISMATCH,
        INTERNAL,
    ];
}

/// True when the code is one this specification reserves.
///
/// A consumer narrows unknown codes to its own set; it never refuses them.
///
/// # Example
///
/// ```
/// use mango_protocol::error::is_reserved_error_code;
///
/// assert!(is_reserved_error_code("CANCELLED"));
/// assert!(!is_reserved_error_code("APP_QUOTA_EXHAUSTED"));
/// ```
#[must_use]
pub fn is_reserved_error_code(code: &str) -> bool {
    codes::RESERVED.contains(&code)
}

/// Why a codec refused bytes. Maps one to one onto the fixture corpus reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecErrorKind {
    /// The bytes are not JSON at all (`invalid-json` in the corpus).
    InvalidJson,
    /// The bytes are JSON but the frame breaks the schema (`schema`).
    Schema,
    /// The frame exceeds the frame limit (`too-large`).
    TooLarge,
    /// A chunk header's format version is not `1` (`chunk-version`).
    ChunkVersion,
    /// A chunk message is shorter than its nine-byte header (`chunk-header`).
    ChunkHeader,
    /// A chunk count is zero, above the bound, or changed mid-frame (`chunk-count`).
    ChunkCount,
    /// A chunk index is out of range or not the expected one (`chunk-index`).
    ChunkIndex,
    /// A chunk carries no payload, or a non-final chunk carries too few bytes (`chunk-dribble`).
    ChunkDribble,
}

impl CodecErrorKind {
    /// The corpus reason string for this kind.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::error::CodecErrorKind;
    ///
    /// assert_eq!(CodecErrorKind::ChunkDribble.reason(), "chunk-dribble");
    /// ```
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid-json",
            Self::Schema => "schema",
            Self::TooLarge => "too-large",
            Self::ChunkVersion => "chunk-version",
            Self::ChunkHeader => "chunk-header",
            Self::ChunkCount => "chunk-count",
            Self::ChunkIndex => "chunk-index",
            Self::ChunkDribble => "chunk-dribble",
        }
    }
}

impl fmt::Display for CodecErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.reason())
    }
}

/// A codec refusal: what went wrong and the received value against the expected shape.
///
/// # Example
///
/// ```
/// use mango_protocol::error::{CodecError, CodecErrorKind};
///
/// let refusal = CodecError::new(CodecErrorKind::TooLarge, "received 5000 bytes, expected 4096");
/// assert_eq!(refusal.kind, CodecErrorKind::TooLarge);
/// assert!(refusal.to_string().starts_with("too-large:"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError {
    /// Which rule the bytes broke.
    pub kind: CodecErrorKind,
    /// The received value and the expected shape, ready to log.
    pub message: String,
}

impl CodecError {
    /// Builds a refusal from a kind and a message naming the received value.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::error::{CodecError, CodecErrorKind};
    ///
    /// let refusal = CodecError::new(CodecErrorKind::Schema, "received {}, expected a frame");
    /// assert_eq!(refusal.message, "received {}, expected a frame");
    /// ```
    #[must_use]
    pub fn new(kind: CodecErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for CodecError {}

#[cfg(test)]
mod tests {
    use super::{CodecError, CodecErrorKind, codes, is_reserved_error_code};

    #[test]
    fn every_reserved_code_is_recognised() {
        for code in codes::RESERVED {
            assert!(is_reserved_error_code(code), "{code} should be reserved");
        }
    }

    #[test]
    fn an_application_code_is_not_reserved() {
        assert!(!is_reserved_error_code("APP_QUOTA_EXHAUSTED"));
        assert!(!is_reserved_error_code("denied"));
    }

    #[test]
    fn reserved_codes_match_the_specification_table() {
        assert_eq!(codes::RESERVED.len(), 10);
        assert_eq!(codes::FRAME_TOO_LARGE, "FRAME_TOO_LARGE");
    }

    #[test]
    fn display_names_the_reason_and_the_message() {
        let refusal = CodecError::new(CodecErrorKind::ChunkIndex, "received 3, expected 1");
        assert_eq!(refusal.to_string(), "chunk-index: received 3, expected 1");
    }

    #[test]
    fn every_kind_has_a_distinct_corpus_reason() {
        let kinds = [
            CodecErrorKind::InvalidJson,
            CodecErrorKind::Schema,
            CodecErrorKind::TooLarge,
            CodecErrorKind::ChunkVersion,
            CodecErrorKind::ChunkHeader,
            CodecErrorKind::ChunkCount,
            CodecErrorKind::ChunkIndex,
            CodecErrorKind::ChunkDribble,
        ];
        let mut reasons: Vec<&str> = kinds.iter().map(|kind| kind.reason()).collect();
        reasons.sort_unstable();
        reasons.dedup();
        assert_eq!(reasons.len(), kinds.len());
    }
}
