//! [`SessionOptions`] and its builder methods.

use std::time::Duration;

use serde_json::{Map, Value};

use crate::frame::PeerInfo;
use crate::version::{PROTOCOL_VERSION, ProtocolVersion};

/// 15 seconds: the default [`SessionOptions::handshake_timeout`].
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// 20 seconds: the default [`SessionOptions::liveness_interval`].
pub const DEFAULT_LIVENESS_INTERVAL: Duration = Duration::from_secs(20);
/// 5 seconds: the default [`SessionOptions::handler_grace`].
pub const DEFAULT_HANDLER_GRACE: Duration = Duration::from_secs(5);
/// `"r"`: the default [`SessionOptions::request_id_prefix`].
pub const DEFAULT_REQUEST_ID_PREFIX: &str = "r";

/// How to open a [`crate::session::Session`]: who this side is, and every
/// tunable the handshake and the driver need.
///
/// # Example
///
/// ```
/// use mango_protocol::frame::PeerInfo;
/// use mango_protocol::session::SessionOptions;
///
/// let peer = PeerInfo {
///     name: "example-runtime".into(),
///     version: "0.1.0".into(),
///     role: "runtime".into(),
/// };
/// let options = SessionOptions::new(peer).with_request_id_prefix("call");
/// assert_eq!(options.request_id_prefix, "call");
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// Who this side is: name, release string, role label.
    pub peer: PeerInfo,
    /// The application's capability object; empty when unset.
    pub capabilities: Map<String, Value>,
    /// Highest wire version this side speaks.
    pub protocol: ProtocolVersion,
    /// Largest frame this side accepts. `None` defers to the port's own
    /// ceiling, then to [`crate::codec::ndjson::DEFAULT_MAX_FRAME_BYTES`].
    pub max_frame_bytes: Option<usize>,
    /// How long to wait for the peer's `hello`.
    pub handshake_timeout: Duration,
    /// Ping cadence after the handshake; `None` disables liveness checking
    /// (mirrors the TypeScript SDK's `livenessIntervalMs: false`).
    pub liveness_interval: Option<Duration>,
    /// Prefix of generated request ids.
    pub request_id_prefix: String,
    /// How long `close()` waits for in-flight handlers to settle before
    /// abandoning them. Rust-only; the TypeScript SDK has no equivalent since
    /// it never awaits its own teardown.
    pub handler_grace: Duration,
}

impl SessionOptions {
    /// Starts from the defaults, naming only who this side is.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::frame::PeerInfo;
    /// use mango_protocol::session::SessionOptions;
    ///
    /// let peer = PeerInfo { name: "hub".into(), version: "1.0.0".into(), role: "hub".into() };
    /// let options = SessionOptions::new(peer);
    /// assert_eq!(options.request_id_prefix, "r");
    /// ```
    #[must_use]
    pub fn new(peer: PeerInfo) -> Self {
        Self {
            peer,
            capabilities: Map::new(),
            protocol: PROTOCOL_VERSION,
            max_frame_bytes: None,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            liveness_interval: Some(DEFAULT_LIVENESS_INTERVAL),
            request_id_prefix: DEFAULT_REQUEST_ID_PREFIX.to_string(),
            handler_grace: DEFAULT_HANDLER_GRACE,
        }
    }

    /// Replaces the capability object announced in `hello.capabilities`.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: Map<String, Value>) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Replaces the wire version this side announces.
    #[must_use]
    pub fn with_protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.protocol = protocol;
        self
    }

    /// Sets the largest frame this side accepts.
    #[must_use]
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = Some(max_frame_bytes);
        self
    }

    /// Sets how long to wait for the peer's `hello`.
    #[must_use]
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }

    /// Sets the ping cadence, or disables liveness checking with `None`.
    #[must_use]
    pub fn with_liveness_interval(mut self, liveness_interval: Option<Duration>) -> Self {
        self.liveness_interval = liveness_interval;
        self
    }

    /// Sets the prefix of generated request ids.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::frame::PeerInfo;
    /// use mango_protocol::session::SessionOptions;
    ///
    /// let peer = PeerInfo { name: "hub".into(), version: "1.0.0".into(), role: "hub".into() };
    /// let options = SessionOptions::new(peer).with_request_id_prefix("call");
    /// assert_eq!(options.request_id_prefix, "call");
    /// ```
    #[must_use]
    pub fn with_request_id_prefix(mut self, request_id_prefix: impl Into<String>) -> Self {
        self.request_id_prefix = request_id_prefix.into();
        self
    }

    /// Sets how long `close()` waits for in-flight handlers to settle.
    #[must_use]
    pub fn with_handler_grace(mut self, handler_grace: Duration) -> Self {
        self.handler_grace = handler_grace;
        self
    }
}
