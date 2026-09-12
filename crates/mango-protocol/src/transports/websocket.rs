//! The WebSocket transport of `spec/transports/websocket.md`: one connection
//! carries one session, frames travel as chunked binary messages under the
//! `mango.v1` subprotocol, and the WebSocket close code carries the reason
//! code.
//!
//! This module owns no server. [`websocket_port`] takes an already-upgraded
//! [`WebSocketStream`] and hands back a [`Port`], so the same code serves a
//! socket a `hyper` upgrade produced, one `tokio-tungstenite` accepted, and
//! one [`connect`](client::connect_websocket) dialled. [`client`] dials;
//! [`server`] does the upgrade-time work — the subprotocol and the bearer —
//! for a peer that has no HTTP stack of its own yet.

use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};

use crate::close::{close_code_for_codec_error, close_codes};
use crate::codec::chunk::{ChunkReassembler, DEFAULT_MAX_MESSAGE_BYTES, encode_chunks};
use crate::codec::ndjson::DEFAULT_MAX_FRAME_BYTES;
use crate::error::{CodecError, CodecErrorKind};
use crate::frame::Frame;
use crate::port::{Inbound, Port, PortClosure, PortRx, PortTx, SendOutcome};

pub mod client;
pub mod server;

/// The subprotocol a Mango Protocol 1 dialler offers and an acceptor selects.
pub const WEBSOCKET_SUBPROTOCOL: &str = "mango.v1";

/// RFC 6455 caps the close reason at 123 UTF-8 bytes; the `close` frame
/// carries the full one.
const MAX_CLOSE_REASON_BYTES: usize = 123;

/// How this transport frames, and how much it will hold for a socket that is
/// not draining.
///
/// # Example
///
/// ```
/// use mango_protocol::transports::websocket::WebSocketOptions;
///
/// let options = WebSocketOptions::default().with_max_message_bytes(2048);
/// assert_eq!(options.max_message_bytes, 2048);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct WebSocketOptions {
    /// Largest frame this port reassembles and sends; 16 MiB by default (§11).
    pub max_frame_bytes: usize,
    /// Message ceiling for the chunker. The reference 16 KiB is also what a
    /// Bun server caps an inbound message at, so a Rust peer never sends one
    /// a Bun hub would drop.
    pub max_message_bytes: usize,
    /// Send a `close` frame before closing the socket. The close code already
    /// carries the reason, so a peer may turn it off; when both are sent the
    /// frame goes first.
    pub send_close_frame: bool,
}

impl Default for WebSocketOptions {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            send_close_frame: true,
        }
    }
}

impl WebSocketOptions {
    /// Sets the frame ceiling this port reassembles and sends within.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::transports::websocket::WebSocketOptions;
    ///
    /// let options = WebSocketOptions::default().with_max_frame_bytes(1 << 20);
    /// assert_eq!(options.max_frame_bytes, 1 << 20);
    /// ```
    #[must_use]
    pub const fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Sets the message ceiling the chunker splits a frame to fit.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::transports::websocket::WebSocketOptions;
    ///
    /// let options = WebSocketOptions::default().with_max_message_bytes(4096);
    /// assert_eq!(options.max_message_bytes, 4096);
    /// ```
    #[must_use]
    pub const fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    /// Stops the port writing a `close` frame ahead of the socket close.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::transports::websocket::WebSocketOptions;
    ///
    /// let options = WebSocketOptions::default().without_close_frame();
    /// assert!(!options.send_close_frame);
    /// ```
    #[must_use]
    pub const fn without_close_frame(mut self) -> Self {
        self.send_close_frame = false;
        self
    }

    /// The tungstenite configuration these options imply.
    ///
    /// `max_write_buffer_size` is the bounded send queue of the spec's
    /// Backpressure section: a socket that is not draining may hold one whole
    /// frame plus the message being written, and a sender that reaches the
    /// ceiling closes with `4400` rather than holding every pending response
    /// for a peer that may never read again.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::transports::websocket::WebSocketOptions;
    ///
    /// let config = WebSocketOptions::default().socket_config();
    /// assert_eq!(config.max_message_size, Some(16 * 1024));
    /// ```
    #[must_use]
    pub fn socket_config(&self) -> WebSocketConfig {
        WebSocketConfig::default()
            // One chunk message is the largest thing either side ever sends,
            // so anything bigger is a peer that is not speaking mango.v1.
            .max_message_size(Some(self.max_message_bytes))
            .max_frame_size(Some(self.max_message_bytes))
            .write_buffer_size(0)
            .max_write_buffer_size(self.max_frame_bytes.saturating_add(self.max_message_bytes))
    }
}

/// One WebSocket connection, as a [`Port`].
///
/// # Example
///
/// ```no_run
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use mango_protocol::frame::PeerInfo;
/// use mango_protocol::session::{Session, SessionOptions};
/// use mango_protocol::transports::websocket::{WebSocketOptions, websocket_port};
/// # async fn upgraded() -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> { unimplemented!() }
///
/// let port = websocket_port(upgraded().await, WebSocketOptions::default());
/// let peer = PeerInfo { name: "hub".into(), version: "1".into(), role: "hub".into() };
/// let (_session, _driver) = Session::spawn(port, SessionOptions::new(peer));
/// # }
/// ```
#[derive(Debug)]
pub struct WebSocketPort<S> {
    stream: WebSocketStream<S>,
    options: WebSocketOptions,
}

/// Wraps an already-upgraded socket as a [`Port`].
///
/// Call it in the same turn the socket was upgraded: the peer sends its
/// `hello` the moment the upgrade completes, and nothing reads the socket
/// until this port does.
///
/// # Example
///
/// ```no_run
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// use mango_protocol::port::Port;
/// use mango_protocol::transports::websocket::{WebSocketOptions, websocket_port};
/// # async fn upgraded() -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> { unimplemented!() }
///
/// let port = websocket_port(upgraded().await, WebSocketOptions::default());
/// assert_eq!(port.max_frame_bytes(), Some(16 * 1024 * 1024));
/// # }
/// ```
#[must_use]
pub fn websocket_port<S>(stream: WebSocketStream<S>, options: WebSocketOptions) -> WebSocketPort<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    WebSocketPort { stream, options }
}

impl<S> Port for WebSocketPort<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Tx = WebSocketTx<S>;
    type Rx = WebSocketRx<S>;

    fn max_frame_bytes(&self) -> Option<usize> {
        Some(self.options.max_frame_bytes)
    }

    fn split(self) -> (Self::Tx, Self::Rx) {
        let options = self.options;
        let (sink, stream) = self.stream.split();
        let sink = SharedSink::new(sink, options);
        let tx = WebSocketTx {
            sink: sink.clone(),
            options,
        };
        let rx = WebSocketRx {
            stream,
            reassembler: ChunkReassembler::new(options.max_message_bytes, options.max_frame_bytes),
            sink,
            closure: None,
            terminal: false,
        };
        (tx, rx)
    }
}

/// The send half of a [`WebSocketPort`].
#[derive(Debug)]
pub struct WebSocketTx<S> {
    sink: SharedSink<S>,
    options: WebSocketOptions,
}

impl<S> PortTx for WebSocketTx<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn send(&mut self, frame: Frame) -> SendOutcome {
        let messages = match encode_chunks(
            &frame,
            self.options.max_message_bytes,
            self.options.max_frame_bytes,
        ) {
            Ok(messages) => messages,
            Err(error) => return SendOutcome::Refused(error),
        };
        self.sink.send_chunks(messages).await
    }

    async fn close(self, code: u16, reason: Option<String>) {
        if self.options.send_close_frame {
            self.sink.send_farewell(code, reason.clone()).await;
        }
        self.sink.close(code, reason.as_deref()).await;
    }
}

/// The receive half of a [`WebSocketPort`].
#[derive(Debug)]
pub struct WebSocketRx<S> {
    stream: SplitStream<WebSocketStream<S>>,
    reassembler: ChunkReassembler,
    sink: SharedSink<S>,
    /// Why the socket ended; held until it is the next thing to hand out.
    closure: Option<PortClosure>,
    terminal: bool,
}

impl<S> PortRx for WebSocketRx<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn recv(&mut self) -> Option<Inbound> {
        loop {
            if let Some(closure) = self.closure.take() {
                self.terminal = true;
                return Some(Inbound::Closed(closure));
            }
            if self.terminal {
                return None;
            }
            // `StreamExt::next` on a `SplitStream` is cancel-safe: a call
            // dropped because another `select!` branch won the race leaves a
            // partially received message in the socket's own buffer.
            match self.stream.next().await {
                Some(Ok(message)) => {
                    if let Some(item) = self.on_message(message).await {
                        return Some(item);
                    }
                }
                Some(Err(error)) => {
                    self.closure = Some(PortClosure::Closed {
                        code: None,
                        reason: Some(error.to_string()),
                    });
                }
                None => {
                    self.closure = Some(PortClosure::Closed {
                        code: None,
                        reason: None,
                    });
                }
            }
        }
    }
}

impl<S> WebSocketRx<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// One incoming message, or `None` when it produced nothing a session
    /// needs to see yet.
    async fn on_message(&mut self, message: Message) -> Option<Inbound> {
        match message {
            Message::Binary(bytes) => match self.reassembler.push(&bytes) {
                Ok(Some(frame)) => Some(Inbound::Frame(frame)),
                Ok(None) => None,
                Err(error) => {
                    self.refuse(error).await;
                    None
                }
            },
            Message::Text(text) => {
                self.refuse(CodecError::new(
                    CodecErrorKind::Schema,
                    format!(
                        "received a text message of {} bytes, expected a binary message, the only kind {WEBSOCKET_SUBPROTOCOL} carries",
                        text.len()
                    ),
                ))
                .await;
                None
            }
            Message::Close(frame) => {
                self.closure = Some(peer_closure(frame.as_ref()));
                None
            }
            // Control frames and raw frames are the socket's own business;
            // tungstenite answers a ping itself.
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => None,
        }
    }

    /// A message the decoder refused: tell the peer with the code the refusal
    /// maps to, then stop. A chunk stream cannot be resynchronised.
    async fn refuse(&mut self, error: CodecError) {
        let code = close_code_for_codec_error(&error);
        self.sink.send_farewell(code, Some(error.to_string())).await;
        self.sink.close(code, Some(&error.to_string())).await;
        self.closure = Some(PortClosure::ProtocolError { error, code });
    }
}

/// What the peer's close frame means to a session: a reason code from the
/// `4000..=4999` table, or the link simply ending.
fn peer_closure(frame: Option<&CloseFrame>) -> PortClosure {
    let Some(frame) = frame else {
        return PortClosure::Closed {
            code: None,
            reason: None,
        };
    };
    let code = u16::from(frame.code);
    if !(close_codes::RELEASED..=crate::close::MAX_CLOSE_CODE).contains(&code) {
        // `1000`, `1006` and the rest of RFC 6455's own range say nothing
        // about why the session ended.
        return PortClosure::Closed {
            code: None,
            reason: None,
        };
    }
    PortClosure::Closed {
        code: Some(code),
        reason: (!frame.reason.is_empty()).then(|| frame.reason.to_string()),
    }
}

/// The socket's send half, shared by the two port halves.
///
/// The receive half needs it for one thing: the farewell a refused message
/// calls for, which the TypeScript port writes from `#failReceive` on the same
/// socket object.
#[derive(Debug)]
struct SharedSink<S>(Arc<Mutex<SinkState<S>>>);

#[derive(Debug)]
struct SinkState<S> {
    sink: SplitSink<WebSocketStream<S>, Message>,
    open: bool,
    options: WebSocketOptions,
}

impl<S> Clone for SharedSink<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S> SharedSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn new(sink: SplitSink<WebSocketStream<S>, Message>, options: WebSocketOptions) -> Self {
        Self(Arc::new(Mutex::new(SinkState {
            sink,
            open: true,
            options,
        })))
    }

    /// Sends one frame's chunks contiguously, so two frames' chunks never
    /// interleave: the lock is held for the whole run.
    async fn send_chunks(&self, messages: Vec<Vec<u8>>) -> SendOutcome {
        let mut state = self.0.lock().await;
        if !state.open {
            return SendOutcome::Closed;
        }
        for message in messages {
            if let Err(error) = state.sink.send(Message::Binary(message.into())).await {
                return state.on_send_error(&error).await;
            }
        }
        match state.sink.flush().await {
            Ok(()) => SendOutcome::Sent,
            Err(error) => state.on_send_error(&error).await,
        }
    }

    /// Writes the farewell of §10 ahead of the socket close; best effort,
    /// since the close code carries the same reason.
    async fn send_farewell(&self, code: u16, reason: Option<String>) {
        let mut state = self.0.lock().await;
        if !state.open {
            return;
        }
        let frame = Frame::Close(crate::frame::Close { code, reason });
        let Ok(messages) = encode_chunks(
            &frame,
            state.options.max_message_bytes,
            state.options.max_frame_bytes,
        ) else {
            // The code is not one a `close` frame may carry. The socket close
            // says the same thing, so the frame is the optional half here.
            return;
        };
        for message in messages {
            if state
                .sink
                .send(Message::Binary(message.into()))
                .await
                .is_err()
            {
                state.open = false;
                return;
            }
        }
        let _ = state.sink.flush().await;
    }

    /// Closes the socket with the reason code, then consumes nothing: the
    /// other half may still be reading the peer's own farewell.
    async fn close(&self, code: u16, reason: Option<&str>) {
        let mut state = self.0.lock().await;
        if !state.open {
            return;
        }
        state.open = false;
        let frame = CloseFrame {
            code: CloseCode::from(code),
            reason: clamp_close_reason(reason.unwrap_or_default()).into(),
        };
        let _ = state.sink.send(Message::Close(Some(frame))).await;
        let _ = state.sink.close().await;
    }
}

impl<S> SinkState<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// A send the stream cannot recover from. A queue that grew past the
    /// ceiling while the socket was not draining is a peer that is not
    /// reading: close with `4400` rather than hold every pending response for
    /// a socket that may never drain (websocket.md, Backpressure). Anything
    /// else is the socket already being gone.
    async fn on_send_error(
        &mut self,
        error: &tokio_tungstenite::tungstenite::Error,
    ) -> SendOutcome {
        let backpressure = matches!(
            error,
            tokio_tungstenite::tungstenite::Error::WriteBufferFull(_)
        );
        if self.open && backpressure {
            self.open = false;
            let frame = CloseFrame {
                code: CloseCode::from(close_codes::PROTOCOL_ERROR),
                reason: clamp_close_reason(
                    "the send queue outgrew one frame limit while the socket was not draining",
                )
                .into(),
            };
            let _ = self.sink.send(Message::Close(Some(frame))).await;
        }
        self.open = false;
        let _ = self.sink.close().await;
        SendOutcome::Closed
    }
}

/// Cuts a close reason down to the 123 UTF-8 bytes RFC 6455 allows, on a
/// character boundary, so a long decoder message cannot make the close itself
/// fail.
fn clamp_close_reason(reason: &str) -> String {
    if reason.len() <= MAX_CLOSE_REASON_BYTES {
        return reason.to_owned();
    }
    let mut end = MAX_CLOSE_REASON_BYTES;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::{WebSocketOptions, clamp_close_reason, peer_closure};
    use crate::port::PortClosure;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    #[test]
    fn a_reason_code_close_frame_becomes_the_sessions_closure() {
        let frame = CloseFrame {
            code: CloseCode::from(4409_u16),
            reason: "superseded".into(),
        };
        assert_eq!(
            peer_closure(Some(&frame)),
            PortClosure::Closed {
                code: Some(4409),
                reason: Some("superseded".into()),
            }
        );
    }

    #[test]
    fn an_rfc_close_code_says_nothing_about_the_session() {
        // 1000 and 1006 are the socket ending, not a reason code from the
        // 4000-4999 table the session reads.
        for code in [1000_u16, 1006, 1011] {
            let frame = CloseFrame {
                code: CloseCode::from(code),
                reason: "".into(),
            };
            assert_eq!(
                peer_closure(Some(&frame)),
                PortClosure::Closed {
                    code: None,
                    reason: None,
                },
                "{code}"
            );
        }
        assert_eq!(
            peer_closure(None),
            PortClosure::Closed {
                code: None,
                reason: None,
            }
        );
    }

    #[test]
    fn a_long_reason_is_cut_to_what_rfc_6455_allows() {
        let long = "é".repeat(200);
        let clamped = clamp_close_reason(&long);
        assert!(clamped.len() <= 123, "{}", clamped.len());
        assert!(long.starts_with(&clamped));
        assert_eq!(clamp_close_reason("short"), "short");
    }

    #[test]
    fn the_send_queue_is_bounded_by_one_frame_limit_plus_a_message() {
        let options = WebSocketOptions::default()
            .with_max_frame_bytes(65_536)
            .with_max_message_bytes(2048);
        let config = options.socket_config();
        assert_eq!(config.max_write_buffer_size, 65_536 + 2048);
        assert_eq!(config.max_message_size, Some(2048));
    }
}
