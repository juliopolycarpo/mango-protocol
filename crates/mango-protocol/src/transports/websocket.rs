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

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};

use crate::close::{close_code_for_codec_error, close_codes};
use crate::codec::chunk::{
    CHUNK_HEADER_BYTES, ChunkReassembler, DEFAULT_MAX_MESSAGE_BYTES, encode_chunks,
};
use crate::codec::ndjson::DEFAULT_MAX_FRAME_BYTES;
use crate::error::{CodecError, CodecErrorKind};
use crate::frame::Frame;
use crate::port::{Inbound, Port, PortClosure, PortRx, PortTx, SendOutcome};

use super::CLOSE_FLUSH_GRACE;

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
    /// The queue the spec's Backpressure section is about is this port's own,
    /// not tungstenite's: it writes straight through to the socket, so its
    /// write buffer never accumulates and could not be measured.
    ///
    /// The ceiling here bounds an *incoming* message, and it is deliberately
    /// not [`WebSocketOptions::max_message_bytes`]. That number is this
    /// sender's own setting — websocket.md calls it "a local setting of at
    /// least 2048 bytes" — and the receiver's obligations, which the spec
    /// lists exhaustively, include no ceiling on a message at all. A peer is
    /// free to put a whole frame in one message, so that is what is allowed;
    /// the reassembler still refuses anything whose payload passes the frame
    /// limit.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::codec::chunk::CHUNK_HEADER_BYTES;
    /// use mango_protocol::transports::websocket::WebSocketOptions;
    ///
    /// // A peer that sends one 2 KiB chunk and a peer that sends the whole
    /// // frame at once are both conforming, so neither is cut off.
    /// let options = WebSocketOptions::default().with_max_message_bytes(2048);
    /// let config = options.socket_config();
    /// assert_eq!(
    ///     config.max_message_size,
    ///     Some(options.max_frame_bytes + CHUNK_HEADER_BYTES)
    /// );
    /// ```
    #[must_use]
    pub fn socket_config(&self) -> WebSocketConfig {
        let incoming = self.max_frame_bytes.saturating_add(CHUNK_HEADER_BYTES);
        WebSocketConfig::default()
            .max_message_size(Some(incoming))
            .max_frame_size(Some(incoming))
            // Every chunk goes to the socket as it is written; this port's own
            // queue is what holds anything back.
            .write_buffer_size(0)
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
        let writer = SocketWriter::spawn(sink, options);
        let tx = WebSocketTx {
            writer: writer.clone(),
            options,
            socket: PhantomData,
        };
        let rx = WebSocketRx {
            stream,
            reassembler: ChunkReassembler::new(options.max_message_bytes, options.max_frame_bytes),
            writer,
            closure: None,
            terminal: false,
        };
        (tx, rx)
    }
}

/// The send half of a [`WebSocketPort`].
#[derive(Debug)]
pub struct WebSocketTx<S> {
    writer: SocketWriter,
    options: WebSocketOptions,
    /// Ties this half to the socket's own type, which the writer task owns.
    socket: PhantomData<fn() -> S>,
}

impl<S> PortTx for WebSocketTx<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn send(&mut self, frame: Frame) -> SendOutcome {
        self.writer.send_frame(&frame).await
    }

    async fn close(self, code: u16, reason: Option<String>) {
        self.writer
            .close(code, reason.as_deref(), self.options.send_close_frame)
            .await;
    }
}

/// The receive half of a [`WebSocketPort`].
#[derive(Debug)]
pub struct WebSocketRx<S> {
    stream: SplitStream<WebSocketStream<S>>,
    reassembler: ChunkReassembler,
    writer: SocketWriter,
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
                // The socket is going; a send queued after this would be
                // reported as sent and never carried.
                self.writer.mark_closed();
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
        let reason = error.to_string();
        // Recorded before the await, never after: `recv` is cancel-safe, and a
        // closure written on the far side of an await is one a caller that
        // lost a `select!` race would never be told about. Losing it here
        // would downgrade a refusal to the plain release an ended socket
        // reports, so a dialler would retry a peer whose frames it cannot
        // read.
        self.closure = Some(PortClosure::ProtocolError { error, code });
        self.writer.close(code, Some(&reason), true).await;
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

/// The socket's send half, behind the one queue per connection that
/// websocket.md's Backpressure section describes.
///
/// A task owns the sink; a send hands its chunks over and returns. That is
/// what makes the queue observable at all: a sender that simply awaited the
/// socket would have no queue to measure, and a peer that stopped reading
/// would stall it for ever rather than be given up on. The counter is the
/// bytes handed over and not yet written, and passing one frame limit is the
/// signal the spec names.
///
/// The receive half holds one of these too, for the farewell a refused
/// message calls for — the same thing the TypeScript port writes from
/// `#failReceive`.
#[derive(Debug, Clone)]
struct SocketWriter {
    commands: mpsc::UnboundedSender<WriteCommand>,
    /// Bytes handed to the writer task and not yet written.
    queued_bytes: Arc<AtomicUsize>,
    /// False once anything closed the socket; every later send is reported as
    /// the transport being gone rather than queued behind a dead one.
    open: Arc<AtomicBool>,
    /// Held, never read: dropping the last handle aborts the writer task, so
    /// one still stuck on a socket that will not take the farewell cannot
    /// outlive the port.
    _task: Arc<AbortOnDrop>,
    options: WebSocketOptions,
}

/// One item for the writer task.
#[derive(Debug)]
enum WriteCommand {
    /// One frame's chunks, written contiguously so two frames never
    /// interleave, and the byte count to release from the queue afterwards.
    Chunks {
        messages: Vec<Vec<u8>>,
        bytes: usize,
    },
    /// Close the socket with this code and stop.
    Close {
        code: u16,
        reason: Option<String>,
        done: oneshot::Sender<()>,
    },
}

/// A writer task that is aborted when the last handle to it goes.
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl SocketWriter {
    fn spawn<S>(sink: SplitSink<WebSocketStream<S>, Message>, options: WebSocketOptions) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (commands, receiver) = mpsc::unbounded_channel();
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let open = Arc::new(AtomicBool::new(true));
        let task = tokio::spawn(drive(
            sink,
            receiver,
            Arc::clone(&queued_bytes),
            Arc::clone(&open),
        ));
        Self {
            commands,
            queued_bytes,
            open,
            _task: Arc::new(AbortOnDrop(task)),
            options,
        }
    }

    /// Queues one frame's chunks.
    ///
    /// Reports [`SendOutcome::Sent`] once they are the writer's, the way the
    /// TypeScript port reports a message the socket buffered. A queue that has
    /// passed one frame limit is a peer that is not reading: the socket is
    /// closed with `4400` and the session is told the transport is gone,
    /// rather than every pending response being held for a socket that may
    /// never drain.
    async fn send_frame(&self, frame: &Frame) -> SendOutcome {
        if !self.open.load(Ordering::Acquire) {
            return SendOutcome::Closed;
        }
        let messages = match encode_chunks(
            frame,
            self.options.max_message_bytes,
            self.options.max_frame_bytes,
        ) {
            Ok(messages) => messages,
            Err(error) => return SendOutcome::Refused(error),
        };
        let bytes: usize = messages.iter().map(Vec::len).sum();
        // What was already waiting, this frame excluded. A frame is never
        // measured against the limit by its own size: one of exactly the frame
        // limit is legal, and its chunk headers would push any total over.
        let waiting = self.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
        if self
            .commands
            .send(WriteCommand::Chunks { messages, bytes })
            .is_err()
        {
            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
            self.open.store(false, Ordering::Release);
            return SendOutcome::Closed;
        }
        if waiting > self.options.max_frame_bytes {
            self.close(
                close_codes::PROTOCOL_ERROR,
                Some("the send queue outgrew one frame limit while the socket was not draining"),
                true,
            )
            .await;
            return SendOutcome::Closed;
        }
        SendOutcome::Sent
    }

    /// Closes the socket with the reason code, having queued the farewell
    /// frame of §10 ahead of it when this port sends one.
    ///
    /// Waits a bounded grace for the close to reach the socket: on a healthy
    /// connection that is immediate, and a peer that will not take it must not
    /// hold up a teardown.
    async fn close(&self, code: u16, reason: Option<&str>, send_close_frame: bool) {
        if !self.open.swap(false, Ordering::AcqRel) {
            return;
        }
        if send_close_frame {
            self.queue_farewell(code, reason);
        }
        let (done, finished) = oneshot::channel();
        if self
            .commands
            .send(WriteCommand::Close {
                code,
                reason: reason.map(ToOwned::to_owned),
                done,
            })
            .is_err()
        {
            return;
        }
        let _ = tokio::time::timeout(CLOSE_FLUSH_GRACE, finished).await;
    }

    /// Records that the socket is gone without writing anything: the peer
    /// closed it, so a frame queued after this would be reported as sent and
    /// never carried.
    fn mark_closed(&self) {
        self.open.store(false, Ordering::Release);
    }

    /// Best effort: a farewell the codec refuses is the optional half here,
    /// since the socket's own close code carries the same reason.
    fn queue_farewell(&self, code: u16, reason: Option<&str>) {
        let frame = Frame::Close(crate::frame::Close {
            code,
            reason: reason.map(ToOwned::to_owned),
        });
        let Ok(messages) = encode_chunks(
            &frame,
            self.options.max_message_bytes,
            self.options.max_frame_bytes,
        ) else {
            return;
        };
        let bytes: usize = messages.iter().map(Vec::len).sum();
        self.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
        let _ = self.commands.send(WriteCommand::Chunks { messages, bytes });
    }
}

/// The writer task: one queue per connection, drained in order.
async fn drive<S>(
    mut sink: SplitSink<WebSocketStream<S>, Message>,
    mut commands: mpsc::UnboundedReceiver<WriteCommand>,
    queued_bytes: Arc<AtomicUsize>,
    open: Arc<AtomicBool>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    while let Some(command) = commands.recv().await {
        match command {
            WriteCommand::Chunks { messages, bytes } => {
                // Released as the writer takes them, not after they are
                // written: what the counter measures is the queue behind the
                // socket, and the frame being written is no longer in it.
                queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                let written = write_chunks(&mut sink, messages).await;
                if !written {
                    open.store(false, Ordering::Release);
                    break;
                }
            }
            WriteCommand::Close { code, reason, done } => {
                let frame = CloseFrame {
                    code: CloseCode::from(code),
                    reason: clamp_close_reason(reason.as_deref().unwrap_or_default()).into(),
                };
                let _ = sink.send(Message::Close(Some(frame))).await;
                let _ = done.send(());
                break;
            }
        }
    }
    let _ = sink.close().await;
}

/// Writes one frame's chunks contiguously. False once the socket refused one,
/// which is the connection being gone rather than a frame to retry.
async fn write_chunks<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    messages: Vec<Vec<u8>>,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    for message in messages {
        if sink.send(Message::Binary(message.into())).await.is_err() {
            return false;
        }
    }
    sink.flush().await.is_ok()
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
    use super::{CHUNK_HEADER_BYTES, WebSocketOptions, clamp_close_reason, peer_closure};
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
    fn the_receive_ceiling_is_what_a_peer_may_send_not_what_this_side_sends() {
        // The message ceiling is the *sender's* local setting, so a peer that
        // puts a whole frame in one message is conforming. Capping arrivals at
        // this side's own ceiling would cut off such a peer on its first bulk
        // result, and the spec lists no ceiling among a receiver's duties.
        let options = WebSocketOptions::default().with_max_message_bytes(2048);
        let config = options.socket_config();
        let whole_frame = Some(options.max_frame_bytes + CHUNK_HEADER_BYTES);
        assert_eq!(config.max_message_size, whole_frame);
        assert_eq!(config.max_frame_size, whole_frame);
    }
}
