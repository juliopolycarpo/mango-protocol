//! `SessionDriver::run` — the `select!` loop that drives one session.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::close::close_codes;
use crate::codec::ndjson::DEFAULT_MAX_FRAME_BYTES;
use crate::error::{RemoteError, codes};
use crate::frame::{Frame, Hello, Limits};
use crate::port::{Inbound, PortClosure, PortRx, PortTx, SendOutcome};
use crate::version::{Negotiation, negotiate};

use super::command::Command;
use super::handle::{RemotePeer, SessionState};
use super::shared::{Shared, lock};
use super::teardown::{self, SessionClosure, Teardown};

/// One outbound item handed to the [`Writer`] task.
///
/// Only `Shutdown` is produced yet; a `Frame` variant joins it once ping/pong
/// and events need to enqueue a frame after the handshake.
enum Outbound {
    /// Finish whatever is queued, then stop and hand the port back.
    Shutdown,
}

/// Owns a port's send half on a dedicated task, so a slow or blocking send
/// never stalls the driver's own `select!` loop.
pub(super) struct Writer<Tx> {
    sender: mpsc::UnboundedSender<Outbound>,
    task: JoinHandle<Tx>,
}

impl<Tx: PortTx> Writer<Tx> {
    /// Spawns the writer task, which owns `tx` until [`Writer::shut_down`].
    pub(super) fn spawn(tx: Tx) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Outbound>();
        let task = tokio::spawn(async move {
            // Only `Shutdown` exists yet, so one recv is the whole job: it
            // arrives, or the sender drops (`None`) — either way, stop.
            let _ = receiver.recv().await;
            tx
        });
        Self { sender, task }
    }

    /// Flushes whatever is queued, then returns the port's send half, or
    /// `None` if the writer task itself panicked (never expected: its body
    /// has no fallible operation besides an already-ignored send).
    pub(super) async fn shut_down(self) -> Option<Tx> {
        let _ = self.sender.send(Outbound::Shutdown);
        drop(self.sender);
        self.task.await.ok()
    }
}

/// Drives one session: owns the port, the command inbox, and (starting with
/// later commits) the in-flight handler tasks.
///
/// Built by [`super::Session::open`]; nothing progresses until this future is
/// polled, typically via [`super::Session::spawn`] or `tokio::spawn(driver.run())`.
pub struct SessionDriver<Tx, Rx> {
    pub(super) shared: Arc<Shared>,
    pub(super) tx: Tx,
    pub(super) rx: Rx,
    pub(super) commands: mpsc::UnboundedReceiver<Command>,
    pub(super) handshake_timeout: Duration,
}

impl<Tx: PortTx, Rx: PortRx> SessionDriver<Tx, Rx> {
    /// Sends `hello`, negotiates the handshake, then answers frames and
    /// commands until something ends the session; tears it down and returns
    /// why.
    pub async fn run(mut self) -> SessionClosure {
        let hello = self.build_hello();
        let hello_outcome = self.tx.send(Frame::Hello(hello)).await;
        if !matches!(hello_outcome, SendOutcome::Sent) {
            // The transport can go away between construction and the first
            // send: a peer that refuses the credential closes the socket the
            // moment it opens.
            let detail = match &hello_outcome {
                SendOutcome::Refused(error) => error.to_string(),
                _ => "the port is already closed".to_string(),
            };
            self.shared.fail_ready(RemoteError::new(
                codes::UNAVAILABLE,
                format!("The transport refused the hello: {detail}"),
            ));
            let writer = Writer::spawn(self.tx);
            return teardown::teardown(
                self.shared,
                Teardown::Local {
                    code: close_codes::RELEASED,
                    reason: Some("hello could not be sent".into()),
                },
                writer,
            )
            .await;
        }

        let writer = Writer::spawn(self.tx);
        let sleep = tokio::time::sleep(self.handshake_timeout);
        tokio::pin!(sleep);

        // self.tx has been moved into the writer, so the rest of this loop
        // only ever borrows individual fields (self.shared, self.rx,
        // self.commands) rather than `self` as a whole.
        let reason = loop {
            let handshaking = lock(&self.shared.inner).state == SessionState::Handshaking;
            tokio::select! {
                biased;
                inbound = self.rx.recv() => match inbound {
                    Some(Inbound::Frame(frame)) => {
                        if let Some(reason) = on_frame(&self.shared, frame) {
                            break reason;
                        }
                    }
                    Some(Inbound::Closed(closure)) => break Teardown::Port(closure),
                    None => break Teardown::Port(PortClosure::Closed { code: None, reason: None }),
                },
                Some(command) = self.commands.recv() => {
                    if let Some(reason) = on_command(command) {
                        break reason;
                    }
                }
                () = &mut sleep, if handshaking => {
                    break on_handshake_timeout(&self.shared, self.handshake_timeout);
                }
            }
        };

        teardown::teardown(self.shared, reason, writer).await
    }

    fn build_hello(&self) -> Hello {
        Hello {
            protocol: self.shared.local_protocol,
            peer: self.shared.local_peer.clone(),
            capabilities: self.shared.local_capabilities.clone(),
            limits: (self.shared.local_max_frame_bytes < DEFAULT_MAX_FRAME_BYTES).then(|| Limits {
                max_frame_bytes: Some(self.shared.local_max_frame_bytes as u64),
            }),
        }
    }
}

/// Routes one inbound frame. `Some` breaks the main loop with that reason.
///
/// A free function, rather than a method, so it only ever borrows `shared`:
/// by the time the main loop runs, `SessionDriver::tx` has already moved into
/// the `Writer` task, and a `&self`-taking method would need every field,
/// `tx` included, to still be there.
fn on_frame(shared: &Shared, frame: Frame) -> Option<Teardown> {
    match frame {
        Frame::Hello(hello) => on_hello(shared, hello),
        Frame::Close(close) => Some(Teardown::PeerFrame {
            code: close.code,
            reason: close.reason,
        }),
        // Req/Res/Err/Evt/Cancel/Ping/Pong: handled starting with the commits
        // that add request dispatch and event/liveness support.
        _ => None,
    }
}

fn on_hello(shared: &Shared, hello: Hello) -> Option<Teardown> {
    if lock(&shared.inner).state != SessionState::Handshaking {
        return Some(Teardown::Local {
            code: close_codes::PROTOCOL_ERROR,
            reason: Some("duplicate hello".into()),
        });
    }
    match negotiate(shared.local_protocol, hello.protocol) {
        Negotiation::Mismatch { close_code } => {
            shared.fail_ready(
                RemoteError::new(
                    codes::PROTOCOL_MISMATCH,
                    format!(
                        "Peer \"{}\" speaks wire major {}; this session speaks major {}.",
                        hello.peer.name, hello.protocol.major, shared.local_protocol.major
                    ),
                )
                .with_detail("local_major", shared.local_protocol.major)
                .with_detail("remote_major", hello.protocol.major)
                .with_detail("close_code", close_code),
            );
            Some(Teardown::Local {
                code: close_code,
                reason: Some("protocol version unsupported".into()),
            })
        }
        Negotiation::Compatible { effective_minor } => {
            let remote = RemotePeer {
                peer: hello.peer,
                protocol: hello.protocol,
                capabilities: hello.capabilities,
                limits: hello.limits,
                effective_minor,
            };
            {
                let mut guard = lock(&shared.inner);
                guard.state = SessionState::Ready;
                guard.remote = Some(remote.clone());
            }
            shared.succeed_ready(remote);
            None
        }
    }
}

fn on_command(command: Command) -> Option<Teardown> {
    match command {
        Command::Close { code, reason } => Some(Teardown::Local { code, reason }),
    }
}

fn on_handshake_timeout(shared: &Shared, handshake_timeout: Duration) -> Teardown {
    let millis = u64::try_from(handshake_timeout.as_millis()).unwrap_or(u64::MAX);
    shared.fail_ready(
        RemoteError::new(
            codes::UNAVAILABLE,
            format!("The peer did not send hello within {millis}ms."),
        )
        .with_detail("timeout_ms", millis),
    );
    Teardown::Local {
        code: close_codes::PROTOCOL_ERROR,
        reason: Some("handshake timeout".into()),
    }
}
