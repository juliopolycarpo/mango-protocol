//! State a `Session` handle and its `SessionDriver` share.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::{mpsc, watch};

use crate::codec::ndjson::DEFAULT_MAX_FRAME_BYTES;
use crate::error::RemoteError;
use crate::frame::PeerInfo;
use crate::version::ProtocolVersion;

use super::command::Command;
use super::handle::{RemotePeer, SessionState};
use super::handler::Handler;
use super::teardown::SessionClosure;

/// Locks a mutex, recovering the guard even if a prior holder panicked.
///
/// Every critical section behind this crate's `std::sync::Mutex`es is a
/// short, panic-free field update, so poisoning never reflects a corrupted
/// invariant here; treating it as recoverable avoids a second panic on top of
/// whatever caused the first one.
pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Driver-mutated fields a handle needs to read synchronously, behind one
/// lock so a reader never observes a half-updated combination.
pub(super) struct Inner {
    pub(super) state: SessionState,
    pub(super) remote: Option<RemotePeer>,
}

/// A method's handler, and the generation it was registered under (so a
/// `HandlerGuard` can tell whether it is still the one in force before
/// unregistering it on drop).
type HandlerRegistry = HashMap<String, (u64, Arc<dyn Handler>)>;

/// The state a [`super::handle::Session`] handle and its `SessionDriver` share.
pub(super) struct Shared {
    pub(super) local_peer: PeerInfo,
    pub(super) local_protocol: ProtocolVersion,
    pub(super) local_capabilities: Map<String, Value>,
    pub(super) local_max_frame_bytes: usize,
    pub(super) inner: Mutex<Inner>,
    pub(super) ready: watch::Sender<Option<Result<RemotePeer, RemoteError>>>,
    pub(super) closure: watch::Sender<Option<SessionClosure>>,
    pub(super) commands: mpsc::UnboundedSender<Command>,
    pub(super) request_id_prefix: String,
    pub(super) request_sequence: AtomicU64,
    /// How many inbound requests this session is answering right now.
    pub(super) in_flight: AtomicUsize,
    pub(super) handlers: Mutex<HandlerRegistry>,
    pub(super) next_generation: AtomicU64,
    /// How long teardown waits for in-flight handlers to settle.
    pub(super) handler_grace: Duration,
}

impl Shared {
    /// The frame ceiling this side may send: the lower of both announced
    /// limits.
    pub(super) fn send_limit_bytes(&self) -> usize {
        let remote_limit = lock(&self.inner)
            .remote
            .as_ref()
            .and_then(|remote| remote.limits.as_ref())
            .and_then(|limits| limits.max_frame_bytes)
            .map_or(DEFAULT_MAX_FRAME_BYTES, |bytes| {
                usize::try_from(bytes).unwrap_or(usize::MAX)
            });
        self.local_max_frame_bytes.min(remote_limit)
    }

    /// Settles the ready watch with `Ok(remote)`, unless something already
    /// settled it (never overwrites an earlier outcome).
    pub(super) fn succeed_ready(&self, remote: RemotePeer) {
        let _ = self.ready.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(Ok(remote));
            true
        });
    }

    /// Settles the ready watch with `Err(error)`, unless something already
    /// settled it. A specific failure (a version mismatch, a handshake
    /// timeout) calls this directly; teardown's own generic failure is a
    /// no-op whenever a specific one already ran.
    pub(super) fn fail_ready(&self, error: RemoteError) {
        let _ = self.ready.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(Err(error));
            true
        });
    }

    /// The next outbound request id: `"{prefix}-{sequence}"`, 1-based.
    pub(super) fn next_request_id(&self) -> String {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}-{sequence}", self.request_id_prefix)
    }

    /// Registers `handler` under `method`, replacing whatever was there.
    pub(super) fn register_handler(
        &self,
        method: String,
        handler: Arc<dyn Handler>,
    ) -> (String, u64) {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        lock(&self.handlers).insert(method.clone(), (generation, handler));
        (method, generation)
    }
}
