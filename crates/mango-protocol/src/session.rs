//! A tokio session over any [`crate::port::Port`]: the request/response
//! multiplexing, cancel, event streams, liveness and close semantics of
//! `packages/protocol/src/session.ts`, without a transport of its own.
//!
//! Build one with [`Session::open`] (you drive the returned [`SessionDriver`])
//! or [`Session::spawn`] (the driver is spawned for you). Either way, the
//! handshake and everything after it only happens while that driver future is
//! being polled — a [`Session`] handle alone is inert.

use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::codec::ndjson::DEFAULT_MAX_FRAME_BYTES;
use crate::port::Port;

mod command;
mod driver;
mod handle;
mod options;
mod shared;
mod teardown;

pub use driver::SessionDriver;
pub use handle::{RemotePeer, Session, SessionState};
pub use options::SessionOptions;
pub use teardown::SessionClosure;

use shared::{Inner, Shared};

impl Session {
    /// Opens a session over `port`, returning the handle and its not-yet-run
    /// driver. Nothing progresses — not even the handshake — until the driver
    /// is polled; [`Session::spawn`] is the default that avoids that footgun.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::frame::PeerInfo;
    /// use mango_protocol::port::port_pair;
    /// use mango_protocol::session::{Session, SessionOptions, SessionState};
    ///
    /// let (a, _b) = port_pair();
    /// let peer = PeerInfo { name: "example".into(), version: "0.1.0".into(), role: "runtime".into() };
    /// let (session, _driver) = Session::open(a, SessionOptions::new(peer));
    /// assert_eq!(session.state(), SessionState::Handshaking);
    /// ```
    #[must_use]
    pub fn open<P: Port>(
        port: P,
        options: SessionOptions,
    ) -> (Session, SessionDriver<P::Tx, P::Rx>) {
        let port_max_frame_bytes = port.max_frame_bytes();
        let (tx, rx) = port.split();
        let local_max_frame_bytes = options
            .max_frame_bytes
            .or(port_max_frame_bytes)
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES);
        let (ready, _) = watch::channel(None);
        let (closure, _) = watch::channel(None);
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            local_peer: options.peer,
            local_protocol: options.protocol,
            local_capabilities: options.capabilities,
            local_max_frame_bytes,
            inner: Mutex::new(Inner {
                state: SessionState::Handshaking,
                remote: None,
            }),
            ready,
            closure,
            commands: commands_tx,
        });
        let session = Session {
            shared: Arc::clone(&shared),
        };
        let driver = SessionDriver {
            shared,
            tx,
            rx,
            commands: commands_rx,
            handshake_timeout: options.handshake_timeout,
        };
        (session, driver)
    }

    /// [`Session::open`] plus `tokio::spawn(driver.run())`.
    ///
    /// # Example
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// use mango_protocol::frame::PeerInfo;
    /// use mango_protocol::port::port_pair;
    /// use mango_protocol::session::{Session, SessionOptions};
    ///
    /// let (a, b) = port_pair();
    /// let peer = |role: &str| PeerInfo {
    ///     name: "example".into(),
    ///     version: "0.1.0".into(),
    ///     role: role.into(),
    /// };
    /// let (session_a, _driver_a) = Session::spawn(a, SessionOptions::new(peer("a")));
    /// let (session_b, _driver_b) = Session::spawn(b, SessionOptions::new(peer("b")));
    /// let remote = session_a.ready().await.expect("handshake succeeds");
    /// assert_eq!(remote.peer.role, "b");
    /// # }
    /// ```
    #[must_use]
    pub fn spawn<P: Port>(
        port: P,
        options: SessionOptions,
    ) -> (Session, JoinHandle<SessionClosure>) {
        let (session, driver) = Self::open(port, options);
        let handle = tokio::spawn(driver.run());
        (session, handle)
    }
}
