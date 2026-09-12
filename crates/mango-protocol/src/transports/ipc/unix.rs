//! The POSIX half of the local socket transport: a Unix domain socket the
//! listener publishes owner-only, and the peer credentials the socket itself
//! carries.

use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};

use crate::close::close_codes;
use crate::transports::deadline::{ConnectDeadline, ConnectError, connect_within};
use crate::transports::ndjson::{NdjsonPort, PortCloser};

use super::{PeerIdentity, PeerUser};

/// Owner-only, the permission local-socket.md requires of a POSIX socket file.
const SOCKET_MODE: u32 = 0o600;

/// The port a dialled connection produces.
pub type IpcPort = NdjsonPort<OwnedReadHalf, OwnedWriteHalf>;

/// The port an accepted connection produces. The same type on POSIX, where
/// both ends of a Unix socket are the same kind of object.
pub type IpcServerPort = IpcPort;

/// A socket file in the user's runtime directory.
pub(super) fn address_for(name: &str) -> PathBuf {
    let directory = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map_or_else(std::env::temp_dir, PathBuf::from);
    directory.join(format!("{name}.sock"))
}

/// A listener on a local socket, and the address it actually bound.
///
/// # Example
///
/// ```no_run
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// use mango_protocol::transports::ipc::listen_ipc;
///
/// let mut listener = listen_ipc("/run/user/1000/mango-hub.sock").await?;
/// let (_port, _identity) = listener.accept().await?;
/// listener.close().await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct IpcListener {
    listener: UnixListener,
    path: PathBuf,
    max_frame_bytes: Option<usize>,
    /// Weak handles to the ports handed out, so shutting down can tell the
    /// sessions still using them. Holding one never keeps a connection alive.
    accepted: Vec<PortCloser<OwnedWriteHalf>>,
}

impl IpcListener {
    /// The address this listener bound.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use mango_protocol::transports::ipc::listen_ipc;
    ///
    /// let listener = listen_ipc("/run/user/1000/mango-hub.sock").await?;
    /// assert!(listener.path().ends_with("mango-hub.sock"));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Sets the frame limit every port this listener produces enforces.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> std::io::Result<()> {
    /// use mango_protocol::transports::ipc::listen_ipc;
    ///
    /// let listener = listen_ipc("/run/user/1000/mango-hub.sock")
    ///     .await?
    ///     .with_max_frame_bytes(1 << 20);
    /// # let _ = listener;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = Some(max_frame_bytes);
        self
    }

    /// Waits for the next connection and hands back its port and the identity
    /// the socket carries.
    ///
    /// # Errors
    ///
    /// Whatever `accept` failed with. Reading the peer's credentials is not
    /// one of those: a platform that will not answer leaves the identity
    /// empty rather than refusing a connection that is otherwise fine.
    pub async fn accept(&mut self) -> io::Result<(IpcServerPort, PeerIdentity)> {
        let (stream, _address) = self.listener.accept().await?;
        let identity = identity_of(&stream);
        let port = self.port(stream);
        // Connections that have since ended are forgotten here rather than
        // accumulating for the life of a long-running listener.
        self.accepted.retain(PortCloser::is_open);
        self.accepted.push(port.closer());
        Ok((port, identity))
    }

    /// Tells every session still open on this listener why, stops accepting,
    /// and removes the socket file.
    ///
    /// local-socket.md: "A listener shutting down sends `close` `4000` to every
    /// session first." The ports moved to whoever called
    /// [`IpcListener::accept`], so this writes the farewell through the handle
    /// kept for each of them; a peer then reads an announced release rather
    /// than inferring one from a socket that vanished.
    pub async fn close(self) {
        for accepted in &self.accepted {
            accepted
                .close(close_codes::RELEASED, Some("listener closing"))
                .await;
        }
        drop(self.listener);
        // Best effort: an address already gone, or replaced by a newer
        // listener that bound after this one stopped, is not this one's to
        // report on.
        let _ = tokio::fs::remove_file(&self.path).await;
    }

    fn port(&self, stream: UnixStream) -> IpcServerPort {
        socket_port(stream, self.max_frame_bytes)
    }
}

/// Listens on a local socket, owner-only from the instant the address exists.
///
/// The socket is bound at a temporary name beside `path`, restricted to
/// `0600` while only this process knows about it, and then published at
/// `path`. `bind` takes its mode from the umask, so binding straight onto the
/// address would publish a world-connectable socket for as long as a `chmod`
/// takes; the TypeScript SDK closes that window by setting the process-wide
/// umask, which a library cannot do without reaching into every other thread.
/// Publishing afterwards is local to this call and costs no such thing.
///
/// A stale socket file left by a previous process is removed first. Anything
/// else at the address is refused rather than replaced: a regular file there
/// is a mistake the caller has to see.
///
/// # Errors
///
/// Whatever binding, restricting or publishing the address failed with, and
/// [`io::ErrorKind::AlreadyExists`] when something that is not a socket
/// already holds the address, or when another listener took it while this one
/// was binding.
pub async fn listen_ipc(path: impl AsRef<Path>) -> io::Result<IpcListener> {
    let path = path.as_ref().to_path_buf();
    remove_stale_socket(&path).await?;

    let staging = staging_path(&path);
    // A crash between these two steps leaves the staging name behind, where
    // the next bind's own staging path (this process's id) will not collide
    // with it and the stale-socket removal never looks. Cheap to clean up by
    // hand, and never mistaken for the published address.
    let _ = tokio::fs::remove_file(&staging).await;
    let listener = UnixListener::bind(&staging)?;
    if let Err(error) = restrict(&staging).await {
        let _ = tokio::fs::remove_file(&staging).await;
        return Err(error);
    }
    if let Err(error) = publish(&staging, &path).await {
        let _ = tokio::fs::remove_file(&staging).await;
        return Err(error);
    }

    Ok(IpcListener {
        listener,
        path,
        max_frame_bytes: None,
        accepted: Vec::new(),
    })
}

/// Connects to a local socket and returns the port for that connection.
///
/// # Errors
///
/// [`ConnectError::Io`] with the operating system's own error when the path
/// has no listener, and [`ConnectError::TimedOut`]/[`ConnectError::Cancelled`]
/// when `deadline` abandoned an attempt nobody completed.
pub async fn connect_ipc(
    path: impl AsRef<Path>,
    deadline: &ConnectDeadline,
) -> Result<IpcPort, ConnectError> {
    let path = path.as_ref();
    let target = path.display().to_string();
    let stream = connect_within(&target, deadline, async {
        Ok(UnixStream::connect(path).await?)
    })
    .await?;
    Ok(socket_port(stream, None))
}

/// One connection, one port: the socket is both the byte source and the sink.
fn socket_port(stream: UnixStream, max_frame_bytes: Option<usize>) -> IpcPort {
    let (reader, writer) = stream.into_split();
    let port = NdjsonPort::new(reader, writer);
    match max_frame_bytes {
        Some(limit) => port.with_max_frame_bytes(limit),
        None => port,
    }
}

/// The credentials the socket carries for the peer that opened it.
fn identity_of(stream: &UnixStream) -> PeerIdentity {
    let Ok(credentials) = stream.peer_cred() else {
        return PeerIdentity::default();
    };
    PeerIdentity {
        process_id: credentials.pid().and_then(|pid| u32::try_from(pid).ok()),
        user: Some(PeerUser {
            uid: credentials.uid(),
            gid: credentials.gid(),
        }),
    }
}

/// Where a listener binds before it publishes: in the address's own directory,
/// so the link onto it stays within one filesystem, and unique per attempt, so
/// two listeners racing for the same address do not stage over each other.
///
/// A short name of its own rather than the address plus a suffix, because this
/// is the path `bind` actually sees and `sun_path` is 104 bytes on macOS. An
/// address close to that limit would otherwise fail to bind at a staging name
/// longer than itself.
fn staging_path(path: &Path) -> PathBuf {
    static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(0);
    let attempt = NEXT_ATTEMPT.fetch_add(1, Ordering::Relaxed);
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    directory.join(format!(".mango-{}-{attempt}", std::process::id()))
}

/// Moves the bound socket onto the address it is published at.
///
/// A hard link, not a rename, because it is the one of the two that refuses
/// rather than replaces: a second listener that bound the same address while
/// this one was setting up gets [`io::ErrorKind::AlreadyExists`], which is the
/// `EADDRINUSE` a plain `bind` onto the address would have produced. A rename
/// would silently take the address over and leave that listener bound to an
/// inode no client can reach.
///
/// A filesystem that will not hard-link a socket falls back to the rename,
/// which is still atomic and still owner-only — it only loses the refusal.
async fn publish(staging: &Path, path: &Path) -> io::Result<()> {
    match tokio::fs::hard_link(staging, path).await {
        Ok(()) => {
            // The link is the address now; the staging name has done its job.
            tokio::fs::remove_file(staging).await
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} was taken by another listener while this one was binding; \
                 expected the address to be free",
                path.display()
            ),
        )),
        Err(_) => tokio::fs::rename(staging, path).await,
    }
}

/// Makes the socket file readable and writable by its owner alone.
async fn restrict(path: &Path) -> io::Result<()> {
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(SOCKET_MODE)).await
}

/// Removes the socket file a previous process left behind. Only a socket is
/// removed: anything else at the address is a mistake to report, not
/// something to delete.
async fn remove_stale_socket(path: &Path) -> io::Result<()> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        // Nothing at the path, which is the ordinary case.
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} is {:?}; expected nothing, or a socket file a previous listener left behind",
                path.display(),
                metadata.file_type()
            ),
        ));
    }
    tokio::fs::remove_file(path).await
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectDeadline, IpcListener, SOCKET_MODE, connect_ipc, listen_ipc, publish, staging_path,
    };
    use crate::close::close_codes;
    use crate::frame::Frame;
    use crate::port::{Inbound, Port, PortRx, PortTx};
    use crate::transports::deadline::ConnectError;
    use std::io;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio_util::sync::CancellationToken;

    static NEXT_ADDRESS: AtomicU64 = AtomicU64::new(0);

    /// A socket path in a directory that goes away with the test. The crate
    /// carries no development dependency for one, and this is the only module
    /// that needs a scratch directory.
    struct Address(PathBuf);

    impl Address {
        fn new() -> Self {
            let unique = NEXT_ADDRESS.fetch_add(1, Ordering::Relaxed);
            let directory =
                std::env::temp_dir().join(format!("mango-ipc-{}-{unique}", std::process::id()));
            std::fs::create_dir_all(&directory).expect("a scratch directory");
            Self(directory)
        }

        fn path(&self) -> PathBuf {
            self.0.join("mango.sock")
        }
    }

    impl Drop for Address {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn listening(address: &Address) -> IpcListener {
        listen_ipc(address.path())
            .await
            .expect("the address is free")
    }

    #[tokio::test]
    async fn the_published_socket_is_owner_only() {
        let address = Address::new();
        let listener = listening(&address).await;

        let mode = std::fs::metadata(listener.path())
            .expect("the socket exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, SOCKET_MODE, "expected {SOCKET_MODE:o}, got {mode:o}");
        listener.close().await;
    }

    #[tokio::test]
    async fn nothing_is_left_beside_the_published_address() {
        let address = Address::new();
        let listener = listening(&address).await;

        // Every name in the directory, rather than a recomputed staging path:
        // the counter moves on every call, so asking `staging_path` again
        // would check a name this listener never used, and the assertion would
        // hold however much was left behind.
        let left: Vec<_> = std::fs::read_dir(&address.0)
            .expect("the scratch directory is readable")
            .map(|entry| entry.expect("an entry").file_name())
            .collect();
        assert_eq!(
            left,
            vec![std::ffi::OsString::from("mango.sock")],
            "the socket is published and the staging name is gone"
        );
        listener.close().await;
    }

    #[test]
    fn a_staging_name_is_short_enough_to_bind_beside_a_long_address() {
        // `sun_path` is 104 bytes on macOS, so the name this binds at must not
        // grow with the address it will be published under.
        let long = format!("/tmp/{}.sock", "a".repeat(80));
        let staging = staging_path(Path::new(&long));
        assert!(
            staging.as_os_str().len() < long.len(),
            "{} is not shorter than {long}",
            staging.display()
        );
        assert_eq!(staging.parent(), Path::new(&long).parent());
    }

    #[tokio::test]
    async fn an_accepted_connection_carries_frames_and_names_its_peer() {
        let address = Address::new();
        let mut listener = listening(&address).await;

        let dial = tokio::spawn({
            let path = address.path();
            async move { connect_ipc(path, &ConnectDeadline::default()).await }
        });
        let (accepted, identity) = listener.accept().await.expect("a connection arrives");
        let dialled = dial
            .await
            .expect("the dial task runs")
            .expect("it connects");

        // The test dials itself, so the peer the socket reports is this very
        // process — the check an application makes before it serves anything.
        assert!(
            identity.user.is_some(),
            "a POSIX socket carries the peer's credentials"
        );
        if let Some(pid) = identity.process_id {
            assert_eq!(pid, std::process::id());
        }

        let (mut dialled_tx, _dialled_rx) = dialled.split();
        let (_accepted_tx, mut accepted_rx) = accepted.split();
        dialled_tx.send(Frame::Ping).await;
        assert_eq!(accepted_rx.recv().await, Some(Inbound::Frame(Frame::Ping)));
        listener.close().await;
    }

    #[tokio::test]
    async fn closing_a_listener_tells_the_sessions_it_accepted() {
        let address = Address::new();
        let mut listener = listening(&address).await;
        let dial = tokio::spawn({
            let path = address.path();
            async move { connect_ipc(path, &ConnectDeadline::default()).await }
        });
        let (_accepted, _identity) = listener.accept().await.expect("a connection arrives");
        let dialled = dial
            .await
            .expect("the dial task runs")
            .expect("it connects");
        let (_dialled_tx, mut dialled_rx) = dialled.split();

        listener.close().await;

        // An announced release, not one inferred from a socket that vanished.
        assert_eq!(
            dialled_rx.recv().await,
            Some(Inbound::Frame(Frame::Close(crate::frame::Close {
                code: close_codes::RELEASED,
                reason: Some("listener closing".into()),
            })))
        );
    }

    #[tokio::test]
    async fn closing_a_listener_takes_its_address_with_it() {
        let address = Address::new();
        let listener = listening(&address).await;
        assert!(address.path().exists());

        listener.close().await;

        assert!(
            !address.path().exists(),
            "a closed listener leaves no address behind"
        );
    }

    #[tokio::test]
    async fn publishing_onto_an_address_that_appeared_meanwhile_is_refused() {
        // The window this closes is between the stale-socket check and the
        // publish, which no test can open on purpose; what can be checked is
        // that the step itself refuses rather than replaces. A rename would
        // have taken the address over and left its listener on an inode no
        // client can reach.
        let address = Address::new();
        let staging = address.0.join("staging.sock");
        let taken = address.path();
        std::fs::write(&staging, b"the socket this listener bound").expect("a staging file");
        std::fs::write(&taken, b"what another listener published").expect("an occupied address");

        let error = publish(&staging, &taken)
            .await
            .expect_err("the address is no longer free");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            error.to_string().contains("while this one was binding"),
            "{error}"
        );
        assert_eq!(
            std::fs::read(&taken).expect("the address is untouched"),
            b"what another listener published"
        );
    }

    #[tokio::test]
    async fn a_stale_socket_file_is_replaced() {
        let address = Address::new();
        // A listener that vanished without closing leaves its address behind.
        drop(listening(&address).await);

        let second = listen_ipc(address.path())
            .await
            .expect("a stale socket is not an occupied address");
        second.close().await;
    }

    #[tokio::test]
    async fn a_regular_file_at_the_address_is_reported_not_replaced() {
        let address = Address::new();
        std::fs::write(address.path(), b"not a socket").expect("the file is written");

        let error = listen_ipc(address.path())
            .await
            .expect_err("a regular file is not a stale socket");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            error.to_string().contains("expected nothing, or a socket"),
            "{error}"
        );
        assert!(
            Path::new(&address.path()).exists(),
            "the caller's own file is still there"
        );
    }

    #[tokio::test]
    async fn dialling_an_address_with_no_listener_reports_the_system_error() {
        let address = Address::new();
        let error = connect_ipc(address.path(), &ConnectDeadline::default())
            .await
            .expect_err("nothing is listening");

        match error {
            ConnectError::Io(error) => assert_eq!(error.kind(), io::ErrorKind::NotFound),
            other => panic!("expected an operating system error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_dial_the_caller_already_gave_up_on_opens_nothing() {
        let address = Address::new();
        let listener = listening(&address).await;
        let token = CancellationToken::new();
        token.cancel();

        let error = connect_ipc(
            address.path(),
            &ConnectDeadline::default().with_cancel(token),
        )
        .await
        .expect_err("a cancelled dial is abandoned");

        assert!(matches!(error, ConnectError::Cancelled { .. }));
        listener.close().await;
    }
}
