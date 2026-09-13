//! Accepting a WebSocket peer: the two things websocket.md puts at the
//! upgrade rather than in a frame — the `mango.v1` subprotocol, and the
//! credential.
//!
//! [`accept_websocket`] is for a peer with no HTTP stack of its own. One that
//! has a server already does the upgrade there and calls
//! [`websocket_port`] with the stream it produced; this
//! crate never depends on an HTTP framework.

use std::fmt;
use std::sync::{Arc, Mutex};

use futures_util::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, header};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async_with_config};

use crate::close::close_codes;

use super::{WEBSOCKET_SUBPROTOCOL, WebSocketOptions, WebSocketPort, websocket_port};

/// Why an upgrade did not become a session.
///
/// Every variant but [`AcceptError::Handshake`] happens *after* the upgrade
/// completed, because a refused upgrade reaches the dialler as a socket that
/// failed to open, with no code to read (websocket.md, Authentication).
///
/// # Example
///
/// ```
/// use mango_protocol::transports::websocket::server::AcceptError;
///
/// let refused = AcceptError::Unauthorized { code: 4401 };
/// assert!(refused.to_string().contains("4401"));
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum AcceptError {
    /// The upgrade itself failed: not an HTTP request, or the socket went
    /// away mid-handshake.
    Handshake(String),
    /// The dialler never offered `mango.v1`, so it is not a Mango Protocol
    /// session. The socket was closed with `4400`.
    Subprotocol,
    /// The credential was refused. The socket was closed with this code —
    /// `4401` unknown, malformed or revoked, `4403` known but disabled,
    /// `4429` rate limited — and no `hello` was sent.
    Unauthorized {
        /// The close code the dialler can read.
        code: u16,
    },
}

impl fmt::Display for AcceptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Handshake(detail) => write!(formatter, "the upgrade failed: {detail}"),
            Self::Subprotocol => write!(
                formatter,
                "the dialler did not offer the {WEBSOCKET_SUBPROTOCOL:?} subprotocol; the socket was closed with {}",
                close_codes::PROTOCOL_ERROR
            ),
            Self::Unauthorized { code } => write!(
                formatter,
                "the credential was refused; the socket was closed with {code} and no hello was sent"
            ),
        }
    }
}

impl std::error::Error for AcceptError {}

/// Completes the upgrade on `io` and returns the port, having selected the
/// subprotocol and put the credential to `authorize`.
///
/// `authorize` is handed the bearer token from `Authorization: Bearer
/// <token>`, or `None` when the dialler sent no credential, and answers with
/// the close code to refuse it with. It runs **before** any `hello`, which is
/// the guarantee websocket.md asks for: a peer whose credential is no good
/// never sees this side's identity.
///
/// The subprotocol is echoed only when it was offered. A dialler that offered
/// nothing is closed with `4400` rather than left on a socket neither side
/// agrees about.
///
/// # Example
///
/// ```no_run
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use mango_protocol::close::close_codes;
/// use mango_protocol::frame::PeerInfo;
/// use mango_protocol::session::{Session, SessionOptions};
/// use mango_protocol::transports::websocket::WebSocketOptions;
/// use mango_protocol::transports::websocket::server::accept_websocket;
/// use tokio::net::TcpListener;
///
/// let listener = TcpListener::bind("127.0.0.1:0").await?;
/// let (socket, _address) = listener.accept().await?;
///
/// let port = accept_websocket(socket, WebSocketOptions::default(), |token| {
///     match token {
///         Some("s3cret") => Ok(()),
///         _ => Err(close_codes::UNAUTHORIZED),
///     }
/// })
/// .await?;
///
/// let peer = PeerInfo { name: "hub".into(), version: "1".into(), role: "hub".into() };
/// let (_session, _driver) = Session::spawn(port, SessionOptions::new(peer));
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// [`AcceptError`] for an upgrade that failed, a dialler that did not offer
/// the subprotocol, or a credential `authorize` refused.
#[allow(
    clippy::result_large_err,
    reason = "the handshake callback's error type is tungstenite's own ErrorResponse"
)]
pub async fn accept_websocket<S, F>(
    io: S,
    options: WebSocketOptions,
    authorize: F,
) -> Result<WebSocketPort<S>, AcceptError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(Option<&str>) -> Result<(), u16>,
{
    let observed = Arc::new(Mutex::new(Upgrade::default()));
    let recorder = Arc::clone(&observed);
    let stream = accept_hdr_async_with_config(
        io,
        move |request: &Request, mut response: Response| {
            let upgrade = Upgrade::read(request);
            // The subprotocol is echoed only when it was offered: selecting
            // one the dialler never asked for is a handshake it may refuse.
            if upgrade.offered_subprotocol {
                response.headers_mut().insert(
                    header::SEC_WEBSOCKET_PROTOCOL,
                    HeaderValue::from_static(WEBSOCKET_SUBPROTOCOL),
                );
            }
            if let Ok(mut recorder) = recorder.lock() {
                *recorder = upgrade;
            }
            Ok(response)
        },
        Some(options.socket_config()),
    )
    .await
    .map_err(|error| AcceptError::Handshake(error.to_string()))?;

    let upgrade = observed
        .lock()
        .map(|upgrade| upgrade.clone())
        .unwrap_or_default();

    if !upgrade.offered_subprotocol {
        close_with(
            stream,
            close_codes::PROTOCOL_ERROR,
            "subprotocol not offered",
        )
        .await;
        return Err(AcceptError::Subprotocol);
    }
    if let Err(code) = authorize(upgrade.bearer.as_deref()) {
        close_with(stream, code, "credential refused").await;
        return Err(AcceptError::Unauthorized { code });
    }
    Ok(websocket_port(stream, options))
}

/// What the upgrade request said, read once inside the handshake callback.
#[derive(Debug, Clone, Default)]
struct Upgrade {
    offered_subprotocol: bool,
    bearer: Option<String>,
}

impl Upgrade {
    fn read(request: &Request) -> Self {
        let headers = request.headers();
        let offered_subprotocol = headers
            .get_all(header::SEC_WEBSOCKET_PROTOCOL)
            .iter()
            .filter_map(|value| value.to_str().ok())
            // One header may list several, comma separated (RFC 6455).
            .flat_map(|value| value.split(','))
            .any(|offered| offered.trim() == WEBSOCKET_SUBPROTOCOL);
        let bearer = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_token)
            .map(ToOwned::to_owned);
        Self {
            offered_subprotocol,
            bearer,
        }
    }
}

/// The token out of an `Authorization` header, when the scheme is `Bearer`.
///
/// # Example
///
/// ```
/// use mango_protocol::transports::websocket::server::bearer_token;
///
/// assert_eq!(bearer_token("Bearer s3cret"), Some("s3cret"));
/// assert_eq!(bearer_token("bearer s3cret"), Some("s3cret"));
/// assert_eq!(bearer_token("Basic abc"), None);
/// ```
#[must_use]
pub fn bearer_token(authorization: &str) -> Option<&str> {
    let (scheme, token) = authorization.split_once(' ')?;
    // RFC 7235 makes the scheme case-insensitive.
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

/// Closes a socket the upgrade produced but the session will not use, with a
/// code the dialler can read.
async fn close_with<S>(mut stream: WebSocketStream<S>, code: u16, reason: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = CloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    };
    let _ = stream.send(Message::Close(Some(frame))).await;
    let _ = stream.close(None).await;
}

#[cfg(test)]
mod tests {
    use super::{AcceptError, Upgrade, bearer_token};
    use crate::transports::websocket::WEBSOCKET_SUBPROTOCOL;
    use tokio_tungstenite::tungstenite::handshake::server::Request;
    use tokio_tungstenite::tungstenite::http::header;

    fn request(headers: &[(&str, &str)]) -> Request {
        let mut builder = Request::builder().uri("/runtime");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(()).expect("a request")
    }

    #[test]
    fn the_subprotocol_is_seen_whether_it_is_alone_or_in_a_list() {
        for offered in [
            WEBSOCKET_SUBPROTOCOL,
            "chat, mango.v1",
            "mango.v1, chat",
            "  mango.v1  ",
        ] {
            let upgrade = Upgrade::read(&request(&[(
                header::SEC_WEBSOCKET_PROTOCOL.as_str(),
                offered,
            )]));
            assert!(upgrade.offered_subprotocol, "{offered:?}");
        }
    }

    #[test]
    fn a_dialler_that_offered_something_else_did_not_offer_this() {
        let upgrade = Upgrade::read(&request(&[(
            header::SEC_WEBSOCKET_PROTOCOL.as_str(),
            "mango.v2",
        )]));
        assert!(!upgrade.offered_subprotocol);
        assert!(!Upgrade::read(&request(&[])).offered_subprotocol);
    }

    #[test]
    fn the_bearer_token_is_read_out_of_the_authorization_header() {
        let upgrade = Upgrade::read(&request(&[(
            header::AUTHORIZATION.as_str(),
            "Bearer s3cret",
        )]));
        assert_eq!(upgrade.bearer.as_deref(), Some("s3cret"));
        assert_eq!(bearer_token("Bearer  padded  "), Some("padded"));
        assert_eq!(bearer_token("Bearer"), None);
        assert_eq!(bearer_token("Bearer "), None);
        assert_eq!(bearer_token("Basic abc"), None);
    }

    #[test]
    fn a_refusal_names_the_code_the_dialler_can_read() {
        assert!(
            AcceptError::Unauthorized { code: 4403 }
                .to_string()
                .contains("4403")
        );
        assert!(
            AcceptError::Subprotocol
                .to_string()
                .contains(WEBSOCKET_SUBPROTOCOL)
        );
    }
}
