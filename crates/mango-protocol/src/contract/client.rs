//! A typed request surface over one session.

use serde::Serialize;
use serde::de::DeserializeOwned;

use serde_json::json;

use crate::catalog::Catalog;
use crate::error::{RemoteError, codes};
use crate::session::{RequestOptions, Session};
use crate::validate::RPC_DISCOVER;

/// A typed request surface over one [`Session`], from [`super::Contract::client`].
///
/// Neither `request` nor `request_with` checks `method` against the
/// contract locally before sending — the peer's own dispatch is the check,
/// exactly as the TypeScript SDK's `ContractClient` relies purely on a
/// compile-time method-name type, which has no runtime equivalent here.
pub struct ContractClient<'a> {
    session: &'a Session,
}

impl<'a> ContractClient<'a> {
    pub(super) fn new(session: &'a Session) -> Self {
        Self { session }
    }

    /// Sends `params`, serialised to the wire, and decodes the peer's result
    /// as `R`. Equivalent to `request_with` with the defaults.
    ///
    /// # Errors
    /// Whatever [`Session::request`] returns, plus `INTERNAL` if `params`
    /// fails to serialise or the peer's result fails to decode as `R`.
    pub async fn request<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R, RemoteError> {
        self.request_with(method, params, RequestOptions::default())
            .await
    }

    /// Asks the peer for the contract it serves (`rpc.discover`, §6.4) and
    /// decodes the answer as a [`Catalog`].
    ///
    /// The catalog is the peer's, not this side's, so a caller that intends
    /// to act on it compiles it with [`super::Contract::from_catalog`], which
    /// runs the same checks a contract built here passes.
    ///
    /// # Errors
    /// `INVALID_REQUEST` against a peer below wire minor 1,
    /// `METHOD_UNSUPPORTED` when the peer serves no contract, and `INTERNAL`
    /// when what came back is not a catalog document.
    pub async fn discover(&self) -> Result<Catalog, RemoteError> {
        self.discover_with(RequestOptions::default()).await
    }

    /// [`ContractClient::discover`], tuned by `options`.
    ///
    /// # Errors
    /// The same as [`ContractClient::discover`].
    pub async fn discover_with(&self, options: RequestOptions) -> Result<Catalog, RemoteError> {
        self.request_with(RPC_DISCOVER, json!({}), options).await
    }

    /// [`ContractClient::request`], tuned by `options`.
    ///
    /// # Errors
    /// The same as [`ContractClient::request`].
    pub async fn request_with<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
        options: RequestOptions,
    ) -> Result<R, RemoteError> {
        let params = serde_json::to_value(params).map_err(|error| {
            RemoteError::new(
                codes::INTERNAL,
                format!("Parameters of \"{method}\" failed to serialise: {error}."),
            )
            .with_detail("method", method.to_string())
        })?;
        let result = self.session.request_with(method, params, options).await?;
        serde_json::from_value(result).map_err(|error| {
            RemoteError::new(
                codes::INTERNAL,
                format!(
                    "Result of \"{method}\" from the peer does not decode as the requested \
                     type: {error}."
                ),
            )
            .with_detail("method", method.to_string())
        })
    }
}
