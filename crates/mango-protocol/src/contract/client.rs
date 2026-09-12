//! A typed request surface over one session.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{RemoteError, codes};
use crate::session::{RequestOptions, Session};

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
