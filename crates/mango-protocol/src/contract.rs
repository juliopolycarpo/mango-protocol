//! Defines and validates an application contract: a [`Catalog`] document
//! plus the compiled `jsonschema` validators that check every method's
//! parameters against it, so a session dispatching through one never
//! recompiles a schema per request.
//!
//! Declare one with [`Contract::builder`], or compile one a peer published
//! with [`Contract::from_catalog`]. [`Contract::serve`] registers typed
//! handlers on a [`crate::session::Session`]; [`Contract::client`] and
//! [`Contract::events`] call and publish through one.

use std::sync::Arc;

use jsonschema::Validator;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::catalog::{Catalog, CatalogEvent, CatalogMethod};
use crate::error::{RemoteError, codes};
use crate::session::Session;
use crate::validate::{ValidationError, is_reserved_method_name};
use crate::version::ProtocolVersion;

mod client;
mod events;
mod params;
mod serve;

pub use client::ContractClient;
pub use events::{ContractEvents, EventOptions, TypedEvent, TypedEventStream};
pub use serve::{ContractHandlers, Guard, ServeGuard, ServeOptions};

/// One method's declaration plus its compiled `params`/`result` validators.
#[derive(Debug)]
struct CompiledMethod {
    definition: CatalogMethod,
    params: Arc<Validator>,
    result: Arc<Validator>,
}

/// A validated, ready-to-serve application contract.
///
/// # Example
///
/// ```
/// use mango_protocol::catalog::CatalogMethod;
/// use mango_protocol::contract::Contract;
/// use serde_json::json;
///
/// let contract = Contract::builder("example", "1.0.0")
///     .method(CatalogMethod {
///         name: "text.echo".into(),
///         description: None,
///         params: json!({ "type": "object", "properties": { "text": { "type": "string" } }, "required": ["text"] }),
///         result: json!({ "type": "object" }),
///         capabilities: vec![],
///         deprecated: false,
///     })
///     .build()
///     .expect("a valid contract");
/// assert_eq!(contract.catalog().methods[0].name, "text.echo");
/// ```
#[derive(Debug)]
pub struct Contract {
    catalog: Catalog,
    methods: Vec<CompiledMethod>,
}

impl Contract {
    /// Starts building a contract named `name` at `version`.
    #[must_use]
    pub fn builder(name: impl Into<String>, version: impl Into<String>) -> ContractBuilder {
        ContractBuilder {
            catalog: Catalog {
                name: name.into(),
                version: version.into(),
                description: None,
                protocol: None,
                methods: Vec::new(),
                events: Vec::new(),
                capabilities: None,
            },
        }
    }

    /// Compiles a contract from a catalog document — one a peer published,
    /// or one read back from storage — checking and compiling it exactly as
    /// [`ContractBuilder::build`] does. The deserialise-then-compile
    /// counterpart to the builder's declare-then-compile path.
    ///
    /// # Errors
    /// Returns [`ValidationError`] for the same reasons
    /// [`ContractBuilder::build`] does.
    pub fn from_catalog(catalog: Catalog) -> Result<Self, ValidationError> {
        compile(catalog)
    }

    /// The catalog document backing this contract: every method's and
    /// event's declaration, plain JSON. Infallible — [`Contract::builder`]
    /// and [`Contract::from_catalog`] already proved it valid; this is a
    /// clone of that proof, not a fresh check.
    #[must_use]
    pub fn catalog(&self) -> Catalog {
        self.catalog.clone()
    }

    /// Validates `params` against `method`'s schema, then deserialises it.
    ///
    /// A schema violation fails with `INVALID_PARAMS`, naming `method`, the
    /// failing RFC 6901 pointer and jsonschema's own message. A schema pass
    /// followed by a deserialisation failure means the server's own Rust
    /// type has drifted from the schema it advertises — a developer bug, not
    /// a bad request — so that becomes `INTERNAL` instead, never
    /// `INVALID_PARAMS`: the wire-visible meaning of `INVALID_PARAMS` stays
    /// exactly what the schema says it is.
    ///
    /// # Errors
    /// `METHOD_UNSUPPORTED` when `method` is not part of this contract;
    /// `INVALID_PARAMS` or `INTERNAL` as above.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::catalog::CatalogMethod;
    /// use mango_protocol::contract::Contract;
    /// use serde::Deserialize;
    /// use serde_json::json;
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct EchoParams {
    ///     text: String,
    /// }
    ///
    /// let contract = Contract::builder("example", "1.0.0")
    ///     .method(CatalogMethod {
    ///         name: "text.echo".into(),
    ///         description: None,
    ///         params: json!({ "type": "object", "properties": { "text": { "type": "string" } }, "required": ["text"] }),
    ///         result: json!({ "type": "object" }),
    ///         capabilities: vec![],
    ///         deprecated: false,
    ///     })
    ///     .build()
    ///     .expect("a valid contract");
    ///
    /// let ok: EchoParams = contract.parse_params("text.echo", json!({ "text": "hi" })).expect("valid");
    /// assert_eq!(ok.text, "hi");
    /// assert_eq!(
    ///     contract.parse_params::<EchoParams>("text.echo", json!({})).unwrap_err().code,
    ///     "INVALID_PARAMS",
    /// );
    /// ```
    pub fn parse_params<P: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<P, RemoteError> {
        let compiled = self
            .method(method)
            .ok_or_else(|| unsupported(&self.catalog.name, method))?;
        check_params(method, &compiled.params, &params)?;
        decode(method, params)
    }

    /// A typed request surface over `session`, scoped to this contract's
    /// methods. Requests are not locally checked against the contract before
    /// sending — the peer's own dispatch is the check, exactly as the
    /// TypeScript SDK's `ContractClient` relies purely on its compile-time
    /// method-name type, not a runtime one.
    #[must_use]
    pub fn client<'s>(&self, session: &'s Session) -> ContractClient<'s> {
        ContractClient::new(session)
    }

    /// Typed event emission and subscription over `session`. Neither `emit`
    /// nor `subscribe` checks `topic` against this contract locally — same
    /// as [`Contract::client`], the peer's own dispatch is the check.
    #[must_use]
    pub fn events<'s>(&self, session: &'s Session) -> ContractEvents<'s> {
        ContractEvents::new(session)
    }

    /// Registers `handlers` on `session`: each is wrapped so a request first
    /// validates against the method's `params` schema, then runs
    /// `options.guard` (seeing the validated params and the method's declared
    /// capabilities — this is what lets a consent gate be a guard instead of
    /// wrapping every handler by hand), then the handler itself, then
    /// (only when `options.validate_results` asks for it) checks the result
    /// against the method's `result` schema.
    ///
    /// `handlers` may cover any subset of this contract's declared methods —
    /// an unregistered declared method falls through to the session's own
    /// `METHOD_UNSUPPORTED`, so a contract can be served incrementally.
    /// Naming a method this contract does not declare is the one thing that
    /// is refused outright.
    ///
    /// # Errors
    /// Returns [`ValidationError`] when `handlers` names a method this
    /// contract does not declare.
    pub fn serve(
        &self,
        session: &Session,
        handlers: ContractHandlers,
        options: ServeOptions,
    ) -> Result<ServeGuard, ValidationError> {
        serve::serve(self, session, handlers, options)
    }

    fn method(&self, name: &str) -> Option<&CompiledMethod> {
        self.methods
            .iter()
            .find(|method| method.definition.name == name)
    }
}

/// The `INVALID_PARAMS`/`INTERNAL` split [`Contract::parse_params`] and
/// [`serve`]'s per-request wrapper both need: a schema violation is always
/// `INVALID_PARAMS` naming the pointer and reason; a schema pass that still
/// fails to decode is `INTERNAL`, since that is the server's own Rust type
/// drifting from the schema it advertises, not a bad request.
fn check_params(method: &str, validator: &Validator, params: &Value) -> Result<(), RemoteError> {
    if let Some((path, reason)) = params::first_violation(validator, params) {
        return Err(RemoteError::new(
            codes::INVALID_PARAMS,
            format!("Parameters of \"{method}\" do not match the contract at {path}: {reason}."),
        )
        .with_detail("method", method.to_string())
        .with_detail("path", path)
        .with_detail("reason", reason));
    }
    Ok(())
}

/// Checks `result` against `method`'s `result` schema — [`ServeOptions`]'s
/// `validate_results` opt-in, run after the handler settles.
fn check_result(method: &str, validator: &Validator, result: &Value) -> Result<(), RemoteError> {
    if let Some((path, reason)) = params::first_violation(validator, result) {
        return Err(RemoteError::new(
            codes::INTERNAL,
            format!("Result of \"{method}\" does not match the contract at {path}: {reason}."),
        )
        .with_detail("method", method.to_string())
        .with_detail("path", path)
        .with_detail("reason", reason));
    }
    Ok(())
}

/// Deserialises parameters already known to have passed their schema; a
/// failure here is `INTERNAL` (see [`check_params`]), never the schema's own
/// code — this is the server's own Rust type drifting from the schema it
/// advertises, not a bad request.
fn decode<P: DeserializeOwned>(method: &str, value: Value) -> Result<P, RemoteError> {
    serde_json::from_value(value).map_err(|error| {
        RemoteError::new(
            codes::INTERNAL,
            format!(
                "Parameters of \"{method}\" passed their schema but failed to decode into the \
                 handler's Rust type: {error}."
            ),
        )
        .with_detail("method", method.to_string())
    })
}

fn unsupported(contract: &str, method: &str) -> RemoteError {
    RemoteError::new(
        codes::METHOD_UNSUPPORTED,
        format!("Method \"{method}\" is not part of contract \"{contract}\"."),
    )
    .with_detail("method", method.to_string())
    .with_detail("contract", contract.to_string())
}

/// Accumulates a contract's declaration; [`ContractBuilder::build`] checks
/// its names and compiles its schemas.
pub struct ContractBuilder {
    catalog: Catalog,
}

impl ContractBuilder {
    /// Sets the contract's prose description.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.catalog.description = Some(description.into());
        self
    }

    /// Sets the lowest wire version the contract needs.
    #[must_use]
    pub fn protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.catalog.protocol = Some(protocol);
        self
    }

    /// Sets the JSON Schema of the `hello.capabilities` object this contract
    /// expects.
    #[must_use]
    pub fn capabilities(mut self, capabilities: Value) -> Self {
        self.catalog.capabilities = Some(capabilities);
        self
    }

    /// Declares one method.
    #[must_use]
    pub fn method(mut self, method: CatalogMethod) -> Self {
        self.catalog.methods.push(method);
        self
    }

    /// Declares one event topic.
    #[must_use]
    pub fn event(mut self, event: CatalogEvent) -> Self {
        self.catalog.events.push(event);
        self
    }

    /// Checks every method's and event topic's name, then compiles every
    /// `params`/`result` schema.
    ///
    /// # Errors
    /// Returns [`ValidationError`] for an out-of-range `name`/`version`, an
    /// invalid or reserved method name or event topic, or a `params`/
    /// `result` document that is not valid JSON Schema 2020-12.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::contract::Contract;
    ///
    /// let error = Contract::builder("", "1").build().unwrap_err();
    /// assert_eq!(error.field, "catalog.name");
    /// ```
    pub fn build(self) -> Result<Contract, ValidationError> {
        compile(self.catalog)
    }
}

/// Checks names — [`Catalog::validate`]'s grammar and length rules, plus the
/// reserved `rpc.` segment it does not know about, since that is a
/// contract-level rule, not a wire-schema one — then compiles every method's
/// schemas. The single path [`ContractBuilder::build`] and
/// [`Contract::from_catalog`] both funnel through, so declare-then-compile
/// and deserialise-then-compile cannot drift apart.
fn compile(catalog: Catalog) -> Result<Contract, ValidationError> {
    catalog.validate()?;
    for (index, method) in catalog.methods.iter().enumerate() {
        assert_not_reserved(&format!("catalog.methods[{index}].name"), &method.name)?;
    }
    for (index, event) in catalog.events.iter().enumerate() {
        assert_not_reserved(&format!("catalog.events[{index}].topic"), &event.topic)?;
    }
    let methods = catalog
        .methods
        .iter()
        .enumerate()
        .map(|(index, method)| {
            Ok(CompiledMethod {
                definition: method.clone(),
                params: Arc::new(params::compile(
                    &format!("catalog.methods[{index}].params"),
                    &method.params,
                )?),
                result: Arc::new(params::compile(
                    &format!("catalog.methods[{index}].result"),
                    &method.result,
                )?),
            })
        })
        .collect::<Result<Vec<_>, ValidationError>>()?;
    Ok(Contract { catalog, methods })
}

fn assert_not_reserved(field: &str, name: &str) -> Result<(), ValidationError> {
    if !is_reserved_method_name(name) {
        return Ok(());
    }
    Err(ValidationError {
        field: field.to_string(),
        received: format!("{name:?}"),
        expected: "a name outside the reserved rpc. namespace".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::Contract;
    use crate::catalog::{CatalogEvent, CatalogMethod};
    use serde::Deserialize;
    use serde_json::{Value, json};

    fn sample_method(name: &str, params: Value, result: Value) -> CatalogMethod {
        CatalogMethod {
            name: name.into(),
            description: None,
            params,
            result,
            capabilities: vec![],
            deprecated: false,
        }
    }

    fn example() -> Contract {
        Contract::builder("example", "1.2.3")
            .description("Test contract")
            .method(sample_method(
                "text.echo",
                json!({ "type": "object", "properties": { "text": { "type": "string" } }, "required": ["text"] }),
                json!({ "type": "object", "properties": { "text": { "type": "string" } }, "required": ["text"] }),
            ))
            .method(sample_method(
                "math.add",
                json!({
                    "type": "object",
                    "properties": { "a": { "type": "number" }, "b": { "type": "number" } },
                    "required": ["a", "b"],
                }),
                json!({ "type": "number" }),
            ))
            .event(CatalogEvent {
                topic: "text.tick".into(),
                description: None,
                payload: json!({ "type": "object" }),
                stream: false,
            })
            .build()
            .expect("a valid contract")
    }

    #[derive(Debug, Deserialize)]
    struct AddParams {
        a: f64,
        b: f64,
    }

    #[test]
    fn parse_params_narrows_or_refuses_with_invalid_params() {
        let contract = example();
        let ok: AddParams = contract
            .parse_params("math.add", json!({ "a": 1, "b": 2 }))
            .expect("valid params narrow");
        assert_eq!((ok.a, ok.b), (1.0, 2.0));

        let error = contract
            .parse_params::<AddParams>("math.add", json!({ "a": "one", "b": 2 }))
            .expect_err("a string is not a number");
        assert_eq!(error.code, "INVALID_PARAMS");
        assert_eq!(
            error.details.as_ref().and_then(|d| d.get("path")),
            Some(&json!("/a"))
        );
    }

    #[test]
    fn parse_params_refuses_an_undeclared_method_with_method_unsupported() {
        let error = example()
            .parse_params::<AddParams>("math.multiply", json!({}))
            .expect_err("not part of the contract");
        assert_eq!(error.code, "METHOD_UNSUPPORTED");
    }

    #[test]
    fn a_schema_pass_that_fails_to_decode_is_internal_not_invalid_params() {
        // The schema only requires an object; a caller's Rust type requires
        // a field the schema does not — exactly the drift `parse_params` is
        // meant to catch as the server's own bug, not the peer's bad request.
        #[derive(Debug, Deserialize)]
        struct StricterThanSchema {
            #[allow(dead_code)]
            required_by_rust_not_by_schema: String,
        }
        let contract = Contract::builder("x", "1")
            .method(sample_method("a.b", json!({ "type": "object" }), json!({})))
            .build()
            .expect("a valid contract");

        let error = contract
            .parse_params::<StricterThanSchema>("a.b", json!({}))
            .expect_err("the schema passes but decoding fails");
        assert_eq!(error.code, "INTERNAL");
    }
}
