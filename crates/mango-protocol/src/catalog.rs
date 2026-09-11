//! The catalog document (§12): an application's methods, events and capabilities.
//!
//! A catalog is a description, not a wire message. SDKs use it to type clients
//! and validate handlers; a peer may publish it through an application method.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frame::present;
use crate::validate::{MAX_NAME_CHARS, ValidationError, is_valid_method_name};
use crate::version::ProtocolVersion;

/// One method a contract offers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "method"))]
pub struct CatalogMethod {
    /// The method name, in the grammar of §6.1.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::method_name")
    )]
    pub name: String,
    /// Prose for a human reading the contract.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present::option"
    )]
    pub description: Option<String>,
    /// JSON Schema 2020-12 document for `req.params`.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::open_object")
    )]
    pub params: Value,
    /// JSON Schema 2020-12 document for `res.result`.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::open_object")
    )]
    pub result: Value,
    /// Members of `hello.capabilities` the responder requires before serving this method.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::capability_names")
    )]
    pub capabilities: Vec<String>,
    /// True when the contract still serves the method but callers should move off it.
    #[serde(default, skip_serializing_if = "is_default_flag")]
    pub deprecated: bool,
}

/// True when a flag is at the value the wire leaves absent rather than stating.
fn is_default_flag(flag: &bool) -> bool {
    !*flag
}

/// One event topic a contract emits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "event"))]
pub struct CatalogEvent {
    /// The topic, in the grammar of §6.1.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::method_name")
    )]
    pub topic: String,
    /// Prose for a human reading the contract.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present::option"
    )]
    pub description: Option<String>,
    /// JSON Schema 2020-12 document for `evt.payload`.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::open_object")
    )]
    pub payload: Value,
    /// True when events on this topic carry a `streamId` and an `end` marker.
    #[serde(default, skip_serializing_if = "is_default_flag")]
    pub stream: bool,
}

/// An application contract, conforming to `spec/schema/1/catalog.json`.
///
/// # Example
///
/// ```
/// use mango_protocol::Catalog;
///
/// let catalog: Catalog = serde_json::from_str(
///     r#"{"name":"c","version":"1.0.0","methods":[]}"#,
/// )
/// .unwrap();
/// assert!(catalog.events.is_empty());
/// assert!(catalog.validate().is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "catalog"))]
pub struct Catalog {
    /// Contract name, 1 to 128 characters.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::peer_label")
    )]
    pub name: String,
    /// Contract version, 1 to 128 characters, opaque to the protocol.
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::peer_label")
    )]
    pub version: String,
    /// Prose for a human reading the contract.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present::option"
    )]
    pub description: Option<String>,
    /// The lowest wire version the contract needs.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present::option"
    )]
    pub protocol: Option<ProtocolVersion>,
    /// Every method the contract offers.
    pub methods: Vec<CatalogMethod>,
    /// Every event topic the contract emits; a missing member reads as none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<CatalogEvent>,
    /// JSON Schema of the `hello.capabilities` object this contract expects.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present::option"
    )]
    #[cfg_attr(
        feature = "schema",
        schemars(schema_with = "crate::schema::constraints::open_object")
    )]
    pub capabilities: Option<Value>,
}

impl Catalog {
    /// Checks the rules the catalog schema states and serde cannot.
    ///
    /// Every method name and event topic must match the grammar of §6.1, and
    /// the contract's own name and version must be 1 to [`MAX_NAME_CHARS`]
    /// characters.
    ///
    /// # Example
    ///
    /// ```
    /// use mango_protocol::Catalog;
    ///
    /// let catalog: Catalog = serde_json::from_str(
    ///     r#"{"name":"c","version":"1","methods":[{"name":"bad","params":{},"result":{}}]}"#,
    /// )
    /// .unwrap();
    /// assert_eq!(catalog.validate().unwrap_err().field, "catalog.methods[0].name");
    /// ```
    pub fn validate(&self) -> Result<(), ValidationError> {
        check_name("catalog.name", &self.name)?;
        check_name("catalog.version", &self.version)?;
        for (index, method) in self.methods.iter().enumerate() {
            check_grammar(&format!("catalog.methods[{index}].name"), &method.name)?;
        }
        for (index, event) in self.events.iter().enumerate() {
            check_grammar(&format!("catalog.events[{index}].topic"), &event.topic)?;
        }
        Ok(())
    }
}

fn check_name(field: &str, value: &str) -> Result<(), ValidationError> {
    let count = value.chars().count();
    if (1..=MAX_NAME_CHARS).contains(&count) {
        return Ok(());
    }
    Err(ValidationError {
        field: field.to_owned(),
        received: format!("a string of {count} characters"),
        expected: format!("a string of 1 to {MAX_NAME_CHARS} characters"),
    })
}

fn check_grammar(field: &str, value: &str) -> Result<(), ValidationError> {
    if is_valid_method_name(value) {
        return Ok(());
    }
    Err(ValidationError {
        field: field.to_owned(),
        received: format!("{value:?}"),
        expected: format!(
            "at least two dot-separated lowercase segments, at most {MAX_NAME_CHARS} characters"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::{Catalog, CatalogEvent, CatalogMethod};
    use crate::version::ProtocolVersion;
    use serde_json::{Value, json};

    fn sample() -> Catalog {
        Catalog {
            name: "fixture-contract".into(),
            version: "1.0.0".into(),
            description: None,
            protocol: Some(ProtocolVersion::new(1, 0)),
            methods: vec![CatalogMethod {
                name: "text.echo".into(),
                description: None,
                params: json!({ "type": "object" }),
                result: json!({ "type": "object" }),
                capabilities: vec!["echo".into()],
                deprecated: false,
            }],
            events: vec![CatalogEvent {
                topic: "text.stream".into(),
                description: None,
                payload: json!({ "type": "object" }),
                stream: true,
            }],
            capabilities: Some(json!({ "type": "object" })),
        }
    }

    #[test]
    fn a_well_formed_catalog_validates() {
        assert!(sample().validate().is_ok());
    }

    #[test]
    fn missing_events_deserialise_as_none_and_defaults_fill_in() {
        let catalog: Catalog = serde_json::from_str(
            r#"{"name":"c","version":"1.0.0","methods":[{"name":"a.b","params":{},"result":{}}]}"#,
        )
        .expect("decodes");
        assert!(catalog.events.is_empty());
        assert!(catalog.methods[0].capabilities.is_empty());
        assert!(!catalog.methods[0].deprecated);
        assert_eq!(catalog.protocol, None);
    }

    #[test]
    fn an_emptied_collection_is_not_serialised() {
        let mut catalog = sample();
        catalog.events.clear();
        catalog.methods[0].capabilities.clear();
        let value: Value = serde_json::to_value(&catalog).expect("serialises");
        assert!(value.get("events").is_none(), "{value}");
        assert!(value["methods"][0].get("capabilities").is_none(), "{value}");
    }

    #[test]
    fn an_absent_optional_is_not_serialised_and_null_is_refused() {
        let value: Value = serde_json::to_value(sample()).expect("serialises");
        assert!(value.get("description").is_none());
        let error = serde_json::from_str::<Catalog>(
            r#"{"name":"c","version":"1","methods":[],"description":null}"#,
        )
        .expect_err("null description is refused");
        assert!(error.to_string().contains("null"), "{error}");
    }

    #[test]
    fn a_single_segment_method_name_is_refused() {
        let mut catalog = sample();
        catalog.methods[0].name = "bad".into();
        let error = catalog.validate().expect_err("single segment");
        assert_eq!(error.field, "catalog.methods[0].name");
        assert!(error.expected.contains("two dot-separated"), "{error}");
    }

    #[test]
    fn a_bad_event_topic_is_refused() {
        let mut catalog = sample();
        catalog.events[0].topic = "Topic".into();
        assert_eq!(
            catalog.validate().expect_err("bad topic").field,
            "catalog.events[0].topic"
        );
    }

    #[test]
    fn an_empty_contract_name_is_refused() {
        let mut catalog = sample();
        catalog.name = String::new();
        assert_eq!(
            catalog.validate().expect_err("empty name").field,
            "catalog.name"
        );
    }

    #[test]
    fn keeps_absent_optional_members_absent() {
        let text =
            r#"{"name":"c","version":"1.0.0","methods":[{"name":"a.b","params":{},"result":{}}]}"#;
        let catalog: Catalog = serde_json::from_str(text).expect("deserialises");

        assert_eq!(
            serde_json::to_string(&catalog).expect("serialises"),
            text,
            "an optional member absent on the wire is absent again when re-serialised"
        );
    }

    #[test]
    fn round_trips_through_json() {
        let text = serde_json::to_string(&sample()).expect("serialises");
        let back: Catalog = serde_json::from_str(&text).expect("deserialises");
        assert_eq!(back, sample());
    }
}
