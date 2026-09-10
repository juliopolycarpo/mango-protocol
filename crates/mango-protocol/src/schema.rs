//! JSON Schema emission for the wire types, behind the `schema` feature.
//!
//! The repository's schema-equality check compares this emission, the
//! TypeScript SDK's TypeBox emission and `spec/schema/1/protocol.json` after a
//! normaliser has flattened the three dialects. Nothing in this module is
//! needed to speak the protocol.

use schemars::generate::SchemaSettings;
use serde_json::{Map, Value, json};

use crate::frame::{Cancel, Close, ErrorResponse, Event, Hello, Request, Response};

/// Frame `$defs` keys that carry a payload, in the order `protocol.json` lists them.
const TAGGED_FRAMES: [&str; 7] = ["hello", "req", "res", "err", "evt", "cancel", "close"];
/// Frame `$defs` keys that are nothing but their tag.
const BARE_FRAMES: [&str; 2] = ["ping", "pong"];
/// Every frame `$defs` key, in the order `protocol.json` lists them.
const FRAME_ORDER: [&str; 9] = [
    "hello", "req", "res", "err", "evt", "cancel", "ping", "pong", "close",
];

/// The `type` member of a frame: a string fixed to the frame's tag.
fn tag_property(tag: &str) -> Value {
    json!({ "type": "string", "const": tag })
}

/// A frame whose only member is its tag, such as `ping`.
fn bare_frame(tag: &str) -> Value {
    json!({
        "type": "object",
        "required": ["type"],
        "properties": { "type": tag_property(tag) },
    })
}

/// Adds the `type` member to a derived frame schema, first in `required`.
fn add_tag(definitions: &mut Map<String, Value>, tag: &str) {
    let Some(Value::Object(frame)) = definitions.get_mut(tag) else {
        return;
    };
    let properties = frame
        .entry("properties")
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(properties) = properties {
        properties.insert("type".to_owned(), tag_property(tag));
    }
    let existing = frame
        .get("required")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut required = vec![json!("type")];
    required.extend(existing.into_iter().filter(|name| name != "type"));
    frame.insert("required".to_owned(), Value::Array(required));
}

/// Emits the wire schema as one document keyed like `spec/schema/1/protocol.json`.
///
/// The result is `{"$defs": {…}}` with one entry per frame type, one per shared
/// object (`peer`, `limits`, `protocolVersion`, `errorPayload`) and a `frame`
/// entry that is a `oneOf` over every frame.
///
/// # Example
///
/// ```
/// use mango_protocol::schema::emit_schema;
///
/// let schema = emit_schema();
/// assert_eq!(schema["$defs"]["ping"]["properties"]["type"]["const"], "ping");
/// assert_eq!(schema["$defs"]["frame"]["oneOf"].as_array().unwrap().len(), 9);
/// ```
#[must_use]
pub fn emit_schema() -> Value {
    let mut generator = SchemaSettings::draft2020_12().into_generator();
    let _ = generator.subschema_for::<Hello>();
    let _ = generator.subschema_for::<Request>();
    let _ = generator.subschema_for::<Response>();
    let _ = generator.subschema_for::<ErrorResponse>();
    let _ = generator.subschema_for::<Event>();
    let _ = generator.subschema_for::<Cancel>();
    let _ = generator.subschema_for::<Close>();

    let mut definitions = generator.take_definitions(true);
    for tag in TAGGED_FRAMES {
        add_tag(&mut definitions, tag);
    }
    for tag in BARE_FRAMES {
        definitions.insert(tag.to_owned(), bare_frame(tag));
    }
    let branches: Vec<Value> = FRAME_ORDER
        .iter()
        .map(|tag| json!({ "$ref": format!("#/$defs/{tag}") }))
        .collect();
    definitions.insert("frame".to_owned(), json!({ "oneOf": branches }));

    json!({ "$defs": Value::Object(definitions) })
}

#[cfg(test)]
mod tests {
    use super::{FRAME_ORDER, emit_schema};
    use serde_json::Value;

    fn definitions() -> Value {
        emit_schema()["$defs"].clone()
    }

    #[test]
    fn every_key_the_specification_names_is_present() {
        let defs = definitions();
        for key in FRAME_ORDER
            .iter()
            .chain(["errorPayload", "peer", "limits", "protocolVersion", "frame"].iter())
        {
            assert!(defs.get(*key).is_some(), "missing $defs/{key}");
        }
    }

    #[test]
    fn frame_is_a_one_of_over_every_frame_type() {
        let defs = definitions();
        let branches = defs["frame"]["oneOf"].as_array().expect("oneOf array");
        let refs: Vec<&str> = branches
            .iter()
            .map(|branch| branch["$ref"].as_str().expect("a $ref"))
            .collect();
        let expected: Vec<String> = FRAME_ORDER
            .iter()
            .map(|tag| format!("#/$defs/{tag}"))
            .collect();
        assert_eq!(refs, expected);
    }

    #[test]
    fn each_frame_requires_its_tag_first() {
        let defs = definitions();
        for tag in FRAME_ORDER {
            let frame = &defs[tag];
            assert_eq!(
                frame["properties"]["type"]["const"],
                Value::String(tag.into())
            );
            assert_eq!(
                frame["required"][0],
                Value::String("type".into()),
                "{tag} must require type first"
            );
        }
    }

    #[test]
    fn the_end_marker_is_a_boolean_fixed_to_true() {
        let evt = definitions()["evt"].clone();
        let text = serde_json::to_string(&evt["properties"]["end"]).expect("serialises");
        assert!(text.contains(r#""const":true"#), "{text}");
        assert!(text.contains(r#""boolean""#), "{text}");
    }

    #[test]
    fn shared_objects_are_referenced_rather_than_inlined() {
        let hello = definitions()["hello"].clone();
        assert_eq!(hello["properties"]["peer"]["$ref"], "#/$defs/peer");
        assert_eq!(
            hello["properties"]["protocol"]["$ref"],
            "#/$defs/protocolVersion"
        );
    }

    #[test]
    fn required_members_match_the_specification() {
        let defs = definitions();
        let required = |key: &str| -> Vec<String> {
            defs[key]["required"]
                .as_array()
                .expect("required array")
                .iter()
                .map(|name| name.as_str().expect("a string").to_owned())
                .collect()
        };
        assert_eq!(required("req"), ["type", "id", "method", "params"]);
        assert_eq!(required("res"), ["type", "id", "result"]);
        assert_eq!(required("err"), ["type", "id", "error"]);
        assert_eq!(required("evt"), ["type", "topic", "seq", "payload"]);
        assert_eq!(required("close"), ["type", "code"]);
        assert_eq!(
            required("hello"),
            ["type", "protocol", "peer", "capabilities"]
        );
    }

    #[test]
    fn the_emission_is_a_json_object_with_only_defs() {
        let schema = emit_schema();
        let object = schema.as_object().expect("an object");
        assert_eq!(object.keys().collect::<Vec<_>>(), vec!["$defs"]);
    }
}
