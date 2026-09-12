//! Mirrors the definition-time and catalog cases of
//! `packages/protocol/tests/contract.test.ts`'s `defineContract` describe
//! block. The five cases that need a session (`serve`/`client`/`events`)
//! join once this crate's contract grows those.
#![cfg(feature = "tokio")]

use mango_protocol::catalog::{Catalog, CatalogEvent, CatalogMethod};
use mango_protocol::contract::Contract;
use serde::Deserialize;
use serde_json::{Value, json};

fn method(
    name: &str,
    params: Value,
    result: Value,
    capabilities: Vec<String>,
    description: Option<&str>,
) -> CatalogMethod {
    CatalogMethod {
        name: name.into(),
        description: description.map(str::to_string),
        params,
        result,
        capabilities,
        deprecated: false,
    }
}

/// The same contract `packages/protocol/tests/contract.test.ts` defines:
/// `text.echo` and `math.add`, events `text.tick` and a streamed
/// `text.stream`, a capability schema, and an explicit protocol floor.
fn example() -> Contract {
    Contract::builder("example", "1.2.3")
        .description("Test contract")
        .protocol(mango_protocol::version::ProtocolVersion::new(1, 0))
        .capabilities(json!({ "type": "object", "properties": { "echo": { "type": "boolean" } } }))
        .method(method(
            "text.echo",
            json!({ "type": "object", "properties": { "text": { "type": "string" } }, "required": ["text"] }),
            json!({ "type": "object", "properties": { "text": { "type": "string" } }, "required": ["text"] }),
            vec!["echo".into()],
            Some("Echoes text"),
        ))
        .method(method(
            "math.add",
            json!({
                "type": "object",
                "properties": { "a": { "type": "number" }, "b": { "type": "number" } },
                "required": ["a", "b"],
            }),
            json!({ "type": "number" }),
            vec![],
            None,
        ))
        .event(CatalogEvent {
            topic: "text.tick".into(),
            description: None,
            payload: json!({ "type": "object", "properties": { "at": { "type": "number" } } }),
            stream: false,
        })
        .event(CatalogEvent {
            topic: "text.stream".into(),
            description: None,
            payload: json!({ "type": "object", "properties": { "line": { "type": "string" } } }),
            stream: true,
        })
        .build()
        .expect("a valid contract")
}

#[test]
fn rejects_invalid_or_reserved_names_at_definition_time() {
    Contract::builder("x", "1")
        .method(method("nodots", json!({}), json!({}), vec![], None))
        .build()
        .expect_err("a single segment is not a method name");

    let reserved = Contract::builder("x", "1")
        .method(method("rpc.discover", json!({}), json!({}), vec![], None))
        .build()
        .expect_err("rpc. is reserved");
    assert!(reserved.expected.contains("reserved"), "{reserved}");
}

#[test]
fn emits_a_plain_json_catalog_that_validates() {
    let catalog = example().catalog();
    assert_eq!(catalog.name, "example");
    assert_eq!(
        catalog
            .methods
            .iter()
            .map(|m| m.name.as_str())
            .collect::<Vec<_>>(),
        vec!["text.echo", "math.add"]
    );
    assert_eq!(catalog.methods[0].capabilities, vec!["echo".to_string()]);
    assert_eq!(
        catalog
            .events
            .iter()
            .map(|e| e.topic.as_str())
            .collect::<Vec<_>>(),
        vec!["text.tick", "text.stream"]
    );
    assert!(catalog.validate().is_ok());

    // "plain JSON": a round trip through serde_json changes nothing.
    let round_tripped: Value =
        serde_json::from_str(&serde_json::to_string(&catalog).expect("serialises"))
            .expect("deserialises");
    assert_eq!(
        round_tripped,
        serde_json::to_value(&catalog).expect("serialises")
    );
}

#[derive(Debug, Deserialize)]
struct EchoParams {
    #[allow(dead_code)]
    text: String,
}

#[test]
fn assert_params_narrows_or_throws() {
    let contract = example();
    contract
        .parse_params::<EchoParams>("text.echo", json!({ "text": "ok" }))
        .expect("a matching value narrows");
    contract
        .parse_params::<EchoParams>("text.echo", json!({ "text": 1 }))
        .expect_err("a number is not the declared string");
}

#[test]
fn from_catalog_compiles_a_catalog_a_peer_published() {
    let catalog = example().catalog();
    let recompiled = Contract::from_catalog(catalog.clone()).expect("the same catalog recompiles");
    assert_eq!(recompiled.catalog(), catalog);
}

#[test]
fn from_catalog_refuses_a_reserved_method_name() {
    let catalog = Catalog {
        name: "x".into(),
        version: "1".into(),
        description: None,
        protocol: None,
        methods: vec![method("rpc.discover", json!({}), json!({}), vec![], None)],
        events: vec![],
        capabilities: None,
    };
    let error = Contract::from_catalog(catalog).expect_err("rpc. is reserved");
    assert!(error.expected.contains("reserved"), "{error}");
}
