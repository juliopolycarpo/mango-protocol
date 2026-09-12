# Adopt the Rust crate

`mango-protocol` is the wire in Rust: the frame types, their validation rules, the NDJSON line
codec, the WebSocket chunk codec, the catalog document types and, behind the `schema` feature,
a JSON Schema emission. Behind the `tokio` feature it also has a session (request/response
multiplexing, cancel, event streams, liveness, graceful close) over any `Port`, and a `Contract`
builder that validates, serves and calls it — see [Use a session](#use-a-session) below. A
transport of its own (WebSocket, stdio) is still a later milestone; today a peer opens a session
over its own `Port` implementation, or writes its own loop over the codec directly.

```toml
[dependencies]
mango-protocol = { version = "0.1", features = ["tokio"] }
serde_json = "1"
```

The codec-only path depends on `serde` and `serde_json` only. `schemars` is pulled in by the
`schema` feature; `tokio`, `tokio-util` and `jsonschema` are pulled in by the `tokio` feature
(the session and the contract builder), which the "Use a session" and "Serve a contract"
sections below need.

## Frames

```rust
use mango_protocol::{Frame, Hello, Limits, PeerInfo, Request, PROTOCOL_VERSION};
use serde_json::{json, Map};

let hello = Frame::Hello(Hello {
    protocol: PROTOCOL_VERSION,
    peer: PeerInfo { name: "my-runtime".into(), version: "1.0.0".into(), role: "runtime".into() },
    capabilities: Map::new(),
    limits: Some(Limits { max_frame_bytes: Some(1 << 20) }),
});
let request = Frame::Req(Request { id: "r-1".into(), method: "fs.read-file".into(), params: json!({ "path": "README.md" }) });
```

`Frame` is `#[serde(tag = "type")]`; optional members serialise as absent, never `null`, and
unknown members are ignored on the way in. `End` is the `evt.end` marker: it serialises as
`true` and refuses anything else.

## Encode and decode lines

```rust
use mango_protocol::{decode_line, encode_line, LineDecoder, DEFAULT_MAX_FRAME_BYTES};

let line = encode_line(&request, DEFAULT_MAX_FRAME_BYTES)?;   // compact JSON plus '\n'
let back = decode_line(&line[..line.len() - 1], DEFAULT_MAX_FRAME_BYTES)?;

let mut decoder = LineDecoder::new(DEFAULT_MAX_FRAME_BYTES);
let outcome = decoder.push(&bytes_from_the_socket);
for frame in outcome.frames { handle(frame); }
if let Some(error) = outcome.error { close_with(error); }
```

`decode_line` parses, then runs `validate`, so a frame that came back is a frame the spec
accepts: lengths, grammars and ranges included. `LineDecoder` buffers partial lines, ignores
blank lines, strips a trailing carriage return, refuses a partial line that already exceeds the
limit, and delivers the frames it decoded before a refused record in the same outcome. After a
refusal it stays refused; the connection is meant to close.

## WebSocket chunks

```rust
use mango_protocol::{encode_chunks, ChunkReassembler};
use mango_protocol::codec::chunk::DEFAULT_MAX_MESSAGE_BYTES;

for message in encode_chunks(&frame, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_FRAME_BYTES)? {
    socket.send_binary(message);
}

let mut reassembler = ChunkReassembler::new(DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_FRAME_BYTES);
if let Some(frame) = reassembler.push(&incoming_binary_message)? { handle(frame); }
```

Every refusal (`CodecErrorKind::ChunkVersion`, `ChunkHeader`, `ChunkCount`, `ChunkIndex`,
`ChunkDribble`, `TooLarge`) resets the reassembler. The transport should close with `4400`.

## Negotiate the version

```rust
use mango_protocol::{negotiate, Negotiation, PROTOCOL_VERSION};

match negotiate(PROTOCOL_VERSION, remote_hello.protocol) {
    Negotiation::Compatible { effective_minor } => start(effective_minor),
    Negotiation::Mismatch { close_code } => close(close_code, "protocol mismatch"), // 4426
}
```

## Codes

`mango_protocol::error::codes` holds the reserved error codes; `is_reserved_error_code` tells
them from application codes. `mango_protocol::close_codes` holds the reserved close codes;
`is_fatal_close_code` says which ones mean "do not redial with this build".

## Catalog

`Catalog`, `CatalogMethod` and `CatalogEvent` deserialise the document the TypeScript
`defineContract().catalog()` produces, so a Rust peer can read a hub's method list, check the
names with `Catalog::validate`, and generate its own types from the embedded JSON Schemas.

## Schema emission

```sh
cargo run --example emit_schema --features schema
cargo run --example emit_catalog_schema --features schema
```

The first prints a `$defs` document equivalent to `spec/schema/1/protocol.json`, the second a
catalog document equivalent to `spec/schema/1/catalog.json`. The repository's `bun run check`
compares both with the spec on every change; you will not need either at runtime.

## Use a session

```rust
use mango_protocol::frame::PeerInfo;
use mango_protocol::port::port_pair;
use mango_protocol::session::{Session, SessionOptions};

let (port_a, port_b) = port_pair(); // swap for a real Port to speak over an actual transport
let peer = |role: &str| PeerInfo { name: "example".into(), version: "0.1.0".into(), role: role.into() };
let (a, _driver_a) = Session::spawn(port_a, SessionOptions::new(peer("a")));
let (b, _driver_b) = Session::spawn(port_b, SessionOptions::new(peer("b")));

a.ready().await?;
b.handle("fs.read-file", |params, _context| async move { Ok(params) }).persist();
let result = a.request("fs.read-file", serde_json::json!({ "path": "README.md" })).await?;
```

`cargo run --example session_pair --features tokio` runs a fuller version end to end: a request,
an event stream and a cancelled call between two in-process sessions.

## Serve a contract

A `Contract` (see [Build a contract](build-a-contract.md)) wraps a session with schema
validation, typed handlers and a policy guard, so a request never reaches your code until its
parameters have passed the method's schema:

```rust
let guard = contract.serve(&session, handlers, ServeOptions::default())?;
let result: MyResult = contract.client(&session).request("fs.read-file", params).await?;
```

See `Contract::client`'s own doc example for the full typed round trip through `serve` and
`ContractHandlers`, and the `Guard` trait's doc example for the policy hook that runs between
schema validation and the handler.

## What is missing, on purpose

Transports of the session's own (WebSocket, stdio) are a later milestone; `Port` is the seam a
transport crate implements against, proven today only by this crate's own in-process pair.
`rpc.discover` is deferred too, out of scope until a consumer needs it. A `tracing` feature is
deferred as well, since no consumer reads a span yet and it would be public surface the docs
lint and the feature powerset would have to carry for nothing.
