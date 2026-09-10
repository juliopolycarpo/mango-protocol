# Adopt the Rust crate

`mango-protocol` is the wire in Rust: the frame types, their validation rules, the NDJSON line
codec, the WebSocket chunk codec, the catalog document types and, behind the `schema` feature,
a JSON Schema emission. It has no session and no transport; those are a later milestone. Today
the crate is for a peer that already owns its I/O loop and wants to speak the same frames as
the TypeScript SDK without re-deriving the rules.

```toml
[dependencies]
mango-protocol = "0.1"
serde_json = "1"
```

The crate depends on `serde` and `serde_json` only. `schemars` is pulled in by the `schema`
feature.

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
```

Prints a `$defs` document equivalent to `spec/schema/1/protocol.json`. The repository's
`bun run check` compares it with the spec on every change; you will not need it at runtime.

## What is missing, on purpose

A session (request multiplexing, cancel, streams, liveness) and the transports are planned as a
tokio-first milestone. Until then, a Rust peer writes its own loop over the codec, which is
about as much code as the mangostudio runtime host had before this crate existed, minus the
framing rules.
