# mango-protocol

Wire types and codec for [Mango Protocol](https://github.com/juliopolycarpo/mango-protocol),
the JSON-Schema contract MangoStudio hubs, runtimes and tools speak over stdio, local
sockets and WebSocket. The TypeScript SDK `@mangostudio/protocol` reads the same frames.

```toml
[dependencies]
mango-protocol = "0.1"
```

```rust
use mango_protocol::{decode_line, encode_line, Frame, Request, DEFAULT_MAX_FRAME_BYTES};
use serde_json::json;

let request = Frame::Req(Request {
    id: "r-1".into(),
    method: "fs.read-file".into(),
    params: json!({ "path": "README.md" }),
});
let line = encode_line(&request, DEFAULT_MAX_FRAME_BYTES)?;
let back = decode_line(&line[..line.len() - 1], DEFAULT_MAX_FRAME_BYTES)?;
assert_eq!(back, request);
# Ok::<(), mango_protocol::CodecError>(())
```

What the crate covers:

- the nine frame types with serde, tolerant of unknown members, optional members absent rather
  than `null`, and `validate` for the lengths, grammars and ranges serde cannot express;
- the NDJSON line codec with a buffering `LineDecoder`, and the WebSocket chunk codec with a
  `ChunkReassembler`, both enforcing the frame limit;
- version negotiation, the reserved error codes and the close-code table;
- the catalog document types a TypeScript `defineContract` produces;
- behind the `schema` feature, a JSON Schema emission the repository proves equal to the spec.

Sessions and transports are a later, tokio-first milestone. Depends on `serde` and `serde_json`
only. MIT.
