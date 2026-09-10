# Mango Protocol

One JSON-Schema wire contract for MangoStudio hubs, runtimes and tools, inspired by JSON-RPC 2.0
and published as a TypeScript SDK (`@mangostudio/protocol`, npm) and a Rust crate
(`mango-protocol`, crates.io). A hub written in TypeScript and a runtime written in Rust speak
the same frames over stdio, local sockets, in-process ports or WebSocket without caring which
language sits on the other end.

> Status: pre-release. Wire version 1.0 is being written; nothing is published yet.

## Why

MangoStudio's hub and runtime already speak a versioned frame protocol. Every new peer (a second
runtime, a language-server gateway, a CLI) would have to re-implement its framing, handshake,
request multiplexing, cancellation, streams, liveness and close semantics. This repository owns
those once. Applications keep their own method catalogs and their own policy: consent, audit,
authentication, reconnect rules.

The success test is simple: adopting the SDK must delete more code from a consumer than it adds.

## Layout

| Path                     | What                                                                         |
| ------------------------ | ---------------------------------------------------------------------------- |
| `spec/`                  | Normative spec, JSON Schema 2020-12 files, conformance fixture corpus        |
| `packages/protocol/`     | `@mangostudio/protocol`: types, codec, session, transports, contract helpers |
| `crates/mango-protocol/` | `mango-protocol`: wire types and codec                                       |
| `docs/`                  | Guides for building contracts and adopting the SDK in TypeScript or Rust     |

## Develop

```sh
bun install
bun run check     # Biome, dprint, tsc, rustfmt, Clippy
bun run test      # bun test, then cargo test
bun run fix       # apply formatters
```

`AGENTS.md` is the contributor guide, including the contract-change procedure every wire change
follows.

## License

MIT
