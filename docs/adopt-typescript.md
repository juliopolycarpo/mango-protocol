# Adopt the TypeScript SDK

`@mangostudio/protocol` gives an application one `Session` over any transport, plus the
contract helper described in [build-a-contract.md](build-a-contract.md). This page walks from
zero to a working peer.

```sh
bun add @mangostudio/protocol
```

The core entry (`@mangostudio/protocol`) is browser-safe. Transports that need the operating
system live under subpaths: `./stdio`, `./ipc`, `./spawn`. `./ws` and `./in-process` are
runtime-neutral. `./testing` is the shared transport test suite.

## One session, any port

A `Port` is the transport: it sends a frame, delivers frames, reports closure, closes with a
code. A `Session` turns a port into requests, events, ping and close semantics:

```ts
import { Session } from '@mangostudio/protocol';

const session = new Session(port, {
  peer: { name: 'my-runtime', version: '1.0.0', role: 'runtime' },
  capabilities: { contracts: { 'example.files': '1.0.0' } },
});
const remote = await session.ready; // both hellos exchanged, minors negotiated
console.log(remote.peer.name, remote.effectiveMinor);
```

Both peers send `hello` as soon as the transport opens; there is no client and no server at
this layer. `ready` rejects with a `RemoteError` of code `PROTOCOL_MISMATCH` when the majors
differ, and with `TIMEOUT` when the peer never says hello (15 seconds by default).

Options worth knowing:

| Option               | Default                   | Meaning                                                                         |
| -------------------- | ------------------------- | ------------------------------------------------------------------------------- |
| `maxFrameBytes`      | the port's limit, 16 MiB  | Largest frame accepted; announced in `hello.limits` when lower than the default |
| `handshakeTimeoutMs` | 15000                     | How long to wait for the peer's `hello`                                         |
| `livenessIntervalMs` | 20000, `false` to disable | Ping interval; one missed pong closes with 4000 and reason `liveness timeout`   |
| `handlers`           | none                      | Method handlers registered before the handshake, so early requests are served   |
| `timers`             | globals                   | Injected timers for tests                                                       |

## Choose a transport

**Child process over stdio.** The child owns stdin and stdout; everything it prints to stdout
must be protocol. Diagnostics go to stderr.

```ts
// child
import { stdioPort } from '@mangostudio/protocol/stdio';
const session = new Session(stdioPort(), { peer });

// parent
import { spawnPort, sshArgv } from '@mangostudio/protocol/spawn';
const child = spawnPort({ argv: ['bun', 'runtime.ts'], cwd, env: { PATH: process.env.PATH } });
const session = new Session(child.port, { peer });
// or over ssh, with the hardened argv preset:
const remote = spawnPort({ argv: sshArgv({ host: 'build-box', command: ['mango-runtime'] }) });
```

`spawnPort` passes only the environment you give it, keeps a tail of stderr for error reports,
and on close sends SIGTERM then SIGKILL after a grace period. The launcher decides what to run;
WSL and container wrappers are argv arrays the application builds.

**Local socket.** A Unix domain socket or a Windows named pipe, NDJSON framed:

```ts
import { connectIpc, listenIpc, ipcPath } from '@mangostudio/protocol/ipc';
const path = ipcPath('mango-hub'); // \\.\pipe\mango-hub or $XDG_RUNTIME_DIR/mango-hub.sock
const server = listenIpc(path, (port) => new Session(port, { peer, handlers }));
const client = new Session(await connectIpc(path), { peer });
```

**WebSocket.** Binary chunked messages under subprotocol `mango.v1`; the SDK never sends text
frames. Authentication is a bearer token on the upgrade request, checked by the HTTP layer
before the port exists.

```ts
import { connectWebSocket, webSocketPort, createWebSocketPort, WEBSOCKET_SUBPROTOCOL } from '@mangostudio/protocol/ws';

// client
const port = await connectWebSocket('wss://hub.example/runtime', { headers: { authorization: `Bearer ${token}` } });

// server, any framework: give the SDK a sink and feed it messages
const { port, onMessage, onClose } = createWebSocketPort({
  send: (bytes) => ws.send(bytes),
  close: (code, reason) => ws.close(code, reason),
});
// then call onMessage(bytes) for every binary message and onClose(code, reason) when the socket closes

// a WHATWG WebSocket object on either side
const port = webSocketPort(socket);
```

**In-process.** Two ports joined by a queue, for tests and for hosting a runtime in the same
process:

```ts
import { createInProcessPortPair } from '@mangostudio/protocol/in-process';
const { a, b } = createInProcessPortPair();
```

Validate mode (the default) encodes and decodes every frame, so tests exercise the codec; clone
mode skips the codec for speed.

## Requests, events, close

```ts
session.handle('fs.read-file', async (params, { signal }) => readFile(params, signal));
const result = await session.request('fs.read-file', { path: 'README.md' }, { timeoutMs: 5000 });

session.emit({ topic: 'fs.changed', payload: { path: 'a.ts' } });
const off = session.onEvent((frame) => console.log(frame.topic, frame.seq));

session.onClose(({ code, reason, fatal }) => {
  if (!fatal) scheduleReconnect();
});
session.close(4000, 'released');
```

Use the contract helper for typed calls; the raw API is for tooling and for the reserved
`rpc.*` space the protocol may add.

## Errors

- A handler throws `RemoteError(code, message, details?)` to answer with a specific code. Any
  other exception becomes `INTERNAL`.
- A request rejects with `RemoteError`; read `error.code`. Reserved codes are in
  `RESERVED_ERROR_CODES`; the application's own codes pass through untouched.
- A malformed frame from the peer closes the session with a `4400` family code; the closure
  carries the `CodecError` that caused it.

## What the SDK does not do

Authentication, consent, audit, reconnect policy, pairing and process supervision belong to
the application. The SDK gives every one of them a hook (a guard, an `onClose` with a fatal
flag, a spawn launcher with an exit promise) and takes no decision itself.

## Test your transport

Run the shared suite against any new port implementation; see
[conformance.md](conformance.md).
