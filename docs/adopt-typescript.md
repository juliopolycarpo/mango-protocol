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

A child that cannot start at all (`ENOENT`, `EACCES`) is not an exception: the port reports
`{ kind: 'closed' }`, `child.exited` resolves with `{ code: null, signal: null }`, and the spawn
error is appended to `child.stderrTail()`, so one code path builds the message either way.
`classifySshExit(status, tail)` turns those two observations into a sentence for an ssh launch.

`await child.startError()` collects the same observations in one shape for a launch that never
reached a handshake: the exit status, the `spawnErrorCode` of a command that never became a
process, and the last line the child wrote. It waits a short grace for the exit, because the
pipes closing and the exit landing are not ordered, and reports `exit: undefined` rather than
inventing a status when the grace runs out.

`spawnPort` passes only the environment you give it, keeps a tail of stderr for error reports,
and on close sends SIGTERM then SIGKILL after a grace period. The launcher decides what to run;
WSL and container wrappers are argv arrays the application builds.

**Local socket.** A Unix domain socket or a Windows named pipe, NDJSON framed:

```ts
import { connectIpc, listenIpc, ipcPath } from '@mangostudio/protocol/ipc';
const path = ipcPath('mango-hub'); // \\.\pipe\mango-hub or $XDG_RUNTIME_DIR/mango-hub.sock
const server = await listenIpc(path, (port) => new Session(port, { peer, handlers }));
const client = new Session(await connectIpc(path, { timeoutMs: 5000 }), { peer });
```

`connectIpc` and `connectWebSocket` both take `timeoutMs` and `signal`. Without one, an
attempt nobody completes — a listener whose accept queue no one drains, a pipe whose server
stopped answering — stays in flight for as long as the process lives. A deadline that passes
destroys what the dial opened and rejects with a `TimeoutError`; an abort rejects with the
reason the caller gave.

On POSIX the socket is owner-only from the moment it exists, and a stale socket
file left by a crashed listener is replaced. On Windows the named pipe is **not**
restricted — Node cannot set a pipe's security descriptor, so any local user may
connect. Check the peer's credentials and close with `4401` before `hello` if the
address alone is not enough trust there.

**WebSocket.** Binary chunked messages under subprotocol `mango.v1`; the SDK never sends text
frames. Authentication is a bearer token on the upgrade request, checked by the HTTP layer
before the port exists.

```ts
import {
  connectWebSocket,
  createWebSocketPort,
  outcomeOfBunSend,
  WEBSOCKET_SUBPROTOCOL,
  webSocketPort,
} from '@mangostudio/protocol/ws';

// client
const port = await connectWebSocket('wss://hub.example/runtime', { headers: { authorization: `Bearer ${token}` } });

// server, any framework: give the SDK a sink and feed it messages
const { port, onMessage, onDrain, onClose } = createWebSocketPort({
  send: (bytes) => outcomeOfBunSend(ws.send(bytes)),
  close: (code, reason) => ws.close(code, reason),
});
// then call onMessage(bytes) for every binary message, onDrain() when backpressure
// clears, and onClose(code, reason) when the socket closes

// a WHATWG WebSocket object on either side
const port = webSocketPort(socket);
```

The sink reports each send as sent, buffered or dropped, so the port can pause its queue under
backpressure and close with `4400` when the socket drops a chunk; `outcomeOfBunSend` maps the
number Bun's `ServerWebSocket.send` returns onto that vocabulary. Only runtimes whose
`WebSocket` takes an options object can set upgrade headers: in a browser, or on Node's global
`WebSocket`, `connectWebSocket` ignores `headers` and the token goes in the URL or a cookie.

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
