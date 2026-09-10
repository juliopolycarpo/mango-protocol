# Transport: WebSocket

One WebSocket connection carries one session. Either side may have opened the connection: a
peer behind NAT dials out to a listening peer, or a peer on a reachable address listens and is
dialled. Once the socket is open the protocol is symmetric and does not care who dialled.

## Subprotocol

The dialling side MUST offer the subprotocol `mango.v1` in the upgrade request; the accepting
side MUST select it. A connection established without it is not a Mango Protocol session.

## Framing: chunked binary messages

WebSocket servers cap the size of one message, and a server shared with other sockets shares
that cap with them. Frames are therefore split above the socket:

- Every message is **binary**. A text message is a protocol error: the receiver closes with
  `4400`.
- A message is a nine-byte header followed by a slice of the frame's UTF-8 bytes (the NDJSON
  line without its terminator):

  | Offset | Size | Field                            |
  | ------ | ---- | -------------------------------- |
  | 0      | 1    | Format version, `1`              |
  | 1      | 4    | Chunk index, unsigned big-endian |
  | 5      | 4    | Chunk count, unsigned big-endian |
  | 9      | …    | Payload                          |

- A frame becomes `count` messages with indexes `0 … count−1`, sent contiguously through one
  queue per connection. Chunks of two frames never interleave.
- The sender's **message ceiling** is a local setting of at least `2048` bytes; the reference
  value is `16384`, which is safe on a server shared with browser sockets. Every chunk carries
  at least one payload byte, and every chunk but the last MUST carry at least `1024`, so a
  receiver can bound the number of chunks a frame may need from the frame limit alone:
  `ceil(frameLimit / 1024)`.
- The receiver reassembles by index. It MUST refuse, and close with `4400`, when: the format
  version is not `1`; the header is short; `count` is `0` or exceeds the bound above; `index`
  is not the one expected; a later chunk's `count` differs from the first's; the accumulated
  payload exceeds the frame limit; a chunk carries no payload; a non-final chunk carries fewer
  than `1024` payload bytes.
- The reassembled bytes are decoded as one NDJSON line (frame limit, schema).

## Authentication

Transport-level, at the upgrade. The reference mechanism is `Authorization: Bearer <token>` on
the upgrade request; the accepting side verifies it before or immediately after the upgrade
and closes with `4401` (unknown, malformed or revoked) or `4403` (known but disabled) without
sending `hello`. Rate limiting closes with `4429` after the upgrade so the dialler can read a
code, since a refused upgrade reaches it as a socket that failed to open.

Applications MAY define other credentials for peers that cannot set headers. The token never
appears inside a frame.

## Liveness

Protocol `ping`/`pong` both ways, on a cadence well under the idle timeout of every proxy and
server on the path (a third of the shortest timeout, at least 5 seconds). WebSocket control
frames MAY be used in addition and MUST NOT be relied on alone.

## Close

The WebSocket close code carries the reason code (`4000`–`4999`), so a `close` frame is
optional on this transport. When both are sent, the frame goes first. The fatal set applies to
the WebSocket close code exactly as it does to `close.code`.

## Backpressure

One queue per connection. The socket reports each send as **sent**, **buffered under
backpressure** (stop until the socket drains) or **dropped** (the message was not accepted).
A dropped chunk desynchronises the stream and is fatal: close with `4400`. A queue that grows
past one frame limit while the socket is not draining is a peer that is not reading; the
sender closes with `4400` rather than holding every pending response for a socket that may
never drain.

## TLS

The protocol does not terminate TLS. Put a reverse proxy in front when the connection crosses
an untrusted network and use `wss://`.
