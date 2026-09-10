# Transport: local socket

A peer listens on a Unix domain socket (POSIX) or a named pipe (Windows); another peer connects
to it. Each accepted connection is one session; a listener serves many sessions at once.

## Framing

Identical to [stdio](stdio.md): NDJSON, one frame per line, `\r` stripped, blank lines ignored,
frame limit enforced per line, refused line ends the session with `4400`.

## Addresses

- POSIX: a filesystem path. Applications choose it; a runtime directory scoped to the user
  (`$XDG_RUNTIME_DIR`, `~/.mango/run/`) is the reference location. The listener creates the
  socket with owner-only permissions (`0600`) and removes a stale file at the same path before
  binding.
- Windows: `\\.\pipe\<name>`, spelled with backslashes. Forward slashes are not equivalent.
  The listener creates the pipe in byte mode. The pipe carries no per-user ACL: the socket API
  the reference SDK builds on cannot attach a security descriptor, so every local user may
  connect.

## Authentication

On POSIX the socket file's permissions admit only the owner. On Windows the pipe admits every
local user, so a listener that needs more than same-machine trust MUST authenticate the peer
before serving requests: it reads the credentials the operating system offers (the pipe
client's token, `SO_PEERCRED`, `getpeereid`) or requires an application credential carried in
`hello.capabilities`, and refuses a peer with `close` `4401`. On POSIX that check is a MAY.

## Liveness

Protocol `ping`/`pong` both ways on a fixed cadence, as for stdio.

## Close

`close` then socket shutdown. A connection that ends without a `close` frame is a `4000`
release. A listener shutting down sends `close` `4000` to every session first.

## Notes

- One session per connection. A client that needs two sessions opens two connections.
- A listener that is superseded by another process at the same path is an application concern:
  the protocol only says the old sessions end with `4409` if the listener chooses to say so.
