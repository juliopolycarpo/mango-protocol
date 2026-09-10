/**
 * The local socket transport of spec/transports/local-socket.md: a Unix domain
 * socket on POSIX, a named pipe on Windows, NDJSON framed exactly as stdio is.
 *
 * One accepted connection is one session; a listener serves many at once.
 */

import { chmod, lstat, unlink } from 'node:fs/promises';
import { connect as connectSocket, createServer, type Server, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CLOSE_CODES } from '../close';
import type { Port } from '../port';
import { asError, createStreamPort } from './node-stream';

/** Owner-only, the permission local-socket.md requires of a POSIX socket file. */
const SOCKET_MODE = 0o600;

/** The umask that makes `bind` create the socket owner-only in the first place. */
const SOCKET_UMASK = 0o077;

/** How long a shutdown waits for a peer to close before destroying its socket. */
const CLOSE_GRACE_MS = 2000;

const WINDOWS = process.platform === 'win32';

export interface IpcOptions {
  /** Largest line the decoder accepts; the 16 MiB default of §11 when absent. */
  readonly maxFrameBytes?: number;
}

/** A listener, and the address it actually bound. */
export interface IpcServer {
  readonly path: string;
  /** Sends `close` 4000 to every open session, then stops listening. */
  close(): Promise<void>;
}

/**
 * The reference address for a named local endpoint: a Windows named pipe, or a
 * socket file in the user's runtime directory (`$XDG_RUNTIME_DIR`, else the
 * system temporary directory).
 *
 * @example
 * ipcPath('mango-hub'); // '/run/user/1000/mango-hub.sock', or '\\\\.\\pipe\\mango-hub'
 */
export function ipcPath(name: string): string {
  assertIpcName(name);
  // Windows named pipes live in a flat namespace spelled with backslashes;
  // forward slashes are not equivalent (local-socket.md, Addresses).
  if (WINDOWS) return `\\\\.\\pipe\\${name}`;
  const runtimeDir = process.env.XDG_RUNTIME_DIR;
  const directory = runtimeDir !== undefined && runtimeDir.length > 0 ? runtimeDir : tmpdir();
  return join(directory, `${name}.sock`);
}

/**
 * Listens on a local socket and hands one port per accepted connection.
 *
 * On POSIX a stale socket file at the same path is removed before binding, and
 * the socket is owner-only from the instant it exists.
 *
 * On Windows the same call creates a named pipe, and the address is **not**
 * restricted: Node exposes no way to set a pipe's security descriptor, so libuv
 * creates it with a NULL one and every local user may connect. An application
 * that needs more than process trust there must check the peer's credentials
 * and answer `close` 4401 before `hello`, as local-socket.md allows.
 *
 * @example
 * const server = await listenIpc(ipcPath('mango-hub'), (port) => new Session(port, { peer }));
 * await server.close();
 */
export async function listenIpc(
  path: string,
  onConnection: (port: Port) => void,
  options: IpcOptions = {}
): Promise<IpcServer> {
  const sockets = new Set<Socket>();
  const ports = new Set<Port>();
  const server = createServer((socket) => {
    sockets.add(socket);
    const port = ipcSocketPort(socket, options);
    ports.add(port);
    // `onClosed` never fires for a close this side chose, so the socket's own
    // end is what prunes the tables: every path through the port destroys it.
    socket.on('close', () => {
      sockets.delete(socket);
      ports.delete(port);
    });
    try {
      onConnection(port);
    } catch (cause) {
      // A throw here would reach `net`'s connection emitter and take the whole
      // listener process down; refuse this one connection instead.
      port.close(CLOSE_CODES.INTERNAL, 'the connection handler refused this connection');
      socket.destroy(asError(cause));
    }
  });

  if (WINDOWS) {
    // Node exposes no way to set a named pipe's security descriptor, so libuv
    // creates it with a NULL one: on Windows the address admits every local
    // user, and an application that needs more must check the peer's
    // credentials and answer `close` 4401 before `hello` (local-socket.md).
    await listening(server, path);
  } else {
    await removeStaleSocket(path);
    await bindOwnerOnly(server, path);
  }
  server.on('error', () => {
    // Listening already succeeded; a later error is a connection this listener
    // never accepted, and it must not become an uncaught exception.
  });

  return {
    path,
    close: async (): Promise<void> => {
      // The spec is explicit: a listener shutting down tells every session first.
      for (const port of ports) port.close(CLOSE_CODES.RELEASED, 'listener closing');
      ports.clear();
      await stopListening(server, sockets);
    },
  };
}

/**
 * Connects to a local socket and returns the port for that connection. The
 * promise rejects with the operating system's error when the path has no
 * listener.
 *
 * @example
 * const session = new Session(await connectIpc(ipcPath('mango-hub')), { peer });
 */
export function connectIpc(path: string, options: IpcOptions = {}): Promise<Port> {
  return new Promise((resolve, reject) => {
    const socket = connectSocket(path);
    const onError = (error: Error): void => {
      socket.destroy();
      reject(error);
    };
    socket.once('error', onError);
    socket.once('connect', () => {
      socket.removeListener('error', onError);
      resolve(ipcSocketPort(socket, options));
    });
  });
}

/**
 * One connection, one port: the socket is both the sink and the byte source.
 *
 * Exported for this module's tests and for a server that already owns its
 * socket; the package entry deliberately publishes only `listenIpc`,
 * `connectIpc` and `ipcPath`.
 *
 * @example
 * const port = ipcSocketPort(await rawConnect(path), {});
 */
export function ipcSocketPort(socket: Socket, options: IpcOptions): Port {
  return createStreamPort(socket, socket, {
    ...(options.maxFrameBytes !== undefined ? { maxFrameBytes: options.maxFrameBytes } : {}),
    onRelease: () => releaseSocket(socket),
  }).port;
}

/**
 * Releases the descriptor once the farewell is on the wire. `end` only
 * half-closes, and nothing reads this socket any more, so waiting for the
 * peer's own FIN would pin the descriptor for the listener's whole lifetime
 * without anyone learning anything from it.
 */
function releaseSocket(socket: Socket): void {
  if (socket.destroyed) return;
  if (socket.writableFinished) {
    socket.destroy();
    return;
  }
  // A socket that never flushes (a peer that stopped reading) still has to go.
  const grace = setTimeout(() => socket.destroy(), CLOSE_GRACE_MS);
  grace.unref();
  socket.once('finish', () => {
    clearTimeout(grace);
    socket.destroy();
  });
}

/** Refuses a name that could escape its directory or name a different pipe. */
function assertIpcName(name: string): void {
  const invalid =
    name.length === 0 || name.includes('/') || name.includes('\\') || name.includes('..');
  if (!invalid) return;
  throw new Error(
    `ipc name is ${JSON.stringify(name)}; expected one non-empty path segment without "/", "\\" or ".."`
  );
}

/**
 * Binds so that the socket is owner-only from the instant it exists. `bind`
 * takes its mode from the umask, so a listener under the usual `022` would
 * publish a world-connectable address for as long as the `chmod` takes; the
 * umask is process-wide, which is the price of closing that window.
 */
async function bindOwnerOnly(server: Server, path: string): Promise<void> {
  const previous = process.umask(SOCKET_UMASK);
  let bound: Promise<void>;
  try {
    // `listen` binds inside this call on both runtimes, so the umask is back
    // before the first yield and no unrelated file is created under it.
    bound = listening(server, path);
  } finally {
    process.umask(previous);
  }
  await bound;
  try {
    // Belt and braces: a platform that ignored the umask still ends up at 0600.
    await chmod(path, SOCKET_MODE);
  } catch (cause) {
    // An address only its owner can reach is the whole authentication story
    // here, so a listener that cannot promise that must not stay open.
    server.close();
    throw asError(cause);
  }
}

/**
 * Removes the socket file a previous process left behind. Only a socket is
 * removed: a regular file at the address is a mistake the caller must see as
 * `EADDRINUSE`, not something to delete.
 */
async function removeStaleSocket(path: string): Promise<void> {
  try {
    const stats = await lstat(path);
    if (!stats.isSocket()) return;
  } catch {
    // Nothing at the path, which is the ordinary case.
    return;
  }
  await unlink(path);
}

function listening(server: Server, path: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error): void => {
      server.removeListener('listening', onListening);
      reject(error);
    };
    const onListening = (): void => {
      server.removeListener('error', onError);
      resolve();
    };
    server.once('error', onError);
    server.once('listening', onListening);
    server.listen(path);
  });
}

/**
 * Stops accepting and waits for the open connections to close. A peer that
 * never answers the `close` frame is disconnected after a bounded grace, so a
 * shutdown is delayed by a rude peer but never blocked by one.
 */
function stopListening(server: Server, sockets: Set<Socket>): Promise<void> {
  return new Promise((resolve) => {
    const grace = setTimeout(() => {
      for (const socket of sockets) socket.destroy();
    }, CLOSE_GRACE_MS);
    grace.unref();
    server.close(() => {
      clearTimeout(grace);
      resolve();
    });
  });
}
