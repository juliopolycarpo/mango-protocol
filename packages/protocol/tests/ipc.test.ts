import { describe, expect, it } from 'bun:test';
import { statSync } from 'node:fs';
import { rename, stat } from 'node:fs/promises';
import {
  connect as connectSocket,
  createServer,
  type IpcSocketConnectOpts,
  type Socket,
} from 'node:net';
import { CLOSE_CODES } from '../src/close';
import { RESERVED_ERROR_CODES } from '../src/errors';
import type { Port } from '../src/port';
import { Session, type SessionOptions } from '../src/session';
import {
  CONFORMANCE_A,
  CONFORMANCE_B,
  CONFORMANCE_HANDLERS,
  type ConformanceFixture,
  itBehavesLikeAMangoTransport,
} from '../src/testing/conformance';
import { connectIpc, ipcPath, ipcSocketPort, listenIpc } from '../src/transports/ipc';

const WINDOWS = process.platform === 'win32';

let addresses = 0;

/** A fresh address per connection, so parallel cases never share a listener. */
function nextPath(): string {
  addresses += 1;
  return ipcPath(`mango-protocol-test-${process.pid}-${addresses}`);
}

/** The port of the next accepted connection. */
function acceptOne(): { readonly accepted: Promise<Port>; accept: (port: Port) => void } {
  let accept: (port: Port) => void = () => undefined;
  const accepted = new Promise<Port>((resolve) => {
    accept = resolve;
  });
  return { accepted, accept };
}

/**
 * A connected socket with no port on it. `drain` discards what the far side
 * writes, for a fixture that only ever writes; a socket handed to
 * `ipcSocketPort` must not be drained, or the port would never see a frame.
 */
function rawConnect(
  path: string,
  drain = false,
  extra: Omit<IpcSocketConnectOpts, 'path'> = {}
): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = connectSocket({ ...extra, path });
    socket.once('error', reject);
    socket.once('connect', () => {
      socket.removeListener('error', reject);
      if (drain) socket.resume();
      resolve(socket);
    });
  });
}

const fixture: ConformanceFixture = {
  async connect(aOptions: SessionOptions, bOptions: SessionOptions) {
    const path = nextPath();
    const { accepted, accept } = acceptOne();
    const server = await listenIpc(path, accept);
    // Side b owns its socket so `drop` can sever the link the way a crash
    // does, with no `close` frame and no FIN.
    const clientSocket = await rawConnect(path);
    const a = new Session(await accepted, aOptions);
    const b = new Session(ipcSocketPort(clientSocket, {}), bOptions);
    // A real socket needs an event loop turn to carry the two hellos; the suite
    // expects a pair that is already connected (or already refused).
    await Promise.allSettled([a.ready, b.ready]);
    return {
      a,
      b,
      drop: () => {
        clientSocket.destroy();
      },
      close: async () => {
        a.close();
        b.close();
        await server.close();
      },
    };
  },

  // Frames are split across `data` chunks on a byte stream, so two concurrent
  // oversized results are a reassembly test the suite already knows how to run.
  chunked: true,

  async connectRaw(aOptions: SessionOptions) {
    const path = nextPath();
    const { accepted, accept } = acceptOne();
    const server = await listenIpc(path, accept);
    const socket = await rawConnect(path, true);
    const a = new Session(await accepted, aOptions);
    return {
      a,
      write: (line: string) => {
        socket.write(line);
      },
      close: async () => {
        a.close();
        socket.destroy();
        await server.close();
      },
    };
  },
};

describe('local socket transport', () => {
  itBehavesLikeAMangoTransport(fixture);

  it('rejects a connection to a path with no listener', async () => {
    await expect(connectIpc(nextPath())).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('sends close 4000 to every open session before it stops listening', async () => {
    const path = nextPath();
    const { accepted, accept } = acceptOne();
    const server = await listenIpc(path, accept);
    const client = new Session(await connectIpc(path), {
      peer: CONFORMANCE_B,
      livenessIntervalMs: false,
    });
    const host = new Session(await accepted, { peer: CONFORMANCE_A, livenessIntervalMs: false });
    await Promise.all([client.ready, host.ready]);

    const closed = new Promise((resolve) => client.onClose(resolve));
    await server.close();

    expect(await closed).toMatchObject({
      code: CLOSE_CODES.RELEASED,
      reason: 'listener closing',
      fatal: false,
    });
  });

  it('reports a peer that vanished as a closure and fails what was in flight', async () => {
    const path = nextPath();
    const { accepted, accept } = acceptOne();
    const server = await listenIpc(path, accept);
    const socket = await rawConnect(path, true);
    const host = new Session(await accepted, {
      peer: CONFORMANCE_A,
      handlers: CONFORMANCE_HANDLERS,
      livenessIntervalMs: false,
    });
    socket.write(
      '{"type":"hello","protocol":{"major":1,"minor":0},"peer":{"name":"raw","version":"0","role":"tool"},"capabilities":{}}\n'
    );
    await host.ready;

    const pending = host.request('test.forever', {});
    await tick();
    socket.destroy();

    await expect(pending).rejects.toMatchObject({ code: RESERVED_ERROR_CODES.UNAVAILABLE });
    expect(host.state).toBe('closed');
    // No `close` frame crossed, so the closure is the plain 4000 release of §10.
    expect(host.closure?.code).toBe(CLOSE_CODES.RELEASED);
    await server.close();
  });

  it('releases a connection the peer never closes', async () => {
    const path = nextPath();
    const { accepted, accept } = acceptOne();
    const server = await listenIpc(path, accept);
    // A rude peer: `allowHalfOpen` keeps it from answering the listener's FIN,
    // so nothing but the listener itself can release the descriptor.
    const rude = await rawConnect(path, true, { allowHalfOpen: true });
    // Writing to a released socket is the assertion below; without a listener
    // the resulting `error` event would be an uncaught exception instead.
    rude.on('error', () => undefined);
    const hostPort = await accepted;

    hostPort.close(CLOSE_CODES.RELEASED, 'done');

    // `end` alone only half-closes: without the release the listener would keep
    // taking this peer's bytes, and hold the descriptor, until it shut down.
    expect(await writesRefusedWithin(rude, 1000)).toBe(true);
    rude.destroy();
    await server.close();
  });

  it('refuses one connection when the handler throws, and keeps listening', async () => {
    const path = nextPath();
    let accepted = 0;
    const server = await listenIpc(path, (port) => {
      accepted += 1;
      if (accepted === 1) throw new Error('handler blew up');
      port.close(CLOSE_CODES.RELEASED, 'served');
    });
    try {
      // A throw out of a `net` connection listener takes the process down; the
      // listener has to survive it and answer the next connection.
      await closedAfterConnect(path);
      await closedAfterConnect(path);
      expect(accepted).toBe(2);
    } finally {
      await server.close();
    }
  });

  // POSIX only: a named pipe has no filesystem entry to leave behind.
  it.skipIf(WINDOWS)('replaces the socket file a crashed listener left behind', async () => {
    const bound = nextPath();
    const stale = nextPath();
    const squatter = createServer();
    await new Promise<void>((resolve) => {
      squatter.listen(bound, () => resolve());
    });
    // Renaming before the close leaves the socket file behind exactly as a
    // process that died without unlinking would.
    await rename(bound, stale);
    await new Promise<void>((resolve) => {
      squatter.close(() => resolve());
    });
    expect(statSync(stale).isSocket()).toBe(true);

    const { accepted, accept } = acceptOne();
    const server = await listenIpc(stale, accept);
    try {
      const client = await connectIpc(stale);
      const port = await accepted;
      expect(port.maxFrameBytes).toBe(16 * 1024 * 1024);
      client.close(CLOSE_CODES.RELEASED);
    } finally {
      await server.close();
    }
  });

  // POSIX only: Windows guards a named pipe with an ACL, not a file mode.
  it.skipIf(WINDOWS)('creates the socket with owner-only permissions', async () => {
    const path = nextPath();
    const server = await listenIpc(path, () => undefined);
    try {
      expect((await stat(path)).mode & 0o777).toBe(0o600);
    } finally {
      await server.close();
    }
  });
});

describe('ipcPath', () => {
  it('names a windows pipe or a socket file in the runtime directory', () => {
    const path = ipcPath('mango-hub');
    if (WINDOWS) expect(path).toBe('\\\\.\\pipe\\mango-hub');
    else expect(path.endsWith('/mango-hub.sock')).toBe(true);
  });

  // POSIX only: the runtime directory has no meaning for a named pipe.
  it.skipIf(WINDOWS)('prefers XDG_RUNTIME_DIR and falls back to the temp directory', () => {
    const original = process.env.XDG_RUNTIME_DIR;
    try {
      process.env.XDG_RUNTIME_DIR = '/run/user/4242';
      expect(ipcPath('mango-hub')).toBe('/run/user/4242/mango-hub.sock');
      delete process.env.XDG_RUNTIME_DIR;
      expect(ipcPath('mango-hub')).not.toBe('/run/user/4242/mango-hub.sock');
      expect(ipcPath('mango-hub').endsWith('/mango-hub.sock')).toBe(true);
    } finally {
      if (original === undefined) delete process.env.XDG_RUNTIME_DIR;
      else process.env.XDG_RUNTIME_DIR = original;
    }
  });

  for (const name of ['', 'a/b', 'a\\b', '..', 'x..y', '../escape']) {
    it(`refuses the name ${JSON.stringify(name)}`, () => {
      expect(() => ipcPath(name)).toThrow(
        `ipc name is ${JSON.stringify(name)}; expected one non-empty path segment`
      );
    });
  }
});

/** True once the far end stopped accepting bytes, meaning it let the socket go. */
async function writesRefusedWithin(socket: Socket, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const refused = await new Promise<boolean>((resolve) => {
      socket.write('{"type":"ping"}\n', (error) => resolve(error !== undefined && error !== null));
    });
    if (refused) return true;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

/** Connects, waits for the listener to hang up, and reports nothing else. */
async function closedAfterConnect(path: string): Promise<void> {
  const socket = await rawConnect(path, true);
  await new Promise<void>((resolve) => socket.once('close', () => resolve()));
}

/** Lets the socket machinery deliver its queued events. */
function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 5));
}
