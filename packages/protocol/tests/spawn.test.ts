import { describe, expect, it } from 'bun:test';
import type { ChildProcess, SpawnOptions as ChildSpawnOptions } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { CLOSE_CODES } from '../src/close';
import type { PortClosure } from '../src/port';
import { Session } from '../src/session';
import { CONFORMANCE_A } from '../src/testing/conformance';
import { type SpawnChild, sanitizedEnv, spawnPort, withErrorCode } from '../src/transports/spawn';

const WINDOWS = process.platform === 'win32';

/** A bare `bun` does not spawn on Windows; the resolved binary always does. */
const BUN = Bun.which('bun') ?? process.execPath;
const ECHO_CHILD = fileURLToPath(new URL('./fixtures/stdio-echo.ts', import.meta.url));
const STUBBORN_CHILD = fileURLToPath(new URL('./fixtures/stubborn-child.ts', import.meta.url));
const MISSING_COMMAND = fileURLToPath(new URL('./fixtures/no-such-binary', import.meta.url));

const decoder = new TextDecoder();

/** A child that never runs: enough of the shape for the launcher to wire it. */
class FakeChildProcess extends EventEmitter {
  readonly stdin = new PassThrough();
  readonly stdout = new PassThrough();
  readonly stderr = new PassThrough();
  readonly pid = 4242;
  readonly exitCode: number | null = null;
  readonly signalCode: string | null = null;

  kill(): boolean {
    return true;
  }
}

/** Records what the launcher asks the child-process API for, and starts nothing. */
class RecordingSpawn {
  readonly calls: {
    command: string;
    args: string[];
    options: ChildSpawnOptions;
  }[] = [];
  readonly child = new FakeChildProcess();

  readonly spawn: SpawnChild = (command, args, options) => {
    this.calls.push({ command, args, options });
    return this.child as unknown as ChildProcess;
  };

  /** The one call the launcher made. */
  get only(): { command: string; args: string[]; options: ChildSpawnOptions } {
    const call = this.calls[0];
    if (call === undefined) throw new Error('the launcher never spawned anything');
    return call;
  }
}

describe('spawn launcher', () => {
  it('completes the handshake and round-trips a request with a real child', async () => {
    const peer = spawnPort({ argv: [BUN, ECHO_CHILD] });
    try {
      expect(peer.pid).toBeGreaterThan(0);
      const session = new Session(peer.port, { peer: CONFORMANCE_A, livenessIntervalMs: false });
      const remote = await session.ready;

      expect(remote.peer.name).toBe('stdio-echo');
      expect(await session.request('test.echo', { text: 'héllo · 🥭' })).toEqual({
        text: 'héllo · 🥭',
      });
    } finally {
      await peer.terminate();
    }
  });

  it('leaves on close: the child sees the close frame and exits zero', async () => {
    const peer = spawnPort({ argv: [BUN, ECHO_CHILD] });
    const session = new Session(peer.port, { peer: CONFORMANCE_A, livenessIntervalMs: false });
    await session.ready;

    session.close();

    expect(await peer.exited).toEqual({ code: 0, signal: null });
  });

  it('keeps a bounded tail of the child stderr and streams it to onStderr', async () => {
    const streamed: string[] = [];
    const peer = spawnPort({
      argv: [BUN, ECHO_CHILD],
      env: { ...sanitizedEnv(), MANGO_TEST_STDERR: 'runtime warming up\n' },
      onStderr: (chunk) => streamed.push(decoder.decode(chunk)),
    });
    try {
      await waitFor(() => streamed.join('').includes('runtime warming up'));
      expect(peer.stderrTail()).toContain('runtime warming up');
    } finally {
      await peer.terminate();
    }
  });

  it('keeps only the last bytes of a stderr larger than the tail', async () => {
    const peer = spawnPort({
      argv: [BUN, '-e', 'process.stderr.write("a".repeat(200) + "TAIL")'],
      stderrTailBytes: 16,
    });
    try {
      await waitFor(() => peer.stderrTail().length > 0);
      await waitFor(() => peer.stderrTail().endsWith('TAIL'));
      expect(peer.stderrTail()).toHaveLength(16);
    } finally {
      await peer.terminate();
    }
  });

  it('resolves exited with the failing status and a stderr tail to report with', async () => {
    // A launcher builds one message out of both, so the tail has to be there
    // by the time the status is: `classifySshExit` quotes the last line of it.
    const peer = spawnPort({
      argv: [BUN, '-e', 'process.stderr.write("final diagnostic"); process.exit(3)'],
    });

    expect(await peer.exited).toEqual({ code: 3, signal: null });
    expect(peer.stderrTail()).toContain('final diagnostic');
  });

  it('reports a child that cannot start as a closure with a resolved exit', async () => {
    const peer = spawnPort({ argv: [MISSING_COMMAND] });
    const closures: PortClosure[] = [];
    peer.port.onClosed((closure) => closures.push(closure));

    expect(await peer.exited).toEqual({ code: null, signal: null });
    expect(closures).toEqual([{ kind: 'closed', reason: expect.stringContaining('ENOENT') }]);
    expect(peer.stderrTail()).toContain('ENOENT');
  });

  it('gives the child exactly the environment it was handed', async () => {
    process.env.MANGO_TEST_LEAK = 'leaked';
    const chunks: string[] = [];
    const peer = spawnPort({
      argv: [BUN, '-e', 'process.stderr.write(JSON.stringify(process.env))'],
      env: { ...sanitizedEnv(), MANGO_TEST_MARKER: 'passed' },
      onStderr: (chunk) => chunks.push(decoder.decode(chunk)),
    });
    try {
      // Waiting for a `}` would fire on the first one in the stream; on a
      // platform with a larger environment that is a partial document.
      const childEnv = await waitForValue(() => parseEnv(chunks.join('')));

      expect(childEnv.MANGO_TEST_MARKER).toBe('passed');
      expect(childEnv.MANGO_TEST_LEAK).toBeUndefined();
    } finally {
      delete process.env.MANGO_TEST_LEAK;
      await peer.terminate();
    }
  });

  // POSIX only: Windows has no signal a process can ignore, so both escalation
  // steps collapse into terminating it (spec/transports/spawn.md, Termination).
  it.skipIf(WINDOWS)('escalates to SIGTERM and then SIGKILL', async () => {
    const streamed: string[] = [];
    const peer = spawnPort({
      argv: [BUN, STUBBORN_CHILD],
      terminateGraceMs: 100,
      killGraceMs: 250,
      onStderr: (chunk) => streamed.push(decoder.decode(chunk)),
    });
    await waitFor(() => streamed.join('').includes('ready'));

    const status = await peer.terminate();

    expect(streamed.join('')).toContain('ignoring SIGTERM');
    expect(status).toEqual({ code: null, signal: 'SIGKILL' });
  });

  it('terminates a child that ignores the end of its stdin', async () => {
    const streamed: string[] = [];
    const peer = spawnPort({
      argv: [BUN, STUBBORN_CHILD],
      terminateGraceMs: 100,
      killGraceMs: 250,
      onStderr: (chunk) => streamed.push(decoder.decode(chunk)),
    });
    await waitFor(() => streamed.join('').includes('ready'));

    const status = await peer.terminate();

    // The signal name is platform-specific; that the child is gone is not.
    expect(status.code === null || status.code !== 0).toBe(true);
  });

  it('terminates the child when the port closes on its own', async () => {
    // A line that is not a frame makes the port refuse and close by itself;
    // this child then ignores the end of its stdin, as a wedged peer would.
    const peer = spawnPort({
      argv: [
        BUN,
        '-e',
        'process.stdout.write("not a frame\\n"); process.on("SIGTERM", () => undefined); setInterval(() => undefined, 1000)',
      ],
      terminateGraceMs: 100,
      killGraceMs: 250,
    });
    const closure = await new Promise<PortClosure>((resolve) => peer.port.onClosed(resolve));
    expect(closure.code).toBe(CLOSE_CODES.PROTOCOL_ERROR);

    const outcome = await Promise.race([
      peer.exited.then((status) => ({ exited: true, status })),
      Bun.sleep(2_000).then(() => ({ exited: false, after: '2000 ms' })),
    ]);

    expect(outcome).toMatchObject({ exited: true });
  });

  it('resolves exited once, however often terminate is called', async () => {
    const peer = spawnPort({ argv: [BUN, ECHO_CHILD] });
    const session = new Session(peer.port, { peer: CONFORMANCE_A, livenessIntervalMs: false });
    await session.ready;

    const first = peer.terminate();
    const second = peer.terminate();

    expect(await first).toBe(await second);
    expect(await peer.exited).toBe(await first);
  });

  it('hides the child console window by default', () => {
    const spawner = new RecordingSpawn();

    spawnPort({ argv: ['runtime', '--stdio'] }, spawner.spawn);

    expect(spawner.only.command).toBe('runtime');
    expect(spawner.only.args).toEqual(['--stdio']);
    // A peer that speaks NDJSON on stdio has no window to show.
    expect(spawner.only.options.windowsHide).toBe(true);
  });

  it('lets a launcher keep the child console window', () => {
    const spawner = new RecordingSpawn();

    spawnPort({ argv: ['runtime'], windowsHide: false }, spawner.spawn);

    expect(spawner.only.options.windowsHide).toBe(false);
  });

  it('hands the child the working directory, the environment and three pipes', () => {
    const spawner = new RecordingSpawn();

    spawnPort({ argv: ['runtime'], cwd: '/tmp/work', env: { PATH: '/usr/bin' } }, spawner.spawn);

    expect(spawner.only.options).toMatchObject({
      cwd: '/tmp/work',
      env: { PATH: '/usr/bin' },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
  });

  it('refuses an empty argv, naming what it received', () => {
    expect(() => spawnPort({ argv: [] })).toThrow(
      'spawn argv is []; expected [command, ...args] with a non-empty command'
    );
    expect(() => spawnPort({ argv: [''] })).toThrow('spawn argv is [""]');
  });
});

describe('sanitizedEnv', () => {
  it('keeps the allowlist and drops everything else', () => {
    expect(
      sanitizedEnv({
        PATH: '/usr/bin',
        HOME: '/home/u',
        USERPROFILE: 'C:\\Users\\u',
        SYSTEMROOT: 'C:\\Windows',
        TEMP: '/tmp',
        TMP: '/tmp',
        TMPDIR: '/tmp',
        LANG: 'en_US.UTF-8',
        LC_ALL: 'C',
        TERM: 'xterm',
        SHELL: '/bin/sh',
        XDG_RUNTIME_DIR: '/run/user/1000',
        EDITOR: 'vi',
        SSH_AUTH_SOCK: '/tmp/agent.sock',
        MANGO_HUB_URL: 'wss://hub',
      })
    ).toEqual({
      PATH: '/usr/bin',
      HOME: '/home/u',
      USERPROFILE: 'C:\\Users\\u',
      SYSTEMROOT: 'C:\\Windows',
      TEMP: '/tmp',
      TMP: '/tmp',
      TMPDIR: '/tmp',
      LANG: 'en_US.UTF-8',
      LC_ALL: 'C',
      TERM: 'xterm',
      SHELL: '/bin/sh',
      XDG_RUNTIME_DIR: '/run/user/1000',
    });
  });

  it('strips a secret-shaped name even when the allowlist admitted its family', () => {
    expect(
      sanitizedEnv({
        LANG: 'C',
        LC_SECRET: 'shh',
        LC_API_TOKEN: 'shh',
        LC_SIGNING_KEY: 'shh',
        LC_PASSWORD_FILE: 'shh',
      })
    ).toEqual({ LANG: 'C' });
  });

  it('matches names case-insensitively, the way Windows spells them', () => {
    expect(
      sanitizedEnv({ Path: 'C:\\bin', SystemRoot: 'C:\\Windows', ProgramData: 'C:\\ProgramData' })
    ).toEqual({ Path: 'C:\\bin', SystemRoot: 'C:\\Windows' });
  });

  it('adds what the application passes on purpose, verbatim', () => {
    expect(sanitizedEnv({ PATH: '/usr/bin' }, { MANGO_TOKEN: 'on-purpose' })).toEqual({
      PATH: '/usr/bin',
      MANGO_TOKEN: 'on-purpose',
    });
  });
});

/** The child's environment, once enough of the document has arrived to parse. */
function parseEnv(text: string): Record<string, string> | undefined {
  try {
    return JSON.parse(text) as Record<string, string>;
  } catch {
    return undefined;
  }
}

/** Polls until `read` produces a value, for a stream that arrives in pieces. */
async function waitForValue<T>(read: () => T | undefined, timeoutMs = 10_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`no value within ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Polls until `ready` holds, so a test never guesses how long a pipe takes. */
async function waitFor(ready: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!ready()) {
    if (Date.now() > deadline) throw new Error(`condition did not hold within ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe('withErrorCode', () => {
  it('names the code a runtime left off the message', () => {
    const bare = Object.assign(new Error('Executable not found in $PATH: "x"'), { code: 'ENOENT' });
    const named = withErrorCode(bare);
    expect(named.message).toBe('ENOENT: Executable not found in $PATH: "x"');
    expect((named as { code?: string }).code).toBe('ENOENT');
    expect(named.cause).toBe(bare);
  });

  it('leaves a message that already names its code, and one with no code, alone', () => {
    const node = Object.assign(new Error('spawn x ENOENT'), { code: 'ENOENT' });
    expect(withErrorCode(node)).toBe(node);
    const plain = new Error('something else');
    expect(withErrorCode(plain)).toBe(plain);
  });
});
