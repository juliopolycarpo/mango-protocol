/**
 * The spawn launcher of spec/transports/spawn.md: start a child process and
 * speak stdio through its pipes. SSH, WSL and container launches are this
 * transport with a different argv in front.
 *
 * The launcher observes and reports; it never guesses why a child failed. It
 * exposes the exit status, a bounded tail of stderr and a termination sequence,
 * and leaves the classification to the caller (see `classifySshExit`).
 */

import {
  type ChildProcess,
  type SpawnOptions as ChildSpawnOptions,
  spawn,
} from 'node:child_process';
import { CLOSE_CODES } from '../close';
import { resolveByteCeiling } from '../codec/limits';
import type { Port, PortClosure } from '../port';
import type { Frame } from '../schemas/frames';
import { type ByteSink, createNdjsonPort, type NdjsonPortHandle } from './ndjson-port';
import { asError, createStreamPort, toBytes } from './node-stream';

/** How the child ended: an exit code, or the signal that killed it. */
export interface ExitStatus {
  readonly code: number | null;
  readonly signal: string | null;
}

export interface SpawnOptions {
  /** The command and its arguments. Never a shell string: arguments stay data. */
  readonly argv: readonly string[];
  readonly cwd?: string;
  /** The child's whole environment. `sanitizedEnv()` when omitted. */
  readonly env?: Readonly<Record<string, string>>;
  /** Largest line the decoder accepts; the 16 MiB default of §11 when absent. */
  readonly maxFrameBytes?: number;
  /** How much of the child's stderr to keep for an error report; 16 KiB. */
  readonly stderrTailBytes?: number;
  /** How long end of stdin has to work before `SIGTERM`; 2 seconds. */
  readonly terminateGraceMs?: number;
  /** How long `SIGTERM` has to work before `SIGKILL`; 2 seconds. */
  readonly killGraceMs?: number;
  /** Called with every stderr chunk, for a launcher that streams diagnostics. */
  readonly onStderr?: (chunk: Uint8Array) => void;
  /**
   * Hide the child's console window on Windows; `true` by default. A peer that
   * speaks NDJSON on stdio has nothing to show, and a wrapper launched through
   * a console host would otherwise flash a window at whoever is watching.
   */
  readonly windowsHide?: boolean;
}

/**
 * The one child-process call the launcher makes, injected so a test can see
 * what the launcher hands it without starting a process.
 *
 * @example
 * spawnPort({ argv: ['runtime'] }, (command, args, options) => fake(command, args, options));
 */
export type SpawnChild = (
  command: string,
  args: string[],
  options: ChildSpawnOptions
) => ChildProcess;

export interface SpawnedPeer {
  readonly port: Port;
  /** Undefined when the child never started. */
  readonly pid: number | undefined;
  /**
   * Resolves exactly once with how the child ended. A child that never started
   * resolves `{ code: null, signal: null }`; `stderrTail()` carries the reason.
   */
  readonly exited: Promise<ExitStatus>;
  /**
   * The last `stderrTailBytes` bytes the child wrote, decoded as UTF-8. Read it
   * next to `exited`; the very last chunk of a child that died mid-write is
   * best effort, as any tail of a pipe is.
   */
  stderrTail(): string;
  /** Closes stdin, then escalates to `SIGTERM` and `SIGKILL`. Idempotent. */
  terminate(): Promise<ExitStatus>;
}

/** Reference size of the stderr tail a launcher keeps (spawn.md, Launching). */
const DEFAULT_STDERR_TAIL_BYTES = 16 * 1024;

/** Reference grace periods of the termination sequence (spawn.md, Termination). */
const DEFAULT_TERMINATE_GRACE_MS = 2000;
const DEFAULT_KILL_GRACE_MS = 2000;

const WINDOWS = process.platform === 'win32';

/** Variables a child is allowed to inherit (spawn.md, Launching). */
const ENV_ALLOWLIST: readonly string[] = [
  'PATH',
  'HOME',
  'USERPROFILE',
  'SYSTEMROOT',
  'TEMP',
  'TMP',
  'TMPDIR',
  'LANG',
  'TERM',
  'SHELL',
  'XDG_RUNTIME_DIR',
];

/** Locale variables are a family, not a fixed list. */
const LOCALE_PREFIX = 'LC_';

/** Secret-shaped names, stripped even when they survived the allowlist. */
const SECRET_PATTERNS: readonly RegExp[] = [/_TOKEN$/, /_SECRET$/, /_KEY$/, /PASSWORD/];

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * The environment a launched child inherits: an allowlist of the variables a
 * program needs to run, with secret-shaped names removed, plus whatever the
 * application adds on purpose.
 *
 * Names are matched case-insensitively because Windows spells its variables in
 * mixed case (`Path`, `SystemRoot`); the original spelling is what the child
 * receives.
 *
 * @example
 * sanitizedEnv(process.env, { MANGO_TOKEN: token }); // PATH, HOME, … plus the token
 */
export function sanitizedEnv(
  source: NodeJS.ProcessEnv = process.env,
  extra: Readonly<Record<string, string>> = {}
): Record<string, string> {
  const kept: Record<string, string> = {};
  for (const [name, value] of Object.entries(source)) {
    if (value === undefined) continue;
    const upper = name.toUpperCase();
    if (!ENV_ALLOWLIST.includes(upper) && !upper.startsWith(LOCALE_PREFIX)) continue;
    if (SECRET_PATTERNS.some((pattern) => pattern.test(upper))) continue;
    kept[name] = value;
  }
  return { ...kept, ...extra };
}

/**
 * Starts a child process and speaks stdio through its pipes.
 *
 * stdout is the frame stream, stdin is the frame stream in the other direction,
 * and stderr goes to a bounded tail plus `onStderr`. A child that cannot start
 * (`ENOENT`, `EACCES`) closes the port, resolves `exited` with
 * `{ code: null, signal: null }` and puts the spawn error in `stderrTail()`.
 *
 * `spawnChild` is the child-process call itself, injected so a test can assert
 * what the launcher asks for without starting anything.
 *
 * @example
 * const peer = spawnPort({ argv: ['bun', 'runtime.ts'] });
 * const session = new Session(peer.port, { peer: { name: 'hub', version: '1', role: 'hub' } });
 * await peer.terminate();
 */
export function spawnPort(options: SpawnOptions, spawnChild: SpawnChild = spawn): SpawnedPeer {
  const command = options.argv[0];
  if (command === undefined || command.length === 0) {
    throw new Error(
      `spawn argv is ${JSON.stringify(options.argv)}; expected [command, ...args] with a non-empty command`
    );
  }
  const tail = new BoundedTail(
    resolveByteCeiling('stderrTailBytes', options.stderrTailBytes, DEFAULT_STDERR_TAIL_BYTES, 1)
  );
  const exit = deferredExit();

  const child = start(spawnChild, command, options.argv.slice(1), options, tail, exit);
  const limit = options.maxFrameBytes !== undefined ? { maxFrameBytes: options.maxFrameBytes } : {};
  const handle =
    child?.stdin && child.stdout
      ? createStreamPort(child.stdout, child.stdin, limit)
      : createNdjsonPort({ sink: unspawnedSink(), ...limit });
  wire(child, handle, options, tail, exit);

  let termination: Promise<ExitStatus> | undefined;
  const terminate = (): Promise<ExitStatus> => {
    termination ??= escalate(child, handle, exit.promise, options);
    return termination;
  };

  return {
    port: launcherPort(handle, terminate),
    pid: child?.pid,
    exited: exit.promise,
    stderrTail: () => tail.text(),
    terminate,
  };
}

/**
 * Spawns the child, or records the failure. A synchronous throw and an
 * asynchronous `error` event mean the same thing to the launcher, and Bun and
 * Node do not agree on which one an unusable argv produces.
 */
function start(
  spawnChild: SpawnChild,
  command: string,
  args: readonly string[],
  options: SpawnOptions,
  tail: BoundedTail,
  exit: DeferredExit
): ChildProcess | undefined {
  try {
    return spawnChild(command, [...args], {
      ...(options.cwd !== undefined ? { cwd: options.cwd } : {}),
      env: options.env !== undefined ? { ...options.env } : sanitizedEnv(),
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: options.windowsHide ?? true,
    });
  } catch (cause) {
    tail.appendText(`\n${withErrorCode(asError(cause)).message}\n`);
    exit.settle({ code: null, signal: null });
    return undefined;
  }
}

/** Connects the child's three pipes to the port, the tail and the exit promise. */
function wire(
  child: ChildProcess | undefined,
  handle: NdjsonPortHandle,
  options: SpawnOptions,
  tail: BoundedTail,
  exit: DeferredExit
): void {
  if (child === undefined) {
    // Nothing will ever drive the port, so report the closure once the caller
    // has had the chance to subscribe.
    queueMicrotask(() => handle.failed(new Error(tail.text().trim() || 'the child never started')));
    return;
  }
  // `exit` rather than `close`: a descendant that inherited the pipes can hold
  // `close` off for as long as it likes, and a shutdown must not wait on one.
  // Node documents that `exit` may precede the stdio close, but Bun and Node
  // both delivered the child's whole stderr first in every probe up to 1 MiB,
  // so the tail a caller reads at `exited` was complete each time.
  child.on('exit', (code, signal) => exit.settle({ code, signal }));
  child.on('error', (cause) => {
    const error = withErrorCode(asError(cause));
    tail.appendText(`\n${error.message}\n`);
    handle.failed(error);
    exit.settle({ code: null, signal: null });
  });
  if (child.stderr) {
    child.stderr.on('data', (chunk: unknown) => {
      const bytes = toBytes(chunk);
      tail.append(bytes);
      options.onStderr?.(bytes);
    });
    child.stderr.on('error', () => {
      // Diagnostics are best effort; a broken stderr must not fail the session.
    });
  }
}

/**
 * The port the launcher hands out: the NDJSON port, plus a `close` that starts
 * the termination sequence the child's lifetime depends on.
 */
function launcherPort(handle: NdjsonPortHandle, terminate: () => Promise<ExitStatus>): Port {
  const inner = handle.port;
  return {
    ...(inner.maxFrameBytes !== undefined ? { maxFrameBytes: inner.maxFrameBytes } : {}),
    send: (frame: Frame) => inner.send(frame),
    onFrame: (listener: (frame: Frame) => void) => inner.onFrame(listener),
    onClosed: (listener: (closure: PortClosure) => void) => inner.onClosed(listener),
    close: (code: number, reason?: string) => {
      inner.close(code, reason);
      void terminate();
    },
  };
}

/**
 * End of stdin, then `SIGTERM`, then `SIGKILL`, each after its grace period.
 * Windows has no POSIX signals, so both signal steps collapse into terminating
 * the process (spawn.md, Termination).
 */
async function escalate(
  child: ChildProcess | undefined,
  handle: NdjsonPortHandle,
  exited: Promise<ExitStatus>,
  options: SpawnOptions
): Promise<ExitStatus> {
  // A `close` frame first, so a conforming child knows why it is leaving; the
  // port ends stdin behind it, which is step 1 of the sequence.
  handle.port.close(CLOSE_CODES.RELEASED, 'launcher terminating');
  if (child?.stdin?.writable) child.stdin.end();
  if (child === undefined) return await exited;

  const terminateGrace = options.terminateGraceMs ?? DEFAULT_TERMINATE_GRACE_MS;
  if (await settledWithin(exited, terminateGrace)) return await exited;
  kill(child, 'SIGTERM');

  const killGrace = options.killGraceMs ?? DEFAULT_KILL_GRACE_MS;
  if (await settledWithin(exited, killGrace)) return await exited;
  kill(child, 'SIGKILL');
  return await exited;
}

function kill(child: ChildProcess, signal: 'SIGTERM' | 'SIGKILL'): void {
  if (child.exitCode !== null || child.signalCode !== null) return;
  // Windows terminates rather than signals; `kill('SIGKILL')` there would be
  // the same call with a name the platform cannot deliver.
  if (WINDOWS) child.kill();
  else child.kill(signal);
}

/** True when the promise settled inside the grace, false when the grace ran out. */
function settledWithin(promise: Promise<unknown>, ms: number): Promise<boolean> {
  return new Promise((resolve) => {
    const grace = setTimeout(() => resolve(false), ms);
    grace.unref();
    void promise.then(
      () => {
        clearTimeout(grace);
        resolve(true);
      },
      () => {
        clearTimeout(grace);
        resolve(true);
      }
    );
  });
}

/** The sink of a child that never started: every write is a broken pipe. */
function unspawnedSink(): ByteSink {
  return {
    write: () => {
      throw new Error(
        'the child process never started; expected a running child, received a failed spawn'
      );
    },
    end: () => {
      // There is no pipe to release.
    },
  };
}

interface DeferredExit {
  readonly promise: Promise<ExitStatus>;
  settle(status: ExitStatus): void;
}

/** Resolves exactly once, whichever of `exit`, `error` or a throw came first. */
function deferredExit(): DeferredExit {
  let resolve: (status: ExitStatus) => void = () => undefined;
  const promise = new Promise<ExitStatus>((settle) => {
    resolve = settle;
  });
  let settled = false;
  return {
    promise,
    settle: (status) => {
      if (settled) return;
      settled = true;
      resolve(status);
    },
  };
}

/** The last N bytes written to it, so a diagnostic never grows without bound. */
class BoundedTail {
  readonly #limit: number;
  #bytes = new Uint8Array(0);

  constructor(limit: number) {
    this.#limit = limit;
  }

  append(chunk: Uint8Array): void {
    const merged = new Uint8Array(this.#bytes.byteLength + chunk.byteLength);
    merged.set(this.#bytes, 0);
    merged.set(chunk, this.#bytes.byteLength);
    this.#bytes =
      merged.byteLength <= this.#limit ? merged : merged.slice(merged.byteLength - this.#limit);
  }

  appendText(text: string): void {
    this.append(encoder.encode(text));
  }

  text(): string {
    return decoder.decode(this.#bytes);
  }
}

/**
 * A spawn error whose message names its code. Node says `spawn x ENOENT`,
 * Bun on Windows says `Executable not found in $PATH: "x"` with the code only
 * on the object; a launcher that reports the message needs the code in it.
 *
 * @example
 * withErrorCode(Object.assign(new Error('not found'), { code: 'ENOENT' })).message; // 'ENOENT: not found'
 */
export function withErrorCode(error: Error): Error {
  const code = (error as { code?: unknown }).code;
  if (typeof code !== 'string' || code.length === 0 || error.message.includes(code)) return error;
  const named = new Error(`${code}: ${error.message}`, { cause: error });
  (named as { code?: string }).code = code;
  return named;
}
