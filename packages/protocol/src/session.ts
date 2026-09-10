import { CLOSE_CODES, isFatalCloseCode } from './close';
import { DEFAULT_MAX_FRAME_BYTES, measureFrameBytes } from './codec/ndjson';
import { type CodecError, RESERVED_ERROR_CODES, RemoteError } from './errors';
import { Listeners } from './listeners';
import type { Port, PortClosure } from './port';
import { isReservedMethodName, isValidMethodName } from './schemas/common';
import type {
  ErrorPayload,
  EventFrame,
  Frame,
  HelloFrame,
  Limits,
  PeerInfo,
  RequestFrame,
} from './schemas/frames';
import { negotiate, PROTOCOL_VERSION, type ProtocolVersion } from './version';

/** What the far peer announced in its `hello`, plus the negotiated minor. */
export interface RemotePeer {
  readonly peer: PeerInfo;
  readonly protocol: ProtocolVersion;
  readonly capabilities: Readonly<Record<string, unknown>>;
  readonly limits?: Limits;
  readonly effectiveMinor: number;
}

export interface RequestOptions {
  /** Aborting sends `cancel`; the promise settles when the peer answers. */
  readonly signal?: AbortSignal;
  /** Local deadline: sends `cancel`, rejects with `TIMEOUT`, ignores the late answer. */
  readonly timeoutMs?: number;
}

export interface EventInput {
  readonly topic: string;
  readonly payload: unknown;
  /** Correlates one multi-frame stream; sequence numbers are per stream. */
  readonly streamId?: string;
  /** Marks the last frame of the stream and releases its counter. */
  readonly end?: true;
}

export interface HandlerContext {
  /** Aborted by a `cancel` frame or by the session closing. */
  readonly signal: AbortSignal;
  readonly id: string;
  readonly method: string;
  readonly session: Session;
}

/**
 * Answers one request. Throw a `RemoteError` to choose the wire code; any
 * other throw becomes `INTERNAL`, and an `AbortError` becomes `CANCELLED`.
 */
export type RequestHandler = (params: unknown, context: HandlerContext) => unknown;

/** Why the session ended, in the vocabulary of the close-code table. */
export interface SessionClosure {
  readonly code: number;
  readonly reason?: string;
  /** True for codes redialing cannot change (`4401`, `4403`, `4409`, `4426`). */
  readonly fatal: boolean;
  /** Present when a refused record ended the session. */
  readonly error?: CodecError;
}

export type SessionState = 'handshaking' | 'ready' | 'closed';

export interface SessionTimers {
  readonly setTimeout: (callback: () => void, ms: number) => unknown;
  readonly clearTimeout: (handle: unknown) => void;
  readonly setInterval: (callback: () => void, ms: number) => unknown;
  readonly clearInterval: (handle: unknown) => void;
}

export interface SessionOptions {
  /** Who this side is: name, release string, role label. */
  readonly peer: PeerInfo;
  /** The application's capability object; `{}` when omitted. */
  readonly capabilities?: Readonly<Record<string, unknown>>;
  /** Highest wire version this side speaks; the SDK's own by default. */
  readonly protocol?: ProtocolVersion;
  /**
   * Largest frame this side accepts. Defaults to the port's own decoder limit.
   * Announced in `hello.limits` when it is below the protocol default, and the
   * lower of both sides' ceilings bounds every frame this side sends.
   */
  readonly maxFrameBytes?: number;
  /** How long to wait for the peer's `hello`; 15 seconds by default. */
  readonly handshakeTimeoutMs?: number;
  /** Ping cadence after the handshake; 20 seconds by default, `false` disables. */
  readonly livenessIntervalMs?: number | false;
  /** Initial handlers; `handle()` adds more at any time. */
  readonly handlers?: Readonly<Record<string, RequestHandler>>;
  /** Prefix of generated request ids; `r` by default. */
  readonly requestIdPrefix?: string;
  /** Injected for tests; the global timers by default. */
  readonly timers?: SessionTimers;
}

const DEFAULT_HANDSHAKE_TIMEOUT_MS = 15_000;
const DEFAULT_LIVENESS_INTERVAL_MS = 20_000;

interface PendingRequest {
  readonly method: string;
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: Error) => void;
  readonly cleanup: () => void;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (error: Error) => void;
}

/**
 * A symmetric Mango Protocol session over any port.
 *
 * Either side may request, answer, emit and cancel. Construct it once the
 * transport is open: the constructor sends `hello`, and `ready` settles when
 * the peer's `hello` has arrived and the majors agree.
 *
 * @example
 * const session = new Session(port, {
 *   peer: { name: 'example-runtime', version: '0.1.0', role: 'runtime' },
 *   handlers: { 'text.echo': (params) => params },
 * });
 * await session.ready;
 * const answer = await session.request('text.echo', { text: 'hi' });
 */
export class Session {
  readonly #port: Port;
  readonly #options: SessionOptions;
  readonly #timers: SessionTimers;
  readonly #localProtocol: ProtocolVersion;
  readonly #localMaxFrameBytes: number;
  readonly #handlers = new Map<string, RequestHandler>();
  readonly #pending = new Map<string, PendingRequest>();
  readonly #active = new Map<string, AbortController>();
  readonly #eventSequences = new Map<string, number>();
  readonly #eventListeners = new Listeners<EventFrame>();
  readonly #pongListeners = new Listeners<void>();
  readonly #closeListeners = new Listeners<SessionClosure>();
  readonly #readyDeferred: Deferred<RemotePeer>;
  readonly #requestIdPrefix: string;
  #state: SessionState = 'handshaking';
  #remote?: RemotePeer;
  #closure?: SessionClosure;
  #announcedClose?: { readonly code: number; readonly reason?: string };
  #requestSequence = 0;
  #handshakeTimer?: unknown;
  #livenessTimer?: unknown;
  #awaitingPong = false;
  #detachFrames: () => void;
  #detachClosed: () => void;

  constructor(port: Port, options: SessionOptions) {
    this.#port = port;
    this.#options = options;
    this.#timers = options.timers ?? globalTimers();
    this.#localProtocol = options.protocol ?? PROTOCOL_VERSION;
    this.#localMaxFrameBytes =
      options.maxFrameBytes ?? port.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    this.#requestIdPrefix = options.requestIdPrefix ?? 'r';
    for (const [method, handler] of Object.entries(options.handlers ?? {})) {
      this.#handlers.set(method, handler);
    }
    this.#readyDeferred = deferred<RemotePeer>();
    this.#detachFrames = port.onFrame((frame) => this.#receive(frame));
    this.#detachClosed = port.onClosed((closure) => this.#handlePortClosed(closure));
    this.#startHandshake();
  }

  /** Settles once both hellos have crossed; rejects when the handshake cannot complete. */
  get ready(): Promise<RemotePeer> {
    return this.#readyDeferred.promise;
  }

  get state(): SessionState {
    return this.#state;
  }

  /** The peer's announcement. Throws before the handshake completes. */
  get remote(): RemotePeer {
    if (!this.#remote) {
      throw new RemoteError(
        RESERVED_ERROR_CODES.UNAVAILABLE,
        'The session handshake has not completed; expected a ready session, received one still handshaking.'
      );
    }
    return this.#remote;
  }

  /** Why the session closed, once it has. */
  get closure(): SessionClosure | undefined {
    return this.#closure;
  }

  /** The frame ceiling this side may send: the lower of both announced limits. */
  get sendLimitBytes(): number {
    const remote = this.#remote?.limits?.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    return Math.min(this.#localMaxFrameBytes, remote);
  }

  /** Registers a handler; returns the function that removes it. */
  handle(method: string, handler: RequestHandler): () => void {
    this.#handlers.set(method, handler);
    return () => {
      if (this.#handlers.get(method) === handler) this.#handlers.delete(method);
    };
  }

  /** Sends a request and resolves with its `result`, or rejects with a `RemoteError`. */
  async request(method: string, params: unknown, options: RequestOptions = {}): Promise<unknown> {
    if (!isValidMethodName(method) || isReservedMethodName(method)) {
      throw new RemoteError(
        RESERVED_ERROR_CODES.INVALID_REQUEST,
        `Method "${method}" is not a valid, unreserved method name; expected two or more dot-separated lowercase segments outside rpc.`
      );
    }
    await this.ready;
    if (this.#state === 'closed') throw this.#unavailable(method);

    const id = `${this.#requestIdPrefix}-${++this.#requestSequence}`;
    const frame: RequestFrame = { type: 'req', id, method, params };
    this.#assertFits(frame, `Request "${method}"`);

    return await new Promise<unknown>((resolve, reject) => {
      let timeout: unknown;
      const abort = (): void => {
        this.#trySend({ type: 'cancel', id });
      };
      const cleanup = (): void => {
        if (timeout !== undefined) this.#timers.clearTimeout(timeout);
        options.signal?.removeEventListener('abort', abort);
      };
      this.#pending.set(id, { method, resolve, reject, cleanup });
      options.signal?.addEventListener('abort', abort, { once: true });
      if (options.timeoutMs !== undefined) {
        timeout = this.#timers.setTimeout(() => {
          const pending = this.#pending.get(id);
          if (!pending) return;
          this.#pending.delete(id);
          pending.cleanup();
          this.#trySend({ type: 'cancel', id });
          reject(
            new RemoteError(
              RESERVED_ERROR_CODES.TIMEOUT,
              `Request "${method}" timed out after ${options.timeoutMs}ms.`,
              { method, timeoutMs: options.timeoutMs }
            )
          );
        }, options.timeoutMs);
      }
      try {
        this.#port.send(frame);
      } catch (error) {
        this.#pending.delete(id);
        cleanup();
        reject(this.#asSendFailure(error, method));
        return;
      }
      if (options.signal?.aborted) abort();
    });
  }

  /**
   * Publishes an event. Sequence numbers are per stream key (`streamId`, else
   * `topic`); `end` releases the counter. Returns false, and sends nothing,
   * before the handshake completes or after the session closed.
   */
  emit(event: EventInput): boolean {
    if (this.#state !== 'ready') return false;
    const key = event.streamId ?? event.topic;
    const seq = this.#eventSequences.get(key) ?? 0;
    const frame: EventFrame = {
      type: 'evt',
      topic: event.topic,
      seq,
      ...(event.streamId !== undefined ? { streamId: event.streamId } : {}),
      payload: event.payload,
      ...(event.end ? { end: true as const } : {}),
    };
    this.#assertFits(frame, `Event "${event.topic}"`);
    if (event.end) this.#eventSequences.delete(key);
    else this.#eventSequences.set(key, seq + 1);
    this.#port.send(frame);
    return true;
  }

  onEvent(listener: (event: EventFrame) => void): () => void {
    return this.#eventListeners.add(listener);
  }

  /** Sends a protocol ping; the peer answers with `pong`. */
  ping(): void {
    this.#port.send({ type: 'ping' });
  }

  onPong(listener: () => void): () => void {
    return this.#pongListeners.add(listener);
  }

  /** Fires once when the session ends; immediately (on a microtask) if it already has. */
  onClose(listener: (closure: SessionClosure) => void): () => void {
    if (this.#closure) {
      const closure = this.#closure;
      let cancelled = false;
      queueMicrotask(() => {
        if (!cancelled) listener(closure);
      });
      return () => {
        cancelled = true;
      };
    }
    return this.#closeListeners.add(listener);
  }

  /** Closes the transport with a reason code and settles everything in flight. */
  close(code: number = CLOSE_CODES.RELEASED, reason?: string): void {
    if (this.#state === 'closed') return;
    try {
      this.#port.close(code, reason);
    } catch {
      // The transport is already gone; the teardown below is what matters.
    }
    this.#teardown({
      code,
      ...(reason !== undefined ? { reason } : {}),
      fatal: isFatalCloseCode(code),
    });
  }

  #startHandshake(): void {
    const hello: HelloFrame = {
      type: 'hello',
      protocol: this.#localProtocol,
      peer: this.#options.peer,
      capabilities: { ...(this.#options.capabilities ?? {}) },
      ...(this.#localMaxFrameBytes < DEFAULT_MAX_FRAME_BYTES
        ? { limits: { maxFrameBytes: this.#localMaxFrameBytes } }
        : {}),
    };
    try {
      this.#port.send(hello);
    } catch (error) {
      // The transport can go away between construction and the first send: a
      // peer that refuses the credential closes the socket the moment it opens.
      this.#failHandshake(
        new RemoteError(
          RESERVED_ERROR_CODES.UNAVAILABLE,
          `The transport refused the hello: ${describe(error)}`
        ),
        CLOSE_CODES.RELEASED,
        'hello could not be sent'
      );
      return;
    }
    const timeoutMs = this.#options.handshakeTimeoutMs ?? DEFAULT_HANDSHAKE_TIMEOUT_MS;
    this.#handshakeTimer = this.#timers.setTimeout(() => {
      if (this.#state !== 'handshaking') return;
      this.#failHandshake(
        new RemoteError(
          RESERVED_ERROR_CODES.UNAVAILABLE,
          `The peer did not send hello within ${timeoutMs}ms.`,
          { timeoutMs }
        ),
        CLOSE_CODES.PROTOCOL_ERROR,
        'handshake timeout'
      );
    }, timeoutMs);
  }

  #failHandshake(error: RemoteError, code: number, reason: string): void {
    this.#readyDeferred.reject(error);
    this.close(code, reason);
  }

  #receive(frame: Frame): void {
    switch (frame.type) {
      case 'hello':
        this.#receiveHello(frame);
        return;
      case 'req':
        void this.#dispatch(frame);
        return;
      case 'res':
        this.#settle(frame.id, (pending) => pending.resolve(frame.result));
        return;
      case 'err':
        this.#settle(frame.id, (pending) =>
          pending.reject(
            new RemoteError(frame.error.code, frame.error.message, frame.error.details)
          )
        );
        return;
      case 'evt':
        if (this.#state === 'ready') this.#eventListeners.emit(frame);
        return;
      case 'cancel':
        this.#active.get(frame.id)?.abort();
        return;
      case 'ping':
        this.#trySend({ type: 'pong' });
        return;
      case 'pong':
        this.#awaitingPong = false;
        this.#pongListeners.emit();
        return;
      case 'close':
        this.#announcedClose = {
          code: frame.code,
          ...(frame.reason !== undefined ? { reason: frame.reason } : {}),
        };
        this.#teardown({
          code: frame.code,
          ...(frame.reason !== undefined ? { reason: frame.reason } : {}),
          fatal: isFatalCloseCode(frame.code),
        });
        return;
      default:
        return;
    }
  }

  #receiveHello(frame: HelloFrame): void {
    if (this.#state !== 'handshaking') {
      this.close(CLOSE_CODES.PROTOCOL_ERROR, 'duplicate hello');
      return;
    }
    const negotiation = negotiate(this.#localProtocol, frame.protocol);
    if (!negotiation.ok) {
      this.#failHandshake(
        new RemoteError(
          RESERVED_ERROR_CODES.PROTOCOL_MISMATCH,
          `Peer "${frame.peer.name}" speaks wire major ${frame.protocol.major}; this session speaks major ${this.#localProtocol.major}.`,
          { local: this.#localProtocol, remote: frame.protocol, closeCode: negotiation.closeCode }
        ),
        negotiation.closeCode,
        'protocol version unsupported'
      );
      return;
    }
    this.#remote = {
      peer: frame.peer,
      protocol: frame.protocol,
      capabilities: frame.capabilities,
      ...(frame.limits !== undefined ? { limits: frame.limits } : {}),
      effectiveMinor: negotiation.effectiveMinor,
    };
    this.#timers.clearTimeout(this.#handshakeTimer);
    this.#handshakeTimer = undefined;
    this.#state = 'ready';
    this.#startLiveness();
    this.#readyDeferred.resolve(this.#remote);
  }

  #startLiveness(): void {
    const interval = this.#options.livenessIntervalMs ?? DEFAULT_LIVENESS_INTERVAL_MS;
    if (interval === false) return;
    this.#livenessTimer = this.#timers.setInterval(() => {
      if (this.#state !== 'ready') return;
      // One missed round trip is the signal: the interval is already several
      // times the round trip a healthy peer needs.
      if (this.#awaitingPong) {
        this.close(CLOSE_CODES.RELEASED, 'liveness timeout');
        return;
      }
      this.#awaitingPong = true;
      this.#trySend({ type: 'ping' });
    }, interval);
  }

  async #dispatch(frame: RequestFrame): Promise<void> {
    if (this.#state !== 'ready') {
      this.#respondError(frame.id, {
        code: RESERVED_ERROR_CODES.UNAVAILABLE,
        message:
          'The session handshake has not completed; requests are refused until both hellos have crossed.',
      });
      return;
    }
    if (this.#active.has(frame.id)) {
      this.#respondError(frame.id, {
        code: RESERVED_ERROR_CODES.INVALID_REQUEST,
        message: `Request id "${frame.id}" is already in flight; expected an id unique among the sender's pending requests.`,
        details: { id: frame.id },
      });
      return;
    }
    if (isReservedMethodName(frame.method)) {
      this.#respondError(frame.id, {
        code: RESERVED_ERROR_CODES.INVALID_REQUEST,
        message: `Method "${frame.method}" is reserved; the rpc. segment belongs to the protocol and defines no method in wire 1.0.`,
        details: { method: frame.method },
      });
      return;
    }
    const handler = this.#handlers.get(frame.method);
    if (!handler) {
      this.#respondError(frame.id, {
        code: RESERVED_ERROR_CODES.METHOD_UNSUPPORTED,
        message: `Method "${frame.method}" has no handler on this peer.`,
        details: { method: frame.method },
      });
      return;
    }

    const controller = new AbortController();
    this.#active.set(frame.id, controller);
    try {
      const result = await handler(frame.params, {
        signal: controller.signal,
        id: frame.id,
        method: frame.method,
        session: this,
      });
      this.#respondResult(frame.id, frame.method, result === undefined ? null : result);
    } catch (error) {
      this.#respondError(frame.id, errorPayloadFor(error, controller.signal));
    } finally {
      this.#active.delete(frame.id);
    }
  }

  #respondResult(id: string, method: string, result: unknown): void {
    const frame: Frame = { type: 'res', id, result };
    const bytes = measureFrameBytes(frame);
    const limit = this.sendLimitBytes;
    if (bytes > limit) {
      this.#respondError(id, {
        code: RESERVED_ERROR_CODES.FRAME_TOO_LARGE,
        message: `The result of "${method}" encodes to ${bytes} bytes; the session limit is ${limit} bytes.`,
        details: { method, bytes, limit },
      });
      return;
    }
    this.#trySend(frame);
  }

  #respondError(id: string, error: ErrorPayload): void {
    this.#trySend({ type: 'err', id, error });
  }

  #settle(id: string, settle: (pending: PendingRequest) => void): void {
    const pending = this.#pending.get(id);
    if (!pending) return;
    this.#pending.delete(id);
    pending.cleanup();
    settle(pending);
  }

  #assertFits(frame: Frame, what: string): void {
    const bytes = measureFrameBytes(frame);
    const limit = this.sendLimitBytes;
    if (bytes <= limit) return;
    throw new RemoteError(
      RESERVED_ERROR_CODES.FRAME_TOO_LARGE,
      `${what} encodes to ${bytes} bytes; the session limit is ${limit} bytes.`,
      { bytes, limit }
    );
  }

  /** Sends when the port is still there; a late frame after teardown has no destination. */
  #trySend(frame: Frame): void {
    if (this.#state === 'closed') return;
    try {
      this.#port.send(frame);
    } catch {
      // The port reports its own closure; nothing else to do with a lost frame.
    }
  }

  #handlePortClosed(closure: PortClosure): void {
    if (closure.kind === 'protocol-error') {
      this.#teardown({
        code: closure.code,
        reason: closure.error.message,
        fatal: isFatalCloseCode(closure.code),
        error: closure.error,
      });
      return;
    }
    const announced = this.#announcedClose;
    const code = closure.code ?? announced?.code ?? CLOSE_CODES.RELEASED;
    const reason = closure.reason ?? announced?.reason;
    this.#teardown({
      code,
      ...(reason !== undefined ? { reason } : {}),
      fatal: isFatalCloseCode(code),
    });
  }

  #teardown(closure: SessionClosure): void {
    if (this.#state === 'closed') return;
    this.#state = 'closed';
    this.#closure = closure;
    this.#detachFrames();
    this.#detachClosed();
    if (this.#handshakeTimer !== undefined) this.#timers.clearTimeout(this.#handshakeTimer);
    if (this.#livenessTimer !== undefined) this.#timers.clearInterval(this.#livenessTimer);
    this.#handshakeTimer = undefined;
    this.#livenessTimer = undefined;
    this.#readyDeferred.reject(
      new RemoteError(
        closure.code === CLOSE_CODES.PROTOCOL_MISMATCH
          ? RESERVED_ERROR_CODES.PROTOCOL_MISMATCH
          : RESERVED_ERROR_CODES.UNAVAILABLE,
        `The session closed before the handshake completed (${closure.code}${closure.reason ? `: ${closure.reason}` : ''}).`,
        { closeCode: closure.code }
      )
    );
    for (const [id, pending] of this.#pending) {
      pending.cleanup();
      pending.reject(this.#unavailable(pending.method, closure, id));
    }
    this.#pending.clear();
    for (const controller of this.#active.values()) controller.abort();
    this.#active.clear();
    this.#eventSequences.clear();
    this.#eventListeners.clear();
    this.#pongListeners.clear();
    const listeners = this.#closeListeners;
    this.#closeListeners.emit(closure);
    listeners.clear();
  }

  #unavailable(method: string, closure = this.#closure, id?: string): RemoteError {
    const why = closure
      ? ` (closed with ${closure.code}${closure.reason ? `: ${closure.reason}` : ''})`
      : '';
    return new RemoteError(
      RESERVED_ERROR_CODES.UNAVAILABLE,
      `Request "${method}" cannot complete: the session is closed${why}.`,
      {
        method,
        ...(id !== undefined ? { id } : {}),
        ...(closure ? { closeCode: closure.code } : {}),
      }
    );
  }

  #asSendFailure(error: unknown, method: string): RemoteError {
    if (error instanceof RemoteError) return error;
    return new RemoteError(
      RESERVED_ERROR_CODES.UNAVAILABLE,
      `Request "${method}" could not be sent: ${describe(error)}`,
      { method }
    );
  }
}

/** Maps what a handler threw onto the wire error payload. */
function errorPayloadFor(error: unknown, signal: AbortSignal): ErrorPayload {
  if (error instanceof RemoteError) {
    return {
      code: error.code,
      message: error.message,
      ...(error.details !== undefined ? { details: { ...error.details } } : {}),
    };
  }
  // CANCELLED is the refusal the handler threw, or a failure after the cancel
  // arrived. A mutation that continued past its last refusal point can still
  // fail, and that failure is what the caller has to recover from.
  if (isAbortError(error) || signal.aborted) {
    return {
      code: RESERVED_ERROR_CODES.CANCELLED,
      message:
        error instanceof Error && error.message ? error.message : 'The request was cancelled.',
    };
  }
  return {
    code: RESERVED_ERROR_CODES.INTERNAL,
    message: error instanceof Error && error.message ? error.message : 'The handler failed.',
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError';
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function deferred<T>(): Deferred<T> {
  let resolve: (value: T) => void = () => undefined;
  let reject: (error: Error) => void = () => undefined;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  // The handshake can fail before anyone awaits `ready`; without a subscriber
  // the rejection would surface as unhandled.
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

function globalTimers(): SessionTimers {
  const unref = (handle: unknown): unknown => {
    (handle as { unref?: () => void }).unref?.();
    return handle;
  };
  return {
    setTimeout: (callback, ms) => unref(setTimeout(callback, ms)),
    clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
    setInterval: (callback, ms) => unref(setInterval(callback, ms)),
    clearInterval: (handle) => clearInterval(handle as ReturnType<typeof setInterval>),
  };
}
