import { describe, expect, it } from 'bun:test';
import { CLOSE_CODES } from '../src/close';
import { RESERVED_ERROR_CODES, RemoteError } from '../src/errors';
import type { Port } from '../src/port';
import type { Frame, HelloFrame } from '../src/schemas/frames';
import { Session, type SessionOptions } from '../src/session';
import { createInProcessPortPair } from '../src/transports/in-process';

const HUB: SessionOptions['peer'] = { name: 'hub', version: '1.0.0', role: 'hub' };
const RUNTIME: SessionOptions['peer'] = { name: 'runtime', version: '1.0.0', role: 'runtime' };

const rawHello = (): HelloFrame => ({
  type: 'hello',
  protocol: { major: 1, minor: 0 },
  peer: RUNTIME,
  capabilities: {},
});

/** Collects every frame a raw port receives so a test can inspect the wire. */
class FrameRecorder {
  readonly frames: Frame[] = [];
  readonly #waiters: ((frame: Frame) => void)[] = [];

  constructor(port: Port) {
    port.onFrame((frame) => {
      this.frames.push(frame);
      for (const waiter of this.#waiters.splice(0)) waiter(frame);
    });
  }

  next(): Promise<Frame> {
    return new Promise((resolve) => this.#waiters.push(resolve));
  }

  async until(predicate: (frame: Frame) => boolean): Promise<Frame> {
    const found = this.frames.find(predicate);
    if (found) return found;
    for (;;) {
      const frame = await this.next();
      if (predicate(frame)) return frame;
    }
  }
}

function pair(options: Partial<SessionOptions> = {}) {
  const ports = createInProcessPortPair();
  const hub = new Session(ports.a, { peer: HUB, livenessIntervalMs: false, ...options });
  const runtime = new Session(ports.b, {
    peer: RUNTIME,
    livenessIntervalMs: false,
    handlers: { 'test.echo': (params) => params },
  });
  return { ports, hub, runtime };
}

function tick(ms = 5): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Session handshake', () => {
  it('resolves ready on both sides with the peer announcement', async () => {
    const { hub, runtime } = pair({ capabilities: { audit: true } });
    const [fromHub, fromRuntime] = await Promise.all([hub.ready, runtime.ready]);
    expect(fromHub.peer).toEqual(RUNTIME);
    expect(fromRuntime.peer).toEqual(HUB);
    expect(fromRuntime.capabilities).toEqual({ audit: true });
    expect(hub.remote.effectiveMinor).toBe(0);
    hub.close();
  });

  it('throws from remote before the handshake completes', () => {
    const ports = createInProcessPortPair();
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    expect(() => session.remote).toThrow(RemoteError);
    session.close();
  });

  it('times out when the peer never says hello', async () => {
    const ports = createInProcessPortPair();
    const session = new Session(ports.a, {
      peer: HUB,
      handshakeTimeoutMs: 20,
      livenessIntervalMs: false,
    });
    await expect(session.ready).rejects.toMatchObject({ code: RESERVED_ERROR_CODES.UNAVAILABLE });
    expect(session.closure).toMatchObject({
      code: CLOSE_CODES.PROTOCOL_ERROR,
      reason: 'handshake timeout',
    });
  });

  it('answers a request that arrives before the handshake with UNAVAILABLE', async () => {
    const ports = createInProcessPortPair();
    const recorder = new FrameRecorder(ports.b);
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    ports.b.send({ type: 'req', id: 'early', method: 'test.echo', params: {} });
    const answer = await recorder.until((frame) => frame.type === 'err');
    expect(answer).toMatchObject({
      type: 'err',
      id: 'early',
      error: { code: RESERVED_ERROR_CODES.UNAVAILABLE },
    });
    session.close();
  });

  it('closes with 4400 on a duplicate hello', async () => {
    const ports = createInProcessPortPair();
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    ports.b.send(rawHello());
    await session.ready;
    ports.b.send(rawHello());
    await tick();
    expect(session.closure).toMatchObject({
      code: CLOSE_CODES.PROTOCOL_ERROR,
      reason: 'duplicate hello',
    });
  });

  it('announces its frame ceiling and honours the lower one', async () => {
    const ports = createInProcessPortPair();
    const recorder = new FrameRecorder(ports.b);
    const session = new Session(ports.a, {
      peer: HUB,
      maxFrameBytes: 8192,
      livenessIntervalMs: false,
    });
    const hello = await recorder.until((frame) => frame.type === 'hello');
    expect(hello).toMatchObject({ type: 'hello', limits: { maxFrameBytes: 8192 } });
    ports.b.send({ ...rawHello(), limits: { maxFrameBytes: 4096 } });
    await session.ready;
    expect(session.sendLimitBytes).toBe(4096);
    session.close();
  });
});

describe('Session requests', () => {
  it('rejects an invalid or reserved method name locally', async () => {
    const { hub } = pair();
    await expect(hub.request('nodots', {})).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.INVALID_REQUEST,
    });
    await expect(hub.request('rpc.discover', {})).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.INVALID_REQUEST,
    });
    hub.close();
  });

  it('maps a handler that throws RemoteError, Error and AbortError onto the wire', async () => {
    const ports = createInProcessPortPair();
    const responder = new Session(ports.b, {
      peer: RUNTIME,
      livenessIntervalMs: false,
      handlers: {
        'test.remote': () => {
          throw new RemoteError('APP_CODE', 'app said no', { why: 'policy' });
        },
        'test.plain': () => {
          throw new Error('unexpected');
        },
        'test.abort': () => {
          throw Object.assign(new Error('gave up'), { name: 'AbortError' });
        },
        'test.void': () => undefined,
      },
    });
    const requester = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    await expect(requester.request('test.remote', {})).rejects.toMatchObject({
      code: 'APP_CODE',
      message: 'app said no',
      details: { why: 'policy' },
    });
    await expect(requester.request('test.plain', {})).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.INTERNAL,
      message: 'unexpected',
    });
    await expect(requester.request('test.abort', {})).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.CANCELLED,
    });
    expect(await requester.request('test.void', {})).toBeNull();
    requester.close();
    responder.close();
  });

  it('answers a duplicate in-flight id with INVALID_REQUEST and keeps the first running', async () => {
    const ports = createInProcessPortPair();
    const recorder = new FrameRecorder(ports.b);
    let release: () => void = () => undefined;
    const session = new Session(ports.a, {
      peer: HUB,
      livenessIntervalMs: false,
      handlers: {
        'test.slow': () => new Promise<string>((resolve) => (release = () => resolve('done'))),
      },
    });
    ports.b.send(rawHello());
    await session.ready;
    ports.b.send({ type: 'req', id: 'dup', method: 'test.slow', params: {} });
    await tick();
    ports.b.send({ type: 'req', id: 'dup', method: 'test.slow', params: {} });
    const refused = await recorder.until((frame) => frame.type === 'err');
    expect(refused).toMatchObject({
      id: 'dup',
      error: { code: RESERVED_ERROR_CODES.INVALID_REQUEST },
    });
    release();
    const answered = await recorder.until((frame) => frame.type === 'res');
    expect(answered).toMatchObject({ id: 'dup', result: 'done' });
    session.close();
  });

  it('ignores a response for an unknown id', async () => {
    const ports = createInProcessPortPair();
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    ports.b.send(rawHello());
    await session.ready;
    ports.b.send({ type: 'res', id: 'nobody', result: 1 });
    await tick();
    expect(session.state).toBe('ready');
    session.close();
  });

  it('fails a request sent after close with UNAVAILABLE', async () => {
    const { hub, runtime } = pair();
    await hub.ready;
    hub.close();
    await expect(hub.request('test.echo', {})).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.UNAVAILABLE,
    });
    runtime.close();
  });

  it('aborts the handler signal when the session closes', async () => {
    const ports = createInProcessPortPair();
    let aborted = false;
    const responder = new Session(ports.b, {
      peer: RUNTIME,
      livenessIntervalMs: false,
      handlers: {
        'test.hang': (_params, context) =>
          new Promise((_resolve, reject) => {
            context.signal.addEventListener('abort', () => {
              aborted = true;
              reject(new Error('closed'));
            });
          }),
      },
    });
    const requester = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    const pending = requester.request('test.hang', {});
    await tick();
    responder.close();
    await expect(pending).rejects.toMatchObject({ code: RESERVED_ERROR_CODES.UNAVAILABLE });
    expect(aborted).toBe(true);
  });
});

describe('Session events and liveness', () => {
  it('drops events emitted before the handshake and after close', async () => {
    const ports = createInProcessPortPair();
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    expect(session.emit({ topic: 'test.early', payload: 1 })).toBe(false);
    ports.b.send(rawHello());
    await session.ready;
    expect(session.emit({ topic: 'test.ok', payload: 1 })).toBe(true);
    session.close();
    expect(session.emit({ topic: 'test.late', payload: 1 })).toBe(false);
  });

  it('closes with a liveness timeout when pongs stop', async () => {
    const ports = createInProcessPortPair();
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: 15 });
    ports.b.send(rawHello());
    await session.ready;
    const closed = new Promise((resolve) => session.onClose(resolve));
    expect(await closed).toMatchObject({ code: CLOSE_CODES.RELEASED, reason: 'liveness timeout' });
  });

  it('stays open while the peer answers pings', async () => {
    const ports = createInProcessPortPair();
    ports.b.onFrame((frame) => {
      if (frame.type === 'ping') ports.b.send({ type: 'pong' });
    });
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: 10 });
    ports.b.send(rawHello());
    await session.ready;
    await tick(60);
    expect(session.state).toBe('ready');
    session.close();
  });

  it('tears down on a received close frame with its code', async () => {
    const ports = createInProcessPortPair();
    const session = new Session(ports.a, { peer: HUB, livenessIntervalMs: false });
    ports.b.send(rawHello());
    await session.ready;
    const closed = new Promise((resolve) => session.onClose(resolve));
    ports.b.send({ type: 'close', code: CLOSE_CODES.UNAUTHORIZED, reason: 'token revoked' });
    expect(await closed).toMatchObject({
      code: CLOSE_CODES.UNAUTHORIZED,
      reason: 'token revoked',
      fatal: true,
    });
  });

  it('fires onClose once, and immediately for a late subscriber', async () => {
    const { hub, runtime } = pair();
    await hub.ready;
    let count = 0;
    hub.onClose(() => {
      count += 1;
    });
    hub.close();
    hub.close();
    await tick();
    expect(count).toBe(1);
    const late = new Promise((resolve) => hub.onClose(resolve));
    expect(await late).toMatchObject({ code: CLOSE_CODES.RELEASED });
    runtime.close();
  });
});
