import { describe, expect, it } from 'bun:test';
import { CHUNK_HEADER_BYTES } from '../src/codec/chunk';
import type { PortClosure } from '../src/port';
import type { Frame } from '../src/schemas/frames';
import {
  createWebSocketPort,
  outcomeOfBunSend,
  type SendOutcome,
  type WebSocketPortHandle,
  type WebSocketPortOptions,
  type WebSocketSink,
} from '../src/transports/websocket';

/** A socket that records what it was asked to do and answers a scripted outcome. */
class FakeWebSocketSink implements WebSocketSink {
  readonly sent: Uint8Array[] = [];
  readonly closes: { code: number; reason: string | undefined }[] = [];
  /** Ordered log of calls, so a test can prove the close frame went out first. */
  readonly calls: string[] = [];
  /** Answers for the next sends, in order; `outcome` answers the rest. */
  readonly scripted: SendOutcome[] = [];
  outcome: SendOutcome | undefined = 'sent';

  send(message: Uint8Array): SendOutcome | undefined {
    const answer = this.scripted.shift() ?? this.outcome;
    this.calls.push(`send:${answer ?? 'undefined'}`);
    if (answer !== 'dropped') this.sent.push(message.slice());
    return answer;
  }

  close(code: number, reason?: string): void {
    this.calls.push(`close:${code}`);
    this.closes.push({ code, reason });
  }
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Version, index and count of one outgoing message. */
function headerOf(message: Uint8Array): { version: number; index: number; count: number } {
  const view = new DataView(message.buffer, message.byteOffset, message.byteLength);
  return { version: view.getUint8(0), index: view.getUint32(1), count: view.getUint32(5) };
}

/** The NDJSON line a run of messages carries. */
function lineOf(messages: readonly Uint8Array[]): string {
  const payloads = messages.map((message) => message.subarray(CHUNK_HEADER_BYTES));
  const size = payloads.reduce((total, payload) => total + payload.byteLength, 0);
  const line = new Uint8Array(size);
  let offset = 0;
  for (const payload of payloads) {
    line.set(payload, offset);
    offset += payload.byteLength;
  }
  return decoder.decode(line);
}

/** One message carrying `line` whole, the way a raw peer would send it. */
function rawChunk(line: string): Uint8Array {
  const payload = encoder.encode(line);
  const message = new Uint8Array(CHUNK_HEADER_BYTES + payload.byteLength);
  const view = new DataView(message.buffer);
  view.setUint8(0, 1);
  view.setUint32(1, 0);
  view.setUint32(5, 1);
  message.set(payload, CHUNK_HEADER_BYTES);
  return message;
}

interface Harness {
  readonly sink: FakeWebSocketSink;
  readonly handle: WebSocketPortHandle;
  readonly closures: PortClosure[];
}

function harness(options: WebSocketPortOptions = {}): Harness {
  const sink = new FakeWebSocketSink();
  const handle = createWebSocketPort(sink, options);
  const closures: PortClosure[] = [];
  handle.port.onClosed((closure) => closures.push(closure));
  return { sink, handle, closures };
}

function blobFrame(bytes: number, id: string): Frame {
  return { type: 'req', id, method: 'test.echo', params: { blob: 'x'.repeat(bytes) } };
}

describe('createWebSocketPort', () => {
  it('sends one frame as a contiguous run of chunks and never interleaves two', () => {
    const { sink, handle } = harness({ maxMessageBytes: 2048 });

    handle.port.send(blobFrame(5000, 'r1'));
    handle.port.send(blobFrame(5000, 'r2'));

    const headers = sink.sent.map(headerOf);
    expect(headers.every((header) => header.version === 1)).toBe(true);
    const count = headers[0]?.count ?? 0;
    expect(count).toBeGreaterThan(1);
    expect(headers.map((header) => header.index)).toEqual([
      ...Array.from({ length: count }, (_value, index) => index),
      ...Array.from({ length: count }, (_value, index) => index),
    ]);
    expect(JSON.parse(lineOf(sink.sent.slice(0, count)))).toMatchObject({ id: 'r1' });
    expect(JSON.parse(lineOf(sink.sent.slice(count)))).toMatchObject({ id: 'r2' });
  });

  it('pauses the queue on a buffered send and resumes it on drain', () => {
    const { sink, handle } = harness({ maxMessageBytes: 2048 });
    sink.scripted.push('buffered');

    handle.port.send(blobFrame(5000, 'r1'));
    const paused = sink.sent.length;
    expect(paused).toBe(1);

    handle.onDrain();
    expect(sink.sent.length).toBeGreaterThan(paused);
    expect(JSON.parse(lineOf(sink.sent))).toMatchObject({ id: 'r1' });
  });

  it('treats a sink that returns nothing as having sent the message', () => {
    const { sink, handle } = harness();
    sink.outcome = undefined;

    handle.port.send({ type: 'ping' });

    expect(sink.sent).toHaveLength(1);
    expect(JSON.parse(lineOf(sink.sent))).toEqual({ type: 'ping' });
  });

  it('closes with 4400 when a paused queue grows past one frame limit', () => {
    const { sink, handle, closures } = harness({ maxFrameBytes: 4096, maxMessageBytes: 2048 });
    sink.outcome = 'buffered';

    handle.port.send(blobFrame(3900, 'r1'));
    expect(sink.closes).toEqual([]);
    handle.port.send(blobFrame(3900, 'r2'));

    expect(sink.closes).toMatchObject([{ code: 4400 }]);
    expect(closures).toHaveLength(1);
    expect(closures[0]).toMatchObject({ kind: 'closed', code: 4400 });
    expect((closures[0] as { reason: string }).reason).toMatch(
      /send queue holds \d+ bytes .*expected at most 4096/
    );
  });

  it('closes with 4400 when the socket drops a chunk', () => {
    const { sink, handle, closures } = harness();
    sink.outcome = 'dropped';

    handle.port.send({ type: 'ping' });

    expect(sink.closes).toMatchObject([{ code: 4400 }]);
    expect(closures[0]).toMatchObject({ kind: 'closed', code: 4400 });
    expect((closures[0] as { reason: string }).reason).toMatch(/dropped a \d+-byte chunk/);
  });

  it('reports a text message as a protocol error and closes with 4400', () => {
    const { sink, handle, closures } = harness();

    handle.onMessage('{"type":"ping"}');

    expect(sink.closes).toMatchObject([{ code: 4400 }]);
    expect(closures[0]).toMatchObject({ kind: 'protocol-error', code: 4400 });
    const closure = closures[0] as { error: Error };
    expect(closure.error.message).toMatch(/message is text of 15 characters; expected a binary/);
  });

  it('reports a refused chunk with the close code the refusal calls for', () => {
    const bad = harness();
    const versionTwo = rawChunk('{"type":"ping"}');
    versionTwo[0] = 2;
    bad.handle.onMessage(versionTwo);
    expect(bad.sink.closes).toMatchObject([{ code: 4400 }]);
    expect(bad.closures[0]).toMatchObject({ kind: 'protocol-error', code: 4400 });
    expect((bad.closures[0] as { error: { kind: string } }).error.kind).toBe('chunk-version');

    const hello = harness();
    hello.handle.onMessage(rawChunk('{"type":"hello","protocolVersion":"1.0.1"}'));
    expect(hello.sink.closes).toMatchObject([{ code: 4426 }]);
    expect(hello.closures[0]).toMatchObject({ kind: 'protocol-error', code: 4426 });
  });

  it('truncates a long refusal to the 123 bytes a close frame allows', () => {
    const { sink, handle } = harness();

    handle.onMessage(rawChunk(`{"type":"hello","note":"${'é'.repeat(400)}"`));

    const reason = sink.closes[0]?.reason ?? '';
    expect(reason.length).toBeGreaterThan(0);
    expect(encoder.encode(reason).byteLength).toBeLessThanOrEqual(123);
  });

  it('delivers a frame once every chunk of it has arrived', () => {
    const sender = harness({ maxMessageBytes: 2048 });
    sender.handle.port.send(blobFrame(5000, 'r1'));

    const receiver = harness({ maxMessageBytes: 2048 });
    const frames: Frame[] = [];
    receiver.handle.port.onFrame((frame) => frames.push(frame));
    for (const [index, message] of sender.sink.sent.entries()) {
      receiver.handle.onMessage(message);
      if (index < sender.sink.sent.length - 1) expect(frames).toHaveLength(0);
    }

    expect(frames).toHaveLength(1);
    expect(frames[0]).toMatchObject({ id: 'r1', method: 'test.echo' });
  });

  it('accepts an ArrayBuffer as readily as a view', () => {
    const sender = harness();
    sender.handle.port.send({ type: 'pong' });
    const message = sender.sink.sent[0] ?? new Uint8Array();

    const receiver = harness();
    const frames: Frame[] = [];
    receiver.handle.port.onFrame((frame) => frames.push(frame));
    receiver.handle.onMessage(message.slice().buffer);

    expect(frames).toEqual([{ type: 'pong' }]);
  });

  it('carries a 4xxx close code to the closure and drops a link-level one', () => {
    const reasoned = harness();
    reasoned.handle.onClose(4409, 'superseded by a newer connection');
    expect(reasoned.closures).toEqual([
      { kind: 'closed', code: 4409, reason: 'superseded by a newer connection' },
    ]);

    const severed = harness();
    severed.handle.onClose(1006, 'Connection ended');
    expect(severed.closures).toEqual([{ kind: 'closed' }]);
  });

  it('reports a socket error as the link ending, without a reason code', () => {
    const { closures } = reportError();
    expect(closures).toEqual([{ kind: 'closed', reason: 'socket reset' }]);
  });

  function reportError(): Harness {
    const built = harness();
    built.handle.onError(new Error('socket reset'));
    return built;
  }

  it('reports a closure at most once', () => {
    const { handle, closures } = harness();

    handle.onClose(4000, 'released');
    handle.onClose(4409, 'superseded');
    handle.onError(new Error('late'));

    expect(closures).toEqual([{ kind: 'closed', code: 4000, reason: 'released' }]);
  });

  it('sends the close frame before closing the socket, and reports no closure', () => {
    const { sink, handle, closures } = harness();

    handle.port.close(4409, 'superseded by a newer connection');

    expect(sink.calls).toEqual(['send:sent', 'close:4409']);
    expect(JSON.parse(lineOf(sink.sent))).toEqual({
      type: 'close',
      code: 4409,
      reason: 'superseded by a newer connection',
    });
    expect(sink.closes).toEqual([{ code: 4409, reason: 'superseded by a newer connection' }]);
    expect(closures).toEqual([]);

    handle.onClose(4409, 'superseded by a newer connection');
    expect(closures).toEqual([]);
  });

  it('skips the close frame when the option is off', () => {
    const { sink, handle } = harness({ sendCloseFrame: false });

    handle.port.close(4000, 'released');

    expect(sink.calls).toEqual(['close:4000']);
  });

  it('throws on a send after the port closed, naming the state', () => {
    const { handle } = harness();
    handle.port.close(4409, 'superseded');

    expect(() => handle.port.send({ type: 'ping' })).toThrow(
      /WebSocket port is closed with code 4409; expected an open port to send a ping frame/
    );
  });

  it('ignores a message that arrives after the port closed', () => {
    const { handle, closures } = harness();
    handle.onClose(4000, 'released');

    handle.onMessage('a text frame nobody should see');

    expect(closures).toEqual([{ kind: 'closed', code: 4000, reason: 'released' }]);
  });

  it('exposes the frame limit it decodes under', () => {
    expect(harness().handle.port.maxFrameBytes).toBe(16 * 1024 * 1024);
    expect(harness({ maxFrameBytes: 8192 }).handle.port.maxFrameBytes).toBe(8192);
  });
});

describe('outcomeOfBunSend', () => {
  it('maps the ServerWebSocket send status onto an outcome', () => {
    expect(outcomeOfBunSend(0)).toBe('dropped');
    expect(outcomeOfBunSend(-1)).toBe('buffered');
    expect(outcomeOfBunSend(24)).toBe('sent');
  });
});
