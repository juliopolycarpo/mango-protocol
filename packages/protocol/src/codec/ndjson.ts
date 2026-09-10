/**
 * The NDJSON line codec of spec/transports/stdio.md: one frame per line, UTF-8,
 * terminated by a single `\n`, bounded by the frame limit of §11.
 *
 * The stream decoder buffers bytes rather than characters, so a chunk boundary
 * inside a multi-byte character never corrupts a frame.
 */

import { CodecError } from '../errors';
import { assertFrame, type Frame } from '../schemas/frames';

/** Frame limit when neither peer announced a lower one (§11). */
export const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

/** Lowest ceiling a peer may announce in `hello.limits.maxFrameBytes` (§11). */
export const MIN_MAX_FRAME_BYTES = 4096;

/** How large one encoded frame may be, without its line terminator. */
export interface FrameLimitOptions {
  readonly maxFrameBytes?: number;
}

/** What one `push` or `finish` produced: frames, and the refusal that ended the stream. */
export type LineDecoderOutcome = {
  readonly frames: readonly Frame[];
  readonly error?: CodecError;
};

const LINE_FEED = 0x0a;
const CARRIAGE_RETURN = 0x0d;
const EMPTY_BYTES = new Uint8Array(0);

/** Space, tab, vertical tab, form feed, carriage return and line feed only: a BOM is not blank. */
const BLANK_LINE = /^[\t\n\v\f\r ]*$/;

const encoder = new TextEncoder();

/** `ignoreBOM` keeps a leading U+FEFF in the output, where JSON.parse refuses it. */
const decoder = new TextDecoder('utf-8', { ignoreBOM: true });

function resolveFrameLimit(options?: FrameLimitOptions): number {
  const limit = options?.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
  if (!Number.isInteger(limit) || limit < MIN_MAX_FRAME_BYTES) {
    throw new RangeError(
      `maxFrameBytes is ${limit}; expected an integer of at least ${MIN_MAX_FRAME_BYTES}`
    );
  }
  return limit;
}

function reasonOf(cause: unknown): string {
  return cause instanceof Error ? cause.message : 'unknown cause';
}

/** Compact JSON, or a `schema` refusal when the frame holds a value JSON cannot carry. */
function serialise(frame: Frame): string {
  try {
    return JSON.stringify(frame);
  } catch (cause) {
    throw new CodecError(
      'schema',
      `frame is not JSON-serialisable: ${reasonOf(cause)}; expected a frame of plain JSON values`
    );
  }
}

function encodeChecked(
  frame: Frame,
  limit: number
): { readonly text: string; readonly bytes: Uint8Array } {
  assertFrame(frame);
  const text = serialise(frame);
  const bytes = encoder.encode(text);
  if (bytes.byteLength > limit) {
    throw new CodecError(
      'too-large',
      `frame is ${bytes.byteLength} bytes; expected at most ${limit}`
    );
  }
  return { text, bytes };
}

/** One record's bytes, less the `\r` an unstripped CRLF terminator left behind. */
function stripCarriageReturn(record: Uint8Array): Uint8Array {
  const last = record.byteLength - 1;
  return last >= 0 && record[last] === CARRIAGE_RETURN ? record.subarray(0, last) : record;
}

function decodeRecord(text: string, byteLength: number, limit: number): Frame {
  if (byteLength > limit) {
    throw new CodecError('too-large', `line is ${byteLength} bytes; expected at most ${limit}`);
  }
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (cause) {
    throw new CodecError(
      'invalid-json',
      `line is not JSON: ${reasonOf(cause)}; expected one JSON object of ${byteLength} bytes to parse`
    );
  }
  assertFrame(value);
  return value;
}

/**
 * Encodes one frame as the UTF-8 bytes of a compact JSON object, without a
 * terminator. Validates the frame first and refuses one over the limit.
 *
 * @example
 * encodeFrameBytes({ type: 'ping' }); // Uint8Array of {"type":"ping"}
 */
export function encodeFrameBytes(frame: Frame, options?: FrameLimitOptions): Uint8Array {
  return encodeChecked(frame, resolveFrameLimit(options)).bytes;
}

/**
 * Encodes one frame as an NDJSON record: compact JSON plus a single `\n`. The
 * limit applies to the line without its terminator.
 *
 * @example
 * encodeLine({ type: 'ping' }); // '{"type":"ping"}\n'
 */
export function encodeLine(frame: Frame, options?: FrameLimitOptions): string {
  return `${encodeChecked(frame, resolveFrameLimit(options)).text}\n`;
}

/**
 * The UTF-8 byte length one frame occupies on the wire, so a sender can compare
 * it with the effective frame limit before committing to a send.
 *
 * @example
 * measureFrameBytes({ type: 'ping' }); // 15
 */
export function measureFrameBytes(frame: Frame): number {
  return encoder.encode(serialise(frame)).byteLength;
}

/**
 * Decodes one NDJSON record, given without its terminator. One trailing `\r` is
 * stripped; a blank record is a refusal here, while a stream decoder ignores it.
 *
 * @example
 * decodeLine('{"type":"ping"}'); // { type: 'ping' }
 */
export function decodeLine(line: string | Uint8Array, options?: FrameLimitOptions): Frame {
  const limit = resolveFrameLimit(options);
  const bytes = stripCarriageReturn(typeof line === 'string' ? encoder.encode(line) : line);
  const text = decoder.decode(bytes);
  if (BLANK_LINE.test(text)) {
    throw new CodecError('empty', 'line is blank; expected one JSON object');
  }
  return decodeRecord(text, bytes.byteLength, limit);
}

/**
 * Splits a byte stream into NDJSON records and decodes each one. Blank and
 * whitespace-only lines are ignored. A refusal ends the stream: the frames
 * decoded before it are delivered with it, and every later call repeats it.
 *
 * @example
 * const decoder = new LineDecoder();
 * decoder.push('{"type":"ping"}\n').frames; // [{ type: 'ping' }]
 */
export class LineDecoder {
  readonly #limit: number;
  #pending: Uint8Array = EMPTY_BYTES;
  #searched = 0;
  #refusal: CodecError | undefined;

  constructor(options?: FrameLimitOptions) {
    this.#limit = resolveFrameLimit(options);
  }

  /** Feeds the next piece of the stream and returns what it completed. */
  push(chunk: Uint8Array | string): LineDecoderOutcome {
    if (this.#refusal !== undefined) return { frames: [], error: this.#refusal };
    const bytes = typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
    this.#pending = concat(this.#pending, bytes);
    return this.#drain(false);
  }

  /** Ends the stream: a final record without a terminator is still decoded. */
  finish(): LineDecoderOutcome {
    if (this.#refusal !== undefined) return { frames: [], error: this.#refusal };
    return this.#drain(true);
  }

  #drain(final: boolean): LineDecoderOutcome {
    const frames: Frame[] = [];
    for (;;) {
      const newline = this.#pending.indexOf(LINE_FEED, this.#searched);
      if (newline === -1) break;
      const record = this.#pending.subarray(0, newline);
      this.#pending = this.#pending.subarray(newline + 1);
      this.#searched = 0;
      const error = this.#consume(record, frames);
      if (error !== undefined) return { frames, error };
    }
    this.#searched = this.#pending.byteLength;
    if (final) {
      const rest = this.#pending;
      this.#pending = EMPTY_BYTES;
      this.#searched = 0;
      const error = this.#consume(rest, frames);
      return error === undefined ? { frames } : { frames, error };
    }
    if (this.#pending.byteLength > this.#limit) {
      return { frames, error: this.#refuse(this.#pending.byteLength) };
    }
    return { frames };
  }

  /** Decodes one record, or latches the refusal that ends the stream. */
  #consume(record: Uint8Array, frames: Frame[]): CodecError | undefined {
    const bytes = stripCarriageReturn(record);
    const text = decoder.decode(bytes);
    if (BLANK_LINE.test(text)) return undefined;
    try {
      frames.push(decodeRecord(text, bytes.byteLength, this.#limit));
      return undefined;
    } catch (error) {
      if (!(error instanceof CodecError)) throw error;
      this.#refusal = error;
      return error;
    }
  }

  /** Refuses a partial line that already passed the limit, before its terminator. */
  #refuse(byteLength: number): CodecError {
    const refusal = new CodecError(
      'too-large',
      `partial line is already ${byteLength} bytes; expected at most ${this.#limit}`
    );
    this.#refusal = refusal;
    this.#pending = EMPTY_BYTES;
    this.#searched = 0;
    return refusal;
  }
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.byteLength === 0) return right.slice();
  const out = new Uint8Array(left.byteLength + right.byteLength);
  out.set(left, 0);
  out.set(right, left.byteLength);
  return out;
}
