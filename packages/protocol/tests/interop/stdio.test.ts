/**
 * TypeScript launcher, Rust child, stdio between them.
 *
 * Only one direction exists for this transport by nature: whoever spawns the
 * process is the launcher, and the child speaks on the pipes it was given.
 */

import { describe, expect, it } from 'bun:test';
import legacyHello from '../../../../spec/fixtures/1/legacy-hello.json';
import { CLOSE_CODES } from '../../src/close';
import { LineDecoder } from '../../src/codec/ndjson';
import { Session } from '../../src/session';
import { CONFORMANCE_A } from '../../src/testing/conformance';
import { type ExitStatus, spawnPort } from '../../src/transports/spawn';
import {
  expectMangoPeerBehaviour,
  INTEROP_ENABLED,
  LEGACY_HELLO_CLOSE_CODE,
  peerBinary,
} from './support';

const describeInterop = INTEROP_ENABLED ? describe : describe.skip;

describeInterop('interop: stdio (TypeScript launches, Rust serves)', () => {
  it('completes the handshake and serves every case over the child pipes', async () => {
    const peer = spawnPort({ argv: [await peerBinary(), '--stdio'] });
    const session = new Session(peer.port, { peer: CONFORMANCE_A, livenessIntervalMs: false });
    let status: ExitStatus | undefined;
    try {
      await expectMangoPeerBehaviour(session);
    } finally {
      session.close(CLOSE_CODES.RELEASED, 'interop done');
      status = await peer.terminate();
    }
    // The child leaves on the end of its stdin; no signal was needed. Asserted
    // after the block, not inside it: a throw in `finally` replaces whatever
    // failed above it, and a child wedged enough to fail the cases above is
    // exactly the one `terminate` has to escalate a signal at.
    expect(status?.signal).toBeNull();
  }, 60_000);

  it('answers a runtime-protocol 1.0.1 hello with 4426', async () => {
    // A raw child, with no port over its pipes: the bytes an old binary
    // really puts on the wire, written by hand.
    const child = Bun.spawn([await peerBinary(), '--stdio'], {
      stdin: 'pipe',
      stdout: 'pipe',
      stderr: 'ignore',
    });
    try {
      child.stdin.write(`${legacyHello.cases[0]?.line ?? ''}\n`);
      await child.stdin.flush();

      const farewell = await readCloseFrame(child.stdout);
      expect(farewell.code).toBe(LEGACY_HELLO_CLOSE_CODE);
    } finally {
      child.kill();
      await child.exited;
    }
  }, 60_000);
});

/** The first `close` frame the child writes, decoded with this SDK's decoder. */
async function readCloseFrame(
  stream: ReadableStream<Uint8Array>
): Promise<{ readonly code: number }> {
  const decoder = new LineDecoder({});
  for await (const chunk of stream) {
    const { frames } = decoder.push(chunk);
    for (const frame of frames) {
      if (frame.type === 'close') return frame;
    }
  }
  throw new Error('the child ended its stdout without a close frame');
}
