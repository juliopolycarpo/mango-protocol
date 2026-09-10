import { describe, expect, it } from 'bun:test';
import { Glob } from 'bun';
import * as protocol from '../src';

const SOURCE_DIR = new URL('../src', import.meta.url).pathname;

describe('package entry point', () => {
  it('re-exports the version, schema, codec, close, error, session and contract surface', () => {
    const exported = Object.keys(protocol);

    for (const name of [
      'PROTOCOL_VERSION',
      'negotiate',
      'CLOSE_CODES',
      'closeCodeForCodecError',
      'CodecError',
      'RemoteError',
      'RESERVED_ERROR_CODES',
      'FrameSchema',
      'isFrame',
      'assertFrame',
      'CatalogSchema',
      'isCatalog',
      'encodeLine',
      'decodeLine',
      'LineDecoder',
      'encodeChunks',
      'ChunkReassembler',
      'maxChunksFor',
      'Session',
      'defineContract',
    ]) {
      expect(exported).toContain(name);
    }
  });

  it('keeps the core browser-safe: no module under src imports node:', async () => {
    const nodeImport = /(?:from|import|require)\s*\(?\s*['"]node:/;
    const files: string[] = [];

    expect(nodeImport.test("import { Buffer } from 'node:buffer';")).toBe(true);

    for await (const path of new Glob('**/*.ts').scan(SOURCE_DIR)) {
      const text = await Bun.file(`${SOURCE_DIR}/${path}`).text();
      expect({ path, importsNode: nodeImport.test(text) }).toEqual({ path, importsNode: false });
      files.push(path);
    }

    expect(files.length).toBeGreaterThan(5);
  });
});
