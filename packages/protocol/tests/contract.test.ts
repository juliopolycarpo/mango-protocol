import { describe, expect, it } from 'bun:test';
import Type from 'typebox';
import { defineContract } from '../src/contract';
import { RESERVED_ERROR_CODES, RemoteError } from '../src/errors';
import { Session, type SessionOptions } from '../src/session';
import { createInProcessPortPair } from '../src/transports/in-process';

const contract = defineContract({
  name: 'example',
  version: '1.2.3',
  description: 'Test contract',
  protocol: { major: 1, minor: 0 },
  methods: {
    'text.echo': {
      params: Type.Object({ text: Type.String() }),
      result: Type.Object({ text: Type.String() }),
      capabilities: ['echo'],
      description: 'Echoes text',
    },
    'math.add': {
      params: Type.Object({ a: Type.Number(), b: Type.Number() }),
      result: Type.Number(),
    },
  },
  events: {
    'text.tick': { payload: Type.Object({ at: Type.Number() }) },
    'text.stream': { payload: Type.Object({ line: Type.String() }), stream: true },
  },
  capabilities: Type.Object({ echo: Type.Boolean() }),
});

const A: SessionOptions['peer'] = { name: 'a', version: '1', role: 'hub' };
const B: SessionOptions['peer'] = { name: 'b', version: '1', role: 'runtime' };

function sessions() {
  const ports = createInProcessPortPair();
  return {
    a: new Session(ports.a, { peer: A, livenessIntervalMs: false }),
    b: new Session(ports.b, { peer: B, livenessIntervalMs: false }),
  };
}

describe('defineContract', () => {
  it('rejects invalid or reserved names at definition time', () => {
    expect(() =>
      defineContract({
        name: 'x',
        version: '1',
        methods: { nodots: { params: Type.Object({}), result: Type.Null() } },
      })
    ).toThrow(TypeError);
    expect(() =>
      defineContract({
        name: 'x',
        version: '1',
        methods: { 'rpc.discover': { params: Type.Object({}), result: Type.Null() } },
      })
    ).toThrow(/reserved/);
  });

  it('emits a plain-JSON catalog that validates', () => {
    const catalog = contract.catalog();
    expect(catalog.name).toBe('example');
    expect(catalog.methods.map((method) => method.name)).toEqual(['text.echo', 'math.add']);
    expect(catalog.methods[0]?.capabilities).toEqual(['echo']);
    expect(catalog.events?.map((event) => event.topic)).toEqual(['text.tick', 'text.stream']);
    expect(Object.getOwnPropertySymbols(catalog.methods[0]?.params ?? {})).toHaveLength(0);
    expect(JSON.parse(JSON.stringify(catalog))).toEqual(catalog);
  });

  it('serves typed handlers and requests through a typed client', async () => {
    const { a, b } = sessions();
    const off = contract.serve(b, {
      'text.echo': ({ text }) => ({ text }),
      'math.add': ({ a: x, b: y }) => x + y,
    });
    const client = contract.client(a);
    expect(await client.request('text.echo', { text: 'hi' })).toEqual({ text: 'hi' });
    expect(await client.request('math.add', { a: 2, b: 3 })).toBe(5);
    off();
    await expect(client.request('math.add', { a: 1, b: 1 })).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.METHOD_UNSUPPORTED,
    });
    a.close();
    b.close();
  });

  it('refuses parameters that fail the schema with INVALID_PARAMS and a path', async () => {
    const { a, b } = sessions();
    contract.serve(b, {
      'text.echo': ({ text }) => ({ text }),
      'math.add': ({ a: x, b: y }) => x + y,
    });
    await expect(a.request('math.add', { a: 'one', b: 2 })).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.INVALID_PARAMS,
      details: { method: 'math.add', path: '/a' },
    });
    a.close();
    b.close();
  });

  it('runs the guard with the declared capabilities before the handler', async () => {
    const { a, b } = sessions();
    const seen: string[][] = [];
    contract.serve(
      b,
      { 'text.echo': ({ text }) => ({ text }), 'math.add': ({ a: x, b: y }) => x + y },
      {
        guard: (method, capabilities) => {
          seen.push([method, ...capabilities]);
          if (capabilities.includes('echo')) {
            throw new RemoteError(RESERVED_ERROR_CODES.DENIED, 'echo was not granted', {
              capability: 'echo',
            });
          }
        },
      }
    );
    await expect(a.request('text.echo', { text: 'hi' })).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.DENIED,
      details: { capability: 'echo' },
    });
    expect(await a.request('math.add', { a: 1, b: 1 })).toBe(2);
    expect(seen).toEqual([['text.echo', 'echo'], ['math.add']]);
    a.close();
    b.close();
  });

  it('validates results when asked', async () => {
    const { a, b } = sessions();
    contract.serve(
      b,
      {
        'text.echo': () => ({ text: 42 }) as unknown as { text: string },
        'math.add': ({ a: x, b: y }) => x + y,
      },
      { validateResults: true }
    );
    await expect(a.request('text.echo', { text: 'hi' })).rejects.toMatchObject({
      code: RESERVED_ERROR_CODES.INTERNAL,
      details: { path: '/text' },
    });
    a.close();
    b.close();
  });

  it('emits and receives typed events', async () => {
    const { a, b } = sessions();
    await Promise.all([a.ready, b.ready]);
    const received: number[] = [];
    const off = contract.events(a).on('text.tick', ({ at }) => received.push(at));
    contract.events(b).emit('text.tick', { at: 1 });
    contract.events(b).emit('text.stream', { line: 'x' }, { streamId: 's', end: true });
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(received).toEqual([1]);
    off();
    a.close();
    b.close();
  });

  it('assertParams narrows or throws', () => {
    expect(() => contract.assertParams('text.echo', { text: 'ok' })).not.toThrow();
    expect(() => contract.assertParams('text.echo', { text: 1 })).toThrow(RemoteError);
  });
});
