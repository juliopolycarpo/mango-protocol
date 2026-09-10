import { describe, expect, it } from 'bun:test';
import negotiation from '../../../spec/fixtures/1/negotiation.json';
import { negotiate, type ProtocolVersion } from '../src/version';

interface NegotiationCase {
  readonly name: string;
  readonly local: ProtocolVersion;
  readonly remote: ProtocolVersion;
  readonly expected: {
    readonly effectiveMinor?: number;
    readonly mismatch?: boolean;
    readonly closeCode?: number;
  };
}

const cases = negotiation.cases as readonly NegotiationCase[];

describe('negotiation corpus', () => {
  it('reads every case of spec/fixtures/1/negotiation.json', () => {
    expect(cases.length).toBeGreaterThan(5);
  });

  for (const item of cases) {
    it(`negotiates ${item.name}`, () => {
      const result = negotiate(item.local, item.remote);

      if (item.expected.mismatch === true) {
        expect(result).toEqual({ ok: false, closeCode: 4426 });
        expect(item.expected.closeCode).toBe(4426);
        return;
      }
      expect(result).toEqual({ ok: true, effectiveMinor: item.expected.effectiveMinor as number });
    });
  }
});
