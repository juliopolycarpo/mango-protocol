import { describe, expect, it } from 'bun:test';
import { PROTOCOL_MAJOR, PROTOCOL_MINOR } from '../src';

describe('protocol version constants', () => {
  it('starts at wire 1.0', () => {
    expect(PROTOCOL_MAJOR).toBe(1);
    expect(PROTOCOL_MINOR).toBe(0);
  });
});
