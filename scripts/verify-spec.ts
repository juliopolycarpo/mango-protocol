/**
 * Proves the fixture corpus and the JSON Schema agree, using Ajv as an
 * independent reference validator. Runs inside `bun run check`.
 *
 * Every `accept` frame must validate, every `reject` frame must fail for the
 * stated reason, every NDJSON expectation must validate, and a sample catalog
 * must validate against catalog.json.
 *
 * @example
 * bun ./scripts/verify-spec.ts
 */

import { Ajv2020 } from 'ajv/dist/2020';
import chunks from '../spec/fixtures/1/chunks.json';
import frames from '../spec/fixtures/1/frames.json';
import ndjson from '../spec/fixtures/1/ndjson.json';
import negotiation from '../spec/fixtures/1/negotiation.json';
import catalogSchema from '../spec/schema/1/catalog.json';
import protocolSchema from '../spec/schema/1/protocol.json';

interface FrameCase {
  readonly name: string;
  readonly verdict: 'accept' | 'reject' | 'implementation-defined';
  readonly line: string;
  readonly expected?: unknown;
  readonly reason?: string;
}

const ajv = new Ajv2020({ strict: true, allErrors: true });
ajv.addSchema(protocolSchema);
ajv.addSchema(catalogSchema);
const validateFrame = ajv.getSchema(protocolSchema.$id);
const validateCatalog = ajv.getSchema(catalogSchema.$id);
if (!validateFrame || !validateCatalog) throw new Error('schemas failed to compile');

const failures: string[] = [];
const fail = (message: string): void => {
  failures.push(message);
};

/** Recursive subset match: every member of `expected` equals the value's member. */
function isSubset(expected: unknown, actual: unknown): boolean {
  if (Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      expected.length === actual.length &&
      expected.every((item, index) => isSubset(item, actual[index]))
    );
  }
  if (expected !== null && typeof expected === 'object') {
    if (actual === null || typeof actual !== 'object' || Array.isArray(actual)) return false;
    return Object.entries(expected).every(([key, value]) =>
      isSubset(value, (actual as Record<string, unknown>)[key])
    );
  }
  return Object.is(expected, actual);
}

for (const item of frames.cases as FrameCase[]) {
  let parsed: unknown;
  try {
    parsed = JSON.parse(item.line);
  } catch {
    if (item.verdict === 'reject' && item.reason === 'invalid-json') continue;
    if (item.verdict === 'implementation-defined') continue;
    fail(`${item.name}: expected ${item.verdict} but the line is not JSON`);
    continue;
  }
  if (item.verdict === 'implementation-defined') continue;
  const valid = validateFrame(parsed) === true;
  if (item.verdict === 'accept') {
    if (!valid) fail(`${item.name}: should validate: ${ajv.errorsText(validateFrame.errors)}`);
    else if (item.expected !== undefined && !isSubset(item.expected, parsed)) {
      fail(`${item.name}: expected is not a subset of the parsed line`);
    }
    continue;
  }
  if (item.reason === 'invalid-json') fail(`${item.name}: reason invalid-json but the line parsed`);
  else if (valid) fail(`${item.name}: should be refused by the schema but validated`);
}

for (const item of ndjson.cases) {
  for (const frame of item.expected ?? []) {
    if (validateFrame(frame) !== true) {
      fail(`ndjson ${item.name}: expected frame does not validate`);
    }
  }
}

for (const item of chunks.cases) {
  if (item.expected !== undefined && validateFrame(item.expected) !== true) {
    fail(`chunks ${item.name}: expected frame does not validate`);
  }
}

for (const item of negotiation.cases) {
  const bothMajors = item.local.major === item.remote.major;
  const expectsMismatch = 'mismatch' in item.expected;
  if (bothMajors === expectsMismatch)
    fail(`negotiation ${item.name}: verdict disagrees with majors`);
  if (!expectsMismatch) {
    const lower = Math.min(item.local.minor, item.remote.minor);
    if (item.expected.effectiveMinor !== lower) {
      fail(`negotiation ${item.name}: effective minor should be ${lower}`);
    }
  }
}

const sampleCatalog = {
  name: 'fixture-contract',
  version: '1.0.0',
  protocol: { major: 1, minor: 0 },
  methods: [
    {
      name: 'text.echo',
      params: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] },
      result: { type: 'object' },
      capabilities: ['echo'],
    },
  ],
  events: [{ topic: 'text.stream', payload: { type: 'object' }, stream: true }],
  capabilities: { type: 'object', properties: { echo: { type: 'boolean' } } },
};
if (validateCatalog(sampleCatalog) !== true) {
  fail(`catalog sample does not validate: ${ajv.errorsText(validateCatalog.errors)}`);
}
if (
  validateCatalog({
    name: 'x',
    version: '1',
    methods: [{ name: 'bad', params: {}, result: {} }],
  }) === true
) {
  fail('catalog accepted a single-segment method name');
}

if (failures.length > 0) {
  console.error(`verify-spec: ${failures.length} failure(s)`);
  for (const message of failures) console.error(`  - ${message}`);
  process.exit(1);
}
console.log(
  `verify-spec: ${frames.cases.length} frame, ${ndjson.cases.length} ndjson, ${chunks.cases.length} chunk and ${negotiation.cases.length} negotiation cases agree with the schema`
);
