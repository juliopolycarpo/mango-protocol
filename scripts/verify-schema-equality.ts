/**
 * Proves the three schema dialects describe the same wire: the normative
 * `spec/schema/1/protocol.json`, the TypeBox emission of the TypeScript SDK and,
 * when Cargo is available, the schemars emission of the Rust crate. Runs inside
 * `bun run check`.
 *
 * Each emission is normalised (the rules live in `schema-equality.ts`), then
 * every `$defs` entry is deep-compared with the spec's entry. A difference is a
 * review flag on the emitter, not on this script: a new normaliser rule is a
 * contract change.
 *
 * @example
 * bun ./scripts/verify-schema-equality.ts            # spec vs TypeScript vs Rust
 * bun ./scripts/verify-schema-equality.ts --ts-only  # skip the Rust emission
 */

import {
  CancelFrameSchema,
  CapabilitiesSchema,
  CloseCodeSchema,
  CloseFrameSchema,
  ErrorCodeSchema,
  ErrorFrameSchema,
  ErrorPayloadSchema,
  EventFrameSchema,
  FrameSchema,
  HelloFrameSchema,
  IdSchema,
  LimitsSchema,
  MethodSchema,
  PeerInfoSchema,
  PingFrameSchema,
  PongFrameSchema,
  ProtocolVersionSchema,
  RequestFrameSchema,
  ResultFrameSchema,
  TopicSchema,
} from '../packages/protocol/src/schemas';
import protocolSchema from '../spec/schema/1/protocol.json';
import { hasCargo, hasFlag, ROOT_DIR, warnNoCargo } from './lib';
import { compareDefinitions, type Definitions, type Json } from './schema-equality';

/** The TypeScript SDK's schemas, keyed like the spec's `$defs`. */
const TYPESCRIPT_DEFINITIONS = JSON.parse(
  JSON.stringify({
    id: IdSchema,
    method: MethodSchema,
    topic: TopicSchema,
    protocolVersion: ProtocolVersionSchema,
    peer: PeerInfoSchema,
    limits: LimitsSchema,
    capabilities: CapabilitiesSchema,
    errorCode: ErrorCodeSchema,
    errorPayload: ErrorPayloadSchema,
    closeCode: CloseCodeSchema,
    hello: HelloFrameSchema,
    req: RequestFrameSchema,
    res: ResultFrameSchema,
    err: ErrorFrameSchema,
    evt: EventFrameSchema,
    cancel: CancelFrameSchema,
    ping: PingFrameSchema,
    pong: PongFrameSchema,
    close: CloseFrameSchema,
    frame: FrameSchema,
  })
) as Definitions;

/** The `$defs` the Rust emission promises; scalar aliases are inlined there. */
const RUST_REQUIRED = [
  'protocolVersion',
  'peer',
  'limits',
  'errorPayload',
  'hello',
  'req',
  'res',
  'err',
  'evt',
  'cancel',
  'ping',
  'pong',
  'close',
  'frame',
];

async function rustDefinitions(): Promise<Definitions> {
  const proc = Bun.spawn(
    ['cargo', 'run', '--quiet', '--locked', '--example', 'emit_schema', '--features', 'schema'],
    { cwd: `${ROOT_DIR}/crates/mango-protocol`, stdout: 'pipe', stderr: 'pipe' }
  );
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (code !== 0) throw new Error(`cargo run --example emit_schema failed (${code}):\n${stderr}`);
  const emitted = JSON.parse(stdout) as { $defs?: Definitions };
  if (!emitted.$defs) throw new Error('the Rust emission has no $defs member');
  return emitted.$defs;
}

const spec = protocolSchema.$defs as Record<string, Json>;
const failures = compareDefinitions('typescript', TYPESCRIPT_DEFINITIONS, Object.keys(spec), spec);
const compared = ['typescript'];
if (!hasFlag('--ts-only')) {
  if (hasCargo()) {
    failures.push(...compareDefinitions('rust', await rustDefinitions(), RUST_REQUIRED, spec));
    compared.push('rust');
  } else {
    warnNoCargo();
  }
}

if (failures.length > 0) {
  console.error(`schema equality failed (${failures.length}):`);
  for (const line of failures) console.error(`  ${line}`);
  process.exit(1);
}
console.log(`schema equality: spec == ${compared.join(' == ')}`);
