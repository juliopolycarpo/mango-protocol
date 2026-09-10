/**
 * The comparison rules behind `verify-schema-equality.ts`: how one JSON Schema
 * dialect is normalised and how two normalised schemas are diffed.
 *
 * Normaliser rules, the only tolerated differences between emitters:
 * - `$ref` to `#/$defs/<name>` is inlined (sibling keywords kept);
 * - `$schema`, `$id`, `title`, `description`, `$comment`, `examples` and
 *   `format` are dropped;
 * - `additionalProperties: true` is dropped (objects are open by default);
 * - a TypeBox `anyOf` whose branches carry distinct `type` consts becomes
 *   `oneOf`;
 * - the `null` alternative schemars adds to `Option` members is stripped
 *   (`type: [T, "null"]` and `anyOf: [T, {type: "null"}]`);
 * - key order is ignored.
 *
 * @example
 * differences(normalise(spec.hello, spec), normalise(emitted.hello, emitted)); // []
 */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };
export type Definitions = Readonly<Record<string, Json>>;

/** Annotation keywords that carry no validation meaning and differ per emitter. */
const ANNOTATION_KEYS = new Set(['$schema', '$id', 'title', 'description', '$comment', 'examples']);

/** Keys dropped during normalisation. */
const DROPPED_KEYS = new Set([...ANNOTATION_KEYS, 'format']);

function isObject(value: Json | undefined): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * True when every branch is an object schema tagged by a distinct `type` const,
 * the shape of the frame union.
 *
 * @example
 * isConstTaggedUnion([{ type: 'object', properties: { type: { const: 'ping' } } }]); // true
 */
export function isConstTaggedUnion(branches: readonly Json[]): boolean {
  const tags = new Set<string>();
  for (const branch of branches) {
    if (!isObject(branch) || !isObject(branch.properties)) return false;
    const tag = branch.properties.type;
    if (!isObject(tag) || typeof tag.const !== 'string' || tags.has(tag.const)) return false;
    tags.add(tag.const);
  }
  return tags.size > 0;
}

/**
 * Removes the `null` alternative schemars adds to `Option` members; the wire
 * says an optional member is absent, never `null`.
 *
 * @example
 * stripNullAlternative({ type: ['string', 'null'] }); // { type: 'string' }
 */
export function stripNullAlternative(schema: JsonObject): JsonObject {
  if (Array.isArray(schema.type)) {
    const types = schema.type.filter((entry) => entry !== 'null');
    return { ...schema, type: types.length === 1 ? (types[0] ?? null) : types };
  }
  const branches = schema.anyOf;
  if (!Array.isArray(branches) || branches.length !== 2) return schema;
  const kept = branches.filter((branch) => !(isObject(branch) && branch.type === 'null'));
  const [only] = kept;
  if (kept.length !== 1 || !isObject(only)) return schema;
  const { anyOf: _anyOf, ...rest } = schema;
  return { ...rest, ...only };
}

function inlineReference(schema: JsonObject, ref: string, definitions: Definitions): JsonObject {
  const name = ref.replace(/^#\/\$defs\//, '');
  const target = definitions[name];
  if (!isObject(target)) throw new Error(`unresolvable $ref ${ref}; expected a #/$defs entry`);
  const { $ref: _ref, ...siblings } = schema;
  return { ...target, ...siblings };
}

/**
 * Normalises one schema into the comparison form described in the module docs.
 *
 * @example
 * normalise({ $ref: '#/$defs/id', description: 'x' }, { id: { type: 'string' } });
 * // { type: 'string' }
 */
export function normalise(schema: Json, definitions: Definitions): Json {
  if (Array.isArray(schema)) return schema.map((entry) => normalise(entry, definitions));
  if (!isObject(schema)) return schema;
  const stripped = stripNullAlternative(schema);
  if (typeof stripped.$ref === 'string') {
    return normalise(inlineReference(stripped, stripped.$ref, definitions), definitions);
  }
  const result: JsonObject = {};
  for (const key of Object.keys(stripped).sort()) {
    const value = stripped[key];
    if (value === undefined || DROPPED_KEYS.has(key)) continue;
    if (key === 'additionalProperties' && value === true) continue;
    if (key === 'anyOf' && Array.isArray(value) && isConstTaggedUnion(value)) {
      result.oneOf = normalise(value, definitions);
      continue;
    }
    result[key] = normalise(value, definitions);
  }
  return result;
}

function describe(value: Json | undefined): string {
  return JSON.stringify(value);
}

/**
 * Every path where two normalised schemas differ, as JSON pointers with the
 * expected and the received value.
 *
 * @example
 * differences({ a: 1 }, { a: 2 }); // ['/a: expected 1, got 2']
 */
export function differences(expected: Json, actual: Json, path = ''): string[] {
  const here = path || '/';
  if (Array.isArray(expected) || Array.isArray(actual)) {
    if (!Array.isArray(expected) || !Array.isArray(actual) || expected.length !== actual.length) {
      return [`${here}: expected ${describe(expected)}, got ${describe(actual)}`];
    }
    return expected.flatMap((entry, index) =>
      differences(entry, actual[index] ?? null, `${path}/${index}`)
    );
  }
  if (isObject(expected) && isObject(actual)) {
    const keys = [...new Set([...Object.keys(expected), ...Object.keys(actual)])].sort();
    return keys.flatMap((key) => {
      const pointer = `${path}/${key}`;
      if (!(key in expected)) return [`${pointer}: unexpected ${describe(actual[key])}`];
      if (!(key in actual)) return [`${pointer}: missing, expected ${describe(expected[key])}`];
      return differences(expected[key] ?? null, actual[key] ?? null, pointer);
    });
  }
  if (Object.is(expected, actual)) return [];
  return [`${here}: expected ${describe(expected)}, got ${describe(actual)}`];
}

/**
 * Compares an emitter's definitions with the spec's. `required` names the keys
 * the emitter must provide; keys it provides beyond those are compared too, and
 * a key the spec does not know is a failure.
 *
 * @example
 * compareDefinitions('typescript', emitted, ['frame'], spec); // [] when equal
 */
export function compareDefinitions(
  label: string,
  emitted: Definitions,
  required: readonly string[],
  spec: Definitions
): string[] {
  const failures = required
    .filter((key) => !(key in emitted))
    .map((key) => `${label}: $defs/${key} is missing`);
  for (const key of Object.keys(emitted)) {
    const expected = spec[key];
    if (expected === undefined) {
      failures.push(`${label}: $defs/${key} is not in the spec`);
      continue;
    }
    const diff = differences(normalise(expected, spec), normalise(emitted[key] ?? null, emitted));
    failures.push(...diff.map((line) => `${label}: $defs/${key}${line}`));
  }
  return failures;
}
