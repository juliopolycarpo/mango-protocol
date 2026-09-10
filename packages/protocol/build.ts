/**
 * Builds the publishable package: one ESM bundle per entry with shared chunks,
 * declarations from tsc, and the spec's schema files copied beside them.
 *
 * @example
 * bun ./build.ts
 */

import { cp, mkdir, rm } from 'node:fs/promises';

/** Every subpath the package exports; an entry whose source is missing is skipped with a warning. */
const ENTRIES = ['index', 'stdio', 'ipc', 'in-process', 'ws', 'spawn', 'testing'] as const;
const ROOT = new URL('.', import.meta.url).pathname;

const entrypoints: string[] = [];
for (const entry of ENTRIES) {
  const path = `${ROOT}src/${entry}.ts`;
  if (await Bun.file(path).exists()) entrypoints.push(path);
  else console.warn(`entry ${entry} has no source yet; skipping`);
}

await rm(`${ROOT}dist`, { recursive: true, force: true });
await rm(`${ROOT}schema`, { recursive: true, force: true });

const result = await Bun.build({
  entrypoints,
  outdir: `${ROOT}dist`,
  target: 'node',
  format: 'esm',
  splitting: true,
  sourcemap: 'linked',
  external: ['typebox', 'typebox/*', 'bun:test'],
  naming: { entry: '[name].js', chunk: 'chunks/[name]-[hash].js' },
});
if (!result.success) {
  for (const log of result.logs) console.error(log);
  process.exit(1);
}

const declarations = Bun.spawnSync(
  ['bunx', 'tsc', '-p', `${ROOT}tsconfig.build.json`, '--emitDeclarationOnly', '--declaration'],
  { stdout: 'inherit', stderr: 'inherit' }
);
if (declarations.exitCode !== 0) process.exit(declarations.exitCode);

await mkdir(`${ROOT}schema/1`, { recursive: true });
await cp(`${ROOT}../../spec/schema/1`, `${ROOT}schema/1`, { recursive: true });
process.stdout.write(
  `built ${entrypoints.length} entries into dist/ and copied the schema files\n`
);
