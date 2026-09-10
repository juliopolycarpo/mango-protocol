/**
 * `bun run check`: Biome, dprint, TypeScript, the spec verifier, the fixture
 * generator's staleness check, rustfmt and Clippy.
 *
 * Flags: `--skip-format` (no Biome/dprint), `--staged` (accepted for the
 * lefthook hook; the repo is small enough to always check everything),
 * `--ts-only`, `--rs-only`.
 */

import { exitWith, hasCargo, hasFlag, runParallel, task, warnNoCargo } from './lib';

const skipFormat = hasFlag('--skip-format');
const tsOnly = hasFlag('--ts-only');
const rsOnly = hasFlag('--rs-only');

const tasks = [];
if (!rsOnly) {
  if (!skipFormat) {
    tasks.push(task('biome', ['bunx', 'biome', 'check', '.']));
    tasks.push(task('dprint', ['bunx', 'dprint', 'check']));
  }
  tasks.push(task('tsc', ['bunx', 'tsc', '--noEmit', '-p', 'packages/protocol/tsconfig.json']));
  tasks.push(task('tsc:scripts', ['bunx', 'tsc', '--noEmit', '-p', 'scripts/tsconfig.json']));
  tasks.push(task('versions', ['bun', './scripts/check-versions.ts']));
  tasks.push(task('verify-spec', ['bun', './scripts/verify-spec.ts']));
  tasks.push(task('fixtures:chunks', ['bun', './scripts/fixtures/generate-chunks.ts', '--check']));
}
if (!tsOnly) {
  if (hasCargo()) {
    if (!skipFormat) tasks.push(task('rustfmt', ['cargo', 'fmt', '--all', '--', '--check']));
    tasks.push(
      task('clippy', [
        'cargo',
        'clippy',
        '--all-targets',
        '--all-features',
        '--locked',
        '--',
        '-D',
        'warnings',
      ])
    );
  } else {
    warnNoCargo();
  }
}

exitWith(await runParallel(tasks));
