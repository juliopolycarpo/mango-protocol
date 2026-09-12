/**
 * `bun run test`: the TypeScript suite, then the Rust suite.
 *
 * Flags: `--ts-only`, `--rs-only`. Extra Bun test arguments go after `--`.
 */

import { exitWith, hasCargo, hasFlag, runSequential, task, warnNoCargo } from './lib';

const tsOnly = hasFlag('--ts-only');
const rsOnly = hasFlag('--rs-only');
const separator = process.argv.indexOf('--');
const bunTestArgs = separator === -1 ? [] : process.argv.slice(separator + 1);

const tasks = [];
if (!rsOnly) tasks.push(task('bun test', ['bun', 'test', '--timeout', '15000', ...bunTestArgs]));
if (!tsOnly) {
  if (hasCargo()) {
    tasks.push(
      task('cargo test', ['cargo', 'test', '--all-targets', '--all-features', '--locked'])
    );
    tasks.push(task('cargo test --doc', ['cargo', 'test', '--doc', '--all-features', '--locked']));
  } else {
    warnNoCargo();
  }
}

exitWith(await runSequential(tasks));
