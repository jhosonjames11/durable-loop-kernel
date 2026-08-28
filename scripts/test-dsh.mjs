import { access } from 'node:fs/promises';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import process from 'node:process';

const projectRoot = resolve(import.meta.dirname, '..');
const expectedDefaultRevision = 'b150a551b8d465e31e418e1b2eaf5e79bbb7d28e';
const checkoutOverride = process.env.DSH_CHECKOUT;
const checkout = resolve(checkoutOverride ?? resolve(projectRoot, '../deepseek-harness'));
const testFile = resolve(projectRoot, 'packages/dsh-adapter/tests/real-composition.test.ts');

async function available(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function revisionOf(path) {
  const full = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: path, encoding: 'utf8' });
  if (full.error || full.status !== 0) return undefined;
  const short = spawnSync('git', ['rev-parse', '--short=8', 'HEAD'], { cwd: path, encoding: 'utf8' });
  if (short.error || short.status !== 0) return undefined;
  return { full: full.stdout.trim(), short: short.stdout.trim() };
}

if (!await available(resolve(checkout, 'package.json'))
  || !await available(resolve(checkout, 'node_modules'))
  || !await available(resolve(checkout, 'node_modules/.bin/tsx'))) {
  console.error(`DSH prerequisite unavailable: expected checkout with installed dependencies at ${checkout}. Set DSH_CHECKOUT=/path/to/deepseek-harness and run its package install.`);
  process.exitCode = 1;
} else {
  const revision = revisionOf(checkout);
  if (revision === undefined) {
    console.error(`DSH prerequisite unavailable: could not resolve git revision at ${checkout}.`);
    process.exitCode = 1;
  } else if (checkoutOverride === undefined && revision.full !== expectedDefaultRevision) {
    console.error(`DSH compatibility mismatch: default checkout ${checkout} must be at ${expectedDefaultRevision}, found ${revision.full}. Set DSH_CHECKOUT=/path/to/deepseek-harness to test an explicit override.`);
    process.exitCode = 1;
  } else {
    const result = spawnSync('corepack', ['pnpm', 'exec', 'tsx', testFile], {
      cwd: checkout,
      env: {
        ...process.env,
        DSH_COMMIT: revision.short,
        TSX_TSCONFIG_PATH: resolve(checkout, 'tsconfig.base.json'),
      },
      stdio: 'inherit',
    });
    if (result.error) {
      console.error(`DSH test launcher failed: ${result.error.message}`);
      process.exitCode = 1;
    } else {
      process.exitCode = result.status ?? 1;
    }
  }
}
