import { access, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import process from 'node:process';

const projectRoot = resolve(import.meta.dirname, '..');
const sourceLockPath = resolve(projectRoot, 'dsh-source.lock.json');
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

function gitText(path, arguments_) {
  const result = spawnSync('git', arguments_, { cwd: path, encoding: 'utf8' });
  return result.error || result.status !== 0 ? undefined : result.stdout.trim();
}

function normalizedGitUrl(value) {
  return value.replace(/^git@github\.com:/u, 'https://github.com/').replace(/\.git$/u, '').replace(/\/$/u, '');
}

async function sourceLock() {
  let parsed;
  try { parsed = JSON.parse(await readFile(sourceLockPath, 'utf8')); } catch {
    throw new Error(`DSH source lock is unreadable: ${sourceLockPath}`);
  }
  if (parsed === null || typeof parsed !== 'object'
    || typeof parsed.repository !== 'string' || typeof parsed.commit !== 'string'
    || typeof parsed.version !== 'string' || typeof parsed.packageManager !== 'string') {
    throw new Error(`DSH source lock is invalid: ${sourceLockPath}`);
  }
  return parsed;
}

const lock = await sourceLock();

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
  } else if (revision.full !== lock.commit) {
    console.error(`DSH compatibility mismatch: checkout ${checkout} must be at source-lock commit ${lock.commit}, found ${revision.full}.`);
    process.exitCode = 1;
  } else {
    const origin = gitText(checkout, ['remote', 'get-url', 'origin']);
    let manifest;
    try { manifest = JSON.parse(await readFile(resolve(checkout, 'package.json'), 'utf8')); } catch { manifest = undefined; }
    if (origin === undefined || normalizedGitUrl(origin) !== normalizedGitUrl(lock.repository)) {
      console.error(`DSH compatibility mismatch: checkout origin must be ${lock.repository}, found ${origin ?? 'none'}.`);
      process.exitCode = 1;
    } else if (manifest?.version !== lock.version || manifest?.packageManager !== lock.packageManager) {
      console.error(`DSH compatibility mismatch: expected DSH ${lock.version} with ${lock.packageManager}, found ${manifest?.version ?? 'unknown'} with ${manifest?.packageManager ?? 'unknown'}.`);
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
}
