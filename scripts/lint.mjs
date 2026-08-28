import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import { extname, join, resolve } from 'node:path';

const projectRoot = resolve(import.meta.dirname, '..');
const checkedDirectories = ['packages', 'scripts'];
const checkedExtensions = new Set(['.json', '.mjs', '.ts']);

async function filesIn(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(entries.map(async (entry) => {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      return filesIn(path);
    }

    return checkedExtensions.has(extname(entry.name)) ? [path] : [];
  }));

  return nested.flat();
}

const violations = [];
for (const relativeDirectory of checkedDirectories) {
  const directory = join(projectRoot, relativeDirectory);
  for (const file of await filesIn(directory)) {
    const source = await readFile(file, 'utf8');
    const lines = source.split('\n');

    for (const [index, line] of lines.entries()) {
      if (/[ \t]+$/.test(line)) {
        violations.push(`${file}:${index + 1}: trailing whitespace`);
      }
    }

    if (!source.endsWith('\n')) {
      violations.push(`${file}: missing final newline`);
    }
  }
}

assert.deepEqual(violations, [], violations.join('\n'));
