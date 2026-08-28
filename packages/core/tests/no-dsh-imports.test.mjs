import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';

import {
  findForbiddenCoreImports,
  findForbiddenCorePackageDependencies,
  isForbiddenModuleSpecifier,
  moduleSpecifiersInSource,
} from '../../../scripts/core-import-boundary.mjs';

const projectRoot = resolve(import.meta.dirname, '../../..');
const coreSourceDirectory = join(projectRoot, 'packages/core/src');
const corePackageJson = join(projectRoot, 'packages/core/package.json');

test('module-specifier parser recognizes TypeScript import forms and supported require bindings', () => {
  const source = [
    "import /* static comment */ 'cordis';",
    "export type { Harness } from '@deepseek/harness';",
    "import adapter = require /* import-equals comment */ ('dsh-runtime');",
    "await import /* dynamic comment */ ('deepseek-harness');",
    "require /* require comment */ ('cordis-plugin');",
    "import { createRequire as makeRequire } from 'node:module';",
    'const packageRequire = makeRequire(import.meta.url);',
    "packageRequire /* created require comment */ ('@deepseek/client');",
    "makeRequire(import.meta.url) /* direct created require comment */ ('dsh');",
    "import * as Module from 'node:module';",
    'const namespaceRequire = Module.createRequire(import.meta.url);',
    "namespaceRequire('cordis-runtime');",
    'let assignedRequire;',
    'assignedRequire = makeRequire(import.meta.url);',
    "assignedRequire('deepseek-sdk');",
    'const commonJsAlias = require;',
    "commonJsAlias('cordis');",
  ].join('\n');

  assert.deepEqual(moduleSpecifiersInSource(source, 'fixture.ts'), [
    'cordis',
    '@deepseek/harness',
    'dsh-runtime',
    'deepseek-harness',
    'cordis-plugin',
    'node:module',
    '@deepseek/client',
    'dsh',
    'node:module',
    'cordis-runtime',
    'deepseek-sdk',
    'cordis',
  ]);
});

test('supported require binding forms allow non-forbidden literal module names', () => {
  const source = [
    "import { createRequire } from 'node:module';",
    "import * as Module from 'node:module';",
    'const direct = createRequire(import.meta.url);',
    'let assigned;',
    'assigned = Module.createRequire(import.meta.url);',
    'const alias = require;',
    "direct('node:fs');",
    "assigned('safe-package');",
    "alias('@company/notdsh-compatible');",
  ].join('\n');

  assert.deepEqual(
    moduleSpecifiersInSource(source, 'fixture.ts').filter(isForbiddenModuleSpecifier),
    [],
  );
});

test('supported require binding forms catch every remediated bypass', () => {
  const source = [
    "import * as Module from 'node:module';",
    'const namespaceRequire = Module.createRequire(import.meta.url);',
    "namespaceRequire('dsh-runtime');",
    "import { createRequire } from 'node:module';",
    'let assignedRequire;',
    'assignedRequire = createRequire(import.meta.url);',
    "assignedRequire('deepseek-sdk');",
    'const commonJsRequire = require;',
    "commonJsRequire('cordis');",
  ].join('\n');

  assert.deepEqual(
    moduleSpecifiersInSource(source, 'fixture.ts').filter(isForbiddenModuleSpecifier),
    ['dsh-runtime', 'deepseek-sdk', 'cordis'],
  );
});

test('forbidden matcher requires an explicit forbidden package-name segment', () => {
  for (const specifier of [
    'dsh',
    'dsh-runtime',
    '@dsh/runtime',
    '@deepseek/harness',
    'deepseek-harness',
    '@company/cordis-plugin',
    'CORDIS',
  ]) {
    assert.equal(isForbiddenModuleSpecifier(specifier), true, specifier);
  }

  for (const specifier of [
    'notdsh-compatible',
    'deepseeker-client',
    'cordisian-plugin',
    '@company/notdsh-compatible',
    'node:module',
  ]) {
    assert.equal(isForbiddenModuleSpecifier(specifier), false, specifier);
  }
});

test('scanner inspects every supported TypeScript extension', async (t) => {
  const fixtureDirectory = await mkdtemp(join(tmpdir(), 'loopgraph-core-boundary-'));
  t.after(() => rm(fixtureDirectory, { recursive: true, force: true }));

  await Promise.all([
    writeFile(join(fixtureDirectory, 'static.ts'), "import 'cordis';\n"),
    writeFile(join(fixtureDirectory, 'component.tsx'), "export * from 'deepseek-sdk';\n"),
    writeFile(join(fixtureDirectory, 'module.mts'), "await import('dsh-runtime');\n"),
    writeFile(join(fixtureDirectory, 'legacy.cts'), "require /* comment */ ('@vendor/cordis-plugin');\n"),
  ]);

  assert.deepEqual(
    await findForbiddenCoreImports(fixtureDirectory),
    [
      `${join(fixtureDirectory, 'component.tsx')}: deepseek-sdk`,
      `${join(fixtureDirectory, 'legacy.cts')}: @vendor/cordis-plugin`,
      `${join(fixtureDirectory, 'module.mts')}: dsh-runtime`,
      `${join(fixtureDirectory, 'static.ts')}: cordis`,
    ],
  );
});

test('scanner rejects the remediated require-binding bypasses', async (t) => {
  const fixtureDirectory = await mkdtemp(join(tmpdir(), 'loopgraph-core-require-boundary-'));
  t.after(() => rm(fixtureDirectory, { recursive: true, force: true }));

  await Promise.all([
    writeFile(join(fixtureDirectory, 'namespace.ts'), [
      "import * as Module from 'node:module';",
      'const r = Module.createRequire(import.meta.url);',
      "r('dsh-runtime');",
    ].join('\n')),
    writeFile(join(fixtureDirectory, 'assignment.ts'), [
      "import { createRequire } from 'node:module';",
      'let r;',
      'r = createRequire(import.meta.url);',
      "r('deepseek-sdk');",
    ].join('\n')),
    writeFile(join(fixtureDirectory, 'commonjs.ts'), [
      'const r = require;',
      "r('cordis');",
    ].join('\n')),
  ]);

  assert.deepEqual(
    await findForbiddenCoreImports(fixtureDirectory),
    [
      `${join(fixtureDirectory, 'assignment.ts')}: deepseek-sdk`,
      `${join(fixtureDirectory, 'commonjs.ts')}: cordis`,
      `${join(fixtureDirectory, 'namespace.ts')}: dsh-runtime`,
    ],
  );
});

test('core package dependency scanner rejects only forbidden dependency segments', () => {
  const packageJson = {
    dependencies: {
      '@deepseek/sdk': '1.0.0',
      'notdsh-compatible': '1.0.0',
    },
    devDependencies: {
      'cordis-plugin': '1.0.0',
    },
    peerDependencies: {
      'dsh-runtime': '1.0.0',
    },
    bundledDependencies: ['cordis-bundle', 'safe-package'],
    bundleDependencies: ['deepseek-bundle', 'safe-package'],
  };

  assert.deepEqual(findForbiddenCorePackageDependencies(packageJson), [
    'dependencies: @deepseek/sdk',
    'devDependencies: cordis-plugin',
    'peerDependencies: dsh-runtime',
    'bundledDependencies: cordis-bundle',
    'bundleDependencies: deepseek-bundle',
  ]);
});

test('core source and manifest remain DSH, DeepSeek, and Cordis free', async () => {
  assert.deepEqual(await findForbiddenCoreImports(coreSourceDirectory), []);
  const packageJson = JSON.parse(await readFile(corePackageJson, 'utf8'));
  assert.deepEqual(findForbiddenCorePackageDependencies(packageJson), []);
});
