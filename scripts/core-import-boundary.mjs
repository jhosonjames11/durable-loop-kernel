import { readFile, readdir } from 'node:fs/promises';
import { extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

const forbiddenSegments = new Set(['dsh', 'deepseek', 'cordis']);
const sourceExtensions = new Set(['.ts', '.tsx', '.mts', '.cts']);
const dependencyFields = [
  'dependencies',
  'devDependencies',
  'peerDependencies',
  'optionalDependencies',
  'bundledDependencies',
  'bundleDependencies',
];

function scriptKindFor(fileName) {
  switch (extname(fileName)) {
    case '.tsx':
      return ts.ScriptKind.TSX;
    case '.mts':
      return ts.ScriptKind.TS;
    case '.cts':
      return ts.ScriptKind.TS;
    default:
      return ts.ScriptKind.TS;
  }
}

function literalText(node) {
  return ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)
    ? node.text
    : undefined;
}

/**
 * Bounded data-flow forms supported for require-like calls:
 *
 * - `require('literal')` and a simple identifier alias assigned from `require`;
 * - `createRequire` imported by name (including an import alias), or accessed as
 *   `namespace.createRequire` from `import * as namespace from 'node:module'`;
 * - a simple identifier assigned from one of those factories, including a later
 *   `identifier = factory(...)` assignment.
 *
 * This is intentionally not a JavaScript interpreter: it does not resolve
 * arbitrary expressions, control flow, destructuring, or scope-sensitive
 * reassignment. It conservatively collects these direct bindings before looking
 * at calls. Whenever a call through a supported binding has a literal module
 * specifier, that literal is reported.
 */
function requireBindingNames(sourceFile) {
  const factoryIdentifiers = new Set();
  const moduleNamespaces = new Set();

  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement) || literalText(statement.moduleSpecifier) !== 'node:module') {
      continue;
    }

    const bindings = statement.importClause?.namedBindings;
    if (bindings && ts.isNamedImports(bindings)) {
      for (const specifier of bindings.elements) {
        if ((specifier.propertyName?.text ?? specifier.name.text) === 'createRequire') {
          factoryIdentifiers.add(specifier.name.text);
        }
      }
    } else if (bindings && ts.isNamespaceImport(bindings)) {
      moduleNamespaces.add(bindings.name.text);
    }
  }

  function isFactoryCall(node) {
    if (!ts.isCallExpression(node)) {
      return false;
    }

    if (ts.isIdentifier(node.expression)) {
      return factoryIdentifiers.has(node.expression.text);
    }

    return ts.isPropertyAccessExpression(node.expression)
      && ts.isIdentifier(node.expression.expression)
      && moduleNamespaces.has(node.expression.expression.text)
      && node.expression.name.text === 'createRequire';
  }

  const requireIdentifiers = new Set(['require']);
  let changed = true;

  // Resolve direct aliases to a fixed point so `const a = require; const b = a`
  // is covered without attempting to model control flow or arbitrary values.
  while (changed) {
    changed = false;

    function recordBinding(name, expression) {
      const isRequireAlias = ts.isIdentifier(expression) && requireIdentifiers.has(expression.text);
      if ((isRequireAlias || isFactoryCall(expression)) && !requireIdentifiers.has(name)) {
        requireIdentifiers.add(name);
        changed = true;
      }
    }

    function visit(node) {
      if (ts.isVariableDeclaration(node)
        && ts.isIdentifier(node.name)
        && node.initializer) {
        recordBinding(node.name.text, node.initializer);
      } else if (ts.isBinaryExpression(node)
        && node.operatorToken.kind === ts.SyntaxKind.EqualsToken
        && ts.isIdentifier(node.left)) {
        recordBinding(node.left.text, node.right);
      }

      ts.forEachChild(node, visit);
    }

    visit(sourceFile);
  }

  return { requireIdentifiers, isFactoryCall };
}

/**
 * Return statically-known module specifiers from imports and require-like calls.
 * TypeScript's parser deliberately ignores interstitial comments in require calls.
 */
export function moduleSpecifiersInSource(source, fileName = 'source.ts') {
  const sourceFile = ts.createSourceFile(
    fileName,
    source,
    ts.ScriptTarget.Latest,
    true,
    scriptKindFor(fileName),
  );
  const {
    requireIdentifiers,
    isFactoryCall,
  } = requireBindingNames(sourceFile);
  const specifiers = [];

  function addSpecifier(node) {
    const specifier = literalText(node);
    if (specifier !== undefined) {
      specifiers.push(specifier);
    }
  }

  function visit(node) {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      if (node.moduleSpecifier) {
        addSpecifier(node.moduleSpecifier);
      }
    } else if (ts.isImportEqualsDeclaration(node)
      && ts.isExternalModuleReference(node.moduleReference)
      && node.moduleReference.expression) {
      addSpecifier(node.moduleReference.expression);
    } else if (ts.isCallExpression(node)) {
      const isDynamicImport = node.expression.kind === ts.SyntaxKind.ImportKeyword;
      const isCommonJsRequire = ts.isIdentifier(node.expression)
        && (node.expression.text === 'require' || requireIdentifiers.has(node.expression.text));
      const isDirectCreatedRequire = isFactoryCall(node.expression);

      if ((isDynamicImport || isCommonJsRequire || isDirectCreatedRequire) && node.arguments[0]) {
        addSpecifier(node.arguments[0]);
      }
    }

    ts.forEachChild(node, visit);
  }

  visit(sourceFile);
  return specifiers;
}

export function isForbiddenModuleSpecifier(specifier) {
  return specifier
    .toLowerCase()
    .split(/[@/_.-]+/u)
    .filter(Boolean)
    .some((segment) => forbiddenSegments.has(segment));
}

async function sourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(entries
    .sort((left, right) => left.name.localeCompare(right.name))
    .map(async (entry) => {
      const path = resolve(directory, entry.name);
      if (entry.isDirectory()) {
        return sourceFiles(path);
      }
      return sourceExtensions.has(extname(entry.name)) ? [path] : [];
    }));

  return nested.flat();
}

export async function findForbiddenCoreImports(directory) {
  const files = (await sourceFiles(directory)).sort();
  const violations = [];

  for (const file of files) {
    const source = await readFile(file, 'utf8');
    for (const specifier of moduleSpecifiersInSource(source, file)) {
      if (isForbiddenModuleSpecifier(specifier)) {
        violations.push(`${file}: ${specifier}`);
      }
    }
  }

  return violations;
}

export function findForbiddenCorePackageDependencies(packageJson) {
  const violations = [];

  for (const field of dependencyFields) {
    const dependencies = packageJson[field];
    const names = Array.isArray(dependencies)
      ? dependencies
      : Object.keys(dependencies ?? {});

    for (const name of names.sort()) {
      if (isForbiddenModuleSpecifier(name)) {
        violations.push(`${field}: ${name}`);
      }
    }
  }

  return violations;
}

async function main() {
  const projectRoot = resolve(import.meta.dirname, '..');
  const [importViolations, packageSource] = await Promise.all([
    findForbiddenCoreImports(resolve(projectRoot, 'packages/core/src')),
    readFile(resolve(projectRoot, 'packages/core/package.json'), 'utf8'),
  ]);
  const packageViolations = findForbiddenCorePackageDependencies(JSON.parse(packageSource));
  const violations = [...importViolations, ...packageViolations];

  if (violations.length > 0) {
    throw new Error(
      `packages/core must remain harness-neutral; forbidden references found:\n${violations.join('\n')}`,
    );
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main();
}
