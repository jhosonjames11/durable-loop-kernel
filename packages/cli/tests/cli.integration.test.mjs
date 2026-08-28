import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';

import { EVENT_VERSION } from '@loopgraph/core';
import { SqliteRunStore } from '@loopgraph/storage';

const projectRoot = resolve(import.meta.dirname, '../../..');
const cli = join(projectRoot, 'packages/cli/dist/index.js');
const actor = 'operator-a';

function loopSpec(revision) {
  return {
    specId: 'fixture-loop', revision, entryNodeId: 'build',
    nodes: [{ nodeId: 'build', kind: 'agent' }],
    edges: [{ fromNodeId: 'build', toNodeId: 'build' }],
  };
}

function generationRevision(generation) {
  const matched = /(?:-|v)(\d+)$/u.exec(generation);
  return (matched === null ? 1 : Number(matched[1])) + 1;
}

async function temporaryDirectory() {
  return mkdtemp(join(tmpdir(), 'loopgraph-cli-test-'));
}

function runCli(arguments_) {
  const result = spawnSync(process.execPath, [cli, ...arguments_], { encoding: 'utf8' });
  assert.notEqual(result.stdout, '');
  return { status: result.status, json: JSON.parse(result.stdout) };
}

function append(store, lease, runId, type, payload, metadata = {}) {
  const revision = store.read(runId).length;
  store.append({
    version: EVENT_VERSION,
    eventId: `fixture-${runId}-${revision + 1}`,
    runId,
    seq: revision + 1,
    occurredAt: new Date(Date.UTC(2026, 7, 25, 0, 0, revision + 1)).toISOString(),
    actor: metadata.actor ?? 'fixture',
    causationId: null,
    correlationId: metadata.correlationId ?? 'fixture-correlation',
    idempotencyKey: `fixture-key-${runId}-${revision + 1}`,
    expectedRevision: revision,
    type,
    payload,
  }, lease);
}

function withStore(filename, runId, setup) {
  const store = new SqliteRunStore({ filename });
  const lease = store.acquire(runId, 'fixture', 10_000);
  assert.ok(lease);
  setup(store, lease);
  assert.equal(store.release(lease), true);
  store.close();
}

function seedValidated(store, lease, runId, generation, attempt) {
  append(store, lease, runId, 'node.dispatch.requested', { nodeId: 'build', attemptId: attempt });
  append(store, lease, runId, 'node.started', { nodeId: 'build', attemptId: attempt });
  append(store, lease, runId, 'node.settled', { nodeId: 'build', attemptId: attempt, outcome: 'SUCCEEDED', outcomeCode: 'FIXTURE_OK' });
  const validationId = `validation-${attempt}`;
  append(store, lease, runId, 'validation.recorded', { nodeId: 'build', attemptId: attempt, validationId, passed: true, evidenceRef: 'super-secret-evidence' });
  const candidateLoopSpec = loopSpec(generationRevision(generation));
  append(store, lease, runId, 'generation.prepared', { generationId: generation, validationId, manifestRef: 'secret://manifest', candidateLoopSpec });
  append(store, lease, runId, 'hitl.requested', {
    requestId: `approval-${attempt}`, promptRef: 'secret://prompt', generationId: generation,
    specId: candidateLoopSpec.specId, specRevision: candidateLoopSpec.revision, candidateLoopSpec, validationId,
  });
  append(store, lease, runId, 'hitl.decided', {
    requestId: `approval-${attempt}`, decision: 'APPROVED', decisionCode: 'FIXTURE_APPROVED',
    approvalSubject: 'fixture-authenticated-approver', approvalReceiptRef: `receipt:${attempt}`,
  });
}

test('compiled operator CLI enforces arguments, redacts inspect, fences revisions, and controls lifecycle', async (t) => {
  const directory = await temporaryDirectory();
  t.after(() => rm(directory, { recursive: true, force: true }));
  const database = join(directory, 'runs.sqlite');
  const runId = 'cli-run';
  withStore(database, runId, (store, lease) => {
    append(store, lease, runId, 'run.created', { loopSpec: loopSpec(1), requiresHitl: true });
  });

  let result = runCli(['pause', '--db', database, '--run', runId]);
  assert.equal(result.status, 1);
  assert.deepEqual(result.json, { ok: false, command: 'pause', code: 'USAGE' });

  result = runCli(['inspect', '--db', database, '--run', runId]);
  assert.equal(result.status, 0);
  assert.equal(result.json.code, 'OK');
  assert.equal(result.json.run.revision, 1);
  assert.deepEqual(result.json.run.activeLoopSpec, {
    specId: 'fixture-loop', revision: 1, entryNodeId: 'build',
    nodes: [{ nodeId: 'build', kind: 'agent' }],
    edges: [{ fromNodeId: 'build', toNodeId: 'build' }],
    nextNodeId: 'build',
  });
  assert.equal(JSON.stringify(result.json).includes('secret://workflow'), false);
  assert.equal(JSON.stringify(result.json).includes('super-secret-evidence'), false);
  assert.equal(Object.hasOwn(result.json.run.timeline.items[0], 'payload'), false);

  result = runCli(['pause', '--db', database, '--run', runId, '--actor', actor, '--expected-revision', '0']);
  assert.equal(result.status, 1);
  assert.equal(result.json.code, 'REVISION_CONFLICT');

  result = runCli(['pause', '--db', database, '--run', runId, '--actor', actor, '--expected-revision', '1']);
  assert.equal(result.status, 0);
  assert.equal(result.json.phase, 'PAUSED');
  assert.equal(result.json.revision, 3);

  result = runCli(['resume', '--db', database, '--run', runId, '--actor', actor, '--expected-revision', '3']);
  assert.equal(result.status, 0);
  assert.equal(result.json.phase, 'READY');
  assert.equal(result.json.revision, 4);
});

test('compiled CLI permits only prepared approved promotions and rolls back only to a prior promotion', async (t) => {
  const directory = await temporaryDirectory();
  t.after(() => rm(directory, { recursive: true, force: true }));
  const database = join(directory, 'runs.sqlite');
  const runId = 'cli-generation-run';
  withStore(database, runId, (store, lease) => {
    append(store, lease, runId, 'run.created', { loopSpec: loopSpec(1), requiresHitl: true });
    seedValidated(store, lease, runId, 'generation-v1', 'attempt-1');
  });

  let result = runCli(['promote', '--db', database, '--run', runId, '--actor', actor, '--expected-revision', '8', '--generation', 'not-prepared']);
  assert.equal(result.status, 1);
  assert.equal(result.json.code, 'PROMOTION_NOT_AUTHORIZED');

  result = runCli(['promote', '--db', database, '--run', runId, '--actor', actor, '--expected-revision', '8', '--generation', 'generation-v1']);
  assert.equal(result.status, 0);
  assert.equal(result.json.revision, 9);

  withStore(database, runId, (store, lease) => {
    seedValidated(store, lease, runId, 'generation-v2', 'attempt-2');
    append(store, lease, runId, 'generation.promoted', { generationId: 'generation-v2' });
  });

  result = runCli(['rollback', '--db', database, '--run', runId, '--actor', actor, '--expected-revision', '17', '--generation', 'generation-v2']);
  assert.equal(result.status, 1);
  assert.equal(result.json.code, 'ROLLBACK_TARGET_INVALID');

  result = runCli(['rollback', '--db', database, '--run', runId, '--actor', actor, '--expected-revision', '17', '--generation', 'generation-v1']);
  assert.equal(result.status, 0);
  assert.equal(result.json.revision, 18);
  assert.equal(result.json.phase, 'READY');
});

test('compiled inspect bounds every historical projection and redacts payloads', async (t) => {
  const directory = await temporaryDirectory();
  t.after(() => rm(directory, { recursive: true, force: true }));
  const database = join(directory, 'runs.sqlite');
  const runId = 'cli-bounded-history-run';
  const historyLimit = 20;
  const oversizedIdentifier = `generation-${'😀'.repeat(300)}`;
  const oversizedActor = `operator-${'😀'.repeat(300)}`;
  const oversizedCorrelation = `correlation-${'😀'.repeat(300)}`;

  withStore(database, runId, (store, lease) => {
    append(store, lease, runId, 'run.created', {
      loopSpec: loopSpec(1),
      requiresHitl: false,
    });
    for (let index = 1; index <= historyLimit + 1; index += 1) {
      const generation = index === historyLimit + 1 ? oversizedIdentifier : `generation-${index}`;
      const attempt = `attempt-${index}`;
      const validation = index === historyLimit + 1 ? oversizedIdentifier : `validation-${index}`;
      append(store, lease, runId, 'node.dispatch.requested', { nodeId: 'build', attemptId: attempt });
      append(store, lease, runId, 'node.started', { nodeId: 'build', attemptId: attempt });
      append(store, lease, runId, 'node.settled', {
        nodeId: 'build', attemptId: attempt, outcome: 'SUCCEEDED', outcomeCode: 'SECRET_OUTCOME_CODE_DO_NOT_EMIT',
      });
      append(store, lease, runId, 'validation.recorded', {
        nodeId: 'build', attemptId: attempt, validationId: validation, passed: true,
        evidenceRef: 'SECRET_EVIDENCE_PAYLOAD_DO_NOT_EMIT',
      });
      append(store, lease, runId, 'generation.prepared', {
        generationId: generation, validationId: validation, manifestRef: 'SECRET_MANIFEST_PAYLOAD_DO_NOT_EMIT',
        candidateLoopSpec: loopSpec(index + 1),
      });
      append(store, lease, runId, 'generation.promoted', { generationId: generation });
      if (index > 1) {
        append(store, lease, runId, 'rollback.applied', {
          targetGenerationId: 'generation-1', reasonCode: 'SECRET_ROLLBACK_REASON_DO_NOT_EMIT',
        });
      }
    }
    for (let index = 0; index < historyLimit + 1; index += 1) {
      append(store, lease, runId, 'rollback.applied', {
        targetGenerationId: index % 2 === 0 ? 'generation-2' : 'generation-1',
        reasonCode: 'SECRET_ROLLBACK_REASON_DO_NOT_EMIT',
      }, { actor: oversizedActor, correlationId: oversizedCorrelation });
    }
  });

  const result = runCli(['inspect', '--db', database, '--run', runId]);
  assert.equal(result.status, 0);
  assert.equal(result.json.code, 'OK');
  for (const name of ['timeline', 'preparedGenerations', 'promotedGenerations', 'rollbackAncestry']) {
    const history = result.json.run[name];
    assert.equal(history.items.length, historyLimit, `${name} item cap`);
    assert.equal(history.total > historyLimit, true, `${name} total`);
    assert.equal(history.truncated, true, `${name} truncation`);
  }
  assert.equal(result.json.run.preparedGenerations.total, historyLimit + 1);
  assert.equal(result.json.run.promotedGenerations.total, historyLimit + 1);
  assert.equal(result.json.run.rollbackAncestry.total, (historyLimit * 2) + 1);
  assert.equal(result.json.run.preparedGenerations.items[0].generationId, 'generation-2');
  assert.equal(Array.from(result.json.run.preparedGenerations.items.at(-1).generationId).length, 256);
  assert.equal(result.json.run.preparedGenerations.items.at(-1).generationId.endsWith('…'), true);
  assert.equal(Array.from(result.json.run.timeline.items.at(-1).actor).length, 256);
  assert.equal(result.json.run.timeline.items.at(-1).actor.endsWith('…'), true);
  assert.equal(Array.from(result.json.run.timeline.items.at(-1).correlation).length, 256);
  assert.equal(result.json.run.timeline.items.at(-1).correlation.endsWith('…'), true);
  assert.equal(Object.hasOwn(result.json.run.timeline.items.at(-1), 'payload'), false);
  const output = JSON.stringify(result.json);
  for (const secret of [
    'SECRET_WORKFLOW_PAYLOAD_DO_NOT_EMIT',
    'SECRET_OUTCOME_CODE_DO_NOT_EMIT',
    'SECRET_EVIDENCE_PAYLOAD_DO_NOT_EMIT',
    'SECRET_MANIFEST_PAYLOAD_DO_NOT_EMIT',
    'SECRET_ROLLBACK_REASON_DO_NOT_EMIT',
  ]) {
    assert.equal(output.includes(secret), false, `${secret} leaked`);
  }
});

test('compiled CLI demo returns bounded data and no temporary path', () => {
  const result = runCli(['demo']);
  assert.equal(result.status, 0);
  assert.equal(result.json.code, 'OK');
  assert.equal(result.json.report.cleanedUp, true);
  assert.equal(JSON.stringify(result.json).includes(tmpdir()), false);
  assert.equal(Object.hasOwn(result.json.report, 'timeline'), false);
});
