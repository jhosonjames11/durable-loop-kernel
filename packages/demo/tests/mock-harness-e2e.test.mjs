import assert from 'node:assert/strict';
import { rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import test from 'node:test';

import { runMockHarnessScenario } from '@loopgraph/demo';

const directory = join(tmpdir(), 'loopgraph-demo-e2e-fixed');

async function removeScenarioDirectory() {
  // This test owns only this deliberately fixed, dedicated test directory.
  assert.equal(basename(directory), 'loopgraph-demo-e2e-fixed');
  await rm(directory, { recursive: true, force: true });
}

test('durable mock harness proves retry, fail-closed HITL, recovery, resume, and rollback', async (t) => {
  await removeScenarioDirectory();
  t.after(removeScenarioDirectory);

  const result = await runMockHarnessScenario(directory);

  assert.deepEqual(result.finalView.promotedGenerations.map(({ generationId }) => generationId), ['generation-v1', 'generation-v2']);
  assert.equal(result.finalView.activeGenerationId, 'generation-v1');
  assert.deepEqual(result.finalView.rollbackAncestry, [{
    rollbackSequence: result.rollbackSequence,
    fromGenerationId: 'generation-v2',
    targetGenerationId: 'generation-v1',
  }]);
  assert.equal(result.finalView.retryCount, 1);

  assert.equal(result.recoveryView.phase, 'PAUSED_RECOVERED');
  assert.equal(result.executionCallsBeforeResume, result.executionCallsAfterRecovery);
  assert.equal(result.executionCallsAfterResume, result.executionCallsAfterRecovery);
  assert.equal(result.persistedEventCount, result.eventTypes.length);
  assert.ok(result.eventTypes.includes('recovery.uncertain'));
  assert.ok(result.eventTypes.includes('rollback.applied'));
  assert.ok(result.eventTypes.includes('generation.prepared'));
  assert.ok(result.eventTypes.includes('generation.promoted'));

  assert.deepEqual(result.validationResults, [false, true, true, true, true]);
  assert.deepEqual(result.humanDecisions, ['ALLOWED_ONCE', 'ALLOWED_ONCE', 'DENIED', 'UNAVAILABLE']);
  for (const callback of result.adapterCallbacks) {
    assert.equal(callback.runId, 'mock-run-001');
    assert.equal(callback.nodeId, 'build');
    assert.match(callback.attemptId, /^attempt-[1-6]$/u);
    assert.match(callback.correlationId, /^correlation-attempt-[1-6]$/u);
  }
  assert.deepEqual(JSON.parse(JSON.stringify(result)), result);
});
