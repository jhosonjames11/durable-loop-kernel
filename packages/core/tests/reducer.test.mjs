import assert from 'node:assert/strict';
import test from 'node:test';

import { EVENT_VERSION, ReducerError, ReducerErrorCode, reduce } from '@loopgraph/core';

const RUN_ID = 'run-001';
const ACTOR = 'operator-001';

function loopSpec(revision = 1, entryNodeId = 'node-build') {
  return {
    specId: 'test-loop', revision, entryNodeId,
    nodes: [{ nodeId: 'node-build', kind: 'agent' }, { nodeId: 'node-next', kind: 'agent' }],
    edges: [
      { fromNodeId: 'node-build', toNodeId: 'node-build' },
      { fromNodeId: 'node-build', toNodeId: 'node-next' },
      { fromNodeId: 'node-next', toNodeId: 'node-next' },
    ],
  };
}

/** Construct a fully deterministic event; no generated ids or clocks are used in tests. */
function event(seq, type, payload, overrides = {}) {
  const normalized = type === 'run.created'
    ? { ...payload, loopSpec: payload.loopSpec ?? loopSpec(1) }
    : type === 'generation.prepared'
      ? { ...payload, candidateLoopSpec: payload.candidateLoopSpec ?? loopSpec(seq) }
      : type === 'validation.superseded'
        ? { ...payload, nextNodeId: payload.nextNodeId ?? 'node-build' }
        : type === 'hitl.requested'
          ? { ...payload, generationId: payload.generationId ?? 'generation-1', specId: payload.specId ?? 'test-loop', specRevision: payload.specRevision ?? 6, validationId: payload.validationId ?? 'validation-5' }
          : payload;
  const result = {
    version: EVENT_VERSION,
    eventId: `event-${String(seq).padStart(3, '0')}`,
    runId: RUN_ID,
    seq,
    occurredAt: `2026-08-25T00:00:${String(seq).padStart(2, '0')}.000Z`,
    actor: ACTOR,
    causationId: null,
    correlationId: 'correlation-001',
    idempotencyKey: `idempotency-${seq}`,
    expectedRevision: seq - 1,
    type,
    payload: normalized,
    ...overrides,
  };
  // Callers occasionally replace the whole payload while constructing a
  // negative case. Keep fixtures protocol-valid unless that test explicitly
  // intends to exercise payload validation.
  if (result.type === 'run.created' && result.payload !== null && typeof result.payload === 'object') {
    result.payload = { ...result.payload, loopSpec: result.payload.loopSpec ?? loopSpec(1) };
  }
  if (result.type === 'generation.prepared' && result.payload !== null && typeof result.payload === 'object') {
    result.payload = { ...result.payload, candidateLoopSpec: result.payload.candidateLoopSpec ?? loopSpec(seq) };
  }
  if (result.type === 'validation.superseded' && result.payload !== null && typeof result.payload === 'object') {
    result.payload = { ...result.payload, nextNodeId: result.payload.nextNodeId ?? 'node-build' };
  }
  if (result.type === 'hitl.requested' && result.payload !== null && typeof result.payload === 'object') {
    result.payload = {
      ...result.payload,
      generationId: result.payload.generationId ?? 'generation-1',
      specId: result.payload.specId ?? 'test-loop',
      specRevision: result.payload.specRevision ?? 6,
      candidateLoopSpec: result.payload.candidateLoopSpec ?? loopSpec(result.payload.specRevision ?? 6),
      validationId: result.payload.validationId ?? 'validation-5',
    };
  }
  if (result.type === 'hitl.decided' && result.payload !== null && typeof result.payload === 'object') {
    result.payload = {
      ...result.payload,
      approvalSubject: result.payload.approvalSubject ?? 'authenticated-test-approver',
      approvalReceiptRef: result.payload.approvalReceiptRef ?? `receipt:${result.payload.requestId ?? seq}`,
    };
  }
  return result;
}

function created(overrides = {}) {
  return event(1, 'run.created', { loopSpec: loopSpec(1), requiresHitl: false }, overrides);
}

function executionEvents(startAt = 2) {
  return [
    event(startAt, 'node.dispatch.requested', {
      nodeId: 'node-build',
      attemptId: `attempt-${startAt}`,
    }),
    event(startAt + 1, 'node.started', {
      nodeId: 'node-build',
      attemptId: `attempt-${startAt}`,
    }),
    event(startAt + 2, 'node.settled', {
      nodeId: 'node-build',
      attemptId: `attempt-${startAt}`,
      outcome: 'SUCCEEDED',
      outcomeCode: 'OK',
    }),
  ];
}

function passedValidation(seq, attemptId = 'attempt-2') {
  return event(seq, 'validation.recorded', {
    nodeId: 'node-build',
    attemptId,
    validationId: `validation-${seq}`,
    passed: true,
    evidenceRef: `evidence:validation-${seq}`,
  });
}

function expectError(code, sequence, callback) {
  assert.throws(callback, (error) => {
    assert.ok(error instanceof ReducerError);
    assert.equal(error.code, code);
    assert.equal(error.sequence, sequence);
    return true;
  });
}

test('creates a ready run with a serializable empty projection', () => {
  const view = reduce([created()]);

  assert.equal(view.revision, 1);
  assert.equal(view.phase, 'READY');
  assert.equal(view.runId, RUN_ID);
  assert.equal(view.activeAttempt, null);
  assert.deepEqual(view.acceptedIdempotencyKeys, ['idempotency-1']);
  assert.deepEqual(JSON.parse(JSON.stringify(view)), view);
});

test('dispatches, starts, and settles a node into validation', () => {
  const view = reduce([created(), ...executionEvents()]);

  assert.equal(view.phase, 'VALIDATING');
  assert.deepEqual(view.activeAttempt, null);
  assert.deepEqual(view.validation, {
    status: 'PENDING',
    nodeId: 'node-build',
    attemptId: 'attempt-2',
    validationId: null,
    evidenceRef: null,
  });
});

test('rejects a node settlement that has not observed its matching start', () => {
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 3, () => reduce([
    created(),
    event(2, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(3, 'node.settled', {
      nodeId: 'node-build', attemptId: 'attempt-2', outcome: 'SUCCEEDED', outcomeCode: 'OK',
    }),
  ]));
});

test('binds validation evidence to the exact node and attempt that settled', () => {
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 5, () => reduce([
    created(),
    ...executionEvents(),
    event(5, 'validation.recorded', {
      nodeId: 'node-other', attemptId: 'attempt-2', validationId: 'validation-5',
      passed: true, evidenceRef: 'evidence:validation-5',
    }),
  ]));
});

test('records a monotonically fenced lease without coupling the pure reducer to a worker adapter', () => {
  const view = reduce([
    created(),
    event(2, 'lease.acquired', { holderId: 'worker-a', fencingToken: 1 }),
    event(3, 'lease.acquired', { holderId: 'worker-b', fencingToken: 2 }),
  ]);

  assert.deepEqual(view.lease, { holderId: 'worker-b', fencingToken: 2 });
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 3, () => reduce([
    created(),
    event(2, 'lease.acquired', { holderId: 'worker-a', fencingToken: 2 }),
    event(3, 'lease.acquired', { holderId: 'worker-b', fencingToken: 2 }),
  ]));
});

test('pauses at a running attempt safe boundary without a bypassable intermediate phase', () => {
  const events = [
    created(),
    event(2, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(3, 'node.started', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(4, 'pause.requested', { reasonCode: 'OPERATOR_REQUEST' }),
    event(5, 'node.settled', {
      nodeId: 'node-build', attemptId: 'attempt-2', outcome: 'SUCCEEDED', outcomeCode: 'OK',
    }),
  ];
  const view = reduce(events);

  assert.equal(view.phase, 'PAUSED');
  assert.equal(view.pauseRequested, false);
  assert.equal(view.activeAttempt, null);
  assert.equal(view.validation.status, 'PENDING');
});

test('a paused run records validation but cannot prepare or promote until explicitly resumed', () => {
  const pausedWithValidation = [
    created(),
    event(2, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(3, 'node.started', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(4, 'pause.requested', { reasonCode: 'OPERATOR_REQUEST' }),
    event(5, 'node.settled', {
      nodeId: 'node-build', attemptId: 'attempt-2', outcome: 'SUCCEEDED', outcomeCode: 'OK',
    }),
    passedValidation(6),
  ];
  const paused = reduce(pausedWithValidation);
  assert.equal(paused.phase, 'PAUSED');
  assert.equal(paused.pauseRequested, false);
  assert.equal(paused.validation.status, 'PASSED');

  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 7, () => reduce([
    ...pausedWithValidation,
    event(7, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-6', manifestRef: 'manifest:generation-1',
    }),
  ]));

  const resumed = reduce([
    ...pausedWithValidation,
    event(7, 'run.resumed', { reasonCode: 'OPERATOR_RESUME' }),
    event(8, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-6', manifestRef: 'manifest:generation-1',
    }),
    event(9, 'generation.promoted', { generationId: 'generation-1' }),
  ]);
  assert.equal(resumed.phase, 'READY');
  assert.equal(resumed.activeGenerationId, 'generation-1');
});

test('a failed execution returns ready and a malicious passed validation cannot prepare or promote', () => {
  const failed = [
    created(),
    event(2, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(3, 'node.started', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(4, 'node.settled', {
      nodeId: 'node-build', attemptId: 'attempt-2', outcome: 'FAILED', outcomeCode: 'MODEL_ERROR',
    }),
  ];
  const view = reduce(failed);
  assert.equal(view.phase, 'READY');
  assert.equal(view.validation.status, 'NONE');
  assert.equal(view.hitl.status, 'NOT_REQUESTED');
  assert.equal(view.retryCount, 1);
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 5, () => reduce([
    ...failed,
    event(5, 'validation.recorded', {
      nodeId: 'node-build', attemptId: 'attempt-2', validationId: 'forged', passed: true, evidenceRef: 'forged:evidence',
    }),
  ]));
  expectError(ReducerErrorCode.GENERATION_NOT_VALIDATED, 5, () => reduce([
    ...failed,
    event(5, 'generation.prepared', { generationId: 'forged', validationId: 'forged', manifestRef: 'manifest:forged' }),
  ]));
  expectError(ReducerErrorCode.GENERATION_NOT_VALIDATED, 5, () => reduce([
    ...failed,
    event(5, 'generation.promoted', { generationId: 'forged' }),
  ]));
});

test('a failed validation returns ready and requires an explicit retry dispatch', () => {
  const events = [
    created(),
    ...executionEvents(),
    event(5, 'validation.recorded', {
      nodeId: 'node-build', attemptId: 'attempt-2', validationId: 'validation-5',
      passed: false, evidenceRef: 'evidence:validation-5',
    }),
  ];
  const view = reduce(events);

  assert.equal(view.phase, 'READY');
  assert.equal(view.retryCount, 1);
  assert.equal(view.validation.status, 'FAILED');
  const retry = reduce([...events, event(6, 'node.dispatch.requested', {
    nodeId: 'node-build', attemptId: 'attempt-6',
  })]);
  assert.equal(retry.phase, 'RUNNING');
  assert.equal(retry.activeAttempt.attemptId, 'attempt-6');
});

test('execution-admission HITL is durable, fail-closed, and distinct from promotion authority', () => {
  const denied = reduce([
    created(),
    event(2, 'admission.hitl.requested', { requestId: 'admission-2', reasonCode: 'DSH_PROMOTION' }),
    event(3, 'admission.hitl.decided', { requestId: 'admission-2', decision: 'UNAVAILABLE', decisionCode: 'DSH_UNAVAILABLE' }),
  ]);
  assert.equal(denied.phase, 'READY');
  assert.deepEqual(denied.admissionHitl, { status: 'UNAVAILABLE', requestId: 'admission-2' });
  assert.deepEqual(denied.hitl, {
    status: 'NOT_REQUESTED', requestId: null, generationId: null,
    specId: null, specRevision: null, candidateLoopSpec: null, validationId: null,
    approvalSubject: null, approvalReceiptRef: null,
  });
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 3, () => reduce([
    created(),
    event(2, 'admission.hitl.requested', { requestId: 'admission-2', reasonCode: 'DSH_PROMOTION' }),
    event(3, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-3' }),
  ]));
});

test('a loop can explicitly supersede validated evidence before advancing to its next node', () => {
  const events = [
    created(),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'validation.superseded', { reasonCode: 'NEXT_GRAPH_EDGE', nextNodeId: 'node-next' }),
    event(7, 'node.dispatch.requested', { nodeId: 'node-next', attemptId: 'attempt-7' }),
  ];
  const view = reduce(events);
  assert.equal(view.phase, 'RUNNING');
  assert.equal(view.validation.status, 'NONE');
  assert.equal(view.activeAttempt.nodeId, 'node-next');
  expectError(ReducerErrorCode.GENERATION_NOT_VALIDATED, 8, () => reduce([
    ...events,
    event(8, 'generation.prepared', {
      generationId: 'stale-generation', validationId: 'validation-5', manifestRef: 'manifest:stale-generation',
    }),
  ]));
});

test('a LoopSpec is canonical graph state, and promotion changes the executable entry node', () => {
  const candidate = loopSpec(2, 'node-next');
  const promoted = reduce([
    created(),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-2', validationId: 'validation-5', manifestRef: 'manifest:generation-2', candidateLoopSpec: candidate,
    }),
    event(7, 'generation.promoted', { generationId: 'generation-2' }),
  ]);
  assert.equal(promoted.loopSpec.revision, 2);
  assert.equal(promoted.loopSpec.entryNodeId, 'node-next');
  assert.equal(promoted.nextNodeId, 'node-next');
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 8, () => reduce([
    ...[
      created(), ...executionEvents(), passedValidation(5),
      event(6, 'generation.prepared', {
        generationId: 'generation-2', validationId: 'validation-5', manifestRef: 'manifest:generation-2', candidateLoopSpec: candidate,
      }),
      event(7, 'generation.promoted', { generationId: 'generation-2' }),
    ],
    event(8, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-8' }),
  ]));
  const running = reduce([
    ...[
      created(), ...executionEvents(), passedValidation(5),
      event(6, 'generation.prepared', {
        generationId: 'generation-2', validationId: 'validation-5', manifestRef: 'manifest:generation-2', candidateLoopSpec: candidate,
      }),
      event(7, 'generation.promoted', { generationId: 'generation-2' }),
    ],
    event(8, 'node.dispatch.requested', { nodeId: 'node-next', attemptId: 'attempt-8' }),
  ]);
  assert.equal(running.activeAttempt.nodeId, 'node-next');
});

test('HITL authority binds the exact graph snapshot and cannot be reused for another candidate', () => {
  const candidate = loopSpec(2, 'node-build');
  const substituted = loopSpec(2, 'node-next');
  const prefix = [
    created({ payload: { loopSpec: loopSpec(1), requiresHitl: true } }),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:generation-1', candidateLoopSpec: candidate,
    }),
  ];
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 7, () => reduce([
    ...prefix,
    event(7, 'hitl.requested', {
      requestId: 'approval-substituted', promptRef: 'prompt:release-1', generationId: 'generation-1',
      specId: 'test-loop', specRevision: 2, candidateLoopSpec: substituted, validationId: 'validation-5',
    }),
  ]));
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 7, () => reduce([
    ...prefix,
    event(7, 'generation.prepared', {
      generationId: 'generation-conflict', validationId: 'validation-5', manifestRef: 'manifest:generation-conflict', candidateLoopSpec: substituted,
    }),
  ]));
});

test('rollback restores the exact promoted LoopSpec rather than engine defaults', () => {
  const versionTwo = loopSpec(2, 'node-next');
  const versionThree = loopSpec(3, 'node-build');
  const events = [
    created(),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-v2', validationId: 'validation-5', manifestRef: 'manifest:generation-v2', candidateLoopSpec: versionTwo,
    }),
    event(7, 'generation.promoted', { generationId: 'generation-v2' }),
    event(8, 'node.dispatch.requested', { nodeId: 'node-next', attemptId: 'attempt-8' }),
    event(9, 'node.started', { nodeId: 'node-next', attemptId: 'attempt-8' }),
    event(10, 'node.settled', { nodeId: 'node-next', attemptId: 'attempt-8', outcome: 'SUCCEEDED', outcomeCode: 'OK' }),
    event(11, 'validation.recorded', {
      nodeId: 'node-next', attemptId: 'attempt-8', validationId: 'validation-11', passed: true, evidenceRef: 'evidence:validation-11',
    }),
    event(12, 'generation.prepared', {
      generationId: 'generation-v3', validationId: 'validation-11', manifestRef: 'manifest:generation-v3', candidateLoopSpec: versionThree,
    }),
    event(13, 'generation.promoted', { generationId: 'generation-v3' }),
    event(14, 'rollback.applied', { targetGenerationId: 'generation-v2', reasonCode: 'OPERATOR_ROLLBACK' }),
  ];
  const restored = reduce(events);
  assert.equal(restored.loopSpec.revision, 2);
  assert.equal(restored.loopSpec.entryNodeId, 'node-next');
  assert.equal(restored.nextNodeId, 'node-next');
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 15, () => reduce([
    ...events,
    event(15, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-15' }),
  ]));
});

test('HITL approval permits a configured generation to be prepared and promoted', () => {
  const events = [
    created({ payload: { loopSpec: loopSpec(1), requiresHitl: true } }),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:generation-1',
      candidateLoopSpec: loopSpec(2),
    }),
    event(7, 'hitl.requested', {
      requestId: 'hitl-7', promptRef: 'prompt:release-1', generationId: 'generation-1',
      specId: 'test-loop', specRevision: 2, validationId: 'validation-5',
    }),
    event(8, 'hitl.decided', { requestId: 'hitl-7', decision: 'APPROVED', decisionCode: 'APPROVED_BY_OPERATOR' }),
    event(9, 'generation.promoted', { generationId: 'generation-1' }),
  ];
  const view = reduce(events);

  assert.equal(view.phase, 'READY');
  assert.equal(view.activeGenerationId, 'generation-1');
  assert.equal(view.hitl.status, 'APPROVED');
  assert.deepEqual(view.promotedGenerations.map(({ generationId }) => generationId), ['generation-1']);
});

test('HITL rejection and unavailability fail closed back to ready', () => {
  for (const decision of ['REJECTED', 'UNAVAILABLE']) {
    const view = reduce([
      created(),
      ...executionEvents(),
      passedValidation(5),
      event(6, 'generation.prepared', {
        generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:generation-1', candidateLoopSpec: loopSpec(2),
      }),
      event(7, 'hitl.requested', {
        requestId: `hitl-${decision}`, promptRef: 'prompt:release-1', generationId: 'generation-1',
        specId: 'test-loop', specRevision: 2, validationId: 'validation-5',
      }),
      event(8, 'hitl.decided', { requestId: `hitl-${decision}`, decision, decisionCode: decision }),
    ]);
    assert.equal(view.phase, 'READY');
    assert.equal(view.hitl.status, decision);
  }
});

test('preparation requires passed validation and promotion requires its exact prepared generation', () => {
  expectError(ReducerErrorCode.GENERATION_NOT_VALIDATED, 2, () => reduce([
    created(),
    event(2, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-none', manifestRef: 'manifest:generation-1',
    }),
  ]));

  expectError(ReducerErrorCode.GENERATION_NOT_PREPARED, 6, () => reduce([
    created(),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.promoted', { generationId: 'generation-missing' }),
  ]));
});

test('a requested HITL gate must be approved before promotion', () => {
  expectError(ReducerErrorCode.HITL_NOT_APPROVED, 8, () => reduce([
    created(),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:generation-1',
      candidateLoopSpec: loopSpec(2),
    }),
    event(7, 'hitl.requested', {
      requestId: 'hitl-7', promptRef: 'prompt:release-1', generationId: 'generation-1',
      specId: 'test-loop', specRevision: 2, validationId: 'validation-5',
    }),
    event(8, 'generation.promoted', { generationId: 'generation-1' }),
  ]));
});

test('a configured HITL gate fails closed unless its request is approved', () => {
  expectError(ReducerErrorCode.HITL_NOT_APPROVED, 7, () => reduce([
    created({ payload: { loopSpec: loopSpec(1), requiresHitl: true } }),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:generation-1',
    }),
    event(7, 'generation.promoted', { generationId: 'generation-1' }),
  ]));
});

test('an unconfigured run promotes a validated prepared generation without HITL', () => {
  const view = reduce([
    created(),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:generation-1',
    }),
    event(7, 'generation.promoted', { generationId: 'generation-1' }),
  ]);
  assert.equal(view.phase, 'READY');
  assert.equal(view.activeGenerationId, 'generation-1');
  assert.equal(view.hitl.status, 'NOT_REQUESTED');
});

test('rollback appends ancestry without deleting promotion history', () => {
  const events = [
    created(), ...executionEvents(), passedValidation(5),
    event(6, 'generation.prepared', { generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:generation-1' }),
    event(7, 'generation.promoted', { generationId: 'generation-1' }),
    ...executionEvents(8),
    passedValidation(11, 'attempt-8'),
    event(12, 'generation.prepared', { generationId: 'generation-2', validationId: 'validation-11', manifestRef: 'manifest:generation-2' }),
    event(13, 'generation.promoted', { generationId: 'generation-2' }),
    event(14, 'rollback.applied', { targetGenerationId: 'generation-1', reasonCode: 'OPERATOR_ROLLBACK' }),
  ];
  const view = reduce(events);

  assert.equal(view.activeGenerationId, 'generation-1');
  assert.deepEqual(view.promotedGenerations.map(({ generationId }) => generationId), ['generation-1', 'generation-2']);
  assert.deepEqual(view.rollbackAncestry, [{
    rollbackSequence: 14,
    fromGenerationId: 'generation-2',
    targetGenerationId: 'generation-1',
  }]);
});

test('cannot roll back to a generation never promoted', () => {
  expectError(ReducerErrorCode.ROLLBACK_TARGET_NOT_PROMOTED, 2, () => reduce([
    created(),
    event(2, 'rollback.applied', { targetGenerationId: 'generation-404', reasonCode: 'OPERATOR_ROLLBACK' }),
  ]));
});

test('recovery uncertainty is terminally paused and never auto-resumes', () => {
  const recovered = [
    created(),
    event(2, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-2' }),
    event(3, 'recovery.uncertain', { reasonCode: 'PROCESS_RESTARTED_DURING_DISPATCH' }),
  ];
  assert.equal(reduce(recovered).phase, 'PAUSED_RECOVERED');
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 4, () => reduce([
    ...recovered,
    event(4, 'node.dispatch.requested', { nodeId: 'node-build', attemptId: 'attempt-4' }),
  ]));
});

test('recovery invalidates an unresolved execution-admission gate and requires explicit resume', () => {
  const recovered = [
    created(),
    event(2, 'admission.hitl.requested', { requestId: 'admission-2', reasonCode: 'DSH_PROMOTION' }),
    event(3, 'recovery.uncertain', { reasonCode: 'PROCESS_RESTARTED_DURING_APPROVAL' }),
  ];
  const view = reduce(recovered);
  assert.equal(view.phase, 'PAUSED_RECOVERED');
  assert.deepEqual(view.admissionHitl, { status: 'NOT_REQUESTED', requestId: null });
  const resumed = reduce([...recovered, event(4, 'run.resumed', { reasonCode: 'OPERATOR_RESUME' })]);
  assert.equal(resumed.phase, 'READY');
});

test('recovery invalidates an unresolved validator and returns an explicit resume to ready', () => {
  const recovered = [
    created(),
    ...executionEvents(),
    event(5, 'recovery.uncertain', { reasonCode: 'PROCESS_RESTARTED_DURING_VALIDATION' }),
  ];
  const view = reduce(recovered);
  assert.equal(view.phase, 'PAUSED_RECOVERED');
  assert.equal(view.validation.status, 'NONE');
  const resumed = reduce([...recovered, event(6, 'run.resumed', { reasonCode: 'OPERATOR_RETRY' })]);
  assert.equal(resumed.phase, 'READY');
});

test('recovery invalidates a pending promotion approval so a late old decision cannot promote', () => {
  const candidate = loopSpec(2);
  const pending = [
    created({ payload: { loopSpec: loopSpec(1), requiresHitl: true } }),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-recovery-1', validationId: 'validation-5', manifestRef: 'manifest:recovery-1',
      candidateLoopSpec: candidate,
    }),
    event(7, 'hitl.requested', {
      requestId: 'hitl-recovery-7', promptRef: 'prompt:recovery-1', generationId: 'generation-recovery-1',
      specId: candidate.specId, specRevision: candidate.revision, validationId: 'validation-5', candidateLoopSpec: candidate,
    }),
    event(8, 'recovery.uncertain', { reasonCode: 'PROCESS_RESTARTED_DURING_PROMOTION_HITL' }),
  ];
  const recovered = reduce(pending);
  assert.equal(recovered.phase, 'PAUSED_RECOVERED');
  assert.equal(recovered.validation.status, 'PASSED');
  assert.equal(recovered.hitl.status, 'NOT_REQUESTED');
  const resumed = reduce([...pending, event(9, 'run.resumed', { reasonCode: 'OPERATOR_RESUME' })]);
  assert.equal(resumed.phase, 'VALIDATED');
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 10, () => reduce([
    ...pending,
    event(9, 'run.resumed', { reasonCode: 'OPERATOR_RESUME' }),
    event(10, 'hitl.decided', {
      requestId: 'hitl-recovery-7', decision: 'APPROVED', decisionCode: 'LATE_OLD_DECISION',
      approvalSubject: 'user:late', approvalReceiptRef: 'receipt:late',
    }),
  ]));
});

test('rejects every major malformed or conflicting stream at the offending sequence', () => {
  expectError(ReducerErrorCode.EMPTY_EVENT_STREAM, null, () => reduce([]));
  expectError(ReducerErrorCode.INVALID_EVENT_VERSION, 1, () => reduce([created({ version: 99 })]));
  expectError(ReducerErrorCode.RUN_ID_MISMATCH, 2, () => reduce([
    created(), event(2, 'lease.acquired', { holderId: ACTOR, fencingToken: 1 }, { runId: 'run-other' }),
  ]));
  expectError(ReducerErrorCode.NON_CONTIGUOUS_SEQUENCE, 3, () => reduce([
    created(), event(3, 'lease.acquired', { holderId: ACTOR, fencingToken: 1 }),
  ]));
  expectError(ReducerErrorCode.STALE_REVISION, 2, () => reduce([
    created(), event(2, 'lease.acquired', { holderId: ACTOR, fencingToken: 1 }, { expectedRevision: 0 }),
  ]));
  expectError(ReducerErrorCode.DUPLICATE_IDEMPOTENCY_KEY, 2, () => reduce([
    created(), event(2, 'lease.acquired', { holderId: ACTOR, fencingToken: 1 }, { idempotencyKey: 'idempotency-1' }),
  ]));
  expectError(ReducerErrorCode.ILLEGAL_TRANSITION, 2, () => reduce([
    created(), event(2, 'node.started', { nodeId: 'node-build', attemptId: 'attempt-2' }),
  ]));
});

test('rejects malformed envelopes and payloads with ReducerError instead of runtime type errors', () => {
  expectError(ReducerErrorCode.INVALID_EVENT, null, () => reduce([null]));
  expectError(ReducerErrorCode.INVALID_EVENT, 1, () => reduce([created({ payload: null })]));
  expectError(ReducerErrorCode.INVALID_EVENT, 5, () => reduce([
    created(), ...executionEvents(), event(5, 'validation.recorded', {
      nodeId: 'node-build', attemptId: 'attempt-2', validationId: 'validation-5',
      passed: 'true', evidenceRef: 'evidence:validation-5',
    }),
  ]));
  expectError(ReducerErrorCode.INVALID_EVENT, 1, () => reduce([
    created({ occurredAt: 'not-a-timestamp' }),
  ]));
});

test('replay is deterministic and leaves the supplied immutable event records untouched', () => {
  const events = [created(), ...executionEvents(), passedValidation(5)];
  Object.freeze(events);
  for (const item of events) {
    Object.freeze(item);
    Object.freeze(item.payload);
  }
  assert.deepEqual(reduce(events), reduce(events));
  assert.deepEqual(events.map((item) => item.seq), [1, 2, 3, 4, 5]);
});

function generatedValidEventStreams() {
  const validated = [created(), ...executionEvents(), passedValidation(5)];
  const promotedWithHitl = [
    created({ payload: { loopSpec: loopSpec(1), requiresHitl: true } }),
    ...executionEvents(),
    passedValidation(5),
    event(6, 'generation.prepared', {
      generationId: 'generation-property-1', validationId: 'validation-5', manifestRef: 'manifest:property-1',
      candidateLoopSpec: loopSpec(2),
    }),
    event(7, 'hitl.requested', {
      requestId: 'hitl-property-7', promptRef: 'prompt:property-1', generationId: 'generation-property-1',
      specId: 'test-loop', specRevision: 2, validationId: 'validation-5',
    }),
    event(8, 'hitl.decided', {
      requestId: 'hitl-property-7', decision: 'APPROVED', decisionCode: 'APPROVED_BY_OPERATOR',
    }),
    event(9, 'generation.promoted', { generationId: 'generation-property-1' }),
  ];
  const rolledBack = [
    ...promotedWithHitl,
    ...executionEvents(10),
    passedValidation(13, 'attempt-10'),
    event(14, 'generation.prepared', {
      generationId: 'generation-property-2', validationId: 'validation-13', manifestRef: 'manifest:property-2',
      candidateLoopSpec: loopSpec(3),
    }),
    event(15, 'hitl.requested', {
      requestId: 'hitl-property-15', promptRef: 'prompt:property-2', generationId: 'generation-property-2',
      specId: 'test-loop', specRevision: 3, validationId: 'validation-13',
    }),
    event(16, 'hitl.decided', {
      requestId: 'hitl-property-15', decision: 'APPROVED', decisionCode: 'APPROVED_BY_OPERATOR',
    }),
    event(17, 'generation.promoted', { generationId: 'generation-property-2' }),
    event(18, 'rollback.applied', {
      targetGenerationId: 'generation-property-1', reasonCode: 'PROPERTY_ROLLBACK',
    }),
  ];

  return [
    { name: 'created', events: [created()] },
    { name: 'validated', events: validated },
    { name: 'hitl-approved promotion', events: promotedWithHitl },
    { name: 'rollback ancestry', events: rolledBack },
  ];
}

test('bounded generated streams replay deterministically and reject their invalid mutation', () => {
  for (const { name, events } of generatedValidEventStreams()) {
    const accepted = reduce(events);
    assert.deepEqual(reduce(events), accepted, `${name} must produce the same view on repeated replay`);

    const invalid = event(events.length + 1, 'node.started', {
      nodeId: 'node-property-invalid',
      attemptId: `attempt-property-invalid-${events.length + 1}`,
    });
    expectError(ReducerErrorCode.ILLEGAL_TRANSITION, invalid.seq, () => reduce([...events, invalid]));
  }
});
