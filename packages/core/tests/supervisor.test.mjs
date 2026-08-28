import assert from 'node:assert/strict';
import test from 'node:test';

import { EVENT_VERSION, reduce, Supervisor } from '@loopgraph/core';

class FakeClock {
  time = 0;
  sequence = 0;
  now() { return this.time; }
  occurredAt() {
    this.sequence += 1;
    return `2026-08-25T00:00:${String(this.sequence).padStart(2, '0')}.000Z`;
  }
}

class ManualTimer {
  callback = undefined;
  every(_intervalMs, callback) {
    this.callback = callback;
    return () => { this.callback = undefined; };
  }
  fire() { this.callback?.(); }
}

class MemoryStore {
  events = [];
  lease = undefined;
  renewals = 0;
  failRenew = false;
  acquire(runId, holderId, ttlMs) {
    this.lease = { runId, holderId, fencingToken: 1, expiresAt: ttlMs };
    return this.lease;
  }
  renew(lease, ttlMs) {
    this.renewals += 1;
    if (this.failRenew || this.lease?.fencingToken !== lease.fencingToken) return null;
    this.lease = { ...lease, expiresAt: ttlMs };
    return this.lease;
  }
  release() { return true; }
  read(runId) { return this.events.filter(event => event.runId === runId); }
  append(event, lease) {
    assert.equal(lease.fencingToken, this.lease?.fencingToken, 'only the current lease can append');
    reduce([...this.read(event.runId), event]);
    this.events.push(event);
  }
}

const artifacts = {
  async put() { return { digest: 'a'.repeat(64), byteSize: 1 }; },
  async get() { return new Uint8Array(); },
  async publishGeneration() {},
  async readManifest() { throw new Error('not used'); },
};

const LOOP_SPEC = {
  specId: 'supervisor-loop', revision: 1, entryNodeId: 'node',
  nodes: [{ nodeId: 'node', kind: 'agent' }],
  edges: [{ fromNodeId: 'node', toNodeId: 'node' }],
};

function supervisor({ execution, validation, store = new MemoryStore(), clock = new FakeClock(), timer = new ManualTimer() }) {
  let id = 0;
  return {
    store,
    clock,
    timer,
    instance: new Supervisor({
      store,
      artifacts,
      execution,
      validation,
      humanGate: { async requestApproval() { return { decision: 'DENIED', decisionCode: 'NOT_USED', approvalSubject: 'test-approver', approvalReceiptRef: 'receipt:not-used' }; } },
      clock,
      leaseTimer: timer,
      ids: { next(namespace) { id += 1; return `${namespace}-${id}`; } },
      actor: 'test-supervisor',
      holderId: 'test-worker',
      leaseTtlMs: 100,
    }),
  };
}

async function createAndDispatch(subject) {
  subject.instance.createRun({ runId: 'run-supervisor', loopSpec: LOOP_SPEC, requiresHitl: true });
  return await subject.instance.dispatchAttempt({
    runId: 'run-supervisor', nodeId: 'node', attemptId: 'attempt', correlationId: 'dsh:agent:1:1',
  });
}

test('failed execution never invokes validation or creates promotion authority', async () => {
  let validationCalls = 0;
  const subject = supervisor({
    execution: { async dispatch() { return { outcome: 'FAILED', outcomeCode: 'EXTERNAL_FAILURE' }; } },
    validation: { async validate() { validationCalls += 1; return { passed: true, evidenceRef: 'malicious:passed' }; } },
  });

  const view = await createAndDispatch(subject);
  assert.equal(validationCalls, 0, 'validation cannot launder a failed execution');
  assert.equal(view.phase, 'READY');
  assert.equal(view.validation.status, 'NONE');
  assert.equal(view.hitl.status, 'NOT_REQUESTED');
  assert.deepEqual(subject.store.events.map(event => event.type), [
    'run.created', 'lease.acquired', 'node.dispatch.requested', 'node.started', 'node.settled',
  ]);
  await assert.rejects(
    subject.instance.prepareGeneration({ runId: 'run-supervisor', generationId: 'forged', artifacts: [{ name: 'x', bytes: new Uint8Array([1]) }] }),
    /passed validation/u,
  );
  await assert.rejects(
    subject.instance.requestHitlAndPromote({
      runId: 'run-supervisor', generationId: 'forged', promptRef: 'prompt:forged', correlationId: 'forged',
    }),
    /exact prepared, validated generation/u,
  );
  assert.equal(subject.store.events.some(event => event.type === 'generation.prepared' || event.type === 'generation.promoted'), false);
});

test('heartbeat renews a lease across a long external callback and permits its durable result', async () => {
  const clock = new FakeClock();
  const timer = new ManualTimer();
  const subject = supervisor({
    clock,
    timer,
    execution: {
      async dispatch() {
        clock.time = 60;
        timer.fire();
        clock.time = 120;
        timer.fire();
        return { outcome: 'SUCCEEDED', outcomeCode: 'LONG_CALLBACK_OK' };
      },
    },
    validation: { async validate() { return { passed: true, evidenceRef: 'evidence:long' }; } },
  });

  const view = await createAndDispatch(subject);
  assert.ok(subject.store.renewals >= 8, 'pre/post appends and heartbeat renewals occurred');
  assert.equal(view.phase, 'VALIDATED');
  assert.ok(subject.store.events.some(event => event.type === 'node.settled'));
  assert.ok(subject.store.events.some(event => event.type === 'validation.recorded'));
});

test('a failed heartbeat fences downstream callback results before they append', async () => {
  const clock = new FakeClock();
  const timer = new ManualTimer();
  const store = new MemoryStore();
  const subject = supervisor({
    store,
    clock,
    timer,
    execution: {
      async dispatch() {
        clock.time = 60;
        store.failRenew = true;
        timer.fire();
        return { outcome: 'SUCCEEDED', outcomeCode: 'TOO_LATE' };
      },
    },
    validation: { async validate() { return { passed: true, evidenceRef: 'must-not-run' }; } },
  });

  subject.instance.createRun({ runId: 'run-supervisor', loopSpec: LOOP_SPEC, requiresHitl: true });
  await assert.rejects(
    subject.instance.dispatchAttempt({ runId: 'run-supervisor', nodeId: 'node', attemptId: 'attempt', correlationId: 'callback' }),
    /lease renewal failed/u,
  );
  assert.deepEqual(store.events.map(event => event.type), [
    'run.created', 'lease.acquired', 'node.dispatch.requested', 'node.started',
  ]);
  assert.equal(store.events.some(event => event.type === 'node.settled' || event.type === 'validation.recorded' || event.type === 'generation.promoted'), false);
});
