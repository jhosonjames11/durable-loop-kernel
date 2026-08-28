import {
  type AdapterCallbackContext,
  type ExecutionAdapter,
  type HumanApprovalOutcome,
  type HumanApprovalRequest,
  type HumanGateAdapter,
  type LoopSpec,
  reduce,
  type RunView,
  Supervisor,
  type SupervisorClock,
  type SupervisorIdFactory,
  type ValidationAdapter,
} from '@loopgraph/core';
import { FileArtifactStore, SqliteRunStore } from '@loopgraph/storage';
import { mkdir } from 'node:fs/promises';

export interface DemoSummary {
  message: string;
  harnessNeutral: boolean;
}

function loopSpec(revision: number): LoopSpec {
  return {
    specId: 'mock-build-loop', revision, entryNodeId: 'build',
    nodes: [{ nodeId: 'build', kind: 'agent' }],
    edges: [{ fromNodeId: 'build', toNodeId: 'build' }],
  };
}

/** Create a deterministic value that proves the demo package can be imported. */
export function createDemoSummary(operator: string): DemoSummary {
  return {
    message: `LoopGraph demo ready for ${operator}`,
    harnessNeutral: true,
  };
}

class DeterministicClock implements SupervisorClock {
  #tick = 0;
  milliseconds = 1_000;

  now(): number { return this.milliseconds; }

  occurredAt(): string {
    this.#tick += 1;
    return `2026-08-25T00:00:${String(this.#tick).padStart(2, '0')}.000Z`;
  }
}

class DeterministicIds implements SupervisorIdFactory {
  #next = 0;

  next(namespace: string): string {
    this.#next += 1;
    return `${namespace}-${String(this.#next).padStart(3, '0')}`;
  }
}

class ScriptedExecution implements ExecutionAdapter {
  readonly calls: AdapterCallbackContext[];
  readonly #crashOnDispatch: number | null;
  #dispatches = 0;

  constructor(calls: AdapterCallbackContext[], crashOnDispatch: number | null = null) {
    this.calls = calls;
    this.#crashOnDispatch = crashOnDispatch;
  }

  async dispatch(context: AdapterCallbackContext) {
    this.calls.push({ ...context });
    this.#dispatches += 1;
    if (this.#dispatches === this.#crashOnDispatch) throw new Error('simulated process crash after dispatch');
    return { outcome: 'SUCCEEDED' as const, outcomeCode: 'MOCK_OK' };
  }
}

class ScriptedValidation implements ValidationAdapter {
  readonly calls: AdapterCallbackContext[];
  readonly results: boolean[];
  #index = 0;

  constructor(calls: AdapterCallbackContext[], results: boolean[]) {
    this.calls = calls;
    this.results = results;
  }

  async validate(context: AdapterCallbackContext) {
    this.calls.push({ ...context });
    const passed = this.results[this.#index];
    this.#index += 1;
    if (passed === undefined) throw new Error('no scripted validation outcome');
    return { passed, evidenceRef: `evidence:${context.attemptId}` };
  }
}

class ScriptedHumanGate implements HumanGateAdapter {
  readonly calls: AdapterCallbackContext[];
  readonly decisions: HumanApprovalOutcome['decision'][];

  constructor(calls: AdapterCallbackContext[], decisions: HumanApprovalOutcome['decision'][]) {
    this.calls = calls;
    this.decisions = decisions;
  }

  async requestApproval(request: HumanApprovalRequest) {
    this.calls.push({
      runId: request.runId,
      nodeId: request.nodeId,
      attemptId: request.attemptId,
      correlationId: request.correlationId,
    });
    const decision = this.decisions.shift();
    if (decision !== 'ALLOWED_ONCE' && decision !== 'DENIED' && decision !== 'UNAVAILABLE') {
      throw new Error('no scripted human decision');
    }
    return {
      decision,
      decisionCode: `MOCK_${decision}`,
      approvalSubject: 'mock-authenticated-approver',
      approvalReceiptRef: `mock-receipt:${request.runId}:${request.generationId}:${this.calls.length}`,
    };
  }
}

export interface MockHarnessScenarioResult {
  readonly finalView: RunView;
  readonly recoveryView: RunView;
  readonly rollbackSequence: number;
  readonly persistedEventCount: number;
  readonly eventTypes: readonly string[];
  readonly executionCallsBeforeResume: number;
  readonly executionCallsAfterRecovery: number;
  readonly executionCallsAfterResume: number;
  readonly validationResults: readonly boolean[];
  readonly humanDecisions: readonly string[];
  readonly adapterCallbacks: readonly AdapterCallbackContext[];
}

function makeSupervisor(
  store: SqliteRunStore,
  artifacts: FileArtifactStore,
  execution: ExecutionAdapter,
  validation: ValidationAdapter,
  humanGate: HumanGateAdapter,
  clock: DeterministicClock,
  ids: DeterministicIds,
  holderId: string,
): Supervisor {
  return new Supervisor({
    store,
    artifacts,
    execution,
    validation,
    humanGate,
    clock,
    ids,
    actor: 'mock-supervisor',
    holderId,
    leaseTtlMs: 100,
  });
}

/**
 * A deterministic, stdout-free integration harness. It uses the public core
 * adapters and the real SQLite/event/artifact contracts, not reducer arrays.
 * It intentionally does not exercise DSH: the real DSH adapter remains the
 * production integration and maps its lifecycle into these same neutral ports.
 */
export async function runMockHarnessScenario(directory: string): Promise<MockHarnessScenarioResult> {
  await mkdir(directory, { recursive: true });
  const databaseFile = `${directory}/runs.sqlite`;
  const artifactDirectory = `${directory}/artifacts`;
  const clock = new DeterministicClock();
  const ids = new DeterministicIds();
  const callbacks: AdapterCallbackContext[] = [];
  const validationResults = [false, true, true, true, true];
  const humanDecisions: HumanApprovalOutcome['decision'][] = [
    'ALLOWED_ONCE', 'ALLOWED_ONCE', 'DENIED', 'UNAVAILABLE',
  ];
  const artifacts = new FileArtifactStore(artifactDirectory);
  const validation = new ScriptedValidation(callbacks, validationResults);
  const human = new ScriptedHumanGate(callbacks, [...humanDecisions]);

  const firstStore = new SqliteRunStore({ filename: databaseFile, clock: () => clock.now() });
  const firstExecution = new ScriptedExecution(callbacks, 3);
  const first = makeSupervisor(firstStore, artifacts, firstExecution, validation, human, clock, ids, 'worker-a');
  first.createRun({ runId: 'mock-run-001', loopSpec: loopSpec(1), requiresHitl: true });

  await first.dispatchAttempt({
    runId: 'mock-run-001', nodeId: 'build', attemptId: 'attempt-1', correlationId: 'correlation-attempt-1',
  });
  await first.dispatchAttempt({
    runId: 'mock-run-001', nodeId: 'build', attemptId: 'attempt-2', correlationId: 'correlation-attempt-2',
  });
  await first.prepareGeneration({
    runId: 'mock-run-001', generationId: 'generation-v1',
    artifacts: [{ name: 'result.txt', bytes: new TextEncoder().encode('version one') }],
    candidateLoopSpec: loopSpec(2),
  });
  await first.requestHitlAndPromote({
    runId: 'mock-run-001', generationId: 'generation-v1', promptRef: 'prompt:generation-v1', correlationId: 'correlation-attempt-2',
  });
  await artifacts.readManifest('generation-v1');

  // This is the durable crash point: dispatch and start are journaled, but no
  // fabricated node.settled event is allowed when the external call is lost.
  await first.dispatchAttempt({
    runId: 'mock-run-001', nodeId: 'build', attemptId: 'attempt-3', correlationId: 'correlation-attempt-3',
  }).then(
    () => { throw new Error('crash adapter unexpectedly settled'); },
    () => undefined,
  );
  firstStore.close();

  // Expire worker-a's old lease deterministically before a fresh process takes over.
  clock.milliseconds += 101;
  const reopenedStore = new SqliteRunStore({ filename: databaseFile, clock: () => clock.now() });
  const recoveredExecution = new ScriptedExecution(callbacks);
  const recovered = makeSupervisor(reopenedStore, artifacts, recoveredExecution, validation, human, clock, ids, 'worker-b');
  const executionCallsBeforeResume = callbacks.length;
  const recoveryView = recovered.recover({ runId: 'mock-run-001', reasonCode: 'PROCESS_CRASH_AFTER_DISPATCH' });
  const executionCallsAfterRecovery = callbacks.length;
  recovered.resume('mock-run-001', 'OPERATOR_CONFIRMED_RESUME');
  const executionCallsAfterResume = callbacks.length;

  await recovered.dispatchAttempt({
    runId: 'mock-run-001', nodeId: 'build', attemptId: 'attempt-4', correlationId: 'correlation-attempt-4',
  });
  await recovered.prepareGeneration({
    runId: 'mock-run-001', generationId: 'generation-v2',
    artifacts: [{ name: 'result.txt', bytes: new TextEncoder().encode('version two') }],
    candidateLoopSpec: loopSpec(3),
  });
  await recovered.requestHitlAndPromote({
    runId: 'mock-run-001', generationId: 'generation-v2', promptRef: 'prompt:generation-v2', correlationId: 'correlation-attempt-4',
  });
  await artifacts.readManifest('generation-v2');
  const finalBeforeReopen = recovered.rollback({
    runId: 'mock-run-001', targetGenerationId: 'generation-v1', reasonCode: 'MOCK_ROLLBACK_TO_V1',
  });
  const rollback = finalBeforeReopen.rollbackAncestry.at(-1);
  if (rollback === undefined) throw new Error('rollback event was not recorded');

  // The neutral port maps both non-allow outcomes to durable, non-promotable
  // decisions. Neither can accidentally replace the rolled-back active V1.
  await recovered.dispatchAttempt({
    runId: 'mock-run-001', nodeId: 'build', attemptId: 'attempt-5', correlationId: 'correlation-attempt-5',
  });
  await recovered.prepareGeneration({
    runId: 'mock-run-001', generationId: 'generation-denied',
    artifacts: [{ name: 'result.txt', bytes: new TextEncoder().encode('denied version') }],
    candidateLoopSpec: loopSpec(4),
  });
  const afterDenied = await recovered.requestHitlAndPromote({
    runId: 'mock-run-001', generationId: 'generation-denied', promptRef: 'prompt:generation-denied', correlationId: 'correlation-attempt-5',
  });
  if (afterDenied.activeGenerationId !== 'generation-v1') throw new Error('denied HITL unexpectedly promoted');
  await recovered.dispatchAttempt({
    runId: 'mock-run-001', nodeId: 'build', attemptId: 'attempt-6', correlationId: 'correlation-attempt-6',
  });
  await recovered.prepareGeneration({
    runId: 'mock-run-001', generationId: 'generation-unavailable',
    artifacts: [{ name: 'result.txt', bytes: new TextEncoder().encode('unavailable version') }],
    candidateLoopSpec: loopSpec(5),
  });
  const afterUnavailable = await recovered.requestHitlAndPromote({
    runId: 'mock-run-001', generationId: 'generation-unavailable', promptRef: 'prompt:generation-unavailable', correlationId: 'correlation-attempt-6',
  });
  if (afterUnavailable.activeGenerationId !== 'generation-v1') throw new Error('unavailable HITL unexpectedly promoted');
  reopenedStore.close();

  // A final independent store proves that SQLite, rather than process memory,
  // retained the full event history and compensating rollback.
  const finalStore = new SqliteRunStore({ filename: databaseFile, clock: () => clock.now() });
  const events = finalStore.read('mock-run-001');
  const finalView = reduce(events);
  finalStore.close();

  return {
    finalView,
    recoveryView,
    rollbackSequence: rollback.rollbackSequence,
    persistedEventCount: events.length,
    eventTypes: events.map(({ type }) => type),
    executionCallsBeforeResume,
    executionCallsAfterRecovery,
    executionCallsAfterResume,
    validationResults,
    humanDecisions,
    adapterCallbacks: callbacks,
  };
}
