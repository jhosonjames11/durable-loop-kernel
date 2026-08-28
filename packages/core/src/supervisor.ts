import { EVENT_VERSION, type AttemptId, type RunEvent, type RunEventType, type RunId } from './events.js';
import type { RunView } from './model.js';
import type {
  AdapterCallbackContext,
  ArtifactStore,
  EventStore,
  ExecutionAdapter,
  HumanGateAdapter,
  LeaseGrant,
  RunLease,
  SupervisorClock,
  SupervisorIdFactory,
  SupervisorLeaseTimer,
  ValidationAdapter,
} from './ports.js';
import { reduce } from './reducer.js';
import { isValidLoopSpec, sameLoopSpecIdentity, type LoopSpec } from './loop-spec.js';

export interface SupervisorDependencies {
  readonly store: EventStore & RunLease;
  readonly artifacts: ArtifactStore;
  readonly execution: ExecutionAdapter;
  readonly validation: ValidationAdapter;
  readonly humanGate: HumanGateAdapter;
  readonly clock: SupervisorClock;
  readonly ids: SupervisorIdFactory;
  readonly actor: string;
  readonly holderId: string;
  readonly leaseTtlMs: number;
  readonly leaseTimer?: SupervisorLeaseTimer;
}

export interface CreateRunInput {
  readonly runId: RunId;
  readonly loopSpec: LoopSpec;
  readonly requiresHitl: boolean;
}

export interface DispatchAttemptInput {
  readonly runId: RunId;
  readonly nodeId: string;
  readonly attemptId: AttemptId;
  readonly correlationId: string;
}

export interface PreparedArtifactInput {
  readonly name: string;
  readonly bytes: Uint8Array;
}

export interface PrepareGenerationInput {
  readonly runId: RunId;
  readonly generationId: string;
  readonly artifacts: readonly PreparedArtifactInput[];
  readonly candidateLoopSpec: LoopSpec;
}

export interface PromotePreparedGenerationInput {
  readonly runId: RunId;
  readonly generationId: string;
  readonly promptRef: string;
  readonly correlationId: string;
}

/** Record an externally-mediated HITL request without implicitly promoting. */
export interface RequestHitlInput {
  readonly runId: RunId;
  readonly generationId: string;
  readonly promptRef: string;
  readonly correlationId: string;
}

export interface RecordHitlDecisionInput {
  readonly runId: RunId;
  readonly requestId: string;
  readonly decision: 'ALLOWED_ONCE' | 'DENIED' | 'UNAVAILABLE';
  readonly decisionCode: string;
  readonly approvalSubject: string;
  readonly approvalReceiptRef: string;
  readonly correlationId: string;
}

export interface RequestAdmissionHitlInput {
  readonly runId: RunId;
  readonly reasonCode: string;
  readonly correlationId: string;
}

export interface RecordAdmissionHitlDecisionInput {
  readonly runId: RunId;
  readonly requestId: string;
  readonly decision: 'ALLOWED_ONCE' | 'DENIED' | 'UNAVAILABLE';
  readonly decisionCode: string;
  readonly correlationId: string;
}

export interface RecoverRunInput {
  readonly runId: RunId;
  readonly reasonCode: string;
}

export interface RollbackInput {
  readonly runId: RunId;
  readonly targetGenerationId: string;
  readonly reasonCode: string;
}

/**
 * Harness-neutral durable orchestration. Adapter outcomes are never state: each
 * is translated to a validated immutable event before it affects the projection.
 */
export class Supervisor {
  readonly #store: EventStore & RunLease;
  readonly #artifacts: ArtifactStore;
  readonly #execution: ExecutionAdapter;
  readonly #validation: ValidationAdapter;
  readonly #humanGate: HumanGateAdapter;
  readonly #clock: SupervisorClock;
  readonly #ids: SupervisorIdFactory;
  readonly #actor: string;
  readonly #holderId: string;
  readonly #leaseTtlMs: number;
  readonly #leaseTimer: SupervisorLeaseTimer;
  #lease: LeaseGrant | null = null;

  constructor(dependencies: SupervisorDependencies) {
    this.#store = dependencies.store;
    this.#artifacts = dependencies.artifacts;
    this.#execution = dependencies.execution;
    this.#validation = dependencies.validation;
    this.#humanGate = dependencies.humanGate;
    this.#clock = dependencies.clock;
    this.#ids = dependencies.ids;
    this.#actor = dependencies.actor;
    this.#holderId = dependencies.holderId;
    this.#leaseTtlMs = dependencies.leaseTtlMs;
    this.#leaseTimer = dependencies.leaseTimer ?? systemLeaseTimer;
  }

  createRun(input: CreateRunInput): RunView {
    if (this.#store.read(input.runId).length !== 0) throw new Error('run already exists');
    if (!isValidLoopSpec(input.loopSpec)) throw new Error('run requires a canonical LoopSpec');
    this.#lease = this.#acquire(input.runId);
    this.#append(input.runId, 'run.created', {
      loopSpec: input.loopSpec,
      requiresHitl: input.requiresHitl,
    });
    this.#recordLease(input.runId);
    return this.view(input.runId);
  }

  /** Acquire a fresh durable lease and conservatively pause uncertain external work. */
  recover(input: RecoverRunInput): RunView {
    const before = this.view(input.runId);
    this.#lease = this.#acquire(input.runId);
    // PAUSED_RECOVERED is already a safe terminal recovery boundary. Recording a
    // new lease event there is intentionally rejected by the reducer.
    if (before.phase !== 'PAUSED_RECOVERED') this.#recordLease(input.runId);
    const view = this.view(input.runId);
    const unsafe = view.phase === 'AWAITING_ADMISSION_HITL' || view.phase === 'RUNNING' || view.phase === 'VALIDATING' || view.phase === 'PAUSE_REQUESTED'
      || (view.phase === 'VALIDATED' && view.hitl.status === 'PENDING');
    if (unsafe) this.#append(input.runId, 'recovery.uncertain', { reasonCode: input.reasonCode });
    return this.view(input.runId);
  }

  /** Resuming never dispatches itself: a caller must issue a new explicit attempt. */
  resume(runId: RunId, reasonCode: string): RunView {
    const view = this.view(runId);
    if (view.phase !== 'PAUSED' && view.phase !== 'PAUSED_RECOVERED') throw new Error('run is not paused');
    this.#append(runId, 'run.resumed', { reasonCode });
    return this.view(runId);
  }

  async dispatchAttempt(input: DispatchAttemptInput): Promise<RunView> {
    this.beginAttempt(input);
    const callback: AdapterCallbackContext = {
      runId: input.runId,
      nodeId: input.nodeId,
      attemptId: input.attemptId,
      correlationId: input.correlationId,
    };
    // An adapter failure deliberately produces no invented settlement. A restart
    // will see the durable in-flight attempt and enter PAUSED_RECOVERED.
    const execution = await this.#withLeaseHeartbeat(input.runId, () => this.#execution.dispatch(callback));
    return this.settleAttempt({ ...input, outcome: execution.outcome, outcomeCode: execution.outcomeCode });
  }

  /** Persist that an external adapter is about to perform one non-resumable attempt. */
  beginAttempt(input: DispatchAttemptInput): RunView {
    const before = this.view(input.runId);
    if (before.phase !== 'READY') throw new Error('attempt dispatch requires READY');
    this.#append(input.runId, 'node.dispatch.requested', {
      nodeId: input.nodeId,
      attemptId: input.attemptId,
    }, input.correlationId);
    this.#append(input.runId, 'node.started', {
      nodeId: input.nodeId,
      attemptId: input.attemptId,
    }, input.correlationId);
    return this.view(input.runId);
  }

  /** Settle a previously begun external attempt at a verified adapter boundary. */
  async settleAttempt(input: DispatchAttemptInput & { readonly outcome: 'SUCCEEDED' | 'FAILED'; readonly outcomeCode: string }): Promise<RunView> {
    const callback: AdapterCallbackContext = {
      runId: input.runId,
      nodeId: input.nodeId,
      attemptId: input.attemptId,
      correlationId: input.correlationId,
    };
    this.#append(input.runId, 'node.settled', {
      nodeId: input.nodeId,
      attemptId: input.attemptId,
      outcome: input.outcome,
      outcomeCode: input.outcomeCode,
    }, input.correlationId);

    // A failed execution is a retry boundary, never validation input. The
    // reducer independently rejects a forged validation after failure.
    if (input.outcome !== 'SUCCEEDED') return this.view(input.runId);
    const validation = await this.#withLeaseHeartbeat(input.runId, () => this.#validation.validate(callback));
    this.#append(input.runId, 'validation.recorded', {
      nodeId: input.nodeId,
      attemptId: input.attemptId,
      validationId: this.#ids.next('validation'),
      passed: validation.passed,
      evidenceRef: validation.evidenceRef,
    }, input.correlationId);
    return this.view(input.runId);
  }

  /** Advance a loop after consuming its passed validation without promoting a version. */
  supersedeValidation(runId: RunId, reasonCode: string, nextNodeId: string, correlationId: string): RunView {
    this.#append(runId, 'validation.superseded', { reasonCode, nextNodeId }, correlationId);
    return this.view(runId);
  }

  async prepareGeneration(input: PrepareGenerationInput): Promise<RunView> {
    const view = this.view(input.runId);
    if (view.phase !== 'VALIDATED' || view.validation.status !== 'PASSED' || view.validation.validationId === null) {
      throw new Error('generation preparation requires a passed validation');
    }
    if (!isValidLoopSpec(input.candidateLoopSpec)
      || input.candidateLoopSpec.specId !== view.loopSpec.specId
      || input.candidateLoopSpec.revision <= view.loopSpec.revision
      || view.preparedGenerations.some(({ candidateLoopSpec }) => sameLoopSpecIdentity(candidateLoopSpec, input.candidateLoopSpec))
      || view.promotedGenerations.some(({ loopSpec }) => sameLoopSpecIdentity(loopSpec, input.candidateLoopSpec))) {
      throw new Error('generation requires a new canonical LoopSpec revision');
    }
    if (input.artifacts.length === 0) throw new Error('generation requires at least one artifact');
    const artifacts = [] as { name: string; digest: string; byteSize: number }[];
    for (const artifact of input.artifacts) {
      const reference = await this.#withLeaseHeartbeat(input.runId, () => this.#artifacts.put(artifact.bytes));
      artifacts.push({ name: artifact.name, ...reference });
    }
    await this.#withLeaseHeartbeat(input.runId, () => this.#artifacts.publishGeneration({
      generationId: input.generationId,
      createdAt: this.#clock.occurredAt(),
      artifacts,
    }));
    this.#append(input.runId, 'generation.prepared', {
      generationId: input.generationId,
      validationId: view.validation.validationId,
      manifestRef: `manifest:${input.generationId}`,
      candidateLoopSpec: input.candidateLoopSpec,
    });
    return this.view(input.runId);
  }

  /**
   * Start a durable HITL audit boundary. It intentionally does not prepare or
   * promote anything; callers may bind an external approval seam to it.
   */
  requestHitl(input: RequestHitlInput): string {
    const view = this.view(input.runId);
    const prepared = view.preparedGenerations.find(({ generationId }) => generationId === input.generationId);
    if (view.phase !== 'VALIDATED' || view.validation.status !== 'PASSED' || view.validation.validationId === null
      || prepared === undefined || prepared.validationId !== view.validation.validationId) {
      throw new Error('HITL requires a completed, passed validation');
    }
    const requestId = this.#ids.next('hitl');
    this.#append(input.runId, 'hitl.requested', {
      requestId, promptRef: input.promptRef, generationId: input.generationId,
      specId: prepared.candidateLoopSpec.specId, specRevision: prepared.candidateLoopSpec.revision,
      candidateLoopSpec: prepared.candidateLoopSpec,
      validationId: view.validation.validationId,
    }, input.correlationId);
    return requestId;
  }

  /** Complete a previously requested external HITL boundary; never promotes. */
  recordHitlDecision(input: RecordHitlDecisionInput): RunView {
    this.#append(input.runId, 'hitl.decided', {
      requestId: input.requestId,
      decision: input.decision === 'ALLOWED_ONCE' ? 'APPROVED' : input.decision === 'DENIED' ? 'REJECTED' : 'UNAVAILABLE',
      decisionCode: input.decisionCode,
      approvalSubject: input.approvalSubject,
      approvalReceiptRef: input.approvalReceiptRef,
    }, input.correlationId);
    return this.view(input.runId);
  }

  /** Open a durable, fail-closed human gate before any external execution begins. */
  requestAdmissionHitl(input: RequestAdmissionHitlInput): string {
    const view = this.view(input.runId);
    if (view.phase !== 'READY' || view.activeAttempt !== null) {
      throw new Error('admission HITL requires a ready run');
    }
    const requestId = this.#ids.next('admission-hitl');
    this.#append(input.runId, 'admission.hitl.requested', { requestId, reasonCode: input.reasonCode }, input.correlationId);
    return requestId;
  }

  /** Complete an execution-admission gate; this never creates promotion authority. */
  recordAdmissionHitlDecision(input: RecordAdmissionHitlDecisionInput): RunView {
    this.#append(input.runId, 'admission.hitl.decided', {
      requestId: input.requestId,
      decision: input.decision === 'ALLOWED_ONCE' ? 'APPROVED' : input.decision === 'DENIED' ? 'REJECTED' : 'UNAVAILABLE',
      decisionCode: input.decisionCode,
    }, input.correlationId);
    return this.view(input.runId);
  }

  async requestHitlAndPromote(input: PromotePreparedGenerationInput): Promise<RunView> {
    const view = this.view(input.runId);
    const prepared = view.preparedGenerations.find(({ generationId }) => generationId === input.generationId);
    if (view.phase !== 'VALIDATED' || view.validation.status !== 'PASSED' || prepared === undefined
      || view.validation.nodeId === null || view.validation.attemptId === null) {
      throw new Error('promotion requires the exact prepared, validated generation');
    }
    const callback: AdapterCallbackContext = {
      runId: input.runId,
      nodeId: view.validation.nodeId,
      attemptId: view.validation.attemptId,
      correlationId: input.correlationId,
    };
    const requestId = this.#ids.next('hitl');
    this.#append(input.runId, 'hitl.requested', {
      requestId, promptRef: input.promptRef, generationId: input.generationId,
      specId: prepared.candidateLoopSpec.specId, specRevision: prepared.candidateLoopSpec.revision,
      candidateLoopSpec: prepared.candidateLoopSpec,
      validationId: view.validation.validationId,
    }, input.correlationId);
    const decision = await this.#withLeaseHeartbeat(
      input.runId,
      () => this.#humanGate.requestApproval({ ...callback, generationId: input.generationId, promptRef: input.promptRef }),
    );
    const approved = decision.decision === 'ALLOWED_ONCE';
    this.#append(input.runId, 'hitl.decided', {
      requestId,
      decision: approved ? 'APPROVED' : (decision.decision === 'UNAVAILABLE' ? 'UNAVAILABLE' : 'REJECTED'),
      decisionCode: decision.decisionCode,
      approvalSubject: decision.approvalSubject,
      approvalReceiptRef: decision.approvalReceiptRef,
    }, input.correlationId);
    if (!approved) return this.view(input.runId);
    this.#append(input.runId, 'generation.promoted', { generationId: input.generationId }, input.correlationId);
    return this.view(input.runId);
  }

  rollback(input: RollbackInput): RunView {
    this.#append(input.runId, 'rollback.applied', {
      targetGenerationId: input.targetGenerationId,
      reasonCode: input.reasonCode,
    });
    return this.view(input.runId);
  }

  /** Renew the current durable ownership proof. A failed renewal fences this supervisor. */
  renewLease(runId: RunId): LeaseGrant {
    const current = this.#requireLease(runId);
    const renewed = this.#store.renew(current, this.#leaseTtlMs);
    if (renewed === null) throw new Error('durable lease renewal failed');
    this.#lease = renewed;
    return renewed;
  }

  view(runId: RunId): RunView {
    return reduce(this.#store.read(runId));
  }

  #acquire(runId: RunId): LeaseGrant {
    const lease = this.#store.acquire(runId, this.#holderId, this.#leaseTtlMs);
    if (lease === null) throw new Error('durable lease is unavailable');
    return lease;
  }

  #recordLease(runId: RunId): void {
    const lease = this.#requireLease(runId);
    this.#append(runId, 'lease.acquired', { holderId: lease.holderId, fencingToken: lease.fencingToken });
  }

  #append(runId: RunId, type: RunEventType, payload: object, correlationId: string | null = null): void {
    // Renew immediately before every append. A completed external call cannot
    // cause a stale owner to write a result after a lost heartbeat.
    this.renewLease(runId);
    const events = this.#store.read(runId);
    const revision = events.length === 0 ? 0 : reduce(events).revision;
    const event = {
      version: EVENT_VERSION,
      eventId: this.#ids.next('event'),
      runId,
      seq: revision + 1,
      occurredAt: this.#clock.occurredAt(),
      actor: this.#actor,
      causationId: null,
      correlationId,
      idempotencyKey: `supervisor:${type}:${this.#ids.next('idempotency')}`,
      expectedRevision: revision,
      type,
      payload,
    } as RunEvent;
    this.#store.append(event, this.#requireLease(runId));
  }

  #requireLease(runId: RunId): LeaseGrant {
    if (this.#lease === null || this.#lease.runId !== runId) throw new Error('supervisor does not own a lease for this run');
    return this.#lease;
  }

  async #withLeaseHeartbeat<T>(runId: RunId, operation: () => Promise<T>): Promise<T> {
    // This deliberately does not claim to cancel a foreign operation if renewal
    // fails. It only fences every later journal append.
    this.renewLease(runId);
    let renewalFailure: Error | null = null;
    const stop = this.#leaseTimer.every(Math.max(1, Math.floor(this.#leaseTtlMs / 2)), () => {
      if (renewalFailure !== null) return;
      try {
        this.renewLease(runId);
      } catch (error) {
        renewalFailure = error instanceof Error ? error : new Error('durable lease renewal failed');
      }
    });
    try {
      const result = await operation();
      if (renewalFailure !== null) throw renewalFailure;
      this.renewLease(runId);
      return result;
    } finally {
      stop();
    }
  }
}

const systemLeaseTimer: SupervisorLeaseTimer = {
  every(intervalMs, callback) {
    const handle = setInterval(callback, intervalMs);
    return () => { clearInterval(handle); };
  },
};
