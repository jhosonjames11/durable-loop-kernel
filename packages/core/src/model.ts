import type { AttemptId, GenerationId, HitlRequestId, RunId, ValidationId } from './events.js';
import type { LoopSpec } from './loop-spec.js';

export type RunPhase =
  | 'READY'
  | 'AWAITING_ADMISSION_HITL'
  | 'RUNNING'
  | 'VALIDATING'
  | 'VALIDATED'
  | 'PAUSE_REQUESTED'
  | 'PAUSED'
  | 'PAUSED_RECOVERED';

export interface ActiveAttempt {
  readonly nodeId: string;
  readonly attemptId: AttemptId;
  readonly status: 'DISPATCH_REQUESTED' | 'STARTED';
}

export interface ValidationState {
  readonly status: 'NONE' | 'PENDING' | 'PASSED' | 'FAILED';
  readonly nodeId: string | null;
  readonly attemptId: AttemptId | null;
  readonly validationId: ValidationId | null;
  readonly evidenceRef: string | null;
}

export interface HitlState {
  readonly status: 'NOT_REQUESTED' | 'PENDING' | 'APPROVED' | 'REJECTED' | 'UNAVAILABLE';
  readonly requestId: HitlRequestId | null;
  readonly generationId: GenerationId | null;
  readonly specId: string | null;
  readonly specRevision: number | null;
  readonly candidateLoopSpec: LoopSpec | null;
  readonly validationId: ValidationId | null;
  readonly approvalSubject: string | null;
  readonly approvalReceiptRef: string | null;
}

/** A pre-execution human gate. It is distinct from promotion authority. */
export interface AdmissionHitlState {
  readonly status: 'NOT_REQUESTED' | 'PENDING' | 'APPROVED' | 'REJECTED' | 'UNAVAILABLE';
  readonly requestId: HitlRequestId | null;
}

export interface LeaseView {
  readonly holderId: string;
  readonly fencingToken: number;
}

export interface PreparedGeneration {
  readonly generationId: GenerationId;
  readonly validationId: ValidationId;
  readonly manifestRef: string;
  readonly preparedAtRevision: number;
  readonly candidateLoopSpec: LoopSpec;
}

export interface PromotedGeneration {
  readonly generationId: GenerationId;
  readonly promotedAtRevision: number;
  readonly loopSpec: LoopSpec;
}

export interface RollbackAncestry {
  readonly rollbackSequence: number;
  readonly fromGenerationId: GenerationId | null;
  readonly targetGenerationId: GenerationId;
}

/** JSON-compatible, event-derived public projection of a single run. */
export interface RunView {
  readonly runId: RunId;
  readonly revision: number;
  readonly phase: RunPhase;
  readonly loopSpec: LoopSpec;
  /** The only graph node that may be dispatched next. */
  readonly nextNodeId: string | null;
  readonly requiresHitl: boolean;
  readonly activeAttempt: ActiveAttempt | null;
  readonly validation: ValidationState;
  readonly admissionHitl: AdmissionHitlState;
  readonly hitl: HitlState;
  readonly pauseRequested: boolean;
  readonly lease: LeaseView | null;
  readonly preparedGenerations: readonly PreparedGeneration[];
  readonly promotedGenerations: readonly PromotedGeneration[];
  readonly activeGenerationId: GenerationId | null;
  readonly rollbackAncestry: readonly RollbackAncestry[];
  readonly retryCount: number;
  readonly recoveryReasonCode: string | null;
  readonly acceptedIdempotencyKeys: readonly string[];
}
