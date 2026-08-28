import type { RunEvent, RunId } from './events.js';
import type { RunView } from './model.js';

/** A time-bounded ownership proof used to fence writers at a durable boundary. */
export interface LeaseGrant {
  readonly runId: RunId;
  readonly holderId: string;
  readonly fencingToken: number;
  readonly expiresAt: number;
}

/** Neutral durable event journal contract. Implementations must append atomically. */
export interface EventStore {
  append(event: RunEvent, lease: LeaseGrant): void;
  read(runId: RunId): readonly RunEvent[];
}

/** Neutral lease contract; an implementation, not process memory, is authoritative. */
export interface RunLease {
  acquire(runId: RunId, holderId: string, ttlMs: number): LeaseGrant | null;
  renew(lease: LeaseGrant, ttlMs: number): LeaseGrant | null;
  release(lease: LeaseGrant): boolean;
}

export interface CheckpointWrite {
  readonly runId: RunId;
  readonly revision: number;
  readonly view: RunView;
}

export interface CheckpointRead {
  readonly runId: RunId;
  readonly revision: number;
  readonly view: RunView;
}

/** Checkpoints are optional rebuild accelerators and never replace the event journal. */
export interface CheckpointStore {
  saveCheckpoint(checkpoint: CheckpointWrite): void;
  readCheckpoint(runId: RunId): CheckpointRead | null;
}

export interface ArtifactReference {
  readonly digest: string;
  readonly byteSize: number;
}

export interface NamedArtifact extends ArtifactReference {
  readonly name: string;
}

/** Complete immutable artifact set for one generation. */
export interface GenerationManifest {
  readonly generationId: string;
  readonly createdAt: string;
  readonly artifacts: readonly NamedArtifact[];
}

/** Content-addressed artifact boundary; identities are hashes, never caller paths. */
export interface ArtifactStore {
  put(bytes: Uint8Array): Promise<ArtifactReference>;
  get(reference: ArtifactReference): Promise<Uint8Array>;
  publishGeneration(manifest: GenerationManifest): Promise<void>;
  readManifest(generationId: string): Promise<GenerationManifest>;
}

export interface RecoveryInput {
  readonly runId: RunId;
  readonly lease: LeaseGrant;
  readonly actor: string;
  readonly eventId: string;
  readonly occurredAt: string;
  readonly reasonCode: string;
}

export interface RecoveryOutput {
  readonly view: RunView;
  readonly recovered: boolean;
  readonly checkpointUsed: boolean;
}

/** Stable identity supplied to every external adapter callback. */
export interface AdapterCallbackContext {
  readonly runId: RunId;
  readonly nodeId: string;
  readonly attemptId: string;
  readonly correlationId: string;
}

export interface ExecutionOutcome {
  readonly outcome: 'SUCCEEDED' | 'FAILED';
  readonly outcomeCode: string;
}

/** Starts one explicit attempt. The result is converted to a node.settled event. */
export interface ExecutionAdapter {
  dispatch(context: AdapterCallbackContext): Promise<ExecutionOutcome>;
}

export interface ValidationOutcome {
  readonly passed: boolean;
  readonly evidenceRef: string;
}

/** Evaluates one settled attempt. The result is converted to validation.recorded. */
export interface ValidationAdapter {
  validate(context: AdapterCallbackContext): Promise<ValidationOutcome>;
}

export interface HumanApprovalRequest extends AdapterCallbackContext {
  readonly generationId: string;
  readonly promptRef: string;
}

/** Only ALLOWED_ONCE is promotable; denied and unavailable outcomes fail closed. */
export interface HumanApprovalOutcome {
  readonly decision: 'ALLOWED_ONCE' | 'DENIED' | 'UNAVAILABLE';
  readonly decisionCode: string;
  /** Authenticated approval principal asserted by the external authority. */
  readonly approvalSubject: string;
  /** Opaque external receipt; operators cannot substitute this with --actor. */
  readonly approvalReceiptRef: string;
}

export interface HumanGateAdapter {
  requestApproval(request: HumanApprovalRequest): Promise<HumanApprovalOutcome>;
}

/** Injectable deterministic boundary for event and manifest timestamps. */
export interface SupervisorClock {
  /** Milliseconds used for deterministic lease scheduling decisions. */
  now(): number;
  occurredAt(): string;
}

/** Injectable timer boundary for lease heartbeats. */
export interface SupervisorLeaseTimer {
  every(intervalMs: number, callback: () => void): () => void;
}

/** Injectable deterministic boundary for all event, validation, and HITL IDs. */
export interface SupervisorIdFactory {
  next(namespace: string): string;
}
