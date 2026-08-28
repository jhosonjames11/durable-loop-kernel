/**
 * The only protocol version understood by this reducer.  Event upgrades belong
 * at an event-store boundary, never in the deterministic domain projection.
 */
export const EVENT_VERSION = 1 as const;

import { isValidLoopSpec, type LoopSpec } from './loop-spec.js';

export type EventVersion = typeof EVENT_VERSION;
export type EventId = string;
export type RunId = string;
export type AttemptId = string;
export type GenerationId = string;
export type ValidationId = string;
export type HitlRequestId = string;

/** Immutable metadata attached to every durable event. */
export interface EventEnvelope<Type extends string, Payload> {
  readonly version: EventVersion;
  readonly eventId: EventId;
  readonly runId: RunId;
  readonly seq: number;
  readonly occurredAt: string;
  readonly actor: string;
  readonly causationId: string | null;
  readonly correlationId: string | null;
  readonly idempotencyKey: string;
  readonly expectedRevision: number;
  readonly type: Type;
  readonly payload: Payload;
}

/** Payloads intentionally carry stable identifiers, codes, and artifact refs only. */
export interface RunCreatedPayload {
  /** The complete canonical graph snapshot; its event record is immutable. */
  readonly loopSpec: LoopSpec;
  /** Whether every generation promotion for this run needs a durable HITL approval. */
  readonly requiresHitl: boolean;
}

export interface LeaseAcquiredPayload {
  readonly holderId: string;
  readonly fencingToken: number;
}

export interface NodeDispatchRequestedPayload {
  readonly nodeId: string;
  readonly attemptId: AttemptId;
}

export interface NodeStartedPayload {
  readonly nodeId: string;
  readonly attemptId: AttemptId;
}

export interface NodeSettledPayload {
  readonly nodeId: string;
  readonly attemptId: AttemptId;
  readonly outcome: 'SUCCEEDED' | 'FAILED';
  readonly outcomeCode: string;
}

export interface ValidationRecordedPayload {
  readonly nodeId: string;
  readonly attemptId: AttemptId;
  readonly validationId: ValidationId;
  readonly passed: boolean;
  readonly evidenceRef: string;
}

/** Explicitly retires passed validation before the graph advances to another node. */
export interface ValidationSupersededPayload {
  readonly reasonCode: string;
  readonly nextNodeId: string;
}

/** A human gate that controls whether a new external execution may start. */
export interface AdmissionHitlRequestedPayload {
  readonly requestId: HitlRequestId;
  readonly reasonCode: string;
}

export interface AdmissionHitlDecidedPayload {
  readonly requestId: HitlRequestId;
  readonly decision: 'APPROVED' | 'REJECTED' | 'UNAVAILABLE';
  readonly decisionCode: string;
}

export interface PauseRequestedPayload {
  readonly reasonCode: string;
}

export interface RunPausedPayload {
  readonly reasonCode: string;
}

export interface RunResumedPayload {
  readonly reasonCode: string;
}

export interface HitlRequestedPayload {
  readonly requestId: HitlRequestId;
  readonly promptRef: string;
  readonly generationId: GenerationId;
  readonly specId: string;
  readonly specRevision: number;
  /** Exact immutable candidate snapshot the approval is allowed to authorize. */
  readonly candidateLoopSpec: LoopSpec;
  readonly validationId: ValidationId;
}

export interface HitlDecidedPayload {
  readonly requestId: HitlRequestId;
  readonly decision: 'APPROVED' | 'REJECTED' | 'UNAVAILABLE';
  readonly decisionCode: string;
  /** Principal asserted by the authenticated human-gate adapter, never CLI --actor. */
  readonly approvalSubject: string;
  /** Opaque durable receipt locator in the external approval authority. */
  readonly approvalReceiptRef: string;
}

export interface GenerationPreparedPayload {
  readonly generationId: GenerationId;
  readonly validationId: ValidationId;
  readonly manifestRef: string;
  /** The candidate being approved is the graph itself, never a helper function. */
  readonly candidateLoopSpec: LoopSpec;
}

export interface GenerationPromotedPayload {
  readonly generationId: GenerationId;
}

export interface RollbackAppliedPayload {
  readonly targetGenerationId: GenerationId;
  readonly reasonCode: string;
}

export interface RecoveryUncertainPayload {
  readonly reasonCode: string;
}

export type RunCreatedEvent = EventEnvelope<'run.created', RunCreatedPayload>;
export type LeaseAcquiredEvent = EventEnvelope<'lease.acquired', LeaseAcquiredPayload>;
export type NodeDispatchRequestedEvent = EventEnvelope<'node.dispatch.requested', NodeDispatchRequestedPayload>;
export type NodeStartedEvent = EventEnvelope<'node.started', NodeStartedPayload>;
export type NodeSettledEvent = EventEnvelope<'node.settled', NodeSettledPayload>;
export type ValidationRecordedEvent = EventEnvelope<'validation.recorded', ValidationRecordedPayload>;
export type ValidationSupersededEvent = EventEnvelope<'validation.superseded', ValidationSupersededPayload>;
export type AdmissionHitlRequestedEvent = EventEnvelope<'admission.hitl.requested', AdmissionHitlRequestedPayload>;
export type AdmissionHitlDecidedEvent = EventEnvelope<'admission.hitl.decided', AdmissionHitlDecidedPayload>;
export type PauseRequestedEvent = EventEnvelope<'pause.requested', PauseRequestedPayload>;
export type RunPausedEvent = EventEnvelope<'run.paused', RunPausedPayload>;
export type RunResumedEvent = EventEnvelope<'run.resumed', RunResumedPayload>;
export type HitlRequestedEvent = EventEnvelope<'hitl.requested', HitlRequestedPayload>;
export type HitlDecidedEvent = EventEnvelope<'hitl.decided', HitlDecidedPayload>;
export type GenerationPreparedEvent = EventEnvelope<'generation.prepared', GenerationPreparedPayload>;
export type GenerationPromotedEvent = EventEnvelope<'generation.promoted', GenerationPromotedPayload>;
export type RollbackAppliedEvent = EventEnvelope<'rollback.applied', RollbackAppliedPayload>;
export type RecoveryUncertainEvent = EventEnvelope<'recovery.uncertain', RecoveryUncertainPayload>;

export type RunEvent =
  | RunCreatedEvent
  | LeaseAcquiredEvent
  | NodeDispatchRequestedEvent
  | NodeStartedEvent
  | NodeSettledEvent
  | ValidationRecordedEvent
  | ValidationSupersededEvent
  | AdmissionHitlRequestedEvent
  | AdmissionHitlDecidedEvent
  | PauseRequestedEvent
  | RunPausedEvent
  | RunResumedEvent
  | HitlRequestedEvent
  | HitlDecidedEvent
  | GenerationPreparedEvent
  | GenerationPromotedEvent
  | RollbackAppliedEvent
  | RecoveryUncertainEvent;

export type RunEventType = RunEvent['type'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function isTimestamp(value: unknown): value is string {
  return isNonEmptyString(value) && Number.isFinite(Date.parse(value));
}

function isNullableIdentifier(value: unknown): boolean {
  return value === null || isNonEmptyString(value);
}

function isKnownEventType(value: unknown): value is RunEventType {
  return value === 'run.created'
    || value === 'lease.acquired'
    || value === 'node.dispatch.requested'
    || value === 'node.started'
    || value === 'node.settled'
    || value === 'validation.recorded'
    || value === 'validation.superseded'
    || value === 'admission.hitl.requested'
    || value === 'admission.hitl.decided'
    || value === 'pause.requested'
    || value === 'run.paused'
    || value === 'run.resumed'
    || value === 'hitl.requested'
    || value === 'hitl.decided'
    || value === 'generation.prepared'
    || value === 'generation.promoted'
    || value === 'rollback.applied'
    || value === 'recovery.uncertain';
}

/**
 * Runtime validation for immutable journal metadata. Storage adapters use this
 * before serializing an event so the reducer never receives malformed input.
 */
export function isValidEventEnvelope(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value)) {
    return false;
  }
  try {
    return isPositiveInteger(value.version)
      && isNonEmptyString(value.eventId)
      && isNonEmptyString(value.runId)
      && isPositiveInteger(value.seq)
      && isTimestamp(value.occurredAt)
      && isNonEmptyString(value.actor)
      && isNullableIdentifier(value.causationId)
      && isNullableIdentifier(value.correlationId)
      && isNonEmptyString(value.idempotencyKey)
      && isNonNegativeInteger(value.expectedRevision)
      && isKnownEventType(value.type)
      && isRecord(value.payload);
  } catch {
    return false;
  }
}

/** Validate the stable payload contract for a known event type. */
export function isValidEventPayload(value: unknown): boolean {
  if (!isValidEventEnvelope(value)) {
    return false;
  }
  const payload = value.payload as Record<string, unknown>;
  try {
    switch (value.type) {
      case 'run.created':
        return isValidLoopSpec(payload.loopSpec) && typeof payload.requiresHitl === 'boolean';
      case 'lease.acquired':
        return isNonEmptyString(payload.holderId) && isPositiveInteger(payload.fencingToken);
      case 'node.dispatch.requested':
      case 'node.started':
        return isNonEmptyString(payload.nodeId) && isNonEmptyString(payload.attemptId);
      case 'node.settled':
        return isNonEmptyString(payload.nodeId)
          && isNonEmptyString(payload.attemptId)
          && (payload.outcome === 'SUCCEEDED' || payload.outcome === 'FAILED')
          && isNonEmptyString(payload.outcomeCode);
      case 'validation.recorded':
        return isNonEmptyString(payload.nodeId)
          && isNonEmptyString(payload.attemptId)
          && isNonEmptyString(payload.validationId)
          && typeof payload.passed === 'boolean'
          && isNonEmptyString(payload.evidenceRef);
      case 'validation.superseded':
        return isNonEmptyString(payload.reasonCode) && isNonEmptyString(payload.nextNodeId);
      case 'admission.hitl.requested':
        return isNonEmptyString(payload.requestId) && isNonEmptyString(payload.reasonCode);
      case 'admission.hitl.decided':
        return isNonEmptyString(payload.requestId)
          && (payload.decision === 'APPROVED' || payload.decision === 'REJECTED' || payload.decision === 'UNAVAILABLE')
          && isNonEmptyString(payload.decisionCode);
      case 'pause.requested':
      case 'run.paused':
      case 'run.resumed':
      case 'recovery.uncertain':
        return isNonEmptyString(payload.reasonCode);
      case 'hitl.requested':
        return isNonEmptyString(payload.requestId) && isNonEmptyString(payload.promptRef)
          && isNonEmptyString(payload.generationId) && isNonEmptyString(payload.specId)
          && isPositiveInteger(payload.specRevision) && isValidLoopSpec(payload.candidateLoopSpec)
          && isNonEmptyString(payload.validationId);
      case 'hitl.decided':
        return isNonEmptyString(payload.requestId)
          && (payload.decision === 'APPROVED' || payload.decision === 'REJECTED' || payload.decision === 'UNAVAILABLE')
          && isNonEmptyString(payload.decisionCode) && isNonEmptyString(payload.approvalSubject)
          && isNonEmptyString(payload.approvalReceiptRef);
      case 'generation.prepared':
        return isNonEmptyString(payload.generationId)
          && isNonEmptyString(payload.validationId)
          && isNonEmptyString(payload.manifestRef) && isValidLoopSpec(payload.candidateLoopSpec);
      case 'generation.promoted':
        return isNonEmptyString(payload.generationId);
      case 'rollback.applied':
        return isNonEmptyString(payload.targetGenerationId) && isNonEmptyString(payload.reasonCode);
      default:
        return false;
    }
  } catch {
    return false;
  }
}

/** True only for a complete event understood by the current reducer protocol. */
export function isValidRunEvent(value: unknown): value is RunEvent {
  return isValidEventEnvelope(value) && value.version === EVENT_VERSION && isValidEventPayload(value);
}
