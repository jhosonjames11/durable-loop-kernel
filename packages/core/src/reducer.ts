import { EVENT_VERSION, isValidEventEnvelope, isValidEventPayload, type RunEvent } from './events.js';
import { ReducerError, ReducerErrorCode } from './errors.js';
import { hasLoopEdge, sameLoopSpec, sameLoopSpecIdentity } from './loop-spec.js';
import type { ActiveAttempt, RunPhase, RunView, ValidationState } from './model.js';

const emptyValidation: ValidationState = {
  status: 'NONE',
  nodeId: null,
  attemptId: null,
  validationId: null,
  evidenceRef: null,
};

const emptyHitl = {
  status: 'NOT_REQUESTED', requestId: null, generationId: null,
  specId: null, specRevision: null, candidateLoopSpec: null, validationId: null,
  approvalSubject: null, approvalReceiptRef: null,
} as const;

function fail(code: ReducerErrorCode, sequence: number | null): never {
  throw new ReducerError(code, sequence);
}

function requireTransition(condition: boolean, sequence: number): asserts condition {
  if (!condition) {
    fail(ReducerErrorCode.ILLEGAL_TRANSITION, sequence);
  }
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function initialView(event: Extract<RunEvent, { type: 'run.created' }>): RunView {
  return {
    runId: event.runId,
    revision: event.seq,
    phase: 'READY',
    loopSpec: event.payload.loopSpec,
    nextNodeId: event.payload.loopSpec.entryNodeId,
    requiresHitl: event.payload.requiresHitl,
    activeAttempt: null,
    validation: emptyValidation,
    admissionHitl: { status: 'NOT_REQUESTED', requestId: null },
    hitl: emptyHitl,
    pauseRequested: false,
    lease: null,
    preparedGenerations: [],
    promotedGenerations: [],
    activeGenerationId: null,
    rollbackAncestry: [],
    retryCount: 0,
    recoveryReasonCode: null,
    acceptedIdempotencyKeys: [event.idempotencyKey],
  };
}

function withAcceptedKey(view: RunView, event: RunEvent): RunView {
  return {
    ...view,
    revision: event.seq,
    acceptedIdempotencyKeys: [...view.acceptedIdempotencyKeys, event.idempotencyKey],
  };
}

function sameAttempt(active: ActiveAttempt | null, nodeId: string, attemptId: string): boolean {
  return active !== null && active.nodeId === nodeId && active.attemptId === attemptId;
}

function canRecoverFrom(view: RunView): boolean {
  return view.phase === 'AWAITING_ADMISSION_HITL'
    || view.phase === 'RUNNING'
    || view.phase === 'VALIDATING'
    || view.phase === 'PAUSE_REQUESTED'
    // A pending HITL decision is an externally in-flight operation even though
    // the compact projection records it under the VALIDATED phase.
    || (view.phase === 'VALIDATED' && view.hitl.status === 'PENDING');
}

function phaseAfterResume(validation: ValidationState): RunPhase {
  if (validation.status === 'PENDING') {
    return 'VALIDATING';
  }
  if (validation.status === 'PASSED') {
    return 'VALIDATED';
  }
  return 'READY';
}

/**
 * Replay one immutable, ordered run stream into a serializable projection.
 * This function is deliberately free of clocks, random ids, I/O, and adapters.
 */
export function reduce(events: readonly RunEvent[]): RunView {
  if (events.length === 0) {
    fail(ReducerErrorCode.EMPTY_EVENT_STREAM, null);
  }

  let view: RunView | undefined;
  const acceptedKeys = new Set<string>();

  for (const rawEvent of events as readonly unknown[]) {
    const eventRecord = typeof rawEvent === 'object' && rawEvent !== null && !Array.isArray(rawEvent)
      ? rawEvent as Record<string, unknown>
      : undefined;
    const sequence = eventRecord !== undefined && isPositiveInteger(eventRecord.seq) ? eventRecord.seq : null;
    if (!isValidEventEnvelope(rawEvent)) {
      fail(ReducerErrorCode.INVALID_EVENT, sequence);
    }
    if (rawEvent.version !== EVENT_VERSION) {
      fail(ReducerErrorCode.INVALID_EVENT_VERSION, sequence);
    }
    if (!isValidEventPayload(rawEvent)) {
      fail(ReducerErrorCode.INVALID_EVENT, sequence);
    }
    const event = rawEvent as unknown as RunEvent;

    if (acceptedKeys.has(event.idempotencyKey)) {
      fail(ReducerErrorCode.DUPLICATE_IDEMPOTENCY_KEY, event.seq);
    }

    if (view === undefined) {
      if (event.type !== 'run.created') {
        fail(ReducerErrorCode.ILLEGAL_TRANSITION, event.seq);
      }
      if (event.seq !== 1) {
        fail(ReducerErrorCode.NON_CONTIGUOUS_SEQUENCE, event.seq);
      }
      if (event.expectedRevision !== 0) {
        fail(ReducerErrorCode.STALE_REVISION, event.seq);
      }
      view = initialView(event);
      acceptedKeys.add(event.idempotencyKey);
      continue;
    }

    if (event.runId !== view.runId) {
      fail(ReducerErrorCode.RUN_ID_MISMATCH, event.seq);
    }
    if (event.seq !== view.revision + 1) {
      fail(ReducerErrorCode.NON_CONTIGUOUS_SEQUENCE, event.seq);
    }
    if (event.expectedRevision !== view.revision) {
      fail(ReducerErrorCode.STALE_REVISION, event.seq);
    }

    const next = withAcceptedKey(view, event);
    switch (event.type) {
      case 'run.created':
        fail(ReducerErrorCode.ILLEGAL_TRANSITION, event.seq);
      case 'lease.acquired':
        requireTransition(
          view.phase !== 'PAUSED_RECOVERED'
            && (view.lease === null || event.payload.fencingToken > view.lease.fencingToken),
          event.seq,
        );
        view = {
          ...next,
          lease: { holderId: event.payload.holderId, fencingToken: event.payload.fencingToken },
        };
        break;
      case 'node.dispatch.requested':
        requireTransition(
          view.phase === 'READY' && !view.pauseRequested && view.activeAttempt === null
            && view.nextNodeId === event.payload.nodeId,
          event.seq,
        );
        view = {
          ...next,
          phase: 'RUNNING',
          activeAttempt: {
            nodeId: event.payload.nodeId,
            attemptId: event.payload.attemptId,
            status: 'DISPATCH_REQUESTED',
          },
        };
        break;
      case 'admission.hitl.requested':
        requireTransition(
          view.phase === 'READY'
            && view.activeAttempt === null
            && view.admissionHitl.status !== 'PENDING',
          event.seq,
        );
        view = {
          ...next,
          phase: 'AWAITING_ADMISSION_HITL',
          admissionHitl: { status: 'PENDING', requestId: event.payload.requestId },
        };
        break;
      case 'admission.hitl.decided': {
        const paused = view.phase === 'PAUSED';
        requireTransition(
          (view.phase === 'AWAITING_ADMISSION_HITL' || paused)
            && view.admissionHitl.status === 'PENDING'
            && view.admissionHitl.requestId === event.payload.requestId,
          event.seq,
        );
        view = {
          ...next,
          phase: paused ? 'PAUSED' : 'READY',
          admissionHitl: { status: event.payload.decision, requestId: event.payload.requestId },
        };
        break;
      }
      case 'node.started':
        requireTransition(
          view.phase === 'RUNNING'
            && view.activeAttempt?.status === 'DISPATCH_REQUESTED'
            && sameAttempt(view.activeAttempt, event.payload.nodeId, event.payload.attemptId),
          event.seq,
        );
        view = {
          ...next,
          activeAttempt: {
            nodeId: event.payload.nodeId,
            attemptId: event.payload.attemptId,
            status: 'STARTED',
          },
        };
        break;
      case 'node.settled': {
        requireTransition(
          (view.phase === 'RUNNING' || view.phase === 'PAUSE_REQUESTED')
            && view.activeAttempt?.status === 'STARTED'
            && sameAttempt(view.activeAttempt, event.payload.nodeId, event.payload.attemptId),
          event.seq,
        );
        // Only a completed node may create validation authority. A failed
        // execution is a retry boundary and clears validation/HITL authority.
        if (event.payload.outcome !== 'SUCCEEDED') {
          view = {
            ...next,
            phase: 'READY',
            activeAttempt: null,
            pauseRequested: false,
            validation: emptyValidation,
            hitl: emptyHitl,
            nextNodeId: event.payload.nodeId,
            retryCount: view.retryCount + 1,
          };
          break;
        }
        const pauseCommitted = view.phase === 'PAUSE_REQUESTED';
        view = {
          ...next,
          phase: pauseCommitted ? 'PAUSED' : 'VALIDATING',
          activeAttempt: null,
          pauseRequested: pauseCommitted ? false : view.pauseRequested,
          validation: {
            status: 'PENDING',
            nodeId: event.payload.nodeId,
            attemptId: event.payload.attemptId,
            validationId: null,
            evidenceRef: null,
          },
          hitl: emptyHitl,
        };
        break;
      }
      case 'validation.recorded': {
        const paused = view.phase === 'PAUSED';
        requireTransition(
          (view.phase === 'VALIDATING' || paused)
            && view.validation.status === 'PENDING'
            && view.validation.nodeId === event.payload.nodeId
            && view.validation.attemptId === event.payload.attemptId,
          event.seq,
        );
        view = {
          ...next,
          phase: paused ? 'PAUSED' : (event.payload.passed ? 'VALIDATED' : 'READY'),
          validation: {
            status: event.payload.passed ? 'PASSED' : 'FAILED',
            nodeId: event.payload.nodeId,
            attemptId: event.payload.attemptId,
            validationId: event.payload.validationId,
            evidenceRef: event.payload.evidenceRef,
          },
          retryCount: event.payload.passed ? view.retryCount : view.retryCount + 1,
        };
        break;
      }
      case 'validation.superseded':
        requireTransition(
          view.phase === 'VALIDATED'
            && view.validation.status === 'PASSED'
            && view.hitl.status !== 'PENDING'
            && hasLoopEdge(view.loopSpec, view.validation.nodeId ?? '', event.payload.nextNodeId),
          event.seq,
        );
        view = {
          ...next,
          phase: 'READY',
          validation: emptyValidation,
          hitl: emptyHitl,
          nextNodeId: event.payload.nextNodeId,
        };
        break;
      case 'pause.requested':
        requireTransition(
          (view.phase === 'READY' || view.phase === 'AWAITING_ADMISSION_HITL' || view.phase === 'RUNNING' || view.phase === 'VALIDATING' || view.phase === 'VALIDATED')
            && !view.pauseRequested,
          event.seq,
        );
        view = { ...next, phase: 'PAUSE_REQUESTED', pauseRequested: true };
        break;
      case 'run.paused':
        requireTransition(
          view.phase === 'PAUSE_REQUESTED' && view.pauseRequested && view.activeAttempt === null,
          event.seq,
        );
        view = { ...next, phase: 'PAUSED', pauseRequested: false };
        break;
      case 'run.resumed':
        requireTransition(
          (view.phase === 'PAUSED' || view.phase === 'PAUSED_RECOVERED')
            && !view.pauseRequested && view.activeAttempt === null,
          event.seq,
        );
        view = { ...next, phase: phaseAfterResume(view.validation), recoveryReasonCode: null };
        break;
      case 'hitl.requested':
        requireTransition(
          view.phase === 'VALIDATED'
            && view.validation.status === 'PASSED'
            && view.hitl.status === 'NOT_REQUESTED',
          event.seq,
        );
        {
          const prepared = view.preparedGenerations.find(({ generationId }) => generationId === event.payload.generationId);
          requireTransition(
            prepared !== undefined && prepared.validationId === event.payload.validationId
              && prepared.candidateLoopSpec.specId === event.payload.specId
              && prepared.candidateLoopSpec.revision === event.payload.specRevision
              && sameLoopSpec(prepared.candidateLoopSpec, event.payload.candidateLoopSpec),
            event.seq,
          );
          view = { ...next, hitl: {
            status: 'PENDING', requestId: event.payload.requestId, generationId: event.payload.generationId,
            specId: event.payload.specId, specRevision: event.payload.specRevision,
            candidateLoopSpec: event.payload.candidateLoopSpec, validationId: event.payload.validationId,
            approvalSubject: null, approvalReceiptRef: null,
          } };
        }
        break;
      case 'hitl.decided':
        requireTransition(
          view.phase === 'VALIDATED'
            && view.hitl.status === 'PENDING'
            && view.hitl.requestId === event.payload.requestId,
          event.seq,
        );
        view = {
          ...next,
          phase: event.payload.decision === 'APPROVED' ? 'VALIDATED' : 'READY',
          hitl: {
            ...view.hitl,
            status: event.payload.decision,
            approvalSubject: event.payload.approvalSubject,
            approvalReceiptRef: event.payload.approvalReceiptRef,
          },
        };
        break;
      case 'generation.prepared':
        if (view.validation.status !== 'PASSED' || view.validation.validationId !== event.payload.validationId) {
          fail(ReducerErrorCode.GENERATION_NOT_VALIDATED, event.seq);
        }
        requireTransition(
          view.phase === 'VALIDATED'
            && !view.preparedGenerations.some(({ generationId }) => generationId === event.payload.generationId)
            && event.payload.candidateLoopSpec.specId === view.loopSpec.specId
            && event.payload.candidateLoopSpec.revision > view.loopSpec.revision
            && !view.preparedGenerations.some(({ candidateLoopSpec }) => sameLoopSpecIdentity(candidateLoopSpec, event.payload.candidateLoopSpec))
            && !view.promotedGenerations.some(({ loopSpec }) => sameLoopSpecIdentity(loopSpec, event.payload.candidateLoopSpec)),
          event.seq,
        );
        view = {
          ...next,
          preparedGenerations: [
            ...view.preparedGenerations,
            {
              generationId: event.payload.generationId,
              validationId: event.payload.validationId,
              manifestRef: event.payload.manifestRef,
              preparedAtRevision: event.seq,
              candidateLoopSpec: event.payload.candidateLoopSpec,
            },
          ],
        };
        break;
      case 'generation.promoted': {
        if (view.validation.status !== 'PASSED' || view.validation.validationId === null) {
          fail(ReducerErrorCode.GENERATION_NOT_VALIDATED, event.seq);
        }
        const prepared = view.preparedGenerations.find(({ generationId }) => generationId === event.payload.generationId);
        if (prepared === undefined || prepared.validationId !== view.validation.validationId) {
          fail(ReducerErrorCode.GENERATION_NOT_PREPARED, event.seq);
        }
        const hitlSatisfied = view.requiresHitl
          ? view.hitl.status === 'APPROVED' && view.hitl.generationId === event.payload.generationId
            && view.hitl.validationId === view.validation.validationId
            && view.hitl.specId === prepared?.candidateLoopSpec.specId && view.hitl.specRevision === prepared?.candidateLoopSpec.revision
            && view.hitl.candidateLoopSpec !== null && sameLoopSpec(view.hitl.candidateLoopSpec, prepared.candidateLoopSpec)
          : view.hitl.status === 'NOT_REQUESTED' || view.hitl.status === 'APPROVED';
        if (!hitlSatisfied) {
          fail(ReducerErrorCode.HITL_NOT_APPROVED, event.seq);
        }
        requireTransition(
          view.phase === 'VALIDATED'
            && !view.promotedGenerations.some(({ generationId }) => generationId === event.payload.generationId),
          event.seq,
        );
        view = {
          ...next,
          phase: 'READY',
          activeGenerationId: event.payload.generationId,
          loopSpec: prepared.candidateLoopSpec,
          nextNodeId: prepared.candidateLoopSpec.entryNodeId,
          validation: emptyValidation,
          promotedGenerations: [
            ...view.promotedGenerations,
            { generationId: event.payload.generationId, promotedAtRevision: event.seq, loopSpec: prepared.candidateLoopSpec },
          ],
        };
        break;
      }
      case 'rollback.applied':
        if (!view.promotedGenerations.some(({ generationId }) => generationId === event.payload.targetGenerationId)) {
          fail(ReducerErrorCode.ROLLBACK_TARGET_NOT_PROMOTED, event.seq);
        }
        requireTransition(view.phase === 'READY' && view.activeAttempt === null, event.seq);
        const target = view.promotedGenerations.find(({ generationId }) => generationId === event.payload.targetGenerationId);
        view = {
          ...next,
          activeGenerationId: event.payload.targetGenerationId,
          loopSpec: target?.loopSpec ?? view.loopSpec,
          nextNodeId: (target?.loopSpec ?? view.loopSpec).entryNodeId,
          rollbackAncestry: [
            ...view.rollbackAncestry,
            {
              rollbackSequence: event.seq,
              fromGenerationId: view.activeGenerationId,
              targetGenerationId: event.payload.targetGenerationId,
            },
          ],
        };
        break;
      case 'recovery.uncertain': {
        requireTransition(canRecoverFrom(view), event.seq);
        // A dispatched/started attempt has no durable settlement, and a pending
        // validator has no durable result. Neither can authorize a later
        // promotion after an operator resume; resume returns to READY so an
        // operator can explicitly choose a fresh retry.
        const lostAttempt = view.activeAttempt !== null;
        const lostValidation = view.validation.status === 'PENDING';
        view = {
          ...next,
          phase: 'PAUSED_RECOVERED',
          activeAttempt: null,
          validation: lostAttempt || lostValidation ? emptyValidation : view.validation,
          admissionHitl: view.admissionHitl.status === 'PENDING'
            ? { status: 'NOT_REQUESTED', requestId: null }
            : view.admissionHitl,
          // A post-validation approval request is another external operation.
          // Its answer might have been delivered while this process was down,
          // so discard its pending authority rather than accepting a late old
          // decision after recovery. The already prepared immutable candidate
          // remains available for an explicitly requested fresh approval.
          hitl: lostAttempt || lostValidation || view.hitl.status === 'PENDING' ? emptyHitl : view.hitl,
          pauseRequested: false,
          recoveryReasonCode: event.payload.reasonCode,
        };
        break;
      }
      default:
        fail(ReducerErrorCode.INVALID_EVENT, null);
    }
    acceptedKeys.add(event.idempotencyKey);
  }

  if (view === undefined) {
    fail(ReducerErrorCode.EMPTY_EVENT_STREAM, null);
  }
  return view;
}
