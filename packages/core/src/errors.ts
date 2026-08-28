export const ReducerErrorCode = {
  EMPTY_EVENT_STREAM: 'EMPTY_EVENT_STREAM',
  INVALID_EVENT: 'INVALID_EVENT',
  INVALID_EVENT_VERSION: 'INVALID_EVENT_VERSION',
  RUN_ID_MISMATCH: 'RUN_ID_MISMATCH',
  NON_CONTIGUOUS_SEQUENCE: 'NON_CONTIGUOUS_SEQUENCE',
  STALE_REVISION: 'STALE_REVISION',
  DUPLICATE_IDEMPOTENCY_KEY: 'DUPLICATE_IDEMPOTENCY_KEY',
  ILLEGAL_TRANSITION: 'ILLEGAL_TRANSITION',
  GENERATION_NOT_VALIDATED: 'GENERATION_NOT_VALIDATED',
  GENERATION_NOT_PREPARED: 'GENERATION_NOT_PREPARED',
  HITL_NOT_APPROVED: 'HITL_NOT_APPROVED',
  ROLLBACK_TARGET_NOT_PROMOTED: 'ROLLBACK_TARGET_NOT_PROMOTED',
} as const;

export type ReducerErrorCode = (typeof ReducerErrorCode)[keyof typeof ReducerErrorCode];

/** A stable, machine-readable failure emitted by the pure event reducer. */
export class ReducerError extends Error {
  public readonly name = 'ReducerError';

  public constructor(
    public readonly code: ReducerErrorCode,
    public readonly sequence: number | null,
  ) {
    super(`${code}${sequence === null ? '' : ` at sequence ${sequence}`}`);
  }
}
