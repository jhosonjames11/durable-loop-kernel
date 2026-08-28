export type StorageErrorCode =
  | 'FENCED'
  | 'STALE_REVISION'
  | 'IDEMPOTENCY_COLLISION'
  | 'EVENT_ID_COLLISION'
  | 'CORRUPT_EVENT'
  | 'SCHEMA_VERSION'
  | 'INVALID_ARGUMENT'
  | 'NOT_FOUND'
  | 'STORAGE_IO'
  | 'UNSAFE_FILESYSTEM'
  | 'DURABILITY_UNAVAILABLE';

export class StorageError extends Error {
  readonly code: StorageErrorCode;

  constructor(code: StorageErrorCode, message: string) {
    super(message);
    this.name = 'StorageError';
    this.code = code;
  }
}
