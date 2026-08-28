import { StorageError } from './storage-error.js';

/**
 * Compare strings by successive UTF-16 code units (not locale collation).
 *
 * JavaScript strings are sequences of UTF-16 code units, so this deliberately
 * gives every persisted JSON object a stable order across process locales and
 * operating systems. For example, `Z` (0x005a) sorts before `a` (0x0061).
 */
export function compareUnicodeCodeUnits(left: string, right: string): number {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const difference = left.charCodeAt(index) - right.charCodeAt(index);
    if (difference !== 0) {
      return difference;
    }
  }
  return left.length - right.length;
}

/** Serialize JSON-compatible values deterministically before durable persistence. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new StorageError('INVALID_ARGUMENT', 'canonical JSON cannot encode a non-finite number');
    }
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(',')}]`;
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => compareUnicodeCodeUnits(left, right));
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(',')}}`;
  }
  throw new StorageError('INVALID_ARGUMENT', 'canonical JSON only accepts JSON-compatible values');
}
