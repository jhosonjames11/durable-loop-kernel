import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

import { EVENT_VERSION, isValidRunEvent, ReducerError, reduce } from '@loopgraph/core';

import { canonicalJson } from './canonical-json.js';
import { StorageError } from './storage-error.js';
import type {
  CheckpointRead,
  CheckpointStore,
  CheckpointWrite,
  EventStore,
  LeaseGrant,
  RecoveryInput,
  RecoveryOutput,
  RunEvent,
  RunId,
  RunLease,
  RunView,
} from '@loopgraph/core';

const SCHEMA_VERSION = 3;
const DEFAULT_MAX_EVENT_BYTES = 1024 * 1024;
const DEFAULT_MAX_EVENT_DEPTH = 32;

export interface SqliteRunStoreOptions {
  readonly filename: string;
  readonly clock?: () => number;
  /** Event data is trusted protocol data, not a secret store. Oversize data is rejected. */
  readonly maxEventBytes?: number;
  readonly maxEventDepth?: number;
}

type SqlRow = Record<string, unknown>;
type MigratedEvent = {
  runId: string;
  seq: number;
  eventId: string;
  idempotencyKey: string;
  expectedRevision: number;
  json: string;
};

function row(value: unknown): SqlRow | undefined {
  return value !== null && typeof value === 'object' ? value as SqlRow : undefined;
}

function asInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function isEventIdConstraint(error: unknown): boolean {
  return error instanceof Error && /UNIQUE constraint failed: run_events\.event_id/.test(error.message);
}

function hash(value: Uint8Array | string): string {
  return createHash('sha256').update(value).digest('hex');
}

function validIdentifier(value: string): boolean {
  return value.length > 0;
}

/**
 * Schema v1 predated first-class LoopSpecs.  Only an untouched legacy run can
 * be migrated without inventing graph transitions or candidate versions: its
 * opaque workflow reference becomes a stable, non-secret compatibility spec.
 * Any progressed v1 stream fails closed instead of fabricating a graph.
 */
function upgradeLegacyCreatedEvent(value: unknown): unknown {
  if (isValidRunEvent(value)) return value;
  const event = row(value);
  const payload = row(event?.payload);
  const workflowRef = asString(payload?.workflowRef);
  if (event?.type !== 'run.created' || workflowRef === undefined || workflowRef.length === 0
    || typeof payload?.requiresHitl !== 'boolean') return value;
  const specId = `legacy-${hash(workflowRef).slice(0, 24)}`;
  return {
    ...event,
    payload: {
      loopSpec: {
        specId,
        revision: 1,
        entryNodeId: 'legacy-entry',
        nodes: [{ nodeId: 'legacy-entry', kind: 'agent' }],
        edges: [{ fromNodeId: 'legacy-entry', toNodeId: 'legacy-entry' }],
      },
      requiresHitl: payload.requiresHitl,
    },
  };
}

function positiveLimit(value: number | undefined, fallback: number, name: string): number {
  const limit = value ?? fallback;
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    throw new StorageError('INVALID_ARGUMENT', `${name} must be a positive safe integer`);
  }
  return limit;
}

/** Check depth/cycles before canonicalJson recursively visits untrusted values. */
function assertJsonBounds(value: unknown, maxDepth: number): void {
  const ancestors = new Set<object>();
  const visit = (candidate: unknown, depth: number): void => {
    if (depth > maxDepth) {
      throw new StorageError('INVALID_ARGUMENT', 'event exceeds the configured JSON nesting limit');
    }
    if (candidate === null || typeof candidate === 'string' || typeof candidate === 'boolean') {
      return;
    }
    if (typeof candidate === 'number') {
      if (!Number.isFinite(candidate)) {
        throw new StorageError('INVALID_ARGUMENT', 'event contains a non-finite number');
      }
      return;
    }
    if (typeof candidate !== 'object' || ancestors.has(candidate)) {
      throw new StorageError('INVALID_ARGUMENT', 'event must contain finite JSON data without cycles');
    }
    const prototype = Object.getPrototypeOf(candidate);
    if (prototype !== Object.prototype && prototype !== null && !Array.isArray(candidate)) {
      throw new StorageError('INVALID_ARGUMENT', 'event must contain plain JSON objects');
    }
    ancestors.add(candidate);
    for (const item of Array.isArray(candidate) ? candidate : Object.values(candidate)) {
      visit(item, depth + 1);
    }
    ancestors.delete(candidate);
  };
  visit(value, 0);
}

/**
 * SQLite is the authority for event sequencing and leases. Event data is
 * protocol data: append validates it and its reducer transition before commit.
 */
export class SqliteRunStore implements EventStore, RunLease, CheckpointStore {
  readonly #database: DatabaseSync;
  readonly #clock: () => number;
  readonly #maxEventBytes: number;
  readonly #maxEventDepth: number;

  constructor({ filename, clock = Date.now, maxEventBytes, maxEventDepth }: SqliteRunStoreOptions) {
    this.#maxEventBytes = positiveLimit(maxEventBytes, DEFAULT_MAX_EVENT_BYTES, 'maxEventBytes');
    this.#maxEventDepth = positiveLimit(maxEventDepth, DEFAULT_MAX_EVENT_DEPTH, 'maxEventDepth');
    this.#database = new DatabaseSync(filename);
    this.#clock = clock;
    try {
      this.#initialize();
    } catch (error) {
      this.#database.close();
      throw error;
    }
  }

  close(): void {
    this.#database.close();
  }

  acquire(runId: RunId, holderId: string, ttlMs: number): LeaseGrant | null {
    this.#assertLeaseArguments(runId, holderId, ttlMs);
    const now = this.#clock();
    const expiresAt = now + ttlMs;
    this.#begin();
    try {
      const active = row(this.#database.prepare(
        'SELECT holder_id, fencing_token, expires_at FROM run_leases WHERE run_id = ?',
      ).get(runId));
      if (active && (asInteger(active.expires_at) ?? 0) > now) {
        this.#rollback();
        return null;
      }
      const counter = row(this.#database.prepare(
        'SELECT fencing_token FROM run_lease_counters WHERE run_id = ?',
      ).get(runId));
      const fencingToken = (asInteger(counter?.fencing_token) ?? 0) + 1;
      this.#database.prepare(
        'INSERT INTO run_lease_counters (run_id, fencing_token) VALUES (?, ?) '
          + 'ON CONFLICT(run_id) DO UPDATE SET fencing_token = excluded.fencing_token',
      ).run(runId, fencingToken);
      this.#database.prepare(
        'INSERT INTO run_leases (run_id, holder_id, fencing_token, expires_at) VALUES (?, ?, ?, ?) '
          + 'ON CONFLICT(run_id) DO UPDATE SET holder_id = excluded.holder_id, '
          + 'fencing_token = excluded.fencing_token, expires_at = excluded.expires_at',
      ).run(runId, holderId, fencingToken, expiresAt);
      this.#commit();
      return { runId, holderId, fencingToken, expiresAt };
    } catch (error) {
      this.#rollbackQuietly();
      throw error;
    }
  }

  renew(lease: LeaseGrant, ttlMs: number): LeaseGrant | null {
    this.#assertLeaseArguments(lease.runId, lease.holderId, ttlMs);
    const now = this.#clock();
    const expiresAt = now + ttlMs;
    this.#begin();
    try {
      const result = this.#database.prepare(
        'UPDATE run_leases SET expires_at = ? WHERE run_id = ? AND holder_id = ? '
          + 'AND fencing_token = ? AND expires_at > ?',
      ).run(expiresAt, lease.runId, lease.holderId, lease.fencingToken, now);
      this.#commit();
      return result.changes === 1 ? { ...lease, expiresAt } : null;
    } catch (error) {
      this.#rollbackQuietly();
      throw error;
    }
  }

  release(lease: LeaseGrant): boolean {
    this.#begin();
    try {
      const result = this.#database.prepare(
        'DELETE FROM run_leases WHERE run_id = ? AND holder_id = ? AND fencing_token = ?',
      ).run(lease.runId, lease.holderId, lease.fencingToken);
      this.#commit();
      return result.changes === 1;
    } catch (error) {
      this.#rollbackQuietly();
      throw error;
    }
  }

  append(event: RunEvent, lease: LeaseGrant): void {
    // This happens before BEGIN IMMEDIATE: malformed input can never enter a transaction.
    const serialized = this.#validateEventForWrite(event);
    if (event.runId !== lease.runId) {
      throw new StorageError('INVALID_ARGUMENT', 'event and lease run IDs must match');
    }
    const now = this.#clock();
    this.#begin();
    try {
      const active = row(this.#database.prepare(
        'SELECT holder_id, fencing_token, expires_at FROM run_leases WHERE run_id = ?',
      ).get(event.runId));
      if (!active || active.holder_id !== lease.holderId || active.fencing_token !== lease.fencingToken
        || (asInteger(active.expires_at) ?? 0) <= now) {
        throw new StorageError('FENCED', 'lease is no longer the active durable owner');
      }
      const duplicate = this.#database.prepare(
        'SELECT seq FROM run_events WHERE run_id = ? AND idempotency_key = ?',
      ).get(event.runId, event.idempotencyKey);
      if (duplicate !== undefined) {
        throw new StorageError('IDEMPOTENCY_COLLISION', 'idempotency key already exists for this run');
      }
      const revisionRow = row(this.#database.prepare(
        'SELECT COALESCE(MAX(seq), 0) AS revision FROM run_events WHERE run_id = ?',
      ).get(event.runId));
      const revision = asInteger(revisionRow?.revision) ?? 0;
      if (event.expectedRevision !== revision || event.seq !== revision + 1) {
        throw new StorageError('STALE_REVISION', 'event expected revision does not match the durable stream');
      }
      try {
        reduce([...this.#readEvents(event.runId), event]);
      } catch (error) {
        if (error instanceof ReducerError) {
          throw new StorageError('INVALID_ARGUMENT', 'event is not replayable from the durable stream');
        }
        throw error;
      }
      this.#database.prepare(
        'INSERT INTO run_events (run_id, seq, event_id, idempotency_key, expected_revision, canonical_json) '
          + 'VALUES (?, ?, ?, ?, ?, ?)',
      ).run(event.runId, event.seq, event.eventId, event.idempotencyKey, event.expectedRevision, serialized);
      this.#commit();
    } catch (error) {
      this.#rollbackQuietly();
      if (isEventIdConstraint(error)) {
        throw new StorageError('EVENT_ID_COLLISION', 'event ID already exists in the durable journal');
      }
      throw error;
    }
  }

  read(runId: RunId): readonly RunEvent[] {
    return this.#readEvents(runId);
  }

  /** Read the current durable lease without exposing SQLite implementation details. */
  readLease(runId: RunId): LeaseGrant | null {
    try {
      const active = row(this.#database.prepare(
        'SELECT holder_id, fencing_token, expires_at FROM run_leases WHERE run_id = ?',
      ).get(runId));
      const holderId = asString(active?.holder_id);
      const fencingToken = asInteger(active?.fencing_token);
      const expiresAt = asInteger(active?.expires_at);
      return holderId === undefined || fencingToken === undefined || expiresAt === undefined
        ? null
        : { runId, holderId, fencingToken, expiresAt };
    } catch {
      throw new StorageError('STORAGE_IO', 'SQLite lease read failed');
    }
  }

  #readEvents(runId: RunId): readonly RunEvent[] {
    let rows: unknown[];
    try {
      rows = this.#database.prepare(
        'SELECT run_id, seq, event_id, idempotency_key, expected_revision, canonical_json '
          + 'FROM run_events WHERE run_id = ? ORDER BY seq ASC',
      ).all(runId);
    } catch {
      throw new StorageError('STORAGE_IO', 'SQLite journal read failed');
    }
    try {
      return rows.map((databaseRow) => {
        const database = row(databaseRow);
        const json = asString(database?.canonical_json);
        const rowRunId = asString(database?.run_id);
        const seq = asInteger(database?.seq);
        const eventId = asString(database?.event_id);
        const idempotencyKey = asString(database?.idempotency_key);
        const expectedRevision = asInteger(database?.expected_revision);
        if (json === undefined || rowRunId === undefined || seq === undefined || eventId === undefined
          || idempotencyKey === undefined || expectedRevision === undefined) {
          throw new StorageError('CORRUPT_EVENT', 'event row has invalid indexed metadata');
        }
        const parsed: unknown = JSON.parse(json);
        if (canonicalJson(parsed) !== json || !isValidRunEvent(parsed)) {
          throw new StorageError('CORRUPT_EVENT', 'event row is not a canonical valid event');
        }
        if (parsed.runId !== rowRunId || parsed.seq !== seq || parsed.eventId !== eventId
          || parsed.idempotencyKey !== idempotencyKey || parsed.expectedRevision !== expectedRevision) {
          throw new StorageError('CORRUPT_EVENT', 'event row metadata does not match its canonical event');
        }
        return parsed;
      });
    } catch (error) {
      if (error instanceof StorageError) {
        throw error;
      }
      throw new StorageError('CORRUPT_EVENT', 'event row has invalid canonical JSON');
    }
  }

  saveCheckpoint(checkpoint: CheckpointWrite): void {
    if (checkpoint.revision < 1 || checkpoint.view.runId !== checkpoint.runId
      || checkpoint.view.revision !== checkpoint.revision) {
      throw new StorageError('INVALID_ARGUMENT', 'checkpoint does not match its run revision');
    }
    const viewJson = canonicalJson(checkpoint.view);
    const checksum = hash(viewJson);
    this.#database.prepare(
      'INSERT INTO run_checkpoints (run_id, revision, view_json, checksum) VALUES (?, ?, ?, ?) '
        + 'ON CONFLICT(run_id) DO UPDATE SET revision = excluded.revision, view_json = excluded.view_json, '
        + 'checksum = excluded.checksum',
    ).run(checkpoint.runId, checkpoint.revision, viewJson, checksum);
  }

  readCheckpoint(runId: RunId): CheckpointRead | null {
    const checkpoint = row(this.#database.prepare(
      'SELECT revision, view_json, checksum FROM run_checkpoints WHERE run_id = ?',
    ).get(runId));
    if (!checkpoint) return null;
    const revision = asInteger(checkpoint.revision);
    const viewJson = asString(checkpoint.view_json);
    const checksum = asString(checkpoint.checksum);
    if (revision === undefined || viewJson === undefined || checksum === undefined || hash(viewJson) !== checksum) return null;
    try {
      const view = JSON.parse(viewJson) as RunView;
      return canonicalJson(view) === viewJson && view.runId === runId && view.revision === revision
        ? { runId, revision, view }
        : null;
    } catch {
      return null;
    }
  }

  #initialize(): void {
    this.#database.exec('PRAGMA foreign_keys = ON');
    this.#database.exec(
      'CREATE TABLE IF NOT EXISTS loopgraph_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)',
    );
    this.#database.exec(`INSERT OR IGNORE INTO loopgraph_schema (id, version) VALUES (1, ${SCHEMA_VERSION})`);
    const schema = row(this.#database.prepare('SELECT version FROM loopgraph_schema WHERE id = 1').get());
    let version = asInteger(schema?.version);
    if (version === 1) {
      this.#migrateV1ToV2();
      version = 2;
    }
    if (version === 2) {
      this.#migrateV2ToV3();
      version = 3;
    }
    if (version !== SCHEMA_VERSION) throw new StorageError('SCHEMA_VERSION', 'unsupported SQLite schema version');
    this.#database.exec(
      'CREATE TABLE IF NOT EXISTS run_events ('
        + 'run_id TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, '
        + 'expected_revision INTEGER NOT NULL, canonical_json TEXT NOT NULL, '
        + 'PRIMARY KEY (run_id, seq), UNIQUE (run_id, idempotency_key), UNIQUE (event_id))',
    );
    this.#database.exec(
      'CREATE TABLE IF NOT EXISTS run_lease_counters (run_id TEXT PRIMARY KEY, fencing_token INTEGER NOT NULL)',
    );
    this.#database.exec(
      'CREATE TABLE IF NOT EXISTS run_leases ('
        + 'run_id TEXT PRIMARY KEY, holder_id TEXT NOT NULL, fencing_token INTEGER NOT NULL, expires_at INTEGER NOT NULL)',
    );
    this.#database.exec(
      'CREATE TABLE IF NOT EXISTS run_checkpoints ('
        + 'run_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, view_json TEXT NOT NULL, checksum TEXT NOT NULL)',
    );
  }

  /** Atomic rebuild adds the non-null, globally unique event_id column to v1. */
  #migrateV1ToV2(): void {
    this.#begin();
    try {
      const names = new Set(this.#database.prepare('PRAGMA table_info(run_events)').all()
        .map((column) => asString(row(column)?.name)));
      for (const name of ['run_id', 'seq', 'idempotency_key', 'expected_revision', 'canonical_json']) {
        if (!names.has(name)) throw new StorageError('SCHEMA_VERSION', 'version 1 journal has an unsupported table layout');
      }
      if (names.has('event_id')) throw new StorageError('SCHEMA_VERSION', 'version 1 journal unexpectedly already has event IDs');
      const oldRows = this.#database.prepare(
        'SELECT run_id, seq, idempotency_key, expected_revision, canonical_json FROM run_events ORDER BY run_id, seq',
      ).all();
      const migrated: MigratedEvent[] = [];
      const eventIds = new Set<string>();
      const streams = new Map<string, RunEvent[]>();
      for (const databaseRow of oldRows) {
        const old = row(databaseRow);
        const runId = asString(old?.run_id);
        const seq = asInteger(old?.seq);
        const idempotencyKey = asString(old?.idempotency_key);
        const expectedRevision = asInteger(old?.expected_revision);
        const json = asString(old?.canonical_json);
        if (runId === undefined || seq === undefined || idempotencyKey === undefined
          || expectedRevision === undefined || json === undefined) {
          throw new StorageError('CORRUPT_EVENT', 'version 1 journal row has invalid indexed metadata');
        }
        let event: unknown;
        try { event = JSON.parse(json); } catch {
          throw new StorageError('CORRUPT_EVENT', 'version 1 journal row has invalid canonical JSON');
        }
        if (canonicalJson(event) !== json) {
          throw new StorageError('CORRUPT_EVENT', 'version 1 journal row cannot be safely migrated');
        }
        event = upgradeLegacyCreatedEvent(event);
        if (!isValidRunEvent(event) || event.runId !== runId
          || event.seq !== seq || event.idempotencyKey !== idempotencyKey || event.expectedRevision !== expectedRevision) {
          throw new StorageError('CORRUPT_EVENT', 'version 1 journal row cannot be safely migrated');
        }
        if (eventIds.has(event.eventId)) throw new StorageError('CORRUPT_EVENT', 'version 1 journal has duplicate event IDs');
        eventIds.add(event.eventId);
        const stream = streams.get(runId) ?? [];
        stream.push(event);
        streams.set(runId, stream);
        migrated.push({ runId, seq, eventId: event.eventId, idempotencyKey, expectedRevision, json: canonicalJson(event) });
      }
      try {
        for (const stream of streams.values()) reduce(stream);
      } catch (error) {
        if (error instanceof ReducerError) {
          throw new StorageError('CORRUPT_EVENT', 'version 1 journal is not replayable');
        }
        throw error;
      }
      this.#database.exec(
        'CREATE TABLE run_events_v2 ('
          + 'run_id TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, '
          + 'expected_revision INTEGER NOT NULL, canonical_json TEXT NOT NULL, '
          + 'PRIMARY KEY (run_id, seq), UNIQUE (run_id, idempotency_key), UNIQUE (event_id))',
      );
      const insert = this.#database.prepare(
        'INSERT INTO run_events_v2 (run_id, seq, event_id, idempotency_key, expected_revision, canonical_json) '
          + 'VALUES (?, ?, ?, ?, ?, ?)',
      );
      for (const event of migrated) {
        insert.run(event.runId, event.seq, event.eventId, event.idempotencyKey, event.expectedRevision, event.json);
      }
      this.#database.exec('DROP TABLE run_events');
      this.#database.exec('ALTER TABLE run_events_v2 RENAME TO run_events');
      this.#database.prepare('UPDATE loopgraph_schema SET version = ? WHERE id = 1').run(2);
      this.#commit();
    } catch (error) {
      this.#rollbackQuietly();
      if (error instanceof StorageError) throw error;
      throw new StorageError('SCHEMA_VERSION', 'version 1 journal migration failed');
    }
  }

  /**
   * v2 introduced globally unique event ids but still let run.created carry an
   * opaque workflowRef. Upgrade only streams that can be represented exactly
   * as a LoopSpec; progressed workflowRef streams fail closed rather than
   * inventing their historical graph or candidate revision.
   */
  #migrateV2ToV3(): void {
    this.#begin();
    try {
      const names = new Set(this.#database.prepare('PRAGMA table_info(run_events)').all()
        .map((column) => asString(row(column)?.name)));
      for (const name of ['run_id', 'seq', 'event_id', 'idempotency_key', 'expected_revision', 'canonical_json']) {
        if (!names.has(name)) throw new StorageError('SCHEMA_VERSION', 'version 2 journal has an unsupported table layout');
      }
      const oldRows = this.#database.prepare(
        'SELECT run_id, seq, event_id, idempotency_key, expected_revision, canonical_json FROM run_events ORDER BY run_id, seq',
      ).all();
      const rewritten: MigratedEvent[] = [];
      const streams = new Map<string, RunEvent[]>();
      for (const databaseRow of oldRows) {
        const old = row(databaseRow);
        const runId = asString(old?.run_id);
        const seq = asInteger(old?.seq);
        const eventId = asString(old?.event_id);
        const idempotencyKey = asString(old?.idempotency_key);
        const expectedRevision = asInteger(old?.expected_revision);
        const json = asString(old?.canonical_json);
        if (runId === undefined || seq === undefined || eventId === undefined || idempotencyKey === undefined
          || expectedRevision === undefined || json === undefined) {
          throw new StorageError('CORRUPT_EVENT', 'version 2 journal row has invalid indexed metadata');
        }
        let event: unknown;
        try { event = JSON.parse(json); } catch {
          throw new StorageError('CORRUPT_EVENT', 'version 2 journal row has invalid canonical JSON');
        }
        if (canonicalJson(event) !== json) {
          throw new StorageError('CORRUPT_EVENT', 'version 2 journal row cannot be safely migrated');
        }
        event = upgradeLegacyCreatedEvent(event);
        if (!isValidRunEvent(event) || event.runId !== runId || event.seq !== seq || event.eventId !== eventId
          || event.idempotencyKey !== idempotencyKey || event.expectedRevision !== expectedRevision) {
          throw new StorageError('CORRUPT_EVENT', 'version 2 journal row cannot be safely migrated');
        }
        const stream = streams.get(runId) ?? [];
        stream.push(event);
        streams.set(runId, stream);
        rewritten.push({ runId, seq, eventId, idempotencyKey, expectedRevision, json: canonicalJson(event) });
      }
      try {
        for (const stream of streams.values()) reduce(stream);
      } catch (error) {
        if (error instanceof ReducerError) {
          throw new StorageError('CORRUPT_EVENT', 'version 2 journal lacks enough LoopSpec evidence to migrate safely');
        }
        throw error;
      }
      const update = this.#database.prepare(
        'UPDATE run_events SET canonical_json = ? WHERE run_id = ? AND seq = ? AND event_id = ?',
      );
      for (const event of rewritten) {
        update.run(event.json, event.runId, event.seq, event.eventId);
      }
      // A v2 checkpoint is a cached old projection, never source of truth.
      const checkpoints = this.#database.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'run_checkpoints'",
      ).get();
      if (checkpoints !== undefined) this.#database.exec('DELETE FROM run_checkpoints');
      this.#database.prepare('UPDATE loopgraph_schema SET version = ? WHERE id = 1').run(3);
      this.#commit();
    } catch (error) {
      this.#rollbackQuietly();
      if (error instanceof StorageError) throw error;
      throw new StorageError('SCHEMA_VERSION', 'version 2 journal migration failed');
    }
  }

  #validateEventForWrite(event: RunEvent): string {
    if (!isValidRunEvent(event)) throw new StorageError('INVALID_ARGUMENT', 'event envelope or payload is invalid');
    assertJsonBounds(event, this.#maxEventDepth);
    let serialized: string;
    try { serialized = canonicalJson(event); } catch {
      throw new StorageError('INVALID_ARGUMENT', 'event cannot be encoded as canonical JSON');
    }
    if (new TextEncoder().encode(serialized).byteLength > this.#maxEventBytes) {
      throw new StorageError('INVALID_ARGUMENT', 'event exceeds the configured serialized byte limit');
    }
    return serialized;
  }

  #assertLeaseArguments(runId: string, holderId: string, ttlMs: number): void {
    if (!validIdentifier(runId) || !validIdentifier(holderId) || !Number.isSafeInteger(ttlMs) || ttlMs <= 0) {
      throw new StorageError('INVALID_ARGUMENT', 'lease run ID, holder ID, and positive TTL are required');
    }
  }

  #begin(): void { this.#database.exec('BEGIN IMMEDIATE'); }
  #commit(): void { this.#database.exec('COMMIT'); }
  #rollback(): void { this.#database.exec('ROLLBACK'); }
  #rollbackQuietly(): void { try { this.#rollback(); } catch { /* no active transaction */ } }
}

/** Conservative restart coordinator: rebuild from the journal and never dispatch work. */
export class RecoveryCoordinator {
  readonly #store: EventStore & CheckpointStore;

  constructor(store: EventStore & CheckpointStore) { this.#store = store; }

  recover(input: RecoveryInput): RecoveryOutput {
    const checkpoint = this.#store.readCheckpoint(input.runId);
    const events = this.#store.read(input.runId);
    const replayedView = reduce(events);
    // The journal remains authoritative. A checkpoint is used only after it
    // exactly matches the deterministic journal head; this makes a cached view
    // part of recovery without letting it invent or skip durable transitions.
    const checkpointUsed = checkpoint !== null && checkpoint.revision === replayedView.revision
      && canonicalJson(checkpoint.view) === canonicalJson(replayedView);
    const view = checkpointUsed && checkpoint !== null ? checkpoint.view : replayedView;
    if (view.phase === 'PAUSED_RECOVERED') return { view, recovered: false, checkpointUsed };
    const unsafe = view.phase === 'AWAITING_ADMISSION_HITL' || view.phase === 'RUNNING' || view.phase === 'VALIDATING' || view.phase === 'PAUSE_REQUESTED'
      || (view.phase === 'VALIDATED' && view.hitl.status === 'PENDING');
    if (!unsafe) return { view, recovered: false, checkpointUsed };
    const event: RunEvent = {
      version: EVENT_VERSION, eventId: input.eventId, runId: input.runId, seq: view.revision + 1,
      occurredAt: input.occurredAt, actor: input.actor, causationId: null, correlationId: null,
      idempotencyKey: `recovery.uncertain:${input.runId}`, expectedRevision: view.revision,
      type: 'recovery.uncertain', payload: { reasonCode: input.reasonCode },
    };
    try { this.#store.append(event, input.lease); } catch (error) {
      if (error instanceof StorageError && error.code === 'IDEMPOTENCY_COLLISION') {
        const recoveredView = reduce(this.#store.read(input.runId));
        if (recoveredView.phase === 'PAUSED_RECOVERED') return { view: recoveredView, recovered: false, checkpointUsed };
      }
      throw error;
    }
    return { view: reduce(this.#store.read(input.runId)), recovered: true, checkpointUsed };
  }
}
