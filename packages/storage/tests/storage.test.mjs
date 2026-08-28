import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { access, mkdtemp, mkdir, readFile, rm, symlink, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';

import { EVENT_VERSION, reduce } from '@loopgraph/core';
import {
  FileArtifactStore,
  RecoveryCoordinator,
  SqliteRunStore,
  StorageError,
} from '@loopgraph/storage';

const RUN_ID = 'run-durable-001';

function loopSpec(revision = 1) {
  return {
    specId: 'durable-loop', revision, entryNodeId: 'build',
    nodes: [{ nodeId: 'build', kind: 'agent' }],
    edges: [{ fromNodeId: 'build', toNodeId: 'build' }],
  };
}

function event(seq, type, payload, overrides = {}) {
  const runId = overrides.runId ?? RUN_ID;
  const result = {
    version: EVENT_VERSION, eventId: `event-${runId}-${seq}-${type}`, runId, seq,
    occurredAt: `2026-08-25T00:00:${String(seq).padStart(2, '0')}.000Z`, actor: 'worker',
    causationId: null, correlationId: 'correlation-durable-001', idempotencyKey: `key-${seq}-${type}`,
    expectedRevision: seq - 1, type, payload, ...overrides,
  };
  if (result.type === 'run.created' && result.payload !== null && typeof result.payload === 'object') {
    result.payload = { ...result.payload, loopSpec: result.payload.loopSpec ?? loopSpec(1) };
  }
  return result;
}
function created(overrides = {}) { return event(1, 'run.created', { loopSpec: loopSpec(1), requiresHitl: false }, overrides); }
function dispatched(overrides = {}) { return event(2, 'node.dispatch.requested', { nodeId: 'build', attemptId: 'attempt-1' }, overrides); }
async function temporaryDirectory(prefix) { return mkdtemp(join(tmpdir(), prefix)); }
function expectStorageError(code, callback) {
  assert.throws(callback, (error) => { assert.ok(error instanceof StorageError); assert.equal(error.code, code); return true; });
}

// Core durability/restart behavior.
test('replays append-only canonical events after a SQLite restart', async (t) => {
  const directory = await temporaryDirectory('loopgraph-store-'); t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = join(directory, 'runs.sqlite'); let now = 1_000;
  const first = new SqliteRunStore({ filename, clock: () => now }); const lease = first.acquire(RUN_ID, 'worker-a', 100); assert.ok(lease);
  first.append(created(), lease); first.append(dispatched(), lease); first.close();
  const second = new SqliteRunStore({ filename, clock: () => now }); t.after(() => second.close());
  const events = second.read(RUN_ID); assert.deepEqual(events.map(({ seq }) => seq), [1, 2]); assert.equal(reduce(events).phase, 'RUNNING');
});

test('append rejects malformed and reducer-unreplayable events before writing', async (t) => {
  const directory = await temporaryDirectory('loopgraph-append-validate-'); t.after(() => rm(directory, { recursive: true, force: true }));
  const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => 1_000 }); t.after(() => store.close());
  const lease = store.acquire(RUN_ID, 'worker-a', 100); assert.ok(lease);
  expectStorageError('INVALID_ARGUMENT', () => store.append(created({ payload: { loopSpec: loopSpec(1), requiresHitl: 'no' } }), lease));
  expectStorageError('INVALID_ARGUMENT', () => store.append(event(1, 'node.dispatch.requested', { nodeId: 'build', attemptId: 'a' }), lease));
  assert.deepEqual(store.read(RUN_ID), []);
});

test('rejects stale revisions and idempotency collisions without appending a partial event', async (t) => {
  const directory = await temporaryDirectory('loopgraph-cas-'); t.after(() => rm(directory, { recursive: true, force: true }));
  const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => 1_000 }); t.after(() => store.close());
  const lease = store.acquire(RUN_ID, 'worker-a', 100); assert.ok(lease); const first = created(); store.append(first, lease);
  expectStorageError('STALE_REVISION', () => store.append(dispatched({ expectedRevision: 0 }), lease));
  expectStorageError('IDEMPOTENCY_COLLISION', () => store.append(first, lease)); assert.deepEqual(store.read(RUN_ID).map(({ seq }) => seq), [1]);
});

test('fencing tokens prevent a stale lease holder from appending after takeover', async (t) => {
  const directory = await temporaryDirectory('loopgraph-fence-'); t.after(() => rm(directory, { recursive: true, force: true })); let now = 1_000;
  const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => now }); t.after(() => store.close());
  const leaseA = store.acquire(RUN_ID, 'worker-a', 10); assert.ok(leaseA); store.append(created(), leaseA); now += 11;
  const leaseB = store.acquire(RUN_ID, 'worker-b', 10); assert.ok(leaseB); expectStorageError('FENCED', () => store.append(dispatched(), leaseA)); store.append(dispatched(), leaseB);
});

test('lease renewal preserves ownership and every post-release acquisition advances fencing', async (t) => {
  const directory = await temporaryDirectory('loopgraph-lease-'); t.after(() => rm(directory, { recursive: true, force: true })); let now = 1_000;
  const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => now }); t.after(() => store.close());
  const first = store.acquire(RUN_ID, 'worker-a', 10); assert.ok(first); now += 5; const renewed = store.renew(first, 20); assert.ok(renewed);
  assert.equal(store.acquire(RUN_ID, 'worker-b', 10), null); assert.equal(store.release(renewed), true); const second = store.acquire(RUN_ID, 'worker-b', 10); assert.ok(second); assert.ok(second.fencingToken > first.fencingToken);
});

test('journal read fails closed on canonical or indexed-column tampering', async (t) => {
  const directory = await temporaryDirectory('loopgraph-tamper-'); t.after(() => rm(directory, { recursive: true, force: true }));
  const filename = join(directory, 'runs.sqlite'); const store = new SqliteRunStore({ filename, clock: () => 1_000 }); const lease = store.acquire(RUN_ID, 'worker-a', 100); assert.ok(lease); store.append(created(), lease);
  const database = new DatabaseSync(filename); database.prepare('UPDATE run_events SET event_id = ? WHERE run_id = ?').run('forged-event-id', RUN_ID); database.close(); expectStorageError('CORRUPT_EVENT', () => store.read(RUN_ID)); store.close();
  const clean = new SqliteRunStore({ filename, clock: () => 1_000 }); const db2 = new DatabaseSync(filename); db2.prepare('UPDATE run_events SET canonical_json = ? WHERE run_id = ?').run('{"x":1}', RUN_ID); db2.close(); expectStorageError('CORRUPT_EVENT', () => clean.read(RUN_ID)); clean.close();
});

test('atomically upgrades a seeded v1 journal and replays it under v3', async (t) => {
  const directory = await temporaryDirectory('loopgraph-v1-'); t.after(() => rm(directory, { recursive: true, force: true })); const filename = join(directory, 'runs.sqlite');
  const first = created(); const json = '{"actor":"worker","causationId":null,"correlationId":"correlation-durable-001","eventId":"event-run-durable-001-1-run.created","expectedRevision":0,"idempotencyKey":"key-1-run.created","occurredAt":"2026-08-25T00:00:01.000Z","payload":{"requiresHitl":false,"workflowRef":"workflow:durable@1"},"runId":"run-durable-001","seq":1,"type":"run.created","version":1}';
  const database = new DatabaseSync(filename);
  database.exec('CREATE TABLE loopgraph_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL); INSERT INTO loopgraph_schema VALUES (1, 1); CREATE TABLE run_events (run_id TEXT NOT NULL, seq INTEGER NOT NULL, idempotency_key TEXT NOT NULL, expected_revision INTEGER NOT NULL, canonical_json TEXT NOT NULL, PRIMARY KEY (run_id, seq), UNIQUE (run_id, idempotency_key));');
  database.prepare('INSERT INTO run_events VALUES (?, ?, ?, ?, ?)').run(first.runId, first.seq, first.idempotencyKey, first.expectedRevision, json); database.close();
  const store = new SqliteRunStore({ filename, clock: () => 1_000 }); t.after(() => store.close()); assert.equal(reduce(store.read(RUN_ID)).phase, 'READY');
  const upgraded = new DatabaseSync(filename); assert.equal(upgraded.prepare('SELECT version FROM loopgraph_schema').get().version, 3); assert.equal(upgraded.prepare('SELECT event_id FROM run_events').get().event_id, first.eventId); upgraded.close();
});

test('upgrades an untouched v2 workflow-reference journal to a marked legacy LoopSpec', async (t) => {
  const directory = await temporaryDirectory('loopgraph-v2-'); t.after(() => rm(directory, { recursive: true, force: true })); const filename = join(directory, 'runs.sqlite');
  const json = '{"actor":"worker","causationId":null,"correlationId":"correlation-durable-001","eventId":"event-v2-run-created","expectedRevision":0,"idempotencyKey":"key-v2-run-created","occurredAt":"2026-08-25T00:00:01.000Z","payload":{"requiresHitl":false,"workflowRef":"workflow:durable@1"},"runId":"run-durable-001","seq":1,"type":"run.created","version":1}';
  const database = new DatabaseSync(filename);
  database.exec('CREATE TABLE loopgraph_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL); INSERT INTO loopgraph_schema VALUES (1, 2); CREATE TABLE run_events (run_id TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, expected_revision INTEGER NOT NULL, canonical_json TEXT NOT NULL, PRIMARY KEY (run_id, seq), UNIQUE (run_id, idempotency_key), UNIQUE (event_id)); CREATE TABLE run_checkpoints (run_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, view_json TEXT NOT NULL, checksum TEXT NOT NULL);');
  database.prepare('INSERT INTO run_events VALUES (?, ?, ?, ?, ?, ?)').run(RUN_ID, 1, 'event-v2-run-created', 'key-v2-run-created', 0, json); database.close();
  const store = new SqliteRunStore({ filename }); t.after(() => store.close());
  const view = reduce(store.read(RUN_ID));
  assert.equal(view.loopSpec.specId.startsWith('legacy-'), true);
  assert.equal(view.loopSpec.entryNodeId, 'legacy-entry');
  const upgraded = new DatabaseSync(filename); assert.equal(upgraded.prepare('SELECT version FROM loopgraph_schema').get().version, 3); upgraded.close();
});

test('refuses a progressed v2 workflow-reference journal instead of inventing graph history', async (t) => {
  const directory = await temporaryDirectory('loopgraph-v2-refuse-'); t.after(() => rm(directory, { recursive: true, force: true })); const filename = join(directory, 'runs.sqlite');
  const createdJson = '{"actor":"worker","causationId":null,"correlationId":"correlation-durable-001","eventId":"event-v2-run-created","expectedRevision":0,"idempotencyKey":"key-v2-run-created","occurredAt":"2026-08-25T00:00:01.000Z","payload":{"requiresHitl":false,"workflowRef":"workflow:durable@1"},"runId":"run-durable-001","seq":1,"type":"run.created","version":1}';
  const dispatchedJson = '{"actor":"worker","causationId":null,"correlationId":"correlation-durable-001","eventId":"event-v2-dispatch","expectedRevision":1,"idempotencyKey":"key-v2-dispatch","occurredAt":"2026-08-25T00:00:02.000Z","payload":{"attemptId":"attempt-1","nodeId":"build"},"runId":"run-durable-001","seq":2,"type":"node.dispatch.requested","version":1}';
  const database = new DatabaseSync(filename);
  database.exec('CREATE TABLE loopgraph_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL); INSERT INTO loopgraph_schema VALUES (1, 2); CREATE TABLE run_events (run_id TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, expected_revision INTEGER NOT NULL, canonical_json TEXT NOT NULL, PRIMARY KEY (run_id, seq), UNIQUE (run_id, idempotency_key), UNIQUE (event_id));');
  const insert = database.prepare('INSERT INTO run_events VALUES (?, ?, ?, ?, ?, ?)');
  insert.run(RUN_ID, 1, 'event-v2-run-created', 'key-v2-run-created', 0, createdJson);
  insert.run(RUN_ID, 2, 'event-v2-dispatch', 'key-v2-dispatch', 1, dispatchedJson);
  database.close();
  expectStorageError('CORRUPT_EVENT', () => new SqliteRunStore({ filename }));
  const unchanged = new DatabaseSync(filename); assert.equal(unchanged.prepare('SELECT version FROM loopgraph_schema').get().version, 2); unchanged.close();
});

test('v1 migration rejects malformed or duplicate event IDs without changing the old journal', async (t) => {
  const directory = await temporaryDirectory('loopgraph-v1-reject-'); t.after(() => rm(directory, { recursive: true, force: true })); const filename = join(directory, 'runs.sqlite');
  const database = new DatabaseSync(filename);
  database.exec('CREATE TABLE loopgraph_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL); INSERT INTO loopgraph_schema VALUES (1, 1); CREATE TABLE run_events (run_id TEXT NOT NULL, seq INTEGER NOT NULL, idempotency_key TEXT NOT NULL, expected_revision INTEGER NOT NULL, canonical_json TEXT NOT NULL, PRIMARY KEY (run_id, seq), UNIQUE (run_id, idempotency_key));');
  database.prepare('INSERT INTO run_events VALUES (?, ?, ?, ?, ?)').run(RUN_ID, 1, 'bad', 0, '{}'); database.close();
  expectStorageError('CORRUPT_EVENT', () => new SqliteRunStore({ filename }));
  const unchanged = new DatabaseSync(filename); assert.equal(unchanged.prepare('SELECT version FROM loopgraph_schema').get().version, 1); assert.equal(unchanged.prepare('PRAGMA table_info(run_events)').all().some((column) => column.name === 'event_id'), false); unchanged.close();

  const duplicateFilename = join(directory, 'duplicate.sqlite'); const duplicate = new DatabaseSync(duplicateFilename);
  duplicate.exec('CREATE TABLE loopgraph_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL); INSERT INTO loopgraph_schema VALUES (1, 1); CREATE TABLE run_events (run_id TEXT NOT NULL, seq INTEGER NOT NULL, idempotency_key TEXT NOT NULL, expected_revision INTEGER NOT NULL, canonical_json TEXT NOT NULL, PRIMARY KEY (run_id, seq), UNIQUE (run_id, idempotency_key));');
  const one = '{"actor":"worker","causationId":null,"correlationId":"correlation-durable-001","eventId":"duplicate-event","expectedRevision":0,"idempotencyKey":"key-1-run.created","occurredAt":"2026-08-25T00:00:01.000Z","payload":{"requiresHitl":false,"workflowRef":"workflow:durable@1"},"runId":"run-durable-001","seq":1,"type":"run.created","version":1}';
  const two = one.replace('key-1-run.created', 'key-1-other').replace('run-durable-001', 'run-durable-002').replace('event-run-durable-001-1-run.created', 'duplicate-event');
  duplicate.prepare('INSERT INTO run_events VALUES (?, ?, ?, ?, ?)').run(RUN_ID, 1, 'key-1-run.created', 0, one); duplicate.prepare('INSERT INTO run_events VALUES (?, ?, ?, ?, ?)').run('run-durable-002', 1, 'key-1-other', 0, two); duplicate.close();
  expectStorageError('CORRUPT_EVENT', () => new SqliteRunStore({ filename: duplicateFilename }));
});

test('recovery verifies a head checkpoint, durably pauses uncertainty, and is idempotent', async (t) => {
  const directory = await temporaryDirectory('loopgraph-recover-'); t.after(() => rm(directory, { recursive: true, force: true })); const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => 1_000 }); t.after(() => store.close());
  const lease = store.acquire(RUN_ID, 'worker-a', 100); assert.ok(lease); store.append(created(), lease); store.append(dispatched(), lease); store.saveCheckpoint({ runId: RUN_ID, revision: 2, view: reduce(store.read(RUN_ID)) });
  const recovery = new RecoveryCoordinator(store); const result = recovery.recover({ runId: RUN_ID, lease, actor: 'recovery-worker', eventId: 'recovery-event-1', occurredAt: '2026-08-25T00:01:00.000Z', reasonCode: 'RECOVERY_UNCERTAIN' }); assert.equal(result.checkpointUsed, true); assert.equal(result.recovered, true); assert.equal(result.view.phase, 'PAUSED_RECOVERED');
});

test('fault-injection matrix pauses every uncertain external boundary without auto-dispatch', async (t) => {
  const candidate = loopSpec(2);
  const fixtures = [
    {
      name: 'admission-hitl', expectedResumePhase: 'READY',
      events: [created({ payload: { loopSpec: loopSpec(1), requiresHitl: true } }), event(2, 'admission.hitl.requested', { requestId: 'admission-2', reasonCode: 'DSH_PROMOTION' })],
    },
    { name: 'dispatch-requested', expectedResumePhase: 'READY', events: [created(), dispatched()] },
    { name: 'execution-started', expectedResumePhase: 'READY', events: [created(), dispatched(), event(3, 'node.started', { nodeId: 'build', attemptId: 'attempt-1' })] },
    {
      name: 'validator-pending', expectedResumePhase: 'READY',
      events: [created(), dispatched(), event(3, 'node.started', { nodeId: 'build', attemptId: 'attempt-1' }), event(4, 'node.settled', { nodeId: 'build', attemptId: 'attempt-1', outcome: 'SUCCEEDED', outcomeCode: 'SETTLED' })],
    },
    {
      name: 'pause-requested-during-execution', expectedResumePhase: 'READY',
      events: [created(), dispatched(), event(3, 'node.started', { nodeId: 'build', attemptId: 'attempt-1' }), event(4, 'pause.requested', { reasonCode: 'OPERATOR_PAUSE' })],
    },
    {
      name: 'promotion-hitl-pending', expectedResumePhase: 'VALIDATED',
      events: [
        created({ payload: { loopSpec: loopSpec(1), requiresHitl: true } }),
        dispatched(), event(3, 'node.started', { nodeId: 'build', attemptId: 'attempt-1' }),
        event(4, 'node.settled', { nodeId: 'build', attemptId: 'attempt-1', outcome: 'SUCCEEDED', outcomeCode: 'SETTLED' }),
        event(5, 'validation.recorded', { nodeId: 'build', attemptId: 'attempt-1', validationId: 'validation-5', passed: true, evidenceRef: 'evidence:5' }),
        event(6, 'generation.prepared', { generationId: 'generation-1', validationId: 'validation-5', manifestRef: 'manifest:1', candidateLoopSpec: candidate }),
        event(7, 'hitl.requested', { requestId: 'hitl-7', promptRef: 'prompt:1', generationId: 'generation-1', specId: candidate.specId, specRevision: candidate.revision, validationId: 'validation-5', candidateLoopSpec: candidate }),
      ],
    },
  ];

  for (const fixture of fixtures) {
    const directory = await temporaryDirectory(`loopgraph-crash-${fixture.name}-`);
    t.after(() => rm(directory, { recursive: true, force: true }));
    const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => 1_000 });
    t.after(() => store.close());
    const lease = store.acquire(RUN_ID, 'worker-a', 100); assert.ok(lease);
    for (const item of fixture.events) store.append(item, lease);
    store.saveCheckpoint({ runId: RUN_ID, revision: fixture.events.length, view: reduce(store.read(RUN_ID)) });

    const recovery = new RecoveryCoordinator(store);
    const first = recovery.recover({
      runId: RUN_ID, lease, actor: 'recovery-worker', eventId: `recovery-${fixture.name}`,
      occurredAt: '2026-08-25T00:01:00.000Z', reasonCode: `CRASH_${fixture.name.toUpperCase().replaceAll('-', '_')}`,
    });
    assert.equal(first.checkpointUsed, true, `${fixture.name} replay must validate the checkpoint`);
    assert.equal(first.recovered, true, `${fixture.name} must append one recovery fact`);
    assert.equal(first.view.phase, 'PAUSED_RECOVERED', `${fixture.name} must fail closed`);
    assert.equal(first.view.activeAttempt, null, `${fixture.name} cannot retain a live callback`);
    assert.equal(first.view.hitl.status, 'NOT_REQUESTED', `${fixture.name} cannot retain a pending promotion authority`);
    assert.equal(store.read(RUN_ID).filter(({ type }) => type === 'node.dispatch.requested').length,
      fixture.events.filter(({ type }) => type === 'node.dispatch.requested').length,
      `${fixture.name} recovery must never auto-dispatch`);

    const second = recovery.recover({
      runId: RUN_ID, lease, actor: 'recovery-worker', eventId: `recovery-repeat-${fixture.name}`,
      occurredAt: '2026-08-25T00:01:01.000Z', reasonCode: 'REPEAT_RECOVERY',
    });
    assert.equal(second.recovered, false, `${fixture.name} recovery is idempotent`);
    const resumed = event(store.read(RUN_ID).length + 1, 'run.resumed', { reasonCode: 'OPERATOR_RESUME' });
    store.append(resumed, lease);
    assert.equal(reduce(store.read(RUN_ID)).phase, fixture.expectedResumePhase, `${fixture.name} needs an explicit resume`);
  }
});

// Filesystem boundary, durability and limits.
test('artifact bytes and manifests fail closed on blob tampering and manifest-only publication', async (t) => {
  const directory = await temporaryDirectory('loopgraph-artifacts-'); t.after(() => rm(directory, { recursive: true, force: true })); const artifacts = new FileArtifactStore(directory);
  const reference = await artifacts.put(new TextEncoder().encode('trusted bytes')); assert.equal(reference.digest, createHash('sha256').update('trusted bytes').digest('hex')); assert.equal(new TextDecoder().decode(await artifacts.get(reference)), 'trusted bytes');
  await writeFile(join(directory, 'blobs', reference.digest), 'tampered'); await assert.rejects(() => artifacts.get(reference), /artifact integrity/i);
  await assert.rejects(() => artifacts.publishGeneration({ generationId: 'generation-partial', createdAt: '2026-08-25T00:00:00.000Z', artifacts: [{ name: 'missing.txt', digest: '0'.repeat(64), byteSize: 1 }] }), /artifact integrity/i);
  await assert.rejects(() => artifacts.readManifest('generation-partial'), /manifest/i);
});

test('artifact root components reject symlink escapes and missing maps to NOT_FOUND', async (t) => {
  const directory = await temporaryDirectory('loopgraph-symlink-'); t.after(() => rm(directory, { recursive: true, force: true })); const outside = await temporaryDirectory('loopgraph-outside-'); t.after(() => rm(outside, { recursive: true, force: true }));
  await symlink(outside, join(directory, 'blobs')); expectStorageError('UNSAFE_FILESYSTEM', () => new FileArtifactStore(directory)); await rm(join(directory, 'blobs'));
  await symlink(outside, join(directory, 'manifests')); expectStorageError('UNSAFE_FILESYSTEM', () => new FileArtifactStore(directory)); await rm(join(directory, 'manifests'));
  const artifacts = new FileArtifactStore(directory); await assert.rejects(() => artifacts.get({ digest: '0'.repeat(64), byteSize: 0 }), (error) => error instanceof StorageError && error.code === 'NOT_FOUND');
});

test('artifact initialization conservatively cleans only stale own temporary files', async (t) => {
  const directory = await temporaryDirectory('loopgraph-temp-cleanup-'); t.after(() => rm(directory, { recursive: true, force: true })); await mkdir(join(directory, 'blobs'), { recursive: true }); await mkdir(join(directory, 'manifests'), { recursive: true });
  const stale = join(directory, 'blobs', '.blob-11111111-1111-1111-1111-111111111111.tmp'); const fresh = join(directory, 'blobs', '.blob-22222222-2222-2222-2222-222222222222.tmp'); await writeFile(stale, 'old'); await writeFile(fresh, 'new'); await utimes(stale, 1, 1);
  const artifacts = new FileArtifactStore(directory); await artifacts.put(new TextEncoder().encode('trigger initialization')); await assert.rejects(() => access(stale)); await access(fresh);
});

test('artifact and manifest configured bounds reject oversized data before publication', async (t) => {
  const directory = await temporaryDirectory('loopgraph-artifact-limits-'); t.after(() => rm(directory, { recursive: true, force: true })); const artifacts = new FileArtifactStore(directory, { maxArtifactBytes: 3, maxArtifactCount: 1, maxManifestBytes: 128 });
  await assert.rejects(() => artifacts.put(new Uint8Array(4)), (error) => error instanceof StorageError && error.code === 'INVALID_ARGUMENT'); const one = await artifacts.put(new Uint8Array([1]));
  await assert.rejects(() => artifacts.publishGeneration({ generationId: 'limit', createdAt: '2026-08-25T00:00:00.000Z', artifacts: [{ name: 'one', ...one }, { name: 'two', digest: '1'.repeat(64), byteSize: 1 }] }), (error) => error instanceof StorageError && error.code === 'INVALID_ARGUMENT');
});

test('event byte and depth bounds reject input without persistence', async (t) => {
  const directory = await temporaryDirectory('loopgraph-event-limits-'); t.after(() => rm(directory, { recursive: true, force: true })); const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => 1_000, maxEventBytes: 200, maxEventDepth: 4 }); t.after(() => store.close()); const lease = store.acquire(RUN_ID, 'worker-a', 100); assert.ok(lease);
  expectStorageError('INVALID_ARGUMENT', () => store.append(created({ payload: { loopSpec: { ...loopSpec(1), specId: `x${'x'.repeat(500)}` }, requiresHitl: false } }), lease));
  expectStorageError('INVALID_ARGUMENT', () => store.append(
    created({
      payload: {
        loopSpec: loopSpec(1), requiresHitl: false,
        deep: { a: { b: { c: { d: true } } } },
      },
    }),
    lease,
  ));
  assert.deepEqual(store.read(RUN_ID), []);
});

test('artifact manifests reject traversal and duplicate name or hash anomalies', async (t) => {
  const directory = await temporaryDirectory('loopgraph-artifact-names-'); t.after(() => rm(directory, { recursive: true, force: true })); const artifacts = new FileArtifactStore(directory); const reference = await artifacts.put(new TextEncoder().encode('same bytes')); const base = { generationId: 'generation-safe', createdAt: '2026-08-25T00:00:00.000Z' };
  await assert.rejects(() => artifacts.publishGeneration({ ...base, artifacts: [{ name: '../escape', ...reference }] }), /safe artifact name/i);
  await assert.rejects(() => artifacts.publishGeneration({ ...base, artifacts: [{ name: 'one.txt', ...reference }, { name: 'one.txt', ...reference }] }), /duplicate artifact name/i);
  await assert.rejects(() => artifacts.publishGeneration({ ...base, artifacts: [{ name: 'one.txt', ...reference }, { name: 'two.txt', ...reference }] }), /duplicate artifact digest/i);
});

test('SQLite independently enforces unique run sequence, idempotency keys, and global event IDs', async (t) => {
  const directory = await temporaryDirectory('loopgraph-constraints-'); t.after(() => rm(directory, { recursive: true, force: true })); const filename = join(directory, 'runs.sqlite'); const store = new SqliteRunStore({ filename }); store.close(); const database = new DatabaseSync(filename); t.after(() => database.close()); const insert = database.prepare('INSERT INTO run_events (run_id, seq, event_id, idempotency_key, expected_revision, canonical_json) VALUES (?, ?, ?, ?, ?, ?)');
  insert.run(RUN_ID, 1, 'direct-event-1', 'direct-key', 0, '{}'); assert.throws(() => insert.run(RUN_ID, 1, 'direct-event-2', 'different-key', 0, '{}'), /UNIQUE constraint failed: run_events\.run_id, run_events\.seq/i); assert.throws(() => insert.run('another-run', 1, 'direct-event-1', 'another-key', 0, '{}'), /UNIQUE constraint failed: run_events\.event_id/i);
});

test('append rejects globally duplicated event IDs atomically', async (t) => {
  const directory = await temporaryDirectory('loopgraph-event-id-collision-'); t.after(() => rm(directory, { recursive: true, force: true })); const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => 1_000 }); t.after(() => store.close()); const firstLease = store.acquire(RUN_ID, 'worker-a', 100); const secondRunId = 'run-durable-002'; const secondLease = store.acquire(secondRunId, 'worker-b', 100); assert.ok(firstLease); assert.ok(secondLease); const eventId = 'globally-unique-event-id'; store.append(created({ eventId }), firstLease); expectStorageError('EVENT_ID_COLLISION', () => store.append(created({ runId: secondRunId, eventId }), secondLease)); assert.deepEqual(store.read(secondRunId), []);
});

test('recovery rejects an expired lease and pauses unsafe work under an active lease', async (t) => {
  const directory = await temporaryDirectory('loopgraph-expired-'); t.after(() => rm(directory, { recursive: true, force: true })); let now = 1_000; const store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => now }); t.after(() => store.close()); const lease = store.acquire(RUN_ID, 'worker-a', 10); assert.ok(lease); store.append(created(), lease); store.append(dispatched(), lease); now += 11; expectStorageError('FENCED', () => store.append(event(3, 'node.started', { nodeId: 'build', attemptId: 'attempt-1' }), lease));
});
