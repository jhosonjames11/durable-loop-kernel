#!/usr/bin/env node
import { createHash, randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  EVENT_VERSION,
  type LeaseGrant,
  type RunEvent,
  type RunEventType,
  type RunView,
  type LoopSpec,
  reduce,
} from '@loopgraph/core';
import { SqliteRunStore, StorageError } from '@loopgraph/storage';

type Command = 'inspect' | 'pause' | 'resume' | 'promote' | 'rollback' | 'demo';
type Arguments = Record<string, string>;
type ResultCode =
  | 'OK'
  | 'USAGE'
  | 'DATABASE_NOT_FOUND'
  | 'RUN_NOT_FOUND'
  | 'REVISION_CONFLICT'
  | 'LEASE_UNAVAILABLE'
  | 'INVALID_TRANSITION'
  | 'PROMOTION_NOT_AUTHORIZED'
  | 'ROLLBACK_TARGET_INVALID'
  | 'STORAGE_ERROR'
  | 'INTERNAL_ERROR';

interface CommandResult {
  readonly ok: boolean;
  readonly command: string;
  readonly code: ResultCode;
  readonly runId?: string;
  readonly revision?: number;
  readonly phase?: string;
  readonly run?: object;
  readonly report?: object;
}

const commands = new Set<Command>(['inspect', 'pause', 'resume', 'promote', 'rollback', 'demo']);
const mutableCommands = new Set<Command>(['pause', 'resume', 'promote', 'rollback']);
const identifierPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
/** Maximum number of newest entries returned for each inspect history. */
const INSPECT_HISTORY_LIMIT = 20;
/** Maximum Unicode code points returned for an inspect identifier or code. */
const INSPECT_TEXT_LIMIT = 256;
const leaseTtlMs = 30_000;

function demoLoopSpec(revision: number): LoopSpec {
  return {
    specId: 'cli-build-loop', revision, entryNodeId: 'build',
    nodes: [{ nodeId: 'build', kind: 'agent' }],
    edges: [{ fromNodeId: 'build', toNodeId: 'build' }],
  };
}

function write(result: CommandResult): void {
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

function failure(command: string, code: Exclude<ResultCode, 'OK'>): CommandResult {
  return { ok: false, command, code };
}

function parse(argv: readonly string[]): { command: Command | null; args: Arguments; valid: boolean } {
  const [candidate, ...rest] = argv;
  if (candidate === undefined || !commands.has(candidate as Command)) return { command: null, args: {}, valid: false };
  const args: Arguments = {};
  for (let index = 0; index < rest.length; index += 2) {
    const key = rest[index];
    const value = rest[index + 1];
    if (key === undefined || value === undefined || !key.startsWith('--') || value.startsWith('--') || args[key] !== undefined) {
      return { command: candidate as Command, args: {}, valid: false };
    }
    args[key] = value;
  }
  return { command: candidate as Command, args, valid: true };
}

function validIdentifier(value: string | undefined): value is string {
  return value !== undefined && identifierPattern.test(value);
}

function expectedRevision(value: string | undefined): number | null {
  if (value === undefined || !/^\d+$/u.test(value)) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function holderFor(actor: string): string {
  return `cli:${createHash('sha256').update(actor).digest('hex')}`;
}

function currentView(store: SqliteRunStore, runId: string): RunView | null {
  const events = store.read(runId);
  return events.length === 0 ? null : reduce(events);
}

interface EventMetadata {
  readonly eventId: string;
  readonly occurredAt: string;
  readonly correlationId: string;
  readonly idempotencyKey: string;
}

function append(
  store: SqliteRunStore,
  lease: LeaseGrant,
  actor: string,
  runId: string,
  expected: number,
  type: RunEventType,
  payload: object,
  metadata?: EventMetadata,
): void {
  const event = {
    version: EVENT_VERSION,
    eventId: metadata?.eventId ?? randomUUID(),
    runId,
    seq: expected + 1,
    occurredAt: metadata?.occurredAt ?? new Date().toISOString(),
    actor,
    causationId: null,
    correlationId: metadata?.correlationId ?? `cli:${randomUUID()}`,
    idempotencyKey: metadata?.idempotencyKey ?? `cli:${randomUUID()}`,
    expectedRevision: expected,
    type,
    payload,
  } as RunEvent;
  store.append(event, lease);
}

function inspectText(value: string): string {
  let count = 0;
  let end = 0;
  let truncationEnd = 0;
  for (const character of value) {
    if (count === INSPECT_TEXT_LIMIT) return `${value.slice(0, truncationEnd)}…`;
    count += 1;
    end += character.length;
    if (count === INSPECT_TEXT_LIMIT - 1) truncationEnd = end;
  }
  return value;
}

function inspectNullableText(value: string | null): string | null {
  return value === null ? null : inspectText(value);
}

function inspectHistory<T, U>(entries: readonly T[], project: (entry: T) => U): { items: U[]; total: number; truncated: boolean } {
  const total = entries.length;
  return {
    items: entries.slice(-INSPECT_HISTORY_LIMIT).map(project),
    total,
    truncated: total > INSPECT_HISTORY_LIMIT,
  };
}

function inspectProjection(view: RunView, lease: LeaseGrant | null, events: readonly RunEvent[]): object {
  return {
    id: inspectText(view.runId),
    revision: view.revision,
    phase: inspectText(view.phase),
    activeLoopSpec: {
      specId: inspectText(view.loopSpec.specId),
      revision: view.loopSpec.revision,
      entryNodeId: inspectText(view.loopSpec.entryNodeId),
      nodes: view.loopSpec.nodes.map(({ nodeId, kind }) => ({ nodeId: inspectText(nodeId), kind: inspectText(kind) })),
      edges: view.loopSpec.edges.map(({ fromNodeId, toNodeId }) => ({ fromNodeId: inspectText(fromNodeId), toNodeId: inspectText(toNodeId) })),
      nextNodeId: inspectNullableText(view.nextNodeId),
    },
    lease: lease === null ? null : {
      holder: inspectText(lease.holderId),
      token: lease.fencingToken,
      expiresAt: lease.expiresAt,
    },
    activeNode: view.activeAttempt === null ? null : {
      nodeId: inspectText(view.activeAttempt.nodeId),
      attemptId: inspectText(view.activeAttempt.attemptId),
      status: inspectText(view.activeAttempt.status),
    },
    validation: {
      status: inspectText(view.validation.status),
      nodeId: inspectNullableText(view.validation.nodeId),
      attemptId: inspectNullableText(view.validation.attemptId),
      validationId: inspectNullableText(view.validation.validationId),
    },
    hitl: {
      required: view.requiresHitl,
      status: inspectText(view.hitl.status),
      requestId: inspectNullableText(view.hitl.requestId),
      generationId: inspectNullableText(view.hitl.generationId),
      specId: inspectNullableText(view.hitl.specId),
      specRevision: view.hitl.specRevision,
      validationId: inspectNullableText(view.hitl.validationId),
      approvalSubject: inspectNullableText(view.hitl.approvalSubject),
    },
    admissionHitl: {
      status: inspectText(view.admissionHitl.status),
      requestId: inspectNullableText(view.admissionHitl.requestId),
    },
    activeGeneration: inspectNullableText(view.activeGenerationId),
    preparedGenerations: inspectHistory(view.preparedGenerations, ({ generationId, validationId, preparedAtRevision }) => ({
      generationId: inspectText(generationId),
      validationId: inspectText(validationId),
      preparedAtRevision,
    })),
    promotedGenerations: inspectHistory(view.promotedGenerations, ({ generationId, promotedAtRevision }) => ({
      generationId: inspectText(generationId),
      promotedAtRevision,
    })),
    rollbackAncestry: inspectHistory(view.rollbackAncestry, ({ rollbackSequence, fromGenerationId, targetGenerationId }) => ({
      rollbackSequence,
      fromGenerationId: inspectNullableText(fromGenerationId),
      targetGenerationId: inspectText(targetGenerationId),
    })),
    timeline: inspectHistory(events, ({ type, seq, occurredAt, actor, correlationId }) => ({
      type: inspectText(type),
      seq,
      time: inspectText(occurredAt),
      actor: inspectText(actor),
      correlation: inspectNullableText(correlationId),
    })),
  };
}

function mapError(command: string, error: unknown): CommandResult {
  if (error instanceof StorageError) {
    if (error.code === 'STALE_REVISION') return failure(command, 'REVISION_CONFLICT');
    if (error.code === 'FENCED') return failure(command, 'LEASE_UNAVAILABLE');
    return failure(command, 'STORAGE_ERROR');
  }
  return failure(command, 'INTERNAL_ERROR');
}

function inspect(args: Arguments): CommandResult {
  const db = args['--db'];
  const runId = args['--run'];
  if (Object.keys(args).length !== 2 || db === undefined || !validIdentifier(runId)) return failure('inspect', 'USAGE');
  if (!existsSync(db)) return failure('inspect', 'DATABASE_NOT_FOUND');
  let store: SqliteRunStore | null = null;
  try {
    store = new SqliteRunStore({ filename: db });
    const events = store.read(runId);
    if (events.length === 0) return failure('inspect', 'RUN_NOT_FOUND');
    const view = reduce(events);
    return { ok: true, command: 'inspect', code: 'OK', runId, revision: view.revision, phase: view.phase, run: inspectProjection(view, store.readLease(runId), events) };
  } catch (error) {
    return mapError('inspect', error);
  } finally {
    store?.close();
  }
}

function mutate(command: Exclude<Command, 'inspect' | 'demo'>, args: Arguments): CommandResult {
  const allowed = command === 'promote' || command === 'rollback'
    ? new Set(['--db', '--run', '--actor', '--expected-revision', '--generation'])
    : new Set(['--db', '--run', '--actor', '--expected-revision']);
  if (Object.keys(args).some((key) => !allowed.has(key)) || [...allowed].some((key) => args[key] === undefined)
    || args['--db'] === undefined || !validIdentifier(args['--run']) || !validIdentifier(args['--actor'])) {
    return failure(command, 'USAGE');
  }
  const revision = expectedRevision(args['--expected-revision']);
  const runId = args['--run'];
  const actor = args['--actor'];
  const generation = args['--generation'];
  if (revision === null || ((command === 'promote' || command === 'rollback') && !validIdentifier(generation))) {
    return failure(command, 'USAGE');
  }

  let store: SqliteRunStore | null = null;
  let lease: LeaseGrant | null = null;
  try {
    store = new SqliteRunStore({ filename: args['--db'] });
    const before = currentView(store, runId);
    if (before === null) return failure(command, 'RUN_NOT_FOUND');
    if (before.revision !== revision) return failure(command, 'REVISION_CONFLICT');
    lease = store.acquire(runId, holderFor(actor), leaseTtlMs);
    if (lease === null) return failure(command, 'LEASE_UNAVAILABLE');
    const view = currentView(store, runId);
    if (view === null || view.revision !== revision) return failure(command, 'REVISION_CONFLICT');

    if (command === 'pause') {
      if (!['READY', 'AWAITING_ADMISSION_HITL', 'RUNNING', 'VALIDATING', 'VALIDATED'].includes(view.phase) || view.pauseRequested) {
        return failure(command, 'INVALID_TRANSITION');
      }
      append(store, lease, actor, runId, revision, 'pause.requested', { reasonCode: 'OPERATOR_PAUSE' });
      const requested = currentView(store, runId);
      if (requested !== null && requested.activeAttempt === null) {
        append(store, lease, actor, runId, requested.revision, 'run.paused', { reasonCode: 'OPERATOR_PAUSE' });
      }
    } else if (command === 'resume') {
      if (view.phase !== 'PAUSED' && view.phase !== 'PAUSED_RECOVERED') return failure(command, 'INVALID_TRANSITION');
      append(store, lease, actor, runId, revision, 'run.resumed', { reasonCode: 'OPERATOR_RESUME' });
    } else if (command === 'promote') {
      const prepared = view.preparedGenerations.find(({ generationId }) => generationId === generation);
      const hitlAllowed = view.requiresHitl
        ? view.hitl.status === 'APPROVED'
        : view.hitl.status === 'NOT_REQUESTED' || view.hitl.status === 'APPROVED';
      if (view.phase !== 'VALIDATED' || view.validation.status !== 'PASSED' || view.validation.validationId === null
        || prepared === undefined || prepared.validationId !== view.validation.validationId || !hitlAllowed) {
        return failure(command, 'PROMOTION_NOT_AUTHORIZED');
      }
      append(store, lease, actor, runId, revision, 'generation.promoted', { generationId: generation });
    } else {
      const priorPromotion = view.promotedGenerations.some(({ generationId }) => generationId === generation)
        && view.activeGenerationId !== generation;
      if (view.phase !== 'READY' || view.activeAttempt !== null || !priorPromotion) return failure(command, 'ROLLBACK_TARGET_INVALID');
      append(store, lease, actor, runId, revision, 'rollback.applied', {
        targetGenerationId: generation,
        reasonCode: 'OPERATOR_ROLLBACK',
      });
    }
    const after = currentView(store, runId);
    if (after === null) return failure(command, 'STORAGE_ERROR');
    return { ok: true, command, code: 'OK', runId, revision: after.revision, phase: after.phase };
  } catch (error) {
    return mapError(command, error);
  } finally {
    if (store !== null && lease !== null) store.release(lease);
    store?.close();
  }
}

async function demo(): Promise<CommandResult> {
  const directory = await mkdtemp(join(tmpdir(), 'loopgraph-cli-'));
  let store: SqliteRunStore | null = null;
  try {
    store = new SqliteRunStore({ filename: join(directory, 'runs.sqlite'), clock: () => 1_000 });
    const runId = 'cli-demo-run';
    const actor = 'cli-demo';
    const lease = store.acquire(runId, holderFor(actor), leaseTtlMs);
    if (lease === null) return failure('demo', 'LEASE_UNAVAILABLE');
    let eventNumber = 0;
    const put = (type: RunEventType, payload: object): void => {
      const events = store?.read(runId) ?? [];
      eventNumber += 1;
      append(store as SqliteRunStore, lease, actor, runId, events.length, type, payload, {
        eventId: `demo-event-${eventNumber}`,
        occurredAt: `2026-08-25T00:00:${String(eventNumber).padStart(2, '0')}.000Z`,
        correlationId: 'demo-correlation',
        idempotencyKey: `demo-key-${eventNumber}`,
      });
    };
    put('run.created', { loopSpec: demoLoopSpec(1), requiresHitl: true });
    put('node.dispatch.requested', { nodeId: 'build', attemptId: 'attempt-1' });
    put('node.started', { nodeId: 'build', attemptId: 'attempt-1' });
    put('node.settled', { nodeId: 'build', attemptId: 'attempt-1', outcome: 'SUCCEEDED', outcomeCode: 'DEMO_OK' });
    put('validation.recorded', { nodeId: 'build', attemptId: 'attempt-1', validationId: 'validation-1', passed: true, evidenceRef: 'evidence:demo' });
    put('generation.prepared', { generationId: 'generation-v1', validationId: 'validation-1', manifestRef: 'manifest:generation-v1', candidateLoopSpec: demoLoopSpec(2) });
    put('hitl.requested', { requestId: 'approval-1', promptRef: 'prompt:generation-v1', generationId: 'generation-v1', specId: 'cli-build-loop', specRevision: 2, candidateLoopSpec: demoLoopSpec(2), validationId: 'validation-1' });
    put('hitl.decided', { requestId: 'approval-1', decision: 'APPROVED', decisionCode: 'DEMO_APPROVED', approvalSubject: 'demo-approver', approvalReceiptRef: 'receipt:demo-approval-1' });
    put('generation.promoted', { generationId: 'generation-v1' });
    put('node.dispatch.requested', { nodeId: 'build', attemptId: 'attempt-2' });
    put('node.started', { nodeId: 'build', attemptId: 'attempt-2' });
    put('node.settled', { nodeId: 'build', attemptId: 'attempt-2', outcome: 'SUCCEEDED', outcomeCode: 'DEMO_OK' });
    put('validation.recorded', { nodeId: 'build', attemptId: 'attempt-2', validationId: 'validation-2', passed: true, evidenceRef: 'evidence:demo' });
    put('generation.prepared', { generationId: 'generation-v2', validationId: 'validation-2', manifestRef: 'manifest:generation-v2', candidateLoopSpec: demoLoopSpec(3) });
    put('hitl.requested', { requestId: 'approval-2', promptRef: 'prompt:generation-v2', generationId: 'generation-v2', specId: 'cli-build-loop', specRevision: 3, candidateLoopSpec: demoLoopSpec(3), validationId: 'validation-2' });
    put('hitl.decided', { requestId: 'approval-2', decision: 'APPROVED', decisionCode: 'DEMO_APPROVED', approvalSubject: 'demo-approver', approvalReceiptRef: 'receipt:demo-approval-2' });
    put('generation.promoted', { generationId: 'generation-v2' });
    put('rollback.applied', { targetGenerationId: 'generation-v1', reasonCode: 'DEMO_ROLLBACK' });
    const view = currentView(store, runId);
    if (view === null) return failure('demo', 'STORAGE_ERROR');
    return {
      ok: true,
      command: 'demo',
      code: 'OK',
      runId,
      revision: view.revision,
      phase: view.phase,
      report: {
        activeGeneration: view.activeGenerationId,
        promotedGenerations: view.promotedGenerations.map(({ generationId }) => generationId),
        rollbackCount: view.rollbackAncestry.length,
        hitl: view.hitl.status,
        eventCount: store.read(runId).length,
        cleanedUp: true,
      },
    };
  } catch (error) {
    return mapError('demo', error);
  } finally {
    store?.close();
    await rm(directory, { recursive: true, force: true });
  }
}

export async function main(argv: readonly string[] = process.argv.slice(2)): Promise<CommandResult> {
  const parsed = parse(argv);
  if (!parsed.valid || parsed.command === null) return failure(parsed.command ?? 'loopgraph', 'USAGE');
  if (parsed.command === 'demo') return Object.keys(parsed.args).length === 0 ? demo() : failure('demo', 'USAGE');
  if (parsed.command === 'inspect') return inspect(parsed.args);
  return mutableCommands.has(parsed.command) ? mutate(parsed.command, parsed.args) : failure(parsed.command, 'USAGE');
}

const result = await main();
write(result);
if (!result.ok) process.exitCode = 1;
