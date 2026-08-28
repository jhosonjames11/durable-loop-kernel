# LoopGraph Supervisor Implementation Plan

> **For Hermes:** Execute this plan task-by-task with isolated implementation and independent review.

**Goal:** Build a DSH-first, harness-neutral, durable LoopGraph Supervisor that makes agent execution, validation, iteration, HITL, pause/resume, promotion, and rollback observable and recoverable.

**Architecture:** `packages/core` is a pure event-sourced state machine with dependency-inverted ports. `packages/storage` provides SQLite append/CAS, immutable checkpoints, artifact manifests, and leases. `packages/dsh-adapter` is a Cordis plugin that maps documented DSH services/events to core ports. A mock adapter proves harness neutrality before the DSH integration test proves the DSH path.

**Tech stack:** Node 22, TypeScript 6, Node built-in test runner or Vitest, SQLite, Cordis/DSH only in the adapter package.

---

## Acceptance contract

1. Every externally visible decision is represented by a versioned immutable event with event id, run id, sequence, wall-clock time, actor, causation id, correlation id, and idempotency key.
2. `reduce(events)` is deterministic and rejects illegal transition streams, stale expected revisions, conflicting idempotency keys, and promotion without passed validation evidence.
3. Restart recovery rebuilds state from the event log; it never assumes an in-flight execution attempt succeeded. A recovered active attempt becomes `PAUSED_RECOVERED` with an explicit uncertainty event.
4. Only a valid run lease holder can dispatch execution commands. Leases are fenced by monotonic tokens, so a stale worker cannot append after takeover.
5. HITL gates are explicit and fail closed when decision delivery is unavailable, expired, cancelled, or rejected.
6. Promotion commits a generation manifest only after all named artifacts are immutable and hash-verified. Rollback appends a compensating event pointing at a prior promoted generation.
7. Core has no DSH/Cordis imports. The DSH adapter uses public `ctx.agents`, `agent/*`, `ctx.approval`, and session facts; no `dsh-agent-loop` import.

## Task 1: Bootstrap workspace and enforce dependency direction

**Files:**
- Create: `package.json`, `tsconfig.base.json`, `pnpm-workspace.yaml`
- Create: `packages/core/package.json`, `packages/storage/package.json`, `packages/dsh-adapter/package.json`, `packages/demo/package.json`
- Create: `packages/core/src/index.ts`, `packages/core/tests/no-dsh-imports.test.mjs`

**Steps:**
1. Add workspace scripts for `typecheck`, `test`, `lint`, and a focused demo test.
2. Write a test/CI check that scans `packages/core/src` and rejects imports matching `deepseek`, `dsh`, or `cordis`.
3. Make the check pass with an empty core entrypoint.
4. Verify with the package manager detected on the host; do not claim a dependency installation succeeded without executing it.

## Task 2: Write the pure event protocol and reducer with TDD

**Files:**
- Create: `packages/core/src/events.ts`
- Create: `packages/core/src/model.ts`
- Create: `packages/core/src/reducer.ts`
- Create: `packages/core/src/errors.ts`
- Create: `packages/core/tests/reducer.test.mjs`

**Steps:**
1. Write failing cases for: create run; start node; pause at boundary; record execution result; attach validation evidence; request/decide HITL; promote; rollback; invalid transition; duplicate idempotency key; stale revision.
2. Define discriminated event types including `RunCreated`, `LeaseAcquired`, `NodeDispatchRequested`, `NodeStarted`, `NodeSettled`, `ValidationRecorded`, `PauseRequested`, `RunPaused`, `HitlRequested`, `HitlDecided`, `GenerationPrepared`, `GenerationPromoted`, `RollbackApplied`, and `RecoveryUncertain`.
3. Implement a pure reducer that returns `RunView` and deterministic domain errors. Do not perform I/O or call adapters from reducer functions.
4. Add property-style sequence tests: all accepted streams replay to the same view; every rejected stream names the violating event sequence.
5. Run focused tests and typecheck.

## Task 3: Add persistence, checkpoint, artifact, and lease contracts

**Files:**
- Create: `packages/core/src/ports.ts`
- Create: `packages/storage/src/sqlite-event-store.ts`
- Create: `packages/storage/src/sqlite-lease.ts`
- Create: `packages/storage/src/artifact-store.ts`
- Create: `packages/storage/src/recovery.ts`
- Create: `packages/storage/tests/recovery.test.mjs`
- Create: `packages/storage/tests/lease-fencing.test.mjs`

**Steps:**
1. Write the first failing recovery test: append run events, simulate restart, reload, and compare the rehydrated `RunView` exactly.
2. Persist append-only events in SQLite with `(run_id, seq)` uniqueness, immutable event ids, expected-revision CAS, and idempotency uniqueness per run.
3. Persist checkpoints only after event append; an event is the authority and a checkpoint is a rebuild optimization.
4. Implement lease acquire/renew/release with an increasing fencing token; test that a previous holder cannot append after a lease takeover.
5. Implement content-addressed artifacts and a generation manifest. Test partial promotion: before manifest commit readers reject the generation; after commit all hashes/sizes verify.
6. On startup, turn a persisted nonterminal `RUNNING`/`VALIDATING` state into an explicit `RecoveryUncertain` + `PAUSED_RECOVERED` sequence under a lease; never silently re-run.
7. Run storage tests against a temporary database and test crash/torn-write handling.

## Task 4: Introduce neutral execution/validation/HITL ports and mock E2E

**Files:**
- Create: `packages/core/src/supervisor.ts`
- Create: `packages/demo/src/mock-harness.ts`
- Create: `packages/demo/tests/loopgraph-e2e.test.mjs`

**Steps:**
1. Define `ExecutionAdapter` with stable attempt id, dispatch, cancellation-at-boundary, and observation callbacks; define `ValidationAdapter` and `HumanGateAdapter` separately.
2. Write a failing end-to-end scenario: execution fails validation, creates a retry iteration, then blocks on HITL, receives approval, and promotes a verified artifact generation.
3. Implement command orchestration around the reducer and ports. Every adapter callback carries the run/node/attempt correlation ids and becomes a journal event.
4. Add a crash recovery scenario that stops after an execution dispatch but before settlement; verify it recovers as paused/uncertain and only resumes after an explicit command.
5. Add a rollback scenario that promotes V1, promotes V2, and rolls back to V1 without deleting V2 history.

## Task 5: Implement the DSH adapter as a real Cordis plugin

**Files:**
- Create: `packages/dsh-adapter/src/index.ts`
- Create: `packages/dsh-adapter/src/dsh-execution-adapter.ts`
- Create: `packages/dsh-adapter/src/dsh-hitl-adapter.ts`
- Create: `packages/dsh-adapter/src/session-correlation.ts`
- Create: `packages/dsh-adapter/tests/real-composition.test.mjs`
- Create: `examples/dsh-loopgraph/cordis.yml`

**Steps:**
1. Add a failing real-composition test that boots a minimal DSH/Cordis composition, mounts the adapter, and observes one complete supervisor run.
2. Use only public DSH `Agent` behavior and documented lifecycle points. Pause by rejecting/holding pre-step admission, not by serializing a live request stream.
3. Map DSH session/agent ids to immutable LoopGraph correlations. Store only safe diagnostic summaries in LoopGraph events; retain raw DSH transcript access under DSH session controls.
4. Implement HITL via `ctx.approval.request`; map `allowed-once` to approval, and `rejected`, `cancelled`, and `unavailable` to non-promotion outcomes. Ensure missing answerers fail closed.
5. Observe durable DSH facts for UI/operator links but keep LoopGraph events authoritative for supervisor transitions.
6. Verify plugin unload disposes its registrations and releases no live authority; add a teardown test.

## Task 6: Operator CLI and interview demo

**Files:**
- Create: `packages/cli/src/bin.ts`
- Create: `packages/cli/src/commands/{inspect,pause,resume,promote,rollback}.ts`
- Create: `docs/demo-script.md`
- Create: `docs/threat-model.md`

**Steps:**
1. Build `inspect` to print a run timeline, current revision/lease, blocked reason, validation evidence, HITL status, active generation, and rollback ancestry.
2. Require expected revision and explicit actor identity for mutating commands; report conflict rather than overwriting concurrent changes.
3. Create a five-minute demo: execute → validation failure → loop → HITL → promotion → simulated crash/recovery → explicit resume → rollback.
4. Add a threat model covering stale worker fencing, forged validation, replay/idempotency, approval bypass, artifact mutation, DSH adapter privilege escalation, and unsafe diagnostic data.
5. Run targeted tests, typecheck, and an independent code review; record only commands that actually ran.

## Risk controls

- Keep DSH as an adapter dependency until the pure-core/mocked path passes.
- Do not advertise exactly-once execution. Provide exactly-once **event append** and at-least-once external dispatch with idempotency keys.
- Treat every recovery of an in-flight call as uncertain and require explicit operator action.
- Keep secrets and raw model/tool payloads out of default journal projections.
- Use durable events as authority; cached checkpoints, UIs, and telemetry are derived views.
