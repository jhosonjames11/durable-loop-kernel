# LoopGraph Supervisor

A **DSH-first, harness-neutral** supervisor for observable, pausable, resumable, and rollback-capable agent loops.

## Decision

The project will be a standalone TypeScript core plus a DeepSeek Harness adapter plugin. We will **not fork DSH**.

- **Harness-neutral core** owns the durable execution graph, append-only event protocol, deterministic reducer, checkpoints, validation gates, HITL decisions, and promotion/rollback state machine.
- **DSH adapter** maps DSH agents, durable session facts, approval events, and live lifecycle events into the core's neutral ports.
- A future adapter can map another harness to the same core without importing DSH internals.

This avoids modifying DSH's swappable agent loop while preserving a first-class DSH integration.

## Required capabilities

1. Observe every node attempt, tool/agent action, validation result, HITL decision, checkpoint, promotion, rollback, pause, and resume as immutable events.
2. Resume deterministically after process restart from an append-only event journal plus content-addressed artifacts; any attempt or validation without a durable outcome becomes `PAUSED_RECOVERED`, then returns to `READY` only after explicit operator resume.
3. Pause at safe boundaries; do not claim an in-flight LLM/tool call is safely resumable until an adapter proves it.
4. Gate transitions using independently recorded validation evidence.
5. Require a durable HITL decision for configured transitions; missing human authority fails closed.
6. Promote versions only from a validated checkpoint; roll back by creating a new compensating event, never rewriting history.

## First-class LoopSpec and authority boundaries

`LoopSpec` is the promoted object—not a Python/TypeScript policy callback. Each
immutable snapshot contains a `specId`, monotonically increasing `revision`,
entry node, canonical node list, and canonical directed edge list. The initial
snapshot is embedded in `run.created`; a candidate snapshot is embedded in
`generation.prepared`; and `generation.promoted` switches the reducer's active
graph and executable `nextNodeId`. `rollback.applied` restores the target
promotion's exact stored snapshot. No reducer replay consults mutable workflow
code or a graph registry.

A promotion HITL request is bound to all of the following durable values:

- its request id;
- the exact passed validation id;
- the candidate generation id;
- the candidate `specId` and revision; and
- a complete copy of the candidate `LoopSpec`.

The reducer compares the full canonical graph, not merely a revision number.
An approval for a different validation, generation, node/edge shape, or reused
spec revision is rejected. The operator CLI can append a promotion only after
this authority is already durable; it cannot create a human approval.

For post-validation promotion, `HumanGateAdapter` must return an authenticated
approval subject and an opaque authority receipt reference. Those fields are
durably recorded on `hitl.decided`; the CLI's `--actor` identifies an operator
control-plane action only and is neither accepted as nor substituted for a
human approval identity. There is deliberately no CLI command to decide HITL.

This repository makes **no self-improvement claim**. It contains no hidden
holdout, keyword classifier, or score-to-success shortcut. A candidate graph
must be supplied explicitly, its execution must settle at a real adapter
boundary, and a separate validator must produce evidence before promotion can
be prepared. DSH's `step/end` is an execution fact, not proof that the task is
complete.

## Reducer pause and promotion policy

A `pause.requested` received while an attempt is running remains a request until that attempt's matching `node.settled` event. Settlement is the safe boundary: the reducer immediately commits `PAUSED`, clears `pauseRequested`, and retains a `PENDING` validation record bound to the settled node and attempt. Validation evidence may be recorded while paused, but the phase remains `PAUSED`; `generation.prepared` and `generation.promoted` are rejected until a durable `run.resumed` event returns the run to `VALIDATING`, `VALIDATED`, or `READY` according to that validation record.

Every `run.created` event explicitly declares `requiresHitl`. A configured gate requires an approved `hitl.requested`/`hitl.decided` record before promotion. An unconfigured run may promote after validation unless it has explicitly requested HITL, in which case that request also requires approval.

An adapter exception after `node.started` records no invented completion. On
restart it becomes `recovery.uncertain` and the run remains `PAUSED_RECOVERED`;
there is no automatic retry of an operation whose external outcome is unknown.
Only an explicit later `run.resumed` plus a new dispatch can attempt work again.

## Mock-harness durable E2E

`npm run test:demo` runs a stdout-free deterministic scenario against the real
`SqliteRunStore` and `FileArtifactStore`, never a reducer-only event array. It
injects an ID factory and clock, records every mock adapter callback with
`runId`/`nodeId`/`attemptId`/`correlationId`, and verifies that reopening the
SQLite journal preserves retry, prepared manifests, HITL decisions, crash
recovery, explicit resume, V1/V2 history, and rollback to V1. The mock's
human gate accepts only `ALLOWED_ONCE`; `DENIED` and `UNAVAILABLE` are recorded
but cannot promote a generation.

This demo is an integration contract for the harness-neutral core, **not a
replacement for the real DSH adapter**. Production DSH lifecycle and Cordis
approval mapping remain covered by `packages/dsh-adapter` and `npm run
test:dsh`; the mock simply proves any conforming adapter can drive the same
neutral ports without importing DSH.

## Operator CLI

`@loopgraph/cli` builds the dependency-free `loopgraph` executable. It is an
operator **control plane**, not a model or agent runner: it never dispatches
model/tool work or invents validation/HITL approvals. It opens an existing
SQLite journal for `inspect` without creating a run and emits only bounded,
structured JSON on stdout.

```sh
loopgraph inspect --db /srv/loopgraph/runs.sqlite --run run-123
loopgraph pause --db /srv/loopgraph/runs.sqlite --run run-123 --actor oncall-a --expected-revision 42
loopgraph resume --db /srv/loopgraph/runs.sqlite --run run-123 --actor oncall-a --expected-revision 44
loopgraph promote --db /srv/loopgraph/runs.sqlite --run run-123 --actor oncall-a --expected-revision 51 --generation generation-v2
loopgraph rollback --db /srv/loopgraph/runs.sqlite --run run-123 --actor oncall-a --expected-revision 58 --generation generation-v1
loopgraph demo
```

Mutations require explicit `--run`, `--actor`, and `--expected-revision`; the
revision is compared before appending through `SqliteRunStore` under a durable
lease whose holder is a SHA-256-derived actor identifier. `inspect` never emits
event payloads, artifact/evidence/prompt references, or raw errors. Stable
result codes include `REVISION_CONFLICT`, `LEASE_UNAVAILABLE`,
`PROMOTION_NOT_AUTHORIZED`, and `ROLLBACK_TARGET_INVALID`. Promotion may only
append `generation.promoted` for an existing prepared generation matching the
current passed validation and the reducer's HITL policy; rollback only targets a
prior durable promotion. The `inspect` histories (`timeline`,
`preparedGenerations`, `promotedGenerations`, and `rollbackAncestry`) each use
`{ items, total, truncated }`: `items` contains at most the newest 20 entries,
while `total` and `truncated` make omitted older history explicit. Returned
identifiers and codes are capped at 256 Unicode code points. See the
[five-minute interview demo](docs/demo-script.md).

## Durable storage boundary

`SqliteRunStore` accepts only complete, current-version reducer events. It bounds
canonical event bytes/nesting (1 MiB / 32 levels by default; configurable), and
is deliberately **not** a store for secrets or arbitrary payloads. Reads verify
canonical JSON plus every indexed event field, so journal edits fail closed.
Checkpoints are cached projections, not alternate truth: recovery replays the
append-only journal and uses a checkpoint only when its canonical view exactly
matches the journal head. A stale or mismatched checkpoint is ignored.

`FileArtifactStore` treats its supplied root as a private trusted directory and
rejects roots, `blobs`, or `manifests` that are symlinks or non-directories. It
uses synced temporary files, atomic hard-link publication, and a directory sync
before returning a newly published blob or manifest. On a filesystem where
directory fsync is unavailable it returns `DURABILITY_UNAVAILABLE` rather than
claiming a crash-durable publication. Defaults bound individual artifacts to
64 MiB, manifests to 1 MiB, and each manifest to 256 artifacts. Only stale,
store-named temp files older than one hour are cleaned at initialization.

The repository pins its own toolchain in `package-lock.json`. The DSH adapter
has no runtime DSH dependency by design; its real integration launcher enforces
the audited DSH source commit for the default checkout rather than substituting
a fake client. Run `npm run typecheck`, `npm run lint`, `npm test`, and
`npm run test:dsh` (with the pinned checkout installed) as separate evidence.

## Repository layout (target)

```text
packages/
  core/          # no DSH imports: graph, events, reducer, ports, invariants
  storage/       # SQLite event store and artifact registry
  dsh-adapter/   # Cordis plugin and DSH-specific mapping
  cli/           # inspect / pause / resume / promote / rollback
  demo/          # mock harness end-to-end scenario
```

## Development status

- [x] Source audit of `deepseek-ai/deepseek-harness` at `b150a551b8d465e31e418e1b2eaf5e79bbb7d28e`.
- [x] Architectural decision: external DSH plugin adapter rather than a DSH fork.
- [x] Implement core event protocol and pure reducer with tests.
- [x] Implement durable SQLite journal, snapshot/checkpoint protocol, and recovery tests.
- [x] Implement DSH Cordis adapter and real-composition integration test.
- [x] Implement CLI/operator view and end-to-end interruption/recovery demo.

### DSH adapter verification

The adapter is a dependency-free external Cordis function plugin. It consumes
only public `ctx.agents`, `ctx.sessions`, `agent/pre-step`, `session/event`
(`step/end` only), `agent/turn-stopping`, and `ctx.approval.request`;
production code does not import the concrete DSH agent loop. It records a DSH
step as *started* before DSH calls the model, then settles it at the next safe
boundary after DSH's durable `step/end` fact. Its optional pre-step HITL gate
is a separately durable execution-admission gate; only `allowed-once` starts
the external attempt, while version promotion continues to require the normal
post-validation HITL authority. Observations are bounded lifecycle metadata
only, and sink failures are contained.

The real composition test is pinned to DeepSeek Harness commit
`b150a551b8d465e31e418e1b2eaf5e79bbb7d28e`. With an installed sibling
checkout, run `npm run test:dsh`; otherwise set
`DSH_CHECKOUT=/absolute/path/to/deepseek-harness npm run test:dsh`. It fails
when the required checkout/dependencies are missing rather than reporting a
skipped success. See the adapter
[README](packages/dsh-adapter/README.md) for the contract and limits.

Read the audit at [`docs/research/deepseek-harness-audit.md`](docs/research/deepseek-harness-audit.md), the decision record at [`docs/adr/001-external-adapter-not-fork.md`](docs/adr/001-external-adapter-not-fork.md), and the implementation blueprint at [`.hermes/plans/2026-08-25-loopgraph-supervisor.md`](.hermes/plans/2026-08-25-loopgraph-supervisor.md).
