# Five-minute operator interview demo

This is an **operator control-plane** walkthrough. `loopgraph` reads the durable
SQLite journal and appends fenced control events; it **does not start model,
agent, tool, validation, or HITL execution**. Those events are produced by the
configured harness adapter.

Run this against a disposable incident/demo database that already contains a
run. The CLI never creates a run from `inspect`, and every mutation requires a
human identity and the revision read immediately before the command.

```sh
export DB=/srv/loopgraph/demo/runs.sqlite
export RUN=mock-run-001
export ACTOR=interviewer-operator
```

## 0:00–0:40 — Inspect

```sh
loopgraph inspect --db "$DB" --run "$RUN"
```

Point out the bounded `timeline`: it has only type, sequence, time, actor, and
correlation metadata. It deliberately does not print event payloads, prompts,
evidence, artifact references, or database paths. Note the revision, phase,
active attempt, validation, HITL state, generations, rollback ancestry, and
safe lease metadata. Copy the reported `revision` into `REV` before each
mutation:

```sh
export REV=42  # replace 42 with the revision from the preceding inspect result
```

## 0:40–1:30 — Explain execution and independent validation

Explain that `node.dispatch.requested` and `node.started` identify an attempt;
`node.settled` is not success authorization. A separately durable
`validation.recorded` event contains the validation status and ID. A generation
must be prepared against that exact passed validation before it is eligible for
promotion. The CLI only observes and controls this state; it cannot forge a
validation, prepared generation, or HITL decision.

```sh
loopgraph inspect --db "$DB" --run "$RUN"
```

## 1:30–2:10 — HITL gate and promotion

Show `hitl.required: true` and an `APPROVED` decision associated with the
prepared generation. The harness/HITL integration records that approval first;
then the operator can request the fenced promotion:

```sh
loopgraph promote --db "$DB" --run "$RUN" --actor "$ACTOR" --expected-revision "$REV" --generation generation-v2
loopgraph inspect --db "$DB" --run "$RUN"
```

The command fails with `PROMOTION_NOT_AUTHORIZED` unless the selected generation
already exists, matches the current passed validation, and the reducer's HITL
policy is satisfied. It does not create approval records or bypass reducer
guards. Set `REV` again from this inspect result.

## 2:10–3:20 — Simulated crash and conservative recovery

Use the deterministic mock integration scenario to demonstrate the actual crash
boundary. It records dispatch/start, simulates a process crash before settlement,
reopens SQLite, and records `recovery.uncertain`; no model call is replayed by
recovery.

```sh
npm run test:demo
loopgraph demo
```

For the live demo database, let the harness recovery coordinator take over (or
show its already-recorded `PAUSED_RECOVERED` state), then inspect it:

```sh
loopgraph inspect --db "$DB" --run "$RUN"
```

The key safety claim is narrow: recovery pauses uncertain external work. It does
not claim to resume an in-flight model or tool call.

## 3:20–4:05 — Explicit operator resume

After checking the recovered state and copying its revision, resume is explicit:

```sh
export REV=57  # replace 57 with the just-inspected revision
loopgraph resume --db "$DB" --run "$RUN" --actor "$ACTOR" --expected-revision "$REV"
loopgraph inspect --db "$DB" --run "$RUN"
```

A stale revision returns structured `REVISION_CONFLICT`; an active owner returns
`LEASE_UNAVAILABLE`. There is no implicit actor or revision, and each mutation
uses a lease owner derived from the actor rather than exposing the actor as the
lease holder.

## 4:05–5:00 — Compensating rollback

After a later validated/HITL-approved `generation-v3` has been promoted, roll
back to a *prior promoted* generation. Copy the latest inspect revision first:

```sh
export REV=73  # replace 73 with the just-inspected revision
loopgraph rollback --db "$DB" --run "$RUN" --actor "$ACTOR" --expected-revision "$REV" --generation generation-v2
loopgraph inspect --db "$DB" --run "$RUN"
```

Rollback appends `rollback.applied`; it never rewrites or deletes V3 history.
The projection shows the new active generation and rollback ancestry. Selecting
the current generation or an unpromoted generation returns
`ROLLBACK_TARGET_INVALID`.

## What the two demos prove

- `npm run test:demo` is the durable **mock harness** E2E: real SQLite and
  artifact storage plus deterministic mock adapters. It proves the
  harness-neutral contract, including crash/recovery and explicit resume.
- `npm run test:dsh` is the separate **real DSH composition integration**. It
  validates the external DSH/Cordis lifecycle and approval mapping against the
  pinned compatible DSH checkout. It is not replaced by the mock E2E.

The `loopgraph demo` command is a bounded, self-cleaning CLI smoke scenario; it
reports promotion/HITL/rollback state without leaving a database under the
source tree.
