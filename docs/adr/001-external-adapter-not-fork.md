# ADR 001: Use an external DSH adapter instead of a DSH fork

- **Status:** Accepted
- **Date:** 2026-08-25

## Context

The interview task requires a LoopGraph Supervisor that is DSH-first but harness-neutral. It must observe agent execution and verification, loop safely, support HITL, recover after restart, and provide promotion/rollback semantics.

DSH is built around Cordis plugins. Its public architecture documents that the concrete agent loop is replaceable and that extension behavior should be mounted as a plugin beside product plugins. DSH's current workflow package explicitly lacks journaling and restart recovery.

## Decision

Implement the product as:

1. A pure TypeScript `@loopgraph/core` package that knows only neutral ports:
   - `ExecutionAdapter`
   - `ValidationAdapter`
   - `HumanGateAdapter`
   - `EventStore`
   - `ArtifactStore`
   - `RunLease`
2. A DSH plugin `@loopgraph/dsh-adapter` that implements the execution and HITL ports through documented Cordis services/events.
3. An independently versioned append-only supervisor journal and content-addressed artifacts.

## State model

```text
READY -> AWAITING_ADMISSION_HITL -> RUNNING -> VALIDATING -> VALIDATED
  ^              |                   |            |             |
  |              v                   v            v             v
  +---------- PAUSED <------------ FAILED <--- PAUSED       PROMOTED
                 |
                 v
          PAUSED_RECOVERED
```

Only reducer-approved commands may transition state. Every command records an immutable intent/event and expected revision. A DSH pre-step is recorded as started before DSH calls the model; the public durable `step/end` fact is reconciled only at the following safe DSH boundary. Compare-and-set revision checking prevents concurrent operators from double-promoting or resuming the same run.

## Rejected alternatives

### Fork DSH

Rejected because it couples the implementation to a pre-release codebase, duplicates upgrade work, and bypasses DSH's documented plugin seams. It also makes harness neutrality harder to prove.

### Use `dsh-workflow` as the supervisor runtime

Rejected because its own contract states that it has no journaling or resume and treats accepted runs as holder-owned.

### Persist only in the DSH session log

Rejected because DSH session facts are conversation-oriented. The supervisor needs a versioned graph state, idempotency keys, artifact generation manifests, leases, independent validation evidence, and compensating rollback events.

## Consequences

- DSH gets the best user experience first, but no DSH type leaks into core domain types.
- Initial effort includes an explicit adapter contract and mock adapter, which is necessary test infrastructure rather than optional abstraction.
- Resume is an operator command after recovery, not automatic execution.
- Promotion and rollback are durable supervisor events, even if the target deployment backend is initially a local filesystem demo.
