# `@loopgraph/dsh-adapter`

A small external Cordis function plugin that observes public DeepSeek Harness
agent lifecycle boundaries and optionally gates a **pre-step** with DSH's public
approval service. It does not implement, import, or own an agent loop.

## Compatibility and real composition test

The adapter is source-compatible with DeepSeek Harness checkout commit
`b150a551b8d465e31e418e1b2eaf5e79bbb7d28e` (the `0.1.1-rc.2` workspace
packages used by that checkout). It is intentionally built without DSH runtime
dependencies; the host supplies Cordis and DSH services.

Run the genuine composition test against an installed checkout:

```sh
# Default checkout: ../deepseek-harness relative to this repository
npm run test:dsh

# Or test an installed checkout explicitly (its actual revision is reported)
DSH_CHECKOUT=/absolute/path/to/deepseek-harness npm run test:dsh
```

The default sibling checkout is required to be exactly the pinned revision above;
an explicit `DSH_CHECKOUT` is an opt-in compatibility probe whose actual short
revision is printed with the test result. The command fails rather than skipping
when that checkout, its `node_modules`, its `tsx` launcher, or its Git revision
is unavailable. The test composes real `Context`, `AgentRegistry`,
`ApprovalService`, `AgentLoop`, and a scripted LLM adapter. `AgentLoop` is
test-only; production source is checked to ensure it does not import it.

## Contract

`createDshAdapter()` accepts:

- a neutral `sink` for safe lifecycle observations;
- `correlateRun(agentId)`, which may associate a live DSH agent with a
  LoopGraph run; and
- optional `hitl(coordinate)`, which returns one of `promotion`, `rollback`,
  or `resume` to require an approval.

Every observation has only a bounded agent/run identifier, lifecycle
coordinates, and (for a denial) one bounded reason code and outcome. Prompt,
session, model, tool, and raw caller-provided content never enter the sink.
Sink exceptions and rejected sink promises are contained and do not alter DSH
agent progress.

The plugin declares only `agents`, `approval`, and `sessions` injection. It
subscribes through public `agent/pre-step`, `session/event` (only
`step/end`), and `agent/turn-stopping` events, and removes all subscriptions
on plugin teardown. `pre-step` writes only a durable *started* attempt; after
DSH has durably emitted `step/end`, the next pre-step or turn-stopping boundary
settles that exact attempt and invokes a host-provided validator. Thus an
in-flight model/tool call is never falsely represented as completed or safely
resumable.

`createDshDurableBridge()` accepts an optional `validator`. Omitting it fails
closed: the completed step receives a failed validation record and can never
prepare or promote a generation. A production validator should emit a bounded
evidence reference (for example a test-report artifact digest), not raw model
or tool content.

## HITL behavior and limits

When a policy gates a pre-step, the durable bridge first appends an
`admission.hitl.requested` event, then the adapter calls
`ctx.approval.request()` while the turn is open with the fixed tool name
`loopgraph-supervisor-hitl`, a fixed reason mapped from the selected reason
code, and the exact live pre-step abort signal. Only `allowed-once` appends
`admission.hitl.decided: APPROVED`, begins the external attempt, and calls
DSH's `next()`. Rejected, cancelled, unavailable, malformed, and thrown
approval results are durably closed and return `{ kind: 'reject' }`. This is an
execution-admission gate, deliberately separate from the post-validation HITL
authority required for version promotion.

A rejected claimed prompt is not transparently resumable. This adapter only
observes and safely admits or rejects the current pre-step; recovery or a later
operator-directed retry must be handled by the host/supervisor with explicit
durable state.

DSH exposes no generic, public “resume token” that can make an in-flight model
or tool call safe to replay. The bridge therefore never stores or replays a
fictional token: after restart it reconciles only a durable `step/end` fact;
otherwise LoopGraph enters `PAUSED_RECOVERED`. The host may reconstruct its DSH
session through DSH's own persistence, but a fresh explicit LoopGraph resume
and dispatch is still required.
