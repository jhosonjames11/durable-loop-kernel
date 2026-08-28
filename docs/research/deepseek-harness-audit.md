# DeepSeek Harness Source Audit

**Audited repository:** `https://github.com/deepseek-ai/deepseek-harness`
**Local checkout:** `/Users/jax/projects/deepseek-harness`
**Commit:** `b150a551b8d465e31e418e1b2eaf5e79bbb7d28e` (`2026-08-21`)
**Observed root version:** `0.1.1-rc.2`
**License:** MIT

## Verified architectural facts

| Need | DSH evidence | Consequence for LoopGraph |
|---|---|---|
| Extend without patching the loop | `docs/architecture.md` states there is no privileged core and plugins mount beside one another; DSH is a Cordis plugin tree. | Use an external Cordis plugin, not a fork. |
| Avoid concrete-loop coupling | `packages/core/agent/README.md` defines an `Agent` interface and `agent/*` vocabulary with zero loop dependency. | Bind the adapter to `ctx.agents` and documented events only. |
| Durable audit facts | DSH session events are append-only durable facts; the session log drives history, resume, fork, transcript, and telemetry. | Mirror supervisor facts into its own durable journal and correlate them with DSH session/agent ids. |
| Pre-step interposition | The documented turn flow exposes `agent/pre-step` before a model request, and `agent/turn-stopping` before turn close. | Pause/resume admission belongs at safe boundaries, not by attempting to serialize a live model stream. |
| HITL approval | `dsh-user-approval` records paired `approval/asked` and `approval/decided` events and fails closed if no answerer exists. | Translate a LoopGraph HITL gate into an approval request; treat unavailable/cancelled as non-promotion. |
| Existing goal state | `dsh-goal` persists revisioned `goal/change` events and deliberately disarms activation on resume. | Do not overload a goal as a graph run. Use it only as an optional DSH-facing objective bridge. |
| Existing workflow gap | `dsh-workflow` explicitly documents no journaling or resume, no saved workflows, and holder-owned runs. | LoopGraph must own durable run/checkpoint recovery rather than reuse workflow runs as the supervisor state store. |
| Existing subagent gap | `dsh-subagent` documents process-local activation ownership and no cross-process lease/mailbox protocol. | Cross-process recovery needs LoopGraph's lease + journal semantics; do not rely on a live DSH handle. |
| Persistence option | DSH provides opt-in SQLite session persistence with WAL, `synchronous=FULL`, append-only events, and secure path checks. | The adapter may correlate to it, but LoopGraph must have an independently versioned persistence schema. |

## DSH seams to consume

The first adapter version should consume only these public seams:

- `ctx.agents` / `Agent` for creation, resume, `whenIdle()`, inbox delivery, and cancellation.
- `agent/pre-step`, `agent/request-error`, and `agent/turn-stopping` for safe-boundary control and observation.
- durable DSH `session/event` facts for correlation and operator drill-down.
- `ctx.approval` for interactive decisions, with fail-closed behavior.
- `ctx.sessions` and configured session persistence only for DSH session reconstruction, never as the authoritative LoopGraph reducer state.

The adapter must not import `dsh-agent-loop` or depend on internal driver objects. DSH explicitly treats its concrete loop as replaceable.

## Important constraints discovered

1. **Safe pause is boundary-based.** DSH cancellation can abort a current driver, but it does not provide a generic step-only checkpoint/resume primitive. LoopGraph will pause before dispatch, after an attempt settles, after validation, or while awaiting HITL.
2. **Durable state and activation are separate.** DSH goals persist intent but intentionally disarm process-local continuation after recovery. LoopGraph must do the same: recovery yields `PAUSED_RECOVERED`; a lease holder must explicitly resume it.
3. **Events are observer-safe, not decision authority.** DSH workflow and subagent events are observe-only. Supervisor decisions must occur in LoopGraph's own transition command/reducer boundary.
4. **Session logs do not provide atomic multi-artifact promotion.** The supervisor needs an explicit generation manifest: artifact hashes are written first, then the promotion event commits last.
5. **DSH is pre-release.** Its storage schema and on-disk formats have no compatibility promise. Adapter compatibility needs a tested DSH version range and capability probe.

## Recommendation

Build an **external plugin adapter** around a standalone core. A fork would create upgrade drag while violating DSH's documented extension model. The core should run against a mock harness in unit/integration tests, then gain a DSH adapter test booted from a real Cordis composition.
