# ADR 002: Promote immutable LoopSpec snapshots, not policy functions

## Context

The supervisor needs a durable unit of promotion and rollback. A mutable
`chooseNextAction()` function or a hard-coded engine state machine cannot be
reviewed, versioned, approved, or reconstructed after a restart.

## Decision

`LoopSpec` is a first-class core type with `specId`, positive revision, entry
node, sorted node list, and sorted edge list. Every run journal embeds the
starting snapshot. A prepared generation embeds its candidate snapshot, and a
promotion switches the reducer to that exact snapshot. Rollback references a
previous promotion and restores its stored snapshot; it never rewrites the
journal.

Canonical ordering and bounded graph sizes make the event payload stable for
storage and replay. A spec identity (`specId`, revision) may appear only once
among prepared or promoted candidates. A HITL request also includes the full
candidate snapshot, so reducer replay requires structural equality before an
approval can authorize promotion.

## Consequences

- The current graph and next executable node are inspectable without loading
  application code.
- Promotion and rollback change graph execution, not just metadata.
- Older workflow-reference-only journals cannot be silently treated as a
  versioned graph. The narrow schema-v1/v2 compatibility migration only
  upgrades an untouched `run.created` record to a clearly marked single-node
  legacy graph; progressed records that lack graph/candidate evidence fail
  closed.
- Candidate creation remains an explicit caller responsibility. This ADR does
  not claim autonomous learning or generalization.
