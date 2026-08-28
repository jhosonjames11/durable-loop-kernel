/**
 * A dependency-free Cordis adapter for the public DeepSeek Harness agent and
 * approval seams. Concrete agent-loop ownership remains a host concern.
 */
import {
  Supervisor,
  type AdapterCallbackContext,
  type SupervisorClock,
  type SupervisorIdFactory,
  type ValidationOutcome,
  type LoopSpec,
  hasLoopEdge,
  isValidLoopSpec,
} from '@loopgraph/core'
import { FileArtifactStore, SqliteRunStore } from '@loopgraph/storage'

/** The only policy-selected reasons admitted to DSH approval audit records. */
export const DSH_HITL_REASON_CODES = ['promotion', 'rollback', 'resume'] as const

/** A bounded, non-prose supervisor gate reason. */
export type DshHitlReasonCode = typeof DSH_HITL_REASON_CODES[number]

/** Closed outcomes exposed by DSH's public approval service. */
export type DshApprovalOutcome = 'allowed-once' | 'rejected' | 'cancelled' | 'unavailable'

/** Safe, bounded lifecycle data exported from this adapter. */
export type DshObservation =
  | { readonly kind: 'pre-step'; readonly agentId: string; readonly runId?: string; readonly turn: number; readonly step: number }
  | { readonly kind: 'step-ended'; readonly agentId: string; readonly runId?: string; readonly turn: number; readonly step: number; readonly eventSeq: number }
  | { readonly kind: 'turn-stopping'; readonly agentId: string; readonly runId?: string; readonly turn: number }
  | {
    readonly kind: 'hitl-denied'
    readonly agentId: string
    readonly runId?: string
    readonly turn: number
    readonly step: number
    readonly reasonCode: DshHitlReasonCode | 'policy-error'
    readonly outcome: Exclude<DshApprovalOutcome, 'allowed-once'>
  }

/** Neutral, non-authoritative recording boundary. Rejection is always contained. */
export interface DshObservationSink {
  record(observation: DshObservation): void | Promise<void>
}

/** Correlates a live DSH agent identity with a supervisor run, if one exists. */
export type DshRunCorrelator = (agentId: string) => string | undefined

/** Coordinates deliberately exclude DSH session, model, tool, and prompt content. */
export interface DshPreStepCoordinate {
  readonly agentId: string
  readonly runId?: string
  readonly turn: number
  readonly step: number
}

/** Returns a bounded reason code to require one approval, or `undefined` to admit the step. */
export type DshHitlPolicy = (coordinate: DshPreStepCoordinate) => DshHitlReasonCode | undefined

/** A completed DSH step expressed without model, tool, or prompt content. */
export interface DshCompletedStep extends DshPreStepCoordinate {
  readonly eventSeq: number
}

/** Host-supplied, independently recorded validator for one completed DSH step. */
export interface DshStepValidator {
  validate(step: DshCompletedStep): Promise<ValidationOutcome>
}

/** Persistent bridge configuration for mapping one actual DSH pre-step to a core run. */
export interface DshDurableBridgeOptions {
  readonly databaseFile: string
  readonly artifactDirectory: string
  readonly loopSpec: LoopSpec
  readonly actor: string
  readonly holderId: string
  readonly leaseTtlMs: number
  readonly clock?: SupervisorClock
  readonly ids?: SupervisorIdFactory
  /** Missing validators fail closed and cannot grant generation-promotion authority. */
  readonly validator?: DshStepValidator
}

/**
 * A DSH pre-step is model-admission work, not an entire DSH agent-loop node.
 * It is journaled as a started LoopGraph attempt before DSH advances the model
 * request. Public `session/event: step/end` is observed without retaining its
 * content, then reconciled at the next safe DSH boundary. The DSH model/tool
 * execution remains owned by DSH and is never claimed as atomically resumable.
 */
export interface DshDurableBridge {
  /** Begin an externally executed DSH step only after any required admission gate clears. */
  preStep(coordinate: DshPreStepCoordinate): Promise<void>
  /** Record one completed DSH public session boundary; it does no async I/O. */
  observeStepEnd(step: DshCompletedStep): void
  /** Reconcile a previously observed end into the LoopGraph journal. */
  reconcile(coordinate: DshPreStepCoordinate): Promise<void>
  requestAdmissionHitl(coordinate: DshPreStepCoordinate, reasonCode: DshHitlReasonCode): Promise<string>
  decideAdmissionHitl(
    coordinate: DshPreStepCoordinate,
    requestId: string,
    outcome: DshApprovalOutcome,
  ): Promise<void>
  close(): void
}

class WallClock implements SupervisorClock {
  now(): number { return Date.now() }
  occurredAt(): string { return new Date().toISOString() }
}

class CounterIds implements SupervisorIdFactory {
  #value = 0
  next(namespace: string): string {
    this.#value += 1
    return `${namespace}:${this.#value}`
  }
}

/** Build a real core Supervisor + SQLite journal bridge without importing any DSH loop implementation. */
export function createDshDurableBridge(options: DshDurableBridgeOptions): DshDurableBridge {
  if (!isValidLoopSpec(options.loopSpec)
    || !options.loopSpec.nodes.some(({ nodeId }) => nodeId === 'dsh-pre-step')
    || !hasLoopEdge(options.loopSpec, 'dsh-pre-step', 'dsh-pre-step')) {
    throw new Error('DSH bridge requires a canonical LoopSpec with a dsh-pre-step self-loop');
  }
  const store = new SqliteRunStore({ filename: options.databaseFile })
  const artifacts = new FileArtifactStore(options.artifactDirectory)
  const clock = options.clock ?? new WallClock()
  const ids = options.ids ?? new CounterIds()
  const supervisors = new Map<string, Supervisor>()
  const pendingSteps = new Map<string, DshCompletedStep>()
  const completedSteps = new Map<string, DshCompletedStep>()
  const attemptFor = (coordinate: DshPreStepCoordinate) => `dsh:${coordinate.agentId}:turn:${coordinate.turn}:step:${coordinate.step}`
  const correlationFor = (coordinate: DshPreStepCoordinate) => `dsh:${coordinate.agentId}:${coordinate.turn}:${coordinate.step}`
  const runFor = (coordinate: DshPreStepCoordinate): string => {
    if (coordinate.runId === undefined) throw new Error('DSH pre-step has no durable run correlation')
    return coordinate.runId
  }
  const supervisorFor = (coordinate: DshPreStepCoordinate): Supervisor => {
    const runId = runFor(coordinate)
    const existing = supervisors.get(runId)
    if (existing !== undefined) return existing
    const supervisor = new Supervisor({
      store,
      artifacts,
      execution: { async dispatch(_context: AdapterCallbackContext) { return { outcome: 'SUCCEEDED' as const, outcomeCode: 'DSH_PRE_STEP_ADMITTED' } } },
      validation: {
        async validate(context: AdapterCallbackContext) {
          const step = completedSteps.get(context.attemptId)
          if (step === undefined) {
            return { passed: false, evidenceRef: `dsh:step-boundary-missing:${context.attemptId}` }
          }
          if (options.validator === undefined) {
            return { passed: false, evidenceRef: `dsh:validator-unconfigured:${step.agentId}:${step.turn}:${step.step}` }
          }
          return options.validator.validate(step)
        },
      },
      humanGate: {
        async requestApproval() {
          return {
            decision: 'UNAVAILABLE' as const,
            decisionCode: 'DSH_GATE_IS_EXTERNAL',
            approvalSubject: 'dsh-external-gate',
            approvalReceiptRef: 'dsh:external-gate-not-used',
          }
        },
      },
      clock,
      ids,
      actor: options.actor,
      holderId: options.holderId,
      leaseTtlMs: options.leaseTtlMs,
    })
    if (store.read(runId).length === 0) {
      supervisor.createRun({ runId, loopSpec: options.loopSpec, requiresHitl: true })
    } else {
      supervisor.recover({ runId, reasonCode: 'DSH_BRIDGE_REATTACH' })
    }
    supervisors.set(runId, supervisor)
    return supervisor
  }
  return {
    async preStep(coordinate) {
      const supervisor = supervisorFor(coordinate)
      const runId = runFor(coordinate)
      const attemptId = attemptFor(coordinate)
      await this.reconcile(coordinate)
      const view = supervisor.view(runId)
      // DSH can re-enter a lifecycle listener; never duplicate the exact durable
      // admission attempt. A different pre-step consumes the prior passed
      // validation before beginning the next graph edge.
      if (view.activeAttempt?.attemptId === attemptId) return
      if (view.phase === 'VALIDATED') {
        supervisor.supersedeValidation(runId, 'DSH_NEXT_STEP', 'dsh-pre-step', correlationFor(coordinate))
      }
      supervisor.beginAttempt({ runId, nodeId: 'dsh-pre-step', attemptId, correlationId: correlationFor(coordinate) })
      pendingSteps.set(coordinate.agentId, { ...coordinate, eventSeq: 0 })
    },
    observeStepEnd(step) {
      const pending = pendingSteps.get(step.agentId)
      if (pending === undefined || pending.turn !== step.turn || pending.step !== step.step) return
      pendingSteps.set(step.agentId, step)
    },
    async reconcile(coordinate) {
      const pending = pendingSteps.get(coordinate.agentId)
      if (pending === undefined || pending.eventSeq === 0) return
      const supervisor = supervisorFor(pending)
      const runId = runFor(pending)
      const attemptId = attemptFor(pending)
      completedSteps.set(attemptId, pending)
      await supervisor.settleAttempt({
        runId,
        nodeId: 'dsh-pre-step',
        attemptId,
        correlationId: correlationFor(pending),
        outcome: 'SUCCEEDED',
        outcomeCode: 'DSH_STEP_END',
      })
      pendingSteps.delete(pending.agentId)
      completedSteps.delete(attemptId)
    },
    async requestAdmissionHitl(coordinate, reasonCode) {
      const supervisor = supervisorFor(coordinate)
      await this.reconcile(coordinate)
      const view = supervisor.view(runFor(coordinate))
      if (view.phase === 'VALIDATED') {
        supervisor.supersedeValidation(runFor(coordinate), 'DSH_NEXT_STEP', 'dsh-pre-step', correlationFor(coordinate))
      }
      return supervisor.requestAdmissionHitl({
        runId: runFor(coordinate),
        reasonCode: `DSH_${reasonCode.toUpperCase()}`,
        correlationId: correlationFor(coordinate),
      })
    },
    async decideAdmissionHitl(coordinate, requestId, outcome) {
      const decision = outcome === 'allowed-once' ? 'ALLOWED_ONCE' : 'DENIED'
      const mapped = outcome === 'unavailable' || outcome === 'cancelled' ? 'UNAVAILABLE' : decision
      supervisorFor(coordinate).recordAdmissionHitlDecision({
        runId: runFor(coordinate),
        requestId,
        decision: mapped,
        decisionCode: `DSH_${outcome.toUpperCase().replace('-', '_')}`,
        correlationId: correlationFor(coordinate),
      })
    },
    close() { store.close() },
  }
}

/** Adapter configuration. Callers supply a sink and agent-to-run correlator. */
export interface DshAdapterOptions {
  readonly sink: DshObservationSink
  readonly correlateRun: DshRunCorrelator
  readonly hitl?: DshHitlPolicy
  /** Optional durable core bridge; bridge errors fail the pre-step closed. */
  readonly durableBridge?: DshDurableBridge
}

interface DshAgent {
  readonly id: string
}

interface DshPreStepPayload {
  readonly agent: DshAgent
  readonly turn: number
  readonly step: number
  /** The live DSH turn cancellation signal. */
  readonly signal: AbortSignal
}

interface DshTurnStoppingPayload {
  readonly agent: DshAgent
  readonly turn: number
}

interface DshSession {
  readonly id: string
}

interface DshSessionEvent {
  readonly type: string
  readonly seq: number
  readonly data: unknown
}

interface DshApprovalService {
  request(request: {
    readonly agent: DshAgent
    readonly toolName: string
    readonly reason: string
    /** Cancels the pending approval with the current DSH turn. */
    readonly signal: AbortSignal
  }): Promise<unknown>
}

type DshPreStepDecision = { readonly kind: 'reject' } | unknown

/** The public Cordis surface this function plugin consumes; no DSH package import is required. */
export interface DshCordisContext {
  readonly agents: unknown
  readonly sessions: unknown
  readonly approval: DshApprovalService
  on(
    event: 'agent/pre-step',
    listener: (payload: DshPreStepPayload, next: () => Promise<DshPreStepDecision>) => Promise<DshPreStepDecision>,
  ): () => unknown
  on(event: 'agent/turn-stopping', listener: (payload: DshTurnStoppingPayload) => void | Promise<void>): () => unknown
  on(event: 'session/event', listener: (session: DshSession, event: DshSessionEvent) => void): () => unknown
}

/** Structural Cordis function-plugin shape for direct `ctx.plugin()` composition. */
export interface DshCordisPlugin {
  (ctx: DshCordisContext): () => void
  readonly inject: readonly ['agents', 'approval', 'sessions']
}

const SAFE_IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/
const SAFE_TOOL_NAME = 'loopgraph-supervisor-hitl'
const REASON_TEXT: Readonly<Record<DshHitlReasonCode, string>> = {
  promotion: 'LoopGraph supervisor requires approval for promotion.',
  rollback: 'LoopGraph supervisor requires approval for rollback.',
  resume: 'LoopGraph supervisor requires approval for resume.',
}

function safeIdentifier(value: unknown): string | undefined {
  return typeof value === 'string' && SAFE_IDENTIFIER.test(value) ? value : undefined
}

function safeAgentId(value: unknown): string {
  return safeIdentifier(value) ?? 'unknown'
}

function safeCoordinate(value: unknown): number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : 0
}

function safeEventSequence(value: unknown): number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : 0
}

function stepPosition(value: unknown): { turn: number; step: number } | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined
  const record = value as Record<string, unknown>
  const turn = safeCoordinate(record.turn)
  const step = safeCoordinate(record.step)
  return turn > 0 && step > 0 ? { turn, step } : undefined
}

function safeOutcome(value: unknown): Exclude<DshApprovalOutcome, 'allowed-once'> {
  return value === 'rejected' || value === 'cancelled' || value === 'unavailable' ? value : 'unavailable'
}

function isReasonCode(value: unknown): value is DshHitlReasonCode {
  return typeof value === 'string' && (DSH_HITL_REASON_CODES as readonly string[]).includes(value)
}

/** Sink failures are observer-local and never alter a DSH turn's decision. */
function recordSafely(sink: DshObservationSink, observation: DshObservation): void {
  try {
    void Promise.resolve(sink.record(observation)).catch(() => undefined)
  } catch {
    // The DSH lifecycle remains authoritative if the neutral observer fails.
  }
}

function withRunId(agentId: string, correlateRun: DshRunCorrelator): string | undefined {
  try {
    return safeIdentifier(correlateRun(agentId))
  } catch {
    return undefined
  }
}

/**
 * Create an external Cordis function plugin. Its declared injections make the
 * topology explicit: only public `ctx.agents`, `ctx.approval`, and
 * `ctx.sessions` lifecycle surfaces are accessed.
 * It observes rejected turns only; it does not claim transparent continuation.
 */
export function createDshAdapter(options: DshAdapterOptions): DshCordisPlugin {
  const plugin = ((ctx: DshCordisContext): (() => void) => {
    // `agents` and `sessions` are intentionally injected and read through DSH's
    // public service surface. Lifecycle payloads carry the exact live agent,
    // avoiding private registry/loop APIs or a second identity lookup.
    void ctx.agents
    void ctx.sessions

    const disposePreStep = ctx.on('agent/pre-step', async (payload, next) => {
      const agentId = safeAgentId(payload.agent.id)
      const turn = safeCoordinate(payload.turn)
      const step = safeCoordinate(payload.step)
      const runId = withRunId(agentId, options.correlateRun)
      const coordinate: DshPreStepCoordinate = {
        agentId,
        ...(runId === undefined ? {} : { runId }),
        turn,
        step,
      }
      recordSafely(options.sink, { kind: 'pre-step', ...coordinate })

      let reasonCode: DshHitlReasonCode | undefined
      if (options.hitl !== undefined) {
        try {
          reasonCode = options.hitl(coordinate)
        } catch {
          recordSafely(options.sink, {
            kind: 'hitl-denied',
            ...coordinate,
            reasonCode: 'policy-error',
            outcome: 'unavailable',
          })
          return { kind: 'reject' }
        }
      }
      if (reasonCode !== undefined && !isReasonCode(reasonCode)) {
        recordSafely(options.sink, {
          kind: 'hitl-denied',
          ...coordinate,
          reasonCode: 'policy-error',
          outcome: 'unavailable',
        })
        return { kind: 'reject' }
      }

      let durableRequestId: string | undefined
      if (reasonCode !== undefined) {
        try {
          durableRequestId = options.durableBridge === undefined
            ? undefined
            : await options.durableBridge.requestAdmissionHitl(coordinate, reasonCode)
        } catch {
          recordSafely(options.sink, {
            kind: 'hitl-denied',
            ...coordinate,
            reasonCode,
            outcome: 'unavailable',
          })
          return { kind: 'reject' }
        }
      }

      if (reasonCode === undefined) {
        try {
          await options.durableBridge?.preStep(coordinate)
          return await next()
        } catch {
          recordSafely(options.sink, {
            kind: 'hitl-denied',
            ...coordinate,
            reasonCode: 'policy-error',
            outcome: 'unavailable',
          })
          return { kind: 'reject' }
        }
      }

      let outcome: Exclude<DshApprovalOutcome, 'allowed-once'> | 'allowed-once'
      try {
        const requested = await ctx.approval.request({
          agent: payload.agent,
          toolName: SAFE_TOOL_NAME,
          reason: REASON_TEXT[reasonCode],
          signal: payload.signal,
        })
        if (requested === 'allowed-once') {
          if (durableRequestId !== undefined) {
            try {
              await options.durableBridge?.decideAdmissionHitl(coordinate, durableRequestId, 'allowed-once')
            } catch {
              recordSafely(options.sink, { kind: 'hitl-denied', ...coordinate, reasonCode, outcome: 'unavailable' })
              return { kind: 'reject' }
            }
          }
          try {
            await options.durableBridge?.preStep(coordinate)
            return await next()
          } catch {
            recordSafely(options.sink, { kind: 'hitl-denied', ...coordinate, reasonCode, outcome: 'unavailable' })
            return { kind: 'reject' }
          }
        }
        outcome = safeOutcome(requested)
      } catch {
        outcome = 'unavailable'
      }
      if (durableRequestId !== undefined) {
        try {
          await options.durableBridge?.decideAdmissionHitl(coordinate, durableRequestId, outcome)
        } catch {
          // The DSH turn is already closed; do not claim a decision reached the
          // journal if its durable append failed.
          outcome = 'unavailable'
        }
      }
      recordSafely(options.sink, {
        kind: 'hitl-denied',
        ...coordinate,
        reasonCode,
        outcome,
      })
      return { kind: 'reject' }
    })

    const disposeSessionEvent = ctx.on('session/event', (session, event) => {
      if (event.type !== 'step/end') return
      const agentId = safeAgentId(session.id)
      const runId = withRunId(agentId, options.correlateRun)
      const position = stepPosition(event.data)
      const eventSeq = safeEventSequence(event.seq)
      if (position === undefined || eventSeq === 0) return
      const step: DshCompletedStep = {
        agentId,
        ...(runId === undefined ? {} : { runId }),
        ...position,
        eventSeq,
      }
      try {
        options.durableBridge?.observeStepEnd(step)
      } catch {
        // Session facts are already durable in DSH. A bridge failure is checked
        // at the next pre-step/turn boundary and never converts into a fake
        // LoopGraph settlement here.
      }
      recordSafely(options.sink, { kind: 'step-ended', ...step })
    })

    const disposeTurnStopping = ctx.on('agent/turn-stopping', async (payload) => {
      const agentId = safeAgentId(payload.agent.id)
      const runId = withRunId(agentId, options.correlateRun)
      const coordinate: DshPreStepCoordinate = {
        agentId,
        ...(runId === undefined ? {} : { runId }),
        turn: safeCoordinate(payload.turn),
        step: 0,
      }
      try {
        await options.durableBridge?.reconcile(coordinate)
      } catch {
        // The completed DSH turn is still retained in its own event log. The
        // next LoopGraph boundary fails closed rather than guessing a result.
      }
      recordSafely(options.sink, {
        kind: 'turn-stopping',
        agentId,
        ...(runId === undefined ? {} : { runId }),
        turn: safeCoordinate(payload.turn),
      })
    })

    return () => {
      void disposePreStep()
      void disposeSessionEvent()
      void disposeTurnStopping()
    }
  }) as DshCordisPlugin
  Object.defineProperty(plugin, 'inject', { value: ['agents', 'approval', 'sessions'] as const })
  return plugin
}
