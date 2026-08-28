/**
 * Real DSH composition contract. Run only through `npm run test:dsh`, which
 * supplies the checked-out DSH source resolver and installed workspace.
 */
import assert from 'node:assert/strict'
import { readFile, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import process from 'node:process'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import LlmRuntime, { LlmAdapter, createUserMessage } from '@deepseek-ai/dsh-llm'
import type { GenerateOptions, LlmResolvedModelInfo, StreamChunk } from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime from '@deepseek-ai/dsh-tools'
import ApprovalService from '@deepseek-ai/dsh-user-approval'
import type { ApprovalOutcome } from '@deepseek-ai/dsh-user-approval'
import { reduce } from '@loopgraph/core'
import { SqliteRunStore } from '@loopgraph/storage'
import { createDshAdapter, createDshDurableBridge } from '../src/index.ts'
import type { DshObservation } from '../src/index.ts'

class ScriptedAdapter extends LlmAdapter {
  readonly requests: GenerateOptions[] = []

  override resolveModel(provider: string, model: string): Promise<LlmResolvedModelInfo> {
    return Promise.resolve({ provider, id: model, name: model })
  }

  async *stream(options: GenerateOptions): AsyncIterable<StreamChunk> {
    this.requests.push(options)
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'text-delta', index: 0, text: 'ok' }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'ok' } }
    yield { type: 'usage', usage: { inputTokens: 1, outputTokens: 1 } }
    yield { type: 'finish', reason: { kind: 'stop' } }
  }
}

function safeShape(observation: DshObservation): Record<string, unknown> {
  return observation as unknown as Record<string, unknown>
}

async function settlesWithin<T>(promise: Promise<T>, milliseconds: number, message: string): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => { reject(new Error(message)) }, milliseconds)
      }),
    ])
  } finally {
    if (timeout !== undefined) clearTimeout(timeout)
  }
}

async function main(): Promise<void> {
  const productionSource = await readFile(new URL('../src/index.ts', import.meta.url), 'utf8')
  assert.doesNotMatch(productionSource, /@deepseek-ai\/dsh-agent-loop/, 'production adapter must not import the concrete loop')

  const durableDirectory = await mkdtemp(join(tmpdir(), 'loopgraph-dsh-composition-'))
  const databaseFile = join(durableDirectory, 'runs.sqlite')
  const durableBridge = createDshDurableBridge({
    databaseFile,
    artifactDirectory: join(durableDirectory, 'artifacts'),
    loopSpec: {
      specId: 'dsh-composition-loop', revision: 1, entryNodeId: 'dsh-pre-step',
      nodes: [{ nodeId: 'dsh-pre-step', kind: 'agent' }],
      edges: [{ fromNodeId: 'dsh-pre-step', toNodeId: 'dsh-pre-step' }],
    },
    actor: 'dsh-composition-test',
    holderId: 'dsh-composition-worker',
    leaseTtlMs: 10_000,
    validator: {
      async validate(step) {
        return { passed: true, evidenceRef: `evidence:dsh-step:${step.agentId}:${step.turn}:${step.step}:${step.eventSeq}` }
      },
    },
  })

  const ctx = new Context()
  const observations: DshObservation[] = []
  const adapter = new ScriptedAdapter()
  const cancellationAsked = Promise.withResolvers<AbortSignal>()
  const cancellationObserved = Promise.withResolvers<void>()
  const gatedAgentIds = new Set<string>()
  const runIdsByAgentId = new Map<string, string>()
  let approvedAgent: object | undefined
  let cancelledAgent: object | undefined
  let lateCancellationGrant: ((outcome: ApprovalOutcome) => void) | undefined
  let approvedPreStepNext = 0
  let allowedContinuations = 0

  await ctx.plugin(LlmRuntime)
  await ctx.plugin(SessionStore)
  await ctx.plugin(SystemPrompt)
  await ctx.plugin(ToolRuntime)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(ApprovalService, { policy: 'ask' })
  // The concrete loop is composition-only test infrastructure. The adapter does
  // not import or require it in production.
  await ctx.plugin(AgentLoop, { agents: [] })
  ctx.llm.registerAdapter(['probe'], adapter)
  ctx.on('approval/request', (request, next) => {
    if (request.agent === approvedAgent) return Promise.resolve<ApprovalOutcome>('allowed-once')
    if (request.agent !== cancelledAgent) return next()
    assert.ok(request.signal, 'adapter forwards the live pre-step signal to approval.request')
    cancellationAsked.resolve(request.signal)
    request.signal.addEventListener('abort', () => { cancellationObserved.resolve() }, { once: true })
    return new Promise<ApprovalOutcome>((resolve) => { lateCancellationGrant = resolve })
  })

  const adapterFiber = ctx.plugin(createDshAdapter({
    sink: {
      record(observation) {
        observations.push(observation)
        // A supervisor sink is non-authoritative: its failure cannot block DSH.
        throw new Error('intentional observer failure')
      },
    },
    correlateRun: (agentId) => runIdsByAgentId.get(agentId),
    hitl: ({ agentId }) => gatedAgentIds.has(agentId) ? 'promotion' : undefined,
    durableBridge,
  }))
  await adapterFiber
  ctx.on('agent/pre-step', async ({ agent }, next) => {
    if (agent === approvedAgent) approvedPreStepNext += 1
    return await next()
  })

  const allowed = await ctx.agents.create({
    sessionId: SessionId('loopgraph-allowed'),
    agentOptions: { provider: 'probe', model: 'probe-model' },
  })
  const gated = await ctx.agents.create({
    sessionId: SessionId('loopgraph-gated'),
    agentOptions: { provider: 'probe', model: 'probe-model' },
  })
  const approved = await ctx.agents.create({
    sessionId: SessionId('loopgraph-approved'),
    agentOptions: { provider: 'probe', model: 'probe-model' },
  })
  const cancelled = await ctx.agents.create({
    sessionId: SessionId('loopgraph-cancelled'),
    agentOptions: { provider: 'probe', model: 'probe-model' },
  })
  runIdsByAgentId.set(allowed.agent.id, 'run-allowed')
  runIdsByAgentId.set(gated.agent.id, 'run-gated')
  runIdsByAgentId.set(approved.agent.id, 'run-approved')
  runIdsByAgentId.set(cancelled.agent.id, 'run-cancelled')
  gatedAgentIds.add(gated.agent.id)
  gatedAgentIds.add(approved.agent.id)
  gatedAgentIds.add(cancelled.agent.id)
  approvedAgent = approved.agent
  cancelledAgent = cancelled.agent
  ctx.on('agent/turn-stopping', ({ agent }) => {
    if (agent !== allowed.agent || allowedContinuations !== 0) return
    allowedContinuations += 1
    allowed.agent.steer(createUserMessage({
      content: [{ type: 'text', text: 'internal continuation' }],
      source: { kind: 'plugin', plugin: 'loopgraph-composition-test' },
    }))
  })

  allowed.agent.followup(createUserMessage({
    content: [{ type: 'text', text: 'raw allowed prompt must never enter observations' }],
    source: { kind: 'user' },
  }))
  await allowed.agent.whenIdle()

  gated.agent.followup(createUserMessage({
    content: [{ type: 'text', text: 'raw gated prompt must never enter observations' }],
    source: { kind: 'user' },
  }))
  await gated.agent.whenIdle()

  approved.agent.followup(createUserMessage({
    content: [{ type: 'text', text: 'raw approved prompt must never enter observations' }],
    source: { kind: 'user' },
  }))
  await approved.agent.whenIdle()

  cancelled.agent.followup(createUserMessage({
    content: [{ type: 'text', text: 'raw cancelled prompt must never enter observations' }],
    source: { kind: 'user' },
  }))
  const cancellationSignal = await settlesWithin(cancellationAsked.promise, 2_000, 'approval answerer did not receive a request')
  assert.equal(cancellationSignal.aborted, false, 'the pending approval starts before cancellation')
  const cancelledIdle = cancelled.agent.whenIdle()
  cancelled.agent.cancel({ kind: 'user' })
  await settlesWithin(cancellationObserved.promise, 2_000, 'approval answerer did not observe turn cancellation')
  await settlesWithin(cancelledIdle, 2_000, 'cancelled approval did not settle the turn')
  lateCancellationGrant?.('allowed-once')
  await new Promise<void>((resolve) => { setTimeout(resolve, 0) })

  assert.equal(ctx.agents.get(allowed.agent.id), allowed.agent, 'adapter composes with public ctx.agents')
  assert.equal(adapter.requests.length, 3, 'the ungated multi-step loop and allowed-once approved step make model requests')
  assert.equal(approvedPreStepNext, 1, 'allowed-once approval reaches the downstream pre-step continuation exactly once')
  assert.deepEqual(
    approved.agent.session.events
      .filter(event => event.type.startsWith('approval/'))
      .map(event => event.type === 'approval/decided' ? { type: event.type, outcome: event.data.outcome } : { type: event.type }),
    [{ type: 'approval/asked' }, { type: 'approval/decided', outcome: 'allowed-once' }],
    'real answerer grant has a paired durable approval audit',
  )
  const approvedApprovalIds = approved.agent.session.events
    .filter(event => event.type === 'approval/asked' || event.type === 'approval/decided')
    .map(event => {
      if (event.type === 'approval/asked' || event.type === 'approval/decided') return event.data.id
      throw new Error('approval audit filter returned a non-approval fact')
    })
  assert.equal(new Set(approvedApprovalIds).size, 1, 'approved audit facts share one approval request id')
  assert.deepEqual(
    gated.agent.session.events
      .filter(event => event.type.startsWith('approval/'))
      .map(event => event.type === 'approval/decided' ? { type: event.type, outcome: event.data.outcome } : { type: event.type }),
    [{ type: 'approval/asked' }, { type: 'approval/decided', outcome: 'unavailable' }],
    'missing answerer must leave durable asked and denied facts',
  )
  assert.deepEqual(
    cancelled.agent.session.events
      .filter(event => event.type.startsWith('approval/'))
      .map(event => event.type === 'approval/decided' ? { type: event.type, outcome: event.data.outcome } : { type: event.type }),
    [{ type: 'approval/asked' }, { type: 'approval/decided', outcome: 'cancelled' }],
    'turn cancellation must settle the pending approval closed',
  )
  assert.equal(adapter.requests.length, 3, 'a late grant after cancellation cannot start a model request')
  assert.ok(
    observations.some(observation => observation.kind === 'hitl-denied'
      && observation.agentId === cancelled.agent.id
      && observation.outcome === 'cancelled'),
    'adapter records the cancelled approval as a denied pre-step',
  )
  for (const observation of observations) {
    const shape = safeShape(observation)
    assert.deepEqual(Object.keys(shape).sort(), Object.keys(shape).filter(key => [
      'kind', 'agentId', 'runId', 'turn', 'step', 'eventSeq', 'reasonCode', 'outcome',
    ].includes(key)).sort(), 'observation contains only the safe schema')
  }
  assert.equal(JSON.stringify(observations).includes('raw allowed prompt'), false)
  assert.equal(JSON.stringify(observations).includes('raw gated prompt'), false)
  assert.equal(JSON.stringify(observations).includes('raw approved prompt'), false)
  assert.equal(JSON.stringify(observations).includes('raw cancelled prompt'), false)
  assert.equal(JSON.stringify(observations).includes('ok'), false)
  assert.deepEqual(
    gated.agent.session.events.filter(event => event.type === 'turn/end').map(event => event.data.reason),
    [{ kind: 'blocked' }],
  )
  assert.deepEqual(
    cancelled.agent.session.events.filter(event => event.type === 'turn/end').map(event => event.data.reason),
    [{ kind: 'aborted', reason: { kind: 'user' } }],
  )

  // The real DSH lifecycle above is also a real core composition: it must
  // persist a correlation-bound execution/validation/admission-HITL audit stream in
  // SQLite, not merely emit observer records from the Cordis plugin.
  durableBridge.close()
  const durableStore = new SqliteRunStore({ filename: databaseFile })
  const allowedEvents = durableStore.read('run-allowed')
  const approvedEvents = durableStore.read('run-approved')
  const deniedEvents = durableStore.read('run-gated')
  const cancelledEvents = durableStore.read('run-cancelled')
  const allowedView = reduce(allowedEvents)
  const approvedView = reduce(approvedEvents)
  const deniedView = reduce(deniedEvents)
  const cancelledView = reduce(cancelledEvents)
  durableStore.close()
  assert.deepEqual(
    allowedEvents.map(event => event.type),
    ['run.created', 'lease.acquired', 'node.dispatch.requested', 'node.started', 'node.settled', 'validation.recorded', 'validation.superseded', 'node.dispatch.requested', 'node.started', 'node.settled', 'validation.recorded'],
    'two real DSH steps advance through a durable validated-to-next-edge loop without synthetic early settlement',
  )
  assert.equal(allowedView.phase, 'VALIDATED')
  assert.equal(allowedView.validation.attemptId, `dsh:${allowed.agent.id}:turn:1:step:2`)
  assert.ok(approvedEvents.length >= 8, 'approved DSH pre-step persists creation, lease, admission, execution, and validation events')
  assert.ok(approvedEvents.every(event => event.correlationId === null || event.correlationId === `dsh:${approved.agent.id}:1:1`), 'approved core events retain their DSH correlation id')
  assert.deepEqual(
    approvedEvents.map(event => event.type),
    ['run.created', 'lease.acquired', 'admission.hitl.requested', 'admission.hitl.decided', 'node.dispatch.requested', 'node.started', 'node.settled', 'validation.recorded'],
    'allowed-once DSH approval authorizes execution before the real step is journaled and independently validated',
  )
  assert.equal(approvedView.phase, 'VALIDATED')
  assert.equal(approvedView.validation.status, 'PASSED')
  assert.equal(approvedView.admissionHitl.status, 'APPROVED', 'allowed-once is the only durable approvable admission outcome')
  assert.equal(approvedView.hitl.status, 'NOT_REQUESTED', 'execution admission never masquerades as version-promotion authority')
  assert.deepEqual(
    deniedEvents.map(event => event.type),
    ['run.created', 'lease.acquired', 'admission.hitl.requested', 'admission.hitl.decided'],
    'denied DSH approval persists the admission audit without beginning external execution',
  )
  assert.equal(deniedView.phase, 'READY')
  assert.equal(deniedView.admissionHitl.status, 'UNAVAILABLE')
  assert.equal(deniedView.promotedGenerations.length, 0, 'denied DSH approval cannot promote a core generation')
  assert.equal(cancelledView.phase, 'READY')
  assert.equal(cancelledView.admissionHitl.status, 'UNAVAILABLE', 'cancelled DSH approval also fails closed in the core journal')

  const observationCountBeforeDispose = observations.length
  await adapterFiber.dispose()
  allowed.agent.followup(createUserMessage({
    content: [{ type: 'text', text: 'post-disposal turn' }],
    source: { kind: 'user' },
  }))
  await allowed.agent.whenIdle()
  assert.equal(adapter.requests.length, 4, 'adapter disposal leaves no gating authority')
  assert.equal(observations.length, observationCountBeforeDispose, 'adapter disposal removes lifecycle observers')

  await allowed.dispose()
  await gated.dispose()
  await approved.dispose()
  await cancelled.dispose()
  await ctx.fiber.dispose()
  await rm(durableDirectory, { recursive: true, force: true })
  console.log(JSON.stringify({
    dshCommit: process.env.DSH_COMMIT ?? 'unknown',
    externalPluginMount: 'validated',
    allowedModelRequests: 2,
    approvedModelRequests: 1,
    deniedModelRequests: 0,
    cancelledModelRequests: 0,
    approvalOutcomes: ['unavailable', 'allowed-once', 'cancelled'],
    disposal: 'validated',
  }))
}

await main()
