/**
 * Immutable, versioned execution graph. A complete copy is embedded in the
 * run journal, so replay never depends on mutable workflow code or a registry.
 */
export interface LoopNode {
  readonly nodeId: string;
  readonly kind: 'agent' | 'tool' | 'validator';
}

export interface LoopEdge {
  readonly fromNodeId: string;
  readonly toNodeId: string;
}

export interface LoopSpec {
  readonly specId: string;
  readonly revision: number;
  readonly entryNodeId: string;
  readonly nodes: readonly LoopNode[];
  readonly edges: readonly LoopEdge[];
}

const identifier = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function text(value: unknown): value is string { return typeof value === 'string' && identifier.test(value); }

/** Validate a bounded canonical graph before it becomes durable supervisor state. */
export function isValidLoopSpec(value: unknown): value is LoopSpec {
  if (!record(value) || !text(value.specId) || typeof value.revision !== 'number' || !Number.isSafeInteger(value.revision) || value.revision < 1
    || !text(value.entryNodeId) || !Array.isArray(value.nodes) || !Array.isArray(value.edges)
    || value.nodes.length < 1 || value.nodes.length > 64 || value.edges.length > 256) return false;
  let previousNode = '';
  const nodes = new Set<string>();
  for (const node of value.nodes) {
    if (!record(node) || !text(node.nodeId) || (node.kind !== 'agent' && node.kind !== 'tool' && node.kind !== 'validator')
      || node.nodeId <= previousNode || nodes.has(node.nodeId)) return false;
    previousNode = node.nodeId;
    nodes.add(node.nodeId);
  }
  if (!nodes.has(value.entryNodeId)) return false;
  let previousEdge = '';
  for (const edge of value.edges) {
    if (!record(edge) || !text(edge.fromNodeId) || !text(edge.toNodeId)
      || !nodes.has(edge.fromNodeId) || !nodes.has(edge.toNodeId)) return false;
    const key = `${edge.fromNodeId}\u0000${edge.toNodeId}`;
    if (key <= previousEdge) return false;
    previousEdge = key;
  }
  return true;
}

export function hasLoopEdge(spec: LoopSpec, fromNodeId: string, toNodeId: string): boolean {
  return spec.edges.some((edge) => edge.fromNodeId === fromNodeId && edge.toNodeId === toNodeId);
}

/** Version identity is unique even if an attacker tries to reuse it with new content. */
export function sameLoopSpecIdentity(left: LoopSpec, right: LoopSpec): boolean {
  return left.specId === right.specId && left.revision === right.revision;
}

/** Structural equality is used when an approval must bind the exact graph. */
export function sameLoopSpec(left: LoopSpec, right: LoopSpec): boolean {
  return left.specId === right.specId
    && left.revision === right.revision
    && left.entryNodeId === right.entryNodeId
    && left.nodes.length === right.nodes.length
    && left.edges.length === right.edges.length
    && left.nodes.every((node, index) => node.nodeId === right.nodes[index]?.nodeId && node.kind === right.nodes[index]?.kind)
    && left.edges.every((edge, index) => edge.fromNodeId === right.edges[index]?.fromNodeId && edge.toNodeId === right.edges[index]?.toNodeId);
}
