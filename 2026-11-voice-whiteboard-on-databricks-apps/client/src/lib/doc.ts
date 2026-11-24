// The diagram document and the pure function that applies ops to it.
// This mirrors server/board.py so optimistic local edits land exactly where the
// server will put them.

import type { BEdge, BNode, Direction, NodePatch, Op } from './types'

export interface Doc {
  title: string
  direction: Direction
  nodes: Record<string, BNode>
  /** Node ids in document order; the automatic layout follows it. */
  order: string[]
  edges: Record<string, BEdge>
}

export const emptyDoc = (): Doc => ({
  title: 'Untitled board',
  direction: 'RIGHT',
  nodes: {},
  order: [],
  edges: {},
})

const NODE_DEFAULTS: Omit<BNode, 'id'> = {
  type: 'shape',
  kind: 'service',
  label: '',
  detail: '',
  parent: null,
  x: null,
  y: null,
  w: null,
  h: null,
  pinned: false,
  color: null,
  touched: false,
  by: 'user',
}

/** Apply ops, returning a new document (structurally shared where unchanged). */
export function applyOps(doc: Doc, ops: Op[]): Doc {
  let { title, direction, nodes, order, edges } = doc
  let nodesCopied = false
  let edgesCopied = false
  const N = () => {
    if (!nodesCopied) {
      nodes = { ...nodes }
      order = [...order]
      nodesCopied = true
    }
    return nodes
  }
  const E = () => {
    if (!edgesCopied) {
      edges = { ...edges }
      edgesCopied = true
    }
    return edges
  }

  for (const op of ops) {
    switch (op.t) {
      case 'node.put': {
        const prev = nodes[op.node.id]
        const next: BNode = { ...NODE_DEFAULTS, ...prev, ...op.node }
        if (
          prev &&
          op.node.parent !== undefined &&
          op.node.parent !== prev.parent &&
          op.node.x === undefined
        ) {
          next.x = next.y = null
          next.pinned = false
        }
        if (next.x === null || next.y === null) {
          next.x = next.y = null
          next.pinned = false
        }
        N()[next.id] = next
        if (!prev) order.push(next.id)
        break
      }
      case 'node.del': {
        const gone = nodes[op.id]
        if (!gone) break
        N()
        delete nodes[op.id]
        order = order.filter((id) => id !== op.id)
        for (const id of order) {
          const child = nodes[id]
          if (child.parent === op.id) {
            nodes[id] = { ...child, parent: gone.parent, x: null, y: null, pinned: false }
          }
        }
        for (const e of Object.values(edges)) {
          if (e.source === op.id || e.target === op.id) delete E()[e.id]
        }
        break
      }
      case 'edge.put': {
        const patch = op.edge
        // One edge per ordered pair, as on the server: a repeat updates the existing one.
        const prev: BEdge | undefined =
          edges[patch.id] ??
          (patch.source && patch.target
            ? Object.values(edges).find(
                (e) => e.source === patch.source && e.target === patch.target,
              )
            : undefined)
        if (!prev && !(patch.source && patch.target && nodes[patch.source] && nodes[patch.target]))
          break
        const base: BEdge = prev ?? {
          id: patch.id,
          source: '',
          target: '',
          label: '',
          dashed: false,
          touched: false,
          by: 'user',
        }
        const next: BEdge = { ...base, ...patch, id: base.id }
        E()[next.id] = next
        break
      }
      case 'edge.del':
        if (edges[op.id]) delete E()[op.id]
        break
      case 'board.set':
        if (op.patch.title !== undefined) title = op.patch.title
        if (op.patch.direction !== undefined) direction = op.patch.direction
        break
      case 'board.reflow':
        N()
        for (const id of order) {
          nodes[id] = { ...nodes[id], x: null, y: null, w: null, h: null, pinned: false }
        }
        break
      case 'board.order': {
        N()
        const listed = op.ids.filter((id) => nodes[id])
        const seen = new Set(listed)
        order = [...listed, ...order.filter((id) => !seen.has(id))]
        break
      }
      case 'board.clear':
        nodes = {}
        order = []
        edges = {}
        nodesCopied = edgesCopied = true
        break
    }
  }
  return { title, direction, nodes, order, edges }
}

/** Ops that undo `ops` when applied to the document as it is *before* them. */
export function invertOps(doc: Doc, ops: Op[]): Op[] {
  const inverse: Op[] = []
  let cur = doc
  for (const op of ops) {
    const step: Op[] = []
    switch (op.t) {
      case 'node.put': {
        const prev = cur.nodes[op.node.id]
        if (!prev) step.push({ t: 'node.del', id: op.node.id })
        else {
          const back: NodePatch = { id: prev.id }
          for (const k of Object.keys(op.node) as (keyof BNode)[])
            Object.assign(back, { [k]: prev[k] })
          // Restoring a parent must restore the position that went with it.
          if ('parent' in op.node)
            Object.assign(back, { x: prev.x, y: prev.y, pinned: prev.pinned })
          step.push({ t: 'node.put', node: back })
        }
        break
      }
      case 'node.del': {
        const prev = cur.nodes[op.id]
        if (!prev) break
        step.push({ t: 'node.put', node: prev })
        for (const child of Object.values(cur.nodes)) {
          if (child.parent === op.id) {
            step.push({
              t: 'node.put',
              node: {
                id: child.id,
                parent: child.parent,
                x: child.x,
                y: child.y,
                pinned: child.pinned,
              },
            })
          }
        }
        for (const e of Object.values(cur.edges)) {
          if (e.source === op.id || e.target === op.id) step.push({ t: 'edge.put', edge: e })
        }
        break
      }
      case 'edge.put': {
        const prev = cur.edges[op.edge.id]
        if (!prev) step.push({ t: 'edge.del', id: op.edge.id })
        else step.push({ t: 'edge.put', edge: prev })
        break
      }
      case 'edge.del': {
        const prev = cur.edges[op.id]
        if (prev) step.push({ t: 'edge.put', edge: prev })
        break
      }
      case 'board.set':
        step.push({
          t: 'board.set',
          patch: {
            ...(op.patch.title !== undefined ? { title: cur.title } : {}),
            ...(op.patch.direction !== undefined ? { direction: cur.direction } : {}),
          },
        })
        break
      case 'board.reflow':
        for (const n of Object.values(cur.nodes)) {
          if (n.pinned || n.w !== null || n.h !== null) {
            step.push({
              t: 'node.put',
              node: { id: n.id, x: n.x, y: n.y, w: n.w, h: n.h, pinned: n.pinned },
            })
          }
        }
        break
      case 'board.order':
        step.push({ t: 'board.order', ids: cur.order })
        break
      case 'board.clear':
        for (const id of cur.order) step.push({ t: 'node.put', node: cur.nodes[id] })
        for (const e of Object.values(cur.edges)) step.push({ t: 'edge.put', edge: e })
        break
    }
    inverse.unshift(...step)
    cur = applyOps(cur, [op])
  }
  return inverse
}

/** Ancestor chain of a node, nearest first. */
export function ancestors(doc: Doc, id: string): string[] {
  const out: string[] = []
  let cur = doc.nodes[id]?.parent
  while (cur && doc.nodes[cur] && !out.includes(cur)) {
    out.push(cur)
    cur = doc.nodes[cur].parent
  }
  return out
}

export const newId = (prefix: string) => `${prefix}${Math.random().toString(36).slice(2, 10)}`
