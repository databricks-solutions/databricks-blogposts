// Automatic layout and connector routing with ELK's layered algorithm.
//
// The models never emit coordinates: they describe structure, and this turns
// structure into a clean drawing. Layout input is a pure function of the
// document (fixed node sizes, document order, a fixed target shape), so every
// participant computes the same picture without positions travelling over the
// wire. Only nodes a person has dragged carry coordinates, and those override
// the layout.
//
// Connectors are routed by ELK together with node placement, so every
// connector gets its own track and space is reserved for it. Routing them
// afterwards, one at a time, is what produces lines through boxes and lines on
// top of each other.
//
// Two arrangements are tried and the one closer to a screen's shape wins:
//
//   together  one layered layout across the whole diagram, in one direction
//   bands     each top-level container laid out on its own, and the containers
//             then arranged along the other axis. A long flow becomes rows (or
//             columns) of containers instead of one very long strip. ELK cannot
//             mix directions in a single pass, so this is composed from one
//             layout per container plus one for the containers themselves,
//             joined at fixed ports on the container borders.

import type { ElkExtendedEdge, ElkNode, ElkPort } from 'elkjs/lib/elk-api'
import type { Doc } from './doc'
import { GROUP_MIN, GROUP_PAD, nodeSize } from './kinds'
import type { BEdge, Direction } from './types'

export interface Rect {
  /** Relative to the parent group (or the canvas for top-level nodes). */
  x: number
  y: number
  w: number
  h: number
}

export interface Point {
  x: number
  y: number
}

/** A connector's path through the automatic layout, in canvas coordinates. */
export interface Route {
  points: Point[]
  /** Where the label goes, if the connector has one. */
  label: Rect | null
}

export interface Layout {
  rects: Record<string, Rect>
  /** Routes for connectors the layout engine placed. Others fall back to simple elbows. */
  routes: Record<string, Route>
  /** Direction top-level things flow in. Differs from the board's when containers are banded. */
  rootDirection: Direction
}

type Side = 'NORTH' | 'SOUTH' | 'EAST' | 'WEST'
type Mins = Record<string, { w: number; h: number }>
type Mode = { kind: 'together'; direction: Direction } | { kind: 'bands'; outer: Direction }

interface Candidate extends Layout {
  mode: Mode
  width: number
  height: number
}

/** Anything that can solve an ELK graph: the library itself, or a worker running it. */
export interface LayoutEngine {
  layout(graph: ElkNode): Promise<ElkNode>
}
let engine: LayoutEngine | null = null

/**
 * Supply the ELK instance to use. The app passes one that runs in a web worker,
 * so a large diagram never stalls the page while it is being laid out.
 */
export function setLayoutEngine(e: LayoutEngine): void {
  engine = e
}

const solve = (graph: ElkNode): Promise<ElkNode> => {
  if (!engine) throw new Error('No layout engine: call setLayoutEngine() first')
  return engine.layout(graph)
}
const ROOT = '__root__'

// What matters is how large the diagram can be shown: a long strip that has to be
// shrunk to fit is unreadable. Arrangements are scored by the zoom at which they
// fit this reference canvas. It is a constant, not the viewer's window, so that
// everyone computes the same layout.
const CANVAS = { w: 1400, h: 800 }
// At or above this zoom the plain arrangement is fine and nothing else is tried.
const COMFORTABLE = 0.85
// An alternative has to be this much larger to replace the current best, so the
// picture does not flip back and forth as a diagram grows.
const SWITCH_GAIN = 1.12

// Spacing is read per (sub)graph, so the same values go on the root and on every container.
const SPACING: Record<string, string> = {
  'elk.layered.spacing.nodeNodeBetweenLayers': '52',
  'elk.spacing.nodeNode': '26',
  'elk.spacing.edgeNode': '20',
  'elk.spacing.edgeEdge': '12',
  'elk.layered.spacing.edgeNodeBetweenLayers': '20',
  'elk.layered.spacing.edgeEdgeBetweenLayers': '12',
  'elk.spacing.componentComponent': '48',
  'elk.spacing.portPort': '12',
}

const LAYERED = (direction: Direction): Record<string, string> => ({
  ...SPACING,
  'elk.algorithm': 'layered',
  'elk.direction': direction,
  'elk.edgeRouting': 'ORTHOGONAL',
  'elk.edgeLabels.inline': 'true',
  // Document order breaks ties (it keeps the picture stable as it grows) but never
  // decides which way a connector points: that comes from the connectors themselves.
  'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
  'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
  'elk.layered.nodePlacement.favorStraightEdges': 'true',
})

// How the bands themselves are placed. Chosen by trying every strategy on real
// boards: this one keeps the bands in a compact block, where the default slides
// each band sideways to straighten a connector and produces a wide staircase.
const BAND_OPTIONS: Record<string, string> = {
  'elk.layered.nodePlacement.strategy': 'BRANDES_KOEPF',
  'elk.layered.nodePlacement.bk.edgeStraightening': 'NONE',
}
// Container titles are small mono capitals; this is their start and per-character advance.
const TITLE_INSET = 16
const TITLE_CHAR_W = 7.3
const NO_PADDING = '[top=0,left=0,bottom=0,right=0]'
const GROUP_PADDING = `[top=${GROUP_PAD.top},left=${GROUP_PAD.side},bottom=${GROUP_PAD.bottom},right=${GROUP_PAD.side}]`

const flip = (d: Direction): Direction => (d === 'RIGHT' ? 'DOWN' : 'RIGHT')

/** Everything the layout depends on. Dragging a top-level node does not change it. */
export function layoutSignature(doc: Doc): string {
  const parts: string[] = [doc.direction]
  for (const id of doc.order) {
    const n = doc.nodes[id]
    const s = nodeSize(n)
    // A child dragged inside a container can make the container grow, and a resized
    // container needs room, so both are layout inputs. (Top-level drags are not.)
    const nudge =
      n.parent && n.pinned ? `@${Math.round((n.x ?? 0) / 4)},${Math.round((n.y ?? 0) / 4)}` : ''
    const size = n.type === 'group' ? `${n.w ?? ''}x${n.h ?? ''}` : `${s.w}x${s.h}`
    parts.push(`${id}:${n.type}:${n.parent ?? ''}:${size}${nudge}`)
  }
  parts.push('|')
  for (const e of Object.values(doc.edges)) parts.push(`${e.source}>${e.target}:${e.label.length}`)
  return parts.join(',')
}

// ---- the document as a tree -------------------------------------------------

interface Tree {
  childrenOf: Map<string | null, string[]>
  parentOf: (id: string) => string | null
  /** The top-level ancestor of a node (itself, if it is top-level). */
  topOf: (id: string) => string
  /** Is `a` an ancestor of `b`? */
  contains: (a: string, b: string) => boolean
}

function tree(doc: Doc): Tree {
  const parentOf = (id: string) => {
    const p = doc.nodes[id]?.parent
    return p && doc.nodes[p] ? p : null
  }
  const childrenOf = new Map<string | null, string[]>()
  for (const id of doc.order) {
    const parent = parentOf(id)
    const list = childrenOf.get(parent) ?? []
    list.push(id)
    childrenOf.set(parent, list)
  }
  const chain = (id: string) => {
    const out: string[] = []
    let cur = parentOf(id)
    while (cur && out.length < 24) {
      out.push(cur)
      cur = parentOf(cur)
    }
    return out
  }
  return {
    childrenOf,
    parentOf,
    topOf: (id) => chain(id).at(-1) ?? id,
    contains: (a, b) => chain(b).includes(a),
  }
}

/** Connectors the engine can route: both ends exist, and neither end contains the other. */
function routable(doc: Doc, t: Tree): BEdge[] {
  return Object.values(doc.edges).filter(
    (e) =>
      doc.nodes[e.source] &&
      doc.nodes[e.target] &&
      e.source !== e.target &&
      !t.contains(e.source, e.target) &&
      !t.contains(e.target, e.source),
  )
}

const elkEdge = (e: BEdge, source = e.source, target = e.target): ElkExtendedEdge => ({
  id: e.id,
  sources: [source],
  targets: [target],
  // Telling ELK about the label makes it leave a gap wide enough to hold it.
  labels: e.label
    ? [{ id: `${e.id}:label`, text: e.label, width: e.label.length * 6.4 + 14, height: 18 }]
    : [],
})

/** ELK nodes for everything under `parent`, nested containers included. */
function subtree(doc: Doc, t: Tree, parent: string | null, mins: Mins): ElkNode[] {
  return (t.childrenOf.get(parent) ?? []).map((id) => {
    const n = doc.nodes[id]
    if (n.type !== 'group') {
      const s = nodeSize(n)
      return { id, width: s.w, height: s.h }
    }
    const kids = subtree(doc, t, id, mins)
    if (!kids.length) {
      return { id, width: Math.max(GROUP_MIN.w, n.w ?? 0), height: Math.max(GROUP_MIN.h, n.h ?? 0) }
    }
    const min = mins[id]
    return {
      id,
      children: kids,
      layoutOptions: {
        ...SPACING,
        'elk.padding': GROUP_PADDING,
        ...(min
          ? {
              'elk.nodeSize.constraints': 'MINIMUM_SIZE',
              'elk.nodeSize.minimum': `(${Math.ceil(min.w)},${Math.ceil(min.h)})`,
            }
          : {}),
      },
    }
  })
}

/** Read node rectangles out of an ELK result. `at` is the canvas position of `res` itself. */
function harvestRects(
  res: ElkNode,
  at: Point,
  rects: Record<string, Rect>,
  abs: Record<string, Rect>,
) {
  for (const c of res.children ?? []) {
    const r = { x: c.x ?? 0, y: c.y ?? 0, w: c.width ?? 0, h: c.height ?? 0 }
    rects[c.id] = r
    abs[c.id] = { x: at.x + r.x, y: at.y + r.y, w: r.w, h: r.h }
    harvestRects(c, abs[c.id], rects, abs)
  }
}

/**
 * Read connector routes out of an ELK result. ELK reports each connector in the
 * coordinate system of its `container` (the innermost node enclosing both ends).
 */
function harvestRoutes(
  res: ElkNode,
  at: Point,
  abs: Record<string, Rect>,
  routes: Record<string, Route>,
) {
  const visit = (node: ElkNode) => {
    for (const e of (node.edges ?? []) as (ElkExtendedEdge & { container?: string })[]) {
      const section = e.sections?.[0]
      if (!section) continue
      const origin = !e.container || e.container === res.id ? at : (abs[e.container] ?? at)
      const shift = (p: Point): Point => ({ x: origin.x + p.x, y: origin.y + p.y })
      const label = e.labels?.[0]
      routes[e.id] = {
        points: [section.startPoint, ...(section.bendPoints ?? []), section.endPoint].map(shift),
        label:
          label && label.x !== undefined && label.y !== undefined
            ? { ...shift({ x: label.x, y: label.y }), w: label.width ?? 0, h: label.height ?? 0 }
            : null,
      }
    }
    for (const c of node.children ?? []) visit(c)
  }
  visit(res)
}

// ---- arrangement 1: one layout for everything -------------------------------

async function together(doc: Doc, t: Tree, direction: Direction, mins: Mins): Promise<Candidate> {
  const res = await solve({
    id: ROOT,
    layoutOptions: {
      ...LAYERED(direction),
      'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
      'elk.padding': NO_PADDING,
    },
    children: subtree(doc, t, null, mins),
    edges: routable(doc, t).map((e) => elkEdge(e)),
  })
  const rects: Record<string, Rect> = {}
  const abs: Record<string, Rect> = {}
  const routes: Record<string, Route> = {}
  harvestRects(res, { x: 0, y: 0 }, rects, abs)
  harvestRoutes(res, { x: 0, y: 0 }, abs, routes)
  return {
    rects,
    routes,
    rootDirection: direction,
    mode: { kind: 'together', direction },
    width: res.width ?? 1,
    height: res.height ?? 1,
  }
}

// ---- arrangement 2: containers as bands -------------------------------------

const SIDE_ORDER: Record<Direction, { out: Side[]; in: Side[] }> = {
  // Leave a container toward the next band and enter it from the previous one
  // where possible; fall back to the sides, then to the "wrong" end.
  DOWN: { out: ['SOUTH', 'EAST', 'WEST', 'NORTH'], in: ['NORTH', 'WEST', 'EAST', 'SOUTH'] },
  RIGHT: { out: ['EAST', 'SOUTH', 'NORTH', 'WEST'], in: ['WEST', 'NORTH', 'SOUTH', 'EAST'] },
}

function overlaps(a: Rect, b: Rect, pad: number): boolean {
  return (
    a.x < b.x + b.w + pad && a.x + a.w > b.x - pad && a.y < b.y + b.h + pad && a.y + a.h > b.y - pad
  )
}

/** The straight run from a node's side to its container's border, as a thin rectangle. */
function stub(node: Rect, side: Side, box: { w: number; h: number }, shift: number): Rect {
  const cx = node.x + node.w / 2 + shift
  const cy = node.y + node.h / 2 + shift
  switch (side) {
    case 'NORTH':
      return { x: cx, y: 0, w: 0, h: node.y }
    case 'SOUTH':
      return { x: cx, y: node.y + node.h, w: 0, h: box.h - node.y - node.h }
    case 'WEST':
      return { x: 0, y: cy, w: node.x, h: 0 }
    default:
      return { x: node.x + node.w, y: cy, w: box.w - node.x - node.w, h: 0 }
  }
}

async function bands(doc: Doc, t: Tree, outer: Direction, mins: Mins): Promise<Candidate> {
  const tops = t.childrenOf.get(null) ?? []
  const edges = routable(doc, t)
  const rects: Record<string, Rect> = {}
  const routes: Record<string, Route> = {}
  /** Rectangles of everything inside a container, relative to that container's corner. */
  const local: Record<string, Rect> = {}
  const localRoutes: Record<string, Record<string, Route>> = {}
  const box: Record<string, { w: number; h: number }> = {}

  // Level 1: the inside of each top-level container, flowing across the bands.
  for (const c of tops) {
    const n = doc.nodes[c]
    if (n.type !== 'group' || !(t.childrenOf.get(c) ?? []).length) continue
    const internal = edges.filter(
      (e) => e.source !== c && e.target !== c && t.topOf(e.source) === c && t.topOf(e.target) === c,
    )
    // A container with a flow inside runs across the bands. One that just holds a
    // set of things (sources, consumers) lines them up along its band instead.
    const direction = internal.length ? flip(outer) : outer
    const res = await solve({
      id: c,
      layoutOptions: {
        ...LAYERED(direction),
        'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
        'elk.padding': GROUP_PADDING,
      },
      children: subtree(doc, t, c, mins),
      edges: internal.map((e) => elkEdge(e)),
    })
    const inner: Record<string, Rect> = {}
    harvestRects(res, { x: 0, y: 0 }, rects, inner)
    Object.assign(local, inner)
    localRoutes[c] = {}
    harvestRoutes(res, { x: 0, y: 0 }, inner, localRoutes[c])
    box[c] = {
      w: Math.max(res.width ?? 0, mins[c]?.w ?? 0, n.w ?? 0),
      h: Math.max(res.height ?? 0, mins[c]?.h ?? 0, n.h ?? 0),
    }
    // The container itself can be the end of a connector; its "run to the border" is empty.
    local[c] = { x: 0, y: 0, w: box[c].w, h: box[c].h }
  }

  // Where each connector that leaves a container crosses its border: a port in
  // line with the shape it belongs to, on the first side with a clear run to it.
  interface PortSpec {
    edge: string
    end: 'source' | 'target'
    node: string
    top: string
    side: Side
    shift: number
  }
  const crossing = edges.filter((e) => t.topOf(e.source) !== t.topOf(e.target))
  const ports: PortSpec[] = []
  for (const e of crossing) {
    for (const end of ['source', 'target'] as const) {
      const node = e[end]
      const top = t.topOf(node)
      if (!box[top]) continue // a loose top-level shape connects directly
      // Things the run to the border must not cross: other shapes in this container,
      // and other containers nested in it (but not the ones this node sits inside).
      const others = (Object.keys(local) as string[]).filter(
        (id) =>
          id !== node &&
          id !== top &&
          t.topOf(id) === top &&
          !t.contains(node, id) &&
          !t.contains(id, node),
      )
      const order = SIDE_ORDER[outer][end === 'source' ? 'out' : 'in']
      const side =
        order.find(
          (s) => !others.some((o) => overlaps(stub(local[node], s, box[top], 0), local[o], 6)),
        ) ?? order[0]
      ports.push({ edge: e.id, end, node, top, side, shift: 0 })
    }
  }
  // Several connectors on the same side of a shape fan out instead of sharing a line.
  const fans = new Map<string, PortSpec[]>()
  for (const p of ports) {
    const key = `${p.node}|${p.side}`
    fans.set(key, [...(fans.get(key) ?? []), p])
  }
  for (const group of fans.values()) {
    const { node, side, top } = group[0]
    const r = local[node]
    const room = (side === 'NORTH' || side === 'SOUTH' ? r.w : r.h) - 24
    const step = group.length > 1 ? Math.min(14, room / (group.length - 1)) : 0
    // A container's title sits in its top-left corner. Connectors entering through
    // the top edge are nudged right of it (as far as the shape's width allows).
    let nudge = 0
    if (side === 'NORTH') {
      // Every container the run passes through on its way up has a title to clear:
      // the top-level one and any nested ones around this shape.
      let titleEnd = TITLE_INSET + doc.nodes[top].label.length * TITLE_CHAR_W + 10
      for (let c = t.parentOf(node); c && c !== top; c = t.parentOf(c)) {
        titleEnd = Math.max(
          titleEnd,
          local[c].x + TITLE_INSET + doc.nodes[c].label.length * TITLE_CHAR_W + 10,
        )
      }
      if (doc.nodes[node].type === 'group' && node !== top) {
        titleEnd = Math.max(
          titleEnd,
          r.x + TITLE_INSET + doc.nodes[node].label.length * TITLE_CHAR_W + 10,
        )
      }
      const first = r.x + r.w / 2 - ((group.length - 1) / 2) * step
      const limit = r.x + r.w - 14 - (group.length - 1) * step
      if (first < titleEnd && limit > first) nudge = Math.min(titleEnd, limit) - first
    }
    group.forEach((p, i) => (p.shift = (i - (group.length - 1) / 2) * step + nudge))
  }
  const portAt = (p: PortSpec): Point => {
    const s = stub(local[p.node], p.side, box[p.top], p.shift)
    if (p.side === 'NORTH') return { x: s.x, y: 0 }
    if (p.side === 'SOUTH') return { x: s.x, y: box[p.top].h }
    if (p.side === 'WEST') return { x: 0, y: s.y }
    return { x: box[p.top].w, y: s.y }
  }
  const nodeAt = (p: PortSpec): Point => {
    const s = stub(local[p.node], p.side, box[p.top], p.shift)
    if (p.side === 'NORTH') return { x: s.x, y: local[p.node].y }
    if (p.side === 'SOUTH') return { x: s.x, y: local[p.node].y + local[p.node].h }
    if (p.side === 'WEST') return { x: local[p.node].x, y: s.y }
    return { x: local[p.node].x + local[p.node].w, y: s.y }
  }
  const portId = (p: PortSpec) => `${p.edge}::${p.end}`
  const portOf = (edge: string, end: 'source' | 'target') =>
    ports.find((p) => p.edge === edge && p.end === end)

  // Level 2: the containers (and any loose top-level shapes) along the main axis.
  const res = await solve({
    id: ROOT,
    layoutOptions: {
      ...LAYERED(outer),
      'elk.padding': NO_PADDING,
      ...BAND_OPTIONS,
    },
    children: tops.map((id): ElkNode => {
      const n = doc.nodes[id]
      if (box[id]) {
        return {
          id,
          width: box[id].w,
          height: box[id].h,
          layoutOptions: { 'elk.portConstraints': 'FIXED_POS' },
          ports: ports
            .filter((p) => p.top === id)
            .map((p): ElkPort => ({
              id: portId(p),
              ...portAt(p),
              width: 0,
              height: 0,
              layoutOptions: { 'elk.port.side': p.side },
            })),
        }
      }
      if (n.type === 'group') {
        return {
          id,
          width: Math.max(GROUP_MIN.w, n.w ?? 0),
          height: Math.max(GROUP_MIN.h, n.h ?? 0),
        }
      }
      const s = nodeSize(n)
      return { id, width: s.w, height: s.h }
    }),
    edges: crossing.map((e) => {
      const from = portOf(e.id, 'source')
      const to = portOf(e.id, 'target')
      return elkEdge(e, from ? portId(from) : e.source, to ? portId(to) : e.target)
    }),
  })

  // Put the two levels together.
  const abs: Record<string, Rect> = {}
  for (const c of res.children ?? []) {
    rects[c.id] = { x: c.x ?? 0, y: c.y ?? 0, w: c.width ?? 0, h: c.height ?? 0 }
    abs[c.id] = rects[c.id]
  }
  for (const [top, inner] of Object.entries(localRoutes)) {
    const at = abs[top]
    for (const [id, r] of Object.entries(inner)) {
      routes[id] = {
        points: r.points.map((p) => ({ x: at.x + p.x, y: at.y + p.y })),
        label: r.label ? { ...r.label, x: at.x + r.label.x, y: at.y + r.label.y } : null,
      }
    }
  }
  const outerRoutes: Record<string, Route> = {}
  harvestRoutes(res, { x: 0, y: 0 }, abs, outerRoutes)
  for (const [id, r] of Object.entries(outerRoutes)) {
    const from = portOf(id, 'source')
    const to = portOf(id, 'target')
    const lift = (p: PortSpec): Point => ({
      x: abs[p.top].x + nodeAt(p).x,
      y: abs[p.top].y + nodeAt(p).y,
    })
    routes[id] = {
      points: [...(from ? [lift(from)] : []), ...r.points, ...(to ? [lift(to)] : [])],
      label: r.label,
    }
  }
  return {
    rects,
    routes,
    rootDirection: outer,
    mode: { kind: 'bands', outer },
    width: res.width ?? 1,
    height: res.height ?? 1,
  }
}

// ---- choosing ---------------------------------------------------------------

/**
 * Minimum sizes for containers that must be bigger than their contents need:
 * ones a person resized, and ones holding a child that was dragged past the edge.
 */
function containerMinimums(doc: Doc, t: Tree, rects: Record<string, Rect>): Mins {
  const mins: Mins = {}
  const raise = (id: string, w: number, h: number) => {
    const cur = mins[id] ?? { w: rects[id]?.w ?? 0, h: rects[id]?.h ?? 0 }
    mins[id] = { w: Math.max(cur.w, w), h: Math.max(cur.h, h) }
  }
  for (const id of doc.order) {
    const n = doc.nodes[id]
    const slot = rects[id]
    if (!slot) continue
    if (n.type === 'group' && ((n.w ?? 0) > slot.w + 0.5 || (n.h ?? 0) > slot.h + 0.5)) {
      raise(id, n.w ?? 0, n.h ?? 0)
    }
    const parent = t.parentOf(id)
    if (!parent || !rects[parent] || !n.pinned || n.x === null || n.y === null) continue
    const right = slot.x + n.x + (n.w ?? slot.w) + GROUP_PAD.side
    const bottom = slot.y + n.y + (n.h ?? slot.h) + GROUP_PAD.bottom
    if (right > rects[parent].w + 0.5 || bottom > rects[parent].h + 0.5)
      raise(parent, right, bottom)
  }
  return mins
}

/** The zoom at which a layout fits the reference canvas (capped at actual size). */
const fitZoom = (c: Candidate) => Math.min(1, CANVAS.w / c.width, CANVAS.h / c.height)

const run = (doc: Doc, t: Tree, mode: Mode, mins: Mins) =>
  mode.kind === 'together'
    ? together(doc, t, mode.direction, mins)
    : bands(doc, t, mode.outer, mins)

export async function computeLayout(doc: Doc): Promise<Layout> {
  if (!doc.order.length) return { rects: {}, routes: {}, rootDirection: doc.direction }
  const t = tree(doc)
  const top = t.childrenOf.get(null) ?? []
  const filled = top.filter(
    (id) => doc.nodes[id].type === 'group' && (t.childrenOf.get(id) ?? []).length,
  )

  // Default: one layered layout across everything, in the board's direction. If
  // that has to be shrunk a lot to fit a screen, try the alternatives and keep the
  // one that can be shown largest.
  let best = await run(doc, t, { kind: 'together', direction: doc.direction }, {})
  if (fitZoom(best) < COMFORTABLE) {
    const alternatives: Mode[] =
      filled.length >= 2
        ? [
            { kind: 'bands', outer: flip(doc.direction) },
            { kind: 'bands', outer: doc.direction },
          ]
        : // Without containers there is nothing to band. (Wrapping the flow onto
          // several rows was tried twice and rejected: branches loop around the
          // outside and the flow becomes hard to follow.) Turning it is the only option.
          [{ kind: 'together', direction: flip(doc.direction) }]
    for (const mode of alternatives) {
      const candidate = await run(doc, t, mode, {})
      if (fitZoom(candidate) > fitZoom(best) * SWITCH_GAIN) best = candidate
      if (fitZoom(best) >= COMFORTABLE) break // good enough: skip the rest
    }
  }
  // Second pass when a container has to be larger than its contents, so that its
  // neighbours are laid out around the larger box rather than underneath it.
  const mins = containerMinimums(doc, t, best.rects)
  if (Object.keys(mins).length) best = await run(doc, t, best.mode, mins)
  return { rects: best.rects, routes: best.routes, rootDirection: best.rootDirection }
}

/** Combine the automatic layout with hand-made offsets and sizes. */
export function resolveRects(doc: Doc, auto: Record<string, Rect>): Record<string, Rect> {
  const out: Record<string, Rect> = {}
  for (const id of doc.order) {
    const n = doc.nodes[id]
    const a = auto[id]
    if (!a) continue // not laid out yet; it appears once its slot is known
    const moved = n.pinned && n.x !== null && n.y !== null
    const size = nodeSize(n)
    out[id] = {
      x: a.x + (moved ? n.x! : 0),
      y: a.y + (moved ? n.y! : 0),
      // A container's size always comes from the layout, which already accounts
      // for its contents, any manual resize, and children dragged toward its edge.
      w: n.type === 'group' ? a.w : size.w,
      h: n.type === 'group' ? a.h : size.h,
    }
  }
  return out
}

/** Absolute canvas rectangle of a node, summing its ancestors' offsets. */
export function absoluteRect(doc: Doc, rects: Record<string, Rect>, id: string): Rect | null {
  const r = rects[id]
  if (!r) return null
  let x = r.x
  let y = r.y
  let cur = doc.nodes[id]?.parent
  let guard = 0
  while (cur && rects[cur] && guard++ < 20) {
    x += rects[cur].x
    y += rects[cur].y
    cur = doc.nodes[cur]?.parent ?? null
  }
  return { x, y, w: r.w, h: r.h }
}
