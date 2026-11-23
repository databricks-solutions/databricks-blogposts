import {
  Background,
  BackgroundVariant,
  ConnectionMode,
  MarkerType,
  MiniMap,
  ReactFlow,
  SelectionMode,
  useReactFlow,
  useViewport,
  type Connection,
  type EdgeChange,
  type NodeChange,
} from '@xyflow/react'
import { LocateFixed, Maximize, Minus, Plus } from 'lucide-react'
import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react'
import { ancestors, applyOps, newId, type Doc } from '@/lib/doc'
import { GROUP_PAD, KIND_SPECS, SWATCHES, nodeSize, swatchFor } from '@/lib/kinds'
import {
  absoluteRect,
  computeLayout,
  layoutSignature,
  resolveRects,
  type Layout,
  type Point,
  type Rect,
} from '@/lib/layout'
import { live } from '@/lib/live'
import { meetDiamond, simplify } from '@/lib/path'
import { chooseSides } from '@/lib/route'
import { useSession } from '@/lib/session'
import type { BNode, Kind, Op } from '@/lib/types'
import { useUi } from '@/lib/ui'
import { edgeTypes, type BoardEdge } from './edges'
import { nodeTypes, type BoardNode, type NodeData } from './nodes'
import { IconButton } from './Tip'

// Margins that keep a fitted diagram clear of the floating palette and mic dock.
const INSET = { left: 116, top: 28, right: 28, bottom: 152 }
// Below this zoom, labels stop being readable; following the drawing never goes under it.
const LEGIBLE_ZOOM = 0.8
const LAYOUT_DEBOUNCE_MS = 30
const reducedMotion = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches

/**
 * Ease rectangles toward their targets frame by frame. Animating in state
 * (rather than with CSS transforms) keeps connectors attached while nodes glide.
 */
function useAnimatedRects(targets: Record<string, Rect>): [Record<string, Rect>, number] {
  const cur = useRef<Record<string, Rect>>({})
  const [frame, bump] = useReducer((n: number) => n + 1, 0)
  useEffect(() => {
    const c = cur.current
    for (const id of Object.keys(c)) if (!targets[id]) delete c[id]
    for (const id of Object.keys(targets)) if (!c[id]) c[id] = { ...targets[id] }
    let raf = 0
    let last = performance.now()
    const instant = reducedMotion()
    const tick = (now: number) => {
      const k = instant ? 1 : 1 - Math.exp(-Math.min(48, now - last) / 80)
      last = now
      const held = useUi.getState().held
      let moving = false
      for (const id of Object.keys(targets)) {
        const r = c[id]
        if (held[id]) {
          c[id] = { ...held[id] }
          continue
        }
        const t = targets[id]
        for (const key of ['x', 'y', 'w', 'h'] as const) {
          const d = t[key] - r[key]
          if (Math.abs(d) > 0.4) {
            r[key] += d * k
            moving = true
          } else r[key] = t[key]
        }
      }
      bump()
      if (moving) raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  }, [targets])
  return [cur.current, frame]
}

/** Deepest group whose rectangle contains the point, ignoring `exclude` and its descendants. */
function containerAt(
  doc: Doc,
  rects: Record<string, Rect>,
  x: number,
  y: number,
  exclude?: string,
) {
  let best: string | null = null
  let bestDepth = -1
  for (const id of doc.order) {
    const n = doc.nodes[id]
    if (n.type !== 'group' || id === exclude) continue
    const chain = ancestors(doc, id)
    if (exclude && chain.includes(exclude)) continue
    const r = absoluteRect(doc, rects, id)
    if (r && x >= r.x && x <= r.x + r.w && y >= r.y && y <= r.y + r.h && chain.length > bestDepth) {
      best = id
      bestDepth = chain.length
    }
  }
  return best
}

/**
 * Apply a structural edit while keeping chosen nodes exactly where they are on
 * screen. The edit changes the automatic layout, so those nodes' offsets are
 * recomputed against their new slots and committed together with the edit.
 * `place` returns, per node id, the position (relative to its parent) it should end at.
 */
async function commitHolding(
  ops: Op[],
  place: (next: Doc, rects: Record<string, Rect>) => Record<string, { x: number; y: number }>,
) {
  const { doc, commit } = useSession.getState()
  const next = applyOps(doc, ops)
  let offsets: Op[] = []
  try {
    const layout = await computeLayout(next)
    const wanted = place(next, resolveRects(next, layout.rects))
    offsets = Object.entries(wanted)
      .filter(([id]) => layout.rects[id])
      .map(([id, p]) => ({
        t: 'node.put' as const,
        node: {
          id,
          x: Math.round(p.x - layout.rects[id].x),
          y: Math.round(p.y - layout.rects[id].y),
          pinned: true,
        },
      }))
  } catch (err) {
    console.error('layout failed', err)
  }
  commit([...ops, ...offsets])
}

/** Create a node by hand at a canvas point (its centre), inside whatever group is there. */
export function useAddNode() {
  const rf = useReactFlow()
  return useCallback(
    (kind: Kind | 'group', at?: { x: number; y: number }) => {
      const { doc } = useSession.getState()
      const pane = document.querySelector('.react-flow')?.getBoundingClientRect()
      const centre =
        at ??
        rf.screenToFlowPosition({
          x: (pane?.left ?? 0) + (pane?.width ?? 800) / 2 + (Math.random() - 0.5) * 80,
          y: (pane?.top ?? 0) + (pane?.height ?? 600) / 2 - 60 + (Math.random() - 0.5) * 80,
        })
      const id = newId(kind === 'group' ? 'g_' : 'n_')
      const draft: BNode = {
        id,
        type: kind === 'group' ? 'group' : 'shape',
        kind: kind === 'group' ? 'service' : kind,
        label: '',
        detail: '',
        parent: kind === 'group' ? null : containerAt(doc, live.rects, centre.x, centre.y),
        x: null,
        y: null,
        w: kind === 'group' ? 320 : null,
        h: kind === 'group' ? 200 : null,
        pinned: false,
        color: null,
        touched: true,
        by: 'user',
      }
      const size = nodeSize(draft)
      void commitHolding([{ t: 'node.put', node: draft }], (next, rects) => {
        const origin = draft.parent ? absoluteRect(next, rects, draft.parent) : null
        return {
          [id]: {
            x: centre.x - size.w / 2 - (origin?.x ?? 0),
            y: centre.y - size.h / 2 - (origin?.y ?? 0),
          },
        }
      }).then(() => {
        useUi.getState().select([id])
        useUi.getState().editNode(id)
      })
    },
    [rf],
  )
}

function PeerCursors() {
  useViewport() // re-render on pan and zoom
  const rf = useReactFlow()
  const peers = useSession((s) => s.peers)
  const me = useSession((s) => s.me)
  const pane = document.querySelector('.react-flow')?.getBoundingClientRect()
  if (!pane) return null
  return (
    <div className="pointer-events-none absolute inset-0 z-20 overflow-hidden" aria-hidden>
      {peers
        .filter((p) => p.id !== me?.id && p.cursor)
        .map((p) => {
          const pos = rf.flowToScreenPosition({ x: p.cursor![0], y: p.cursor![1] })
          return (
            <div
              key={p.id}
              className="absolute top-0 left-0 transition-transform duration-75 ease-linear"
              style={{ transform: `translate(${pos.x - pane.left}px, ${pos.y - pane.top}px)` }}
            >
              <svg width="16" height="18" viewBox="0 0 16 18" className="drop-shadow-sm">
                <path
                  d="M1 1l13 7.2-5.6 1.5L5.6 16z"
                  fill={p.color}
                  stroke="#fff"
                  strokeWidth="1.2"
                  strokeLinejoin="round"
                />
              </svg>
              <span
                className="absolute top-4 left-3 rounded px-1.5 py-px font-mono text-[10px] whitespace-nowrap text-white"
                style={{ background: p.color }}
              >
                {p.name}
              </span>
            </div>
          )
        })}
    </div>
  )
}

function ViewControls({ onFit }: { onFit: () => void }) {
  const rf = useReactFlow()
  const follow = useUi((s) => s.follow)
  const setFollow = useUi((s) => s.setFollow)
  const { zoom } = useViewport()
  return (
    <div className="absolute bottom-4 left-4 z-10 flex items-center gap-0.5 rounded-lg border border-line bg-white p-1 shadow-float">
      <IconButton label="Zoom out" side="top" onClick={() => rf.zoomOut({ duration: 160 })}>
        <Minus size={15} aria-hidden />
      </IconButton>
      <span className="tabular w-10 text-center font-mono text-[11px] text-body">
        {Math.round(zoom * 100)}%
      </span>
      <IconButton label="Zoom in" side="top" onClick={() => rf.zoomIn({ duration: 160 })}>
        <Plus size={15} aria-hidden />
      </IconButton>
      <IconButton label="Fit the whole diagram" side="top" onClick={onFit}>
        <Maximize size={15} aria-hidden />
      </IconButton>
      <IconButton
        label={follow ? 'Following the drawing' : 'Follow the drawing as it grows'}
        side="top"
        active={follow}
        onClick={() => {
          setFollow(!follow)
          if (!follow) onFit()
        }}
      >
        <LocateFixed size={15} aria-hidden />
      </IconButton>
    </div>
  )
}

export function Canvas() {
  const doc = useSession((s) => s.doc)
  const peers = useSession((s) => s.peers)
  const me = useSession((s) => s.me)
  const flashes = useSession((s) => s.flashes)
  const lastAiChange = useSession((s) => s.lastAiChange)
  const status = useSession((s) => s.status)
  const commit = useSession((s) => s.commit)
  const send = useSession((s) => s.send)
  const selectedNodes = useUi((s) => s.selectedNodes)
  const selectedEdges = useUi((s) => s.selectedEdges)
  const held = useUi((s) => s.held)
  const follow = useUi((s) => s.follow)
  const rf = useReactFlow()
  const addNode = useAddNode()
  const wrapper = useRef<HTMLDivElement>(null)

  // ---- layout ---------------------------------------------------------------
  const signature = useMemo(() => layoutSignature(doc), [doc])
  const [layout, setLayout] = useState<Layout>({ rects: {}, routes: {}, rootDirection: 'RIGHT' })
  useEffect(() => {
    let alive = true
    // While the AI is streaming, changes arrive in quick bursts. A short pause lets a
    // burst settle into one layout instead of computing several that are thrown away.
    const timer = window.setTimeout(() => {
      computeLayout(useSession.getState().doc)
        .then((r) => alive && setLayout(r))
        .catch((err) => console.error('layout failed', err))
    }, LAYOUT_DEBOUNCE_MS)
    return () => {
      alive = false
      window.clearTimeout(timer)
    }
  }, [signature])
  /** Where the automatic layout puts each node, in canvas coordinates. */
  const slots = useMemo(() => {
    const out: Record<string, Rect> = {}
    for (const id of doc.order) {
      const r = absoluteRect(doc, layout.rects, id)
      if (r) out[id] = r
    }
    return out
  }, [doc, layout])
  const targets = useMemo(() => resolveRects(doc, layout.rects), [doc, layout])
  const [rects, frame] = useAnimatedRects(targets)
  live.rects = targets
  live.auto = layout.rects

  // ---- React Flow nodes and edges ------------------------------------------
  const peerSelection = useMemo(() => {
    const map: Record<string, { color: string; name: string }> = {}
    for (const p of peers) {
      if (p.id === me?.id) continue
      for (const id of p.selection) map[id] ??= { color: p.color, name: p.name }
    }
    return map
  }, [peers, me])

  const dataCache = useRef<Record<string, NodeData>>({})
  const nodes = useMemo<BoardNode[]>(() => {
    const selected = new Set(selectedNodes)
    const depth = (id: string) => ancestors(doc, id).length
    const ids = doc.order.filter((id) => rects[id]).sort((a, b) => depth(a) - depth(b))
    const now = Date.now()
    const cache: Record<string, NodeData> = {}
    const out = ids.map((id) => {
      const n = doc.nodes[id]
      const r = held[id] ?? rects[id]
      const flash = flashes[id] && now - flashes[id] < 1500 ? flashes[id] : 0
      const peer = peerSelection[id] ?? null
      const solo = selected.size === 1
      const prev = dataCache.current[id]
      // Reuse the data object when nothing in it changed, so nodes skip re-rendering mid-glide.
      const data =
        prev &&
        prev.node === n &&
        prev.flash === flash &&
        prev.peer === peer &&
        prev.soloSelected === solo
          ? prev
          : { node: n, flash, peer, soloSelected: solo }
      cache[id] = data
      return {
        id,
        type: n.type === 'group' ? 'frame' : 'shape',
        position: { x: r.x, y: r.y },
        width: r.w,
        height: r.h,
        // Sizes are known up front, so nodes are interactive without waiting to be measured.
        measured: { width: r.w, height: r.h },
        parentId: n.parent && rects[n.parent] ? n.parent : undefined,
        selected: selected.has(id),
        data,
      } satisfies BoardNode
    })
    dataCache.current = cache
    return out
    // `frame` advances while rectangles are easing toward their targets.
  }, [doc, rects, frame, held, selectedNodes, flashes, peerSelection])

  const edges = useMemo<BoardEdge[]>(() => {
    const selected = new Set(selectedEdges)
    // Where things are right now (mid-glide or mid-drag included) and where they will settle.
    const shown = { ...rects, ...held }
    const now: Record<string, Rect> = {}
    const settled: Record<string, Rect> = {}
    for (const id of doc.order) {
      const a = absoluteRect(doc, shown, id)
      if (a) now[id] = a
      const b = absoluteRect(doc, targets, id)
      if (b) settled[id] = b
    }
    const shapes = doc.order.filter((id) => doc.nodes[id].type === 'shape' && settled[id])
    const near = (a: number, b: number) => Math.abs(a - b) < 0.75

    return Object.values(doc.edges)
      .filter((e) => settled[e.source] && settled[e.target] && now[e.source] && now[e.target])
      .map((e) => {
        // The engine's route is valid when both ends sit in their automatic slots, or
        // have been carried the same distance from them (a whole container dragged, or
        // everything gliding together): then the route simply travels with them.
        const route = layout.routes[e.id]
        const a = now[e.source]
        const b = now[e.target]
        const sa = slots[e.source]
        const sb = slots[e.target]
        let path: Point[] | null = null
        let labelAt: Point | null = null
        if (
          route &&
          sa &&
          sb &&
          near(a.w, sa.w) &&
          near(a.h, sa.h) &&
          near(b.w, sb.w) &&
          near(b.h, sb.h)
        ) {
          const dx = a.x - sa.x
          const dy = a.y - sa.y
          if (near(dx, b.x - sb.x) && near(dy, b.y - sb.y)) {
            path = simplify(route.points.map((p) => ({ x: p.x + dx, y: p.y + dy })))
            if (path.length >= 2) {
              if (doc.nodes[e.source].kind === 'decision' && doc.nodes[e.source].type === 'shape') {
                path[0] = meetDiamond(path[0], path[1], a)
              }
              if (doc.nodes[e.target].kind === 'decision' && doc.nodes[e.target].type === 'shape') {
                path[path.length - 1] = meetDiamond(path[path.length - 1], path[path.length - 2], b)
              }
              if (route.label) {
                labelAt = {
                  x: route.label.x + route.label.w / 2 + dx,
                  y: route.label.y + route.label.h / 2 + dy,
                }
              }
            } else path = null
          }
        }
        return {
          id: e.id,
          source: e.source,
          target: e.target,
          type: 'floating' as const,
          selected: selected.has(e.id),
          data: {
            edge: e,
            route: path,
            labelAt,
            // Fallback elbow, chosen against where nodes will end up rather than where
            // they are mid-glide, so it does not change sides while things are moving.
            sides: path
              ? null
              : chooseSides(
                  settled[e.source],
                  settled[e.target],
                  shapes
                    .filter((id) => id !== e.source && id !== e.target)
                    .map((id) => settled[id]),
                  // Inside a container, connectors follow the board's direction; between
                  // containers they follow whichever way the containers were arranged.
                  doc.nodes[e.source].parent === doc.nodes[e.target].parent
                    ? doc.direction
                    : layout.rootDirection,
                ),
          },
          markerEnd: {
            type: MarkerType.ArrowClosed,
            width: 16,
            height: 16,
            color: selected.has(e.id) ? '#2272B4' : '#618794',
          },
        }
      })
    // `frame` advances while rectangles are easing toward their targets.
  }, [doc, layout, slots, targets, rects, frame, held, selectedEdges])

  // ---- selection and dragging ----------------------------------------------
  const onNodesChange = useCallback((changes: NodeChange<BoardNode>[]) => {
    const ui = useUi.getState()
    let selection: Set<string> | null = null
    for (const c of changes) {
      if (c.type === 'select') {
        selection ??= new Set(ui.selectedNodes)
        if (c.selected) selection.add(c.id)
        else selection.delete(c.id)
      } else if (c.type === 'position' && c.dragging && c.position) {
        const r = live.rects[c.id]
        if (r) ui.hold(c.id, { x: c.position.x, y: c.position.y, w: r.w, h: r.h })
      }
    }
    if (selection) ui.select([...selection], ui.selectedEdges)
  }, [])

  const onEdgesChange = useCallback((changes: EdgeChange<BoardEdge>[]) => {
    const ui = useUi.getState()
    const selection = new Set(ui.selectedEdges)
    let changed = false
    for (const c of changes) {
      if (c.type !== 'select') continue
      changed = true
      if (c.selected) selection.add(c.id)
      else selection.delete(c.id)
    }
    if (changed) ui.select(ui.selectedNodes, [...selection])
  }, [])

  const dragOrigin = useRef<Record<string, Pick<BNode, 'x' | 'y' | 'pinned' | 'parent'>>>({})
  const lastDragSend = useRef(0)

  const onNodeDragStart = useCallback((_: unknown, __: BoardNode, dragged: BoardNode[]) => {
    const d = useSession.getState().doc
    dragOrigin.current = {}
    for (const n of dragged) {
      const cur = d.nodes[n.id]
      if (cur)
        dragOrigin.current[n.id] = { x: cur.x, y: cur.y, pinned: cur.pinned, parent: cur.parent }
    }
  }, [])

  const onNodeDrag = useCallback(
    (_: unknown, __: BoardNode, dragged: BoardNode[]) => {
      // Others watch the node move live; these intermediate positions are not undo steps.
      const now = performance.now()
      if (now - lastDragSend.current < 50) return
      lastDragSend.current = now
      const ops = dragged.map((n) => offsetOp(n.id, n.position)).filter((op): op is Op => !!op)
      commit(ops, { undoable: false })
    },
    [commit],
  )

  const onNodeDragStop = useCallback(
    (_: unknown, __: BoardNode, dragged: BoardNode[]) => {
      const d = useSession.getState().doc
      const ui = useUi.getState()
      const ops: Op[] = []
      const inverse: Op[] = []
      for (const n of dragged) {
        const before = dragOrigin.current[n.id]
        const cur = d.nodes[n.id]
        const size = live.rects[n.id]
        if (!before || !cur || !size) continue
        inverse.push({ t: 'node.put', node: { id: n.id, ...before } })
        if (dragged.length === 1) {
          // Dropping a node into (or out of) a container changes what it belongs to;
          // it then takes its place in that container's layout.
          const base = cur.parent ? absoluteRect(d, live.rects, cur.parent) : null
          const cx = n.position.x + (base?.x ?? 0) + size.w / 2
          const cy = n.position.y + (base?.y ?? 0) + size.h / 2
          const target = containerAt(d, live.rects, cx, cy, n.id)
          if (target !== cur.parent) {
            ops.push({
              t: 'node.put',
              node: { id: n.id, parent: target, x: null, y: null, pinned: false },
            })
            continue
          }
        }
        const op = offsetOp(n.id, n.position)
        if (op) ops.push(op)
        ui.hold(n.id, { x: n.position.x, y: n.position.y, w: size.w, h: size.h })
      }
      commit(ops, { inverse })
      // Release on the next frame, once the animator has adopted the dropped positions.
      requestAnimationFrame(() => useUi.getState().release(dragged.map((n) => n.id)))
    },
    [commit],
  )

  const onConnect = useCallback(
    (c: Connection) => {
      if (!c.source || !c.target || c.source === c.target) return
      const d = useSession.getState().doc
      const ops: Op[] = [
        {
          t: 'edge.put',
          edge: { id: newId('e_'), source: c.source, target: c.target, label: '', dashed: false },
        },
      ]
      // A shape dropped on the canvas by hand sits wherever it was put. Its first
      // connector gives it a place in the flow, so it lets go of that spot and joins.
      const linked = new Set(Object.values(d.edges).flatMap((e) => [e.source, e.target]))
      for (const id of [c.source, c.target]) {
        if (d.nodes[id]?.pinned && !linked.has(id)) {
          ops.push({ t: 'node.put', node: { id, x: null, y: null, pinned: false } })
        }
      }
      commit(ops)
    },
    [commit],
  )

  // Tell collaborators what I have selected.
  useEffect(() => {
    send({ type: 'select', ids: selectedNodes })
  }, [selectedNodes, send])

  // ---- viewport -------------------------------------------------------------
  const fit = useCallback(
    (duration = 450, mode: 'all' | 'follow' = 'all') => {
      const { doc: d, flashes: recent } = useSession.getState()
      const pane = wrapper.current
      const bounds = (ids: string[]) => {
        const boxes = ids.map((id) => absoluteRect(d, live.rects, id)).filter((r): r is Rect => !!r)
        if (!boxes.length) return null
        const x = Math.min(...boxes.map((b) => b.x))
        const y = Math.min(...boxes.map((b) => b.y))
        return {
          x,
          y,
          w: Math.max(1, Math.max(...boxes.map((b) => b.x + b.w)) - x),
          h: Math.max(1, Math.max(...boxes.map((b) => b.y + b.h)) - y),
        }
      }
      const all = bounds(d.order.filter((id) => !d.nodes[id].parent))
      if (!pane || !all) return
      // Fit into the part of the pane not covered by the palette (left) and the dock (bottom).
      const availW = Math.max(240, pane.clientWidth - INSET.left - INSET.right)
      const availH = Math.max(200, pane.clientHeight - INSET.top - INSET.bottom)
      const zoomFor = (b: { w: number; h: number }) => Math.min(1.1, availW / b.w, availH / b.h)
      let zoom = Math.max(0.2, zoomFor(all))
      let focus = all
      if (mode === 'follow' && zoom < LEGIBLE_ZOOM) {
        // The whole diagram would be too small to read. Stay legible and frame what
        // the AI just touched instead; the rest is a scroll (or the Fit button) away.
        const now = Date.now()
        const fresh = bounds(d.order.filter((id) => recent[id] && now - recent[id] < 3000))
        if (fresh) {
          focus = fresh
          zoom = Math.max(zoom, Math.min(LEGIBLE_ZOOM, zoomFor(fresh)))
        }
      }
      // Centre on the focus, then pull back so the view never drifts past the diagram's edges.
      const place = (
        start: number,
        size: number,
        allStart: number,
        allSize: number,
        inset: number,
        avail: number,
      ) => {
        if (allSize * zoom <= avail) return inset + (avail - allSize * zoom) / 2 - allStart * zoom
        const centred = inset + (avail - size * zoom) / 2 - start * zoom
        const max = inset - allStart * zoom
        const min = inset + avail - (allStart + allSize) * zoom
        return Math.min(max, Math.max(min, centred))
      }
      void rf.setViewport(
        {
          x: place(focus.x, focus.w, all.x, all.w, INSET.left, availW),
          y: place(focus.y, focus.h, all.y, all.h, INSET.top, availH),
          zoom,
        },
        { duration: reducedMotion() ? 0 : duration },
      )
    },
    [rf],
  )

  const fitted = useRef(false)
  useEffect(() => {
    if (!Object.keys(targets).length) return
    const first = !fitted.current
    if (!first && !(follow && Date.now() - lastAiChange < 4000)) return
    const timer = window.setTimeout(() => {
      fit(first ? 0 : 450, first ? 'all' : 'follow')
      fitted.current = true
    }, 60)
    return () => window.clearTimeout(timer)
  }, [targets, follow, lastAiChange, fit])

  // Keep the diagram framed when the window (or the transcript panel) changes size,
  // unless the user has taken the camera.
  useEffect(() => {
    const pane = wrapper.current
    if (!pane) return
    let timer = 0
    const observer = new ResizeObserver(() => {
      window.clearTimeout(timer)
      timer = window.setTimeout(() => {
        if (useUi.getState().follow && fitted.current) fit(200)
      }, 120)
    })
    observer.observe(pane)
    return () => {
      observer.disconnect()
      window.clearTimeout(timer)
    }
  }, [fit])

  // ---- pointer presence -----------------------------------------------------
  const lastCursor = useRef(0)
  const onMouseMove = (e: React.MouseEvent) => {
    const now = performance.now()
    if (now - lastCursor.current < 45) return
    lastCursor.current = now
    const p = rf.screenToFlowPosition({ x: e.clientX, y: e.clientY })
    send({ type: 'cursor', x: Math.round(p.x), y: Math.round(p.y) })
  }

  // ---- keyboard -------------------------------------------------------------
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const el = e.target as HTMLElement | null
      if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA' || el.isContentEditable))
        return
      if (document.querySelector('[role="menu"]')) return
      const ui = useUi.getState()
      const session = useSession.getState()
      const mod = e.metaKey || e.ctrlKey
      if (mod && e.key.toLowerCase() === 'z') {
        e.preventDefault()
        if (e.shiftKey) session.redo()
        else session.undo()
      } else if (mod && e.key.toLowerCase() === 'y') {
        e.preventDefault()
        session.redo()
      } else if (mod && e.key.toLowerCase() === 'a') {
        e.preventDefault()
        ui.select(session.doc.order, Object.keys(session.doc.edges))
      } else if (e.key === 'Backspace' || e.key === 'Delete') {
        if (!ui.selectedNodes.length && !ui.selectedEdges.length) return
        e.preventDefault()
        session.commit([
          ...ui.selectedEdges.map((id) => ({ t: 'edge.del' as const, id })),
          ...ui.selectedNodes.map((id) => ({ t: 'node.del' as const, id })),
        ])
        ui.select([], [])
      } else if (e.key === 'Escape') {
        ui.select([], [])
      } else if ((e.key === 'Enter' || e.key === 'F2') && ui.selectedNodes.length === 1) {
        e.preventDefault()
        ui.editNode(ui.selectedNodes[0])
      } else if (e.key.startsWith('Arrow') && ui.selectedNodes.length) {
        // Keyboard alternative to dragging: nudge the offset from the automatic slot.
        e.preventDefault()
        const step = e.shiftKey ? 32 : 8
        const dx = e.key === 'ArrowLeft' ? -step : e.key === 'ArrowRight' ? step : 0
        const dy = e.key === 'ArrowUp' ? -step : e.key === 'ArrowDown' ? step : 0
        const ops: Op[] = []
        for (const id of ui.selectedNodes) {
          const n = session.doc.nodes[id]
          if (n)
            ops.push({
              t: 'node.put',
              node: { id, x: (n.x ?? 0) + dx, y: (n.y ?? 0) + dy, pinned: true },
            })
        }
        session.commit(ops)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  const empty = doc.order.length === 0 && status === 'open'
  return (
    <div
      ref={wrapper}
      className="relative size-full"
      onMouseMove={onMouseMove}
      onMouseLeave={() => send({ type: 'cursor', x: null, y: null })}
      onDoubleClick={(e) => {
        // Double-click empty canvas: drop a shape there and start typing its name.
        if ((e.target as HTMLElement).classList.contains('react-flow__pane')) {
          addNode('service', rf.screenToFlowPosition({ x: e.clientX, y: e.clientY }))
        }
      }}
      onDragOver={(e) => {
        if (e.dataTransfer.types.includes('application/x-wb-kind')) {
          e.preventDefault()
          e.dataTransfer.dropEffect = 'copy'
        }
      }}
      onDrop={(e) => {
        const kind = e.dataTransfer.getData('application/x-wb-kind') as Kind | 'group' | ''
        if (!kind) return
        e.preventDefault()
        addNode(kind, rf.screenToFlowPosition({ x: e.clientX, y: e.clientY }))
      }}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeDragStart={onNodeDragStart}
        onNodeDrag={onNodeDrag}
        onNodeDragStop={onNodeDragStop}
        onConnect={onConnect}
        onEdgeDoubleClick={(_, edge) => useUi.getState().editEdge(edge.id)}
        onMoveStart={(e) => {
          if (e) useUi.getState().setFollow(false) // the user took the camera
        }}
        connectionMode={ConnectionMode.Loose}
        selectionMode={SelectionMode.Partial}
        selectionOnDrag
        panOnScroll
        panOnDrag={[1, 2]}
        zoomOnDoubleClick={false}
        deleteKeyCode={null}
        elevateNodesOnSelect={false}
        minZoom={0.15}
        maxZoom={2.5}
        nodeDragThreshold={2}
        connectionLineStyle={{ strokeWidth: 1.5 }}
      >
        <Background variant={BackgroundVariant.Dots} gap={22} size={1.4} color="#D5D9DB" />
        <MiniMap
          pannable
          zoomable
          position="bottom-right"
          className="max-xl:hidden"
          style={{ width: 168, height: 108 }}
          nodeStrokeWidth={2}
          nodeBorderRadius={6}
          nodeColor={(n) => {
            const b = (n.data as NodeData).node
            return b.type === 'group' ? swatchFor(b).fill : swatchFor(b).line
          }}
          nodeStrokeColor={(n) => {
            const b = (n.data as NodeData).node
            return SWATCHES[b.color ?? (b.type === 'group' ? 'navy' : KIND_SPECS[b.kind].color)]
              .line
          }}
        />
      </ReactFlow>
      <PeerCursors />
      <ViewControls onFit={() => fit()} />
      {empty && (
        <div className="pointer-events-none absolute inset-0 grid place-items-center pb-28">
          <div className="max-w-md px-6 text-center">
            <p className="font-mono text-[11px] tracking-[0.12em] text-lava-700 uppercase">
              Empty board
            </p>
            <h2 className="mt-2 text-[28px] leading-[1.15] font-bold text-ink">
              Take the mic and <span className="text-lava-600">describe it</span>.
            </h2>
            <p className="mt-3 text-[15px] leading-relaxed text-body">
              Talk through an architecture or a process and the diagram draws itself as you speak.
              You can also drag shapes in from the left, or double-click anywhere to add one.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}

/** A dragged position, expressed as the offset from the node's automatic slot. */
function offsetOp(id: string, position: { x: number; y: number }): Op | null {
  const slot = live.auto[id]
  if (!slot) return null
  // Inside a container, content is anchored to the top-left, so a container can
  // grow right and down to follow a dragged child but not up or left.
  const inside = !!useSession.getState().doc.nodes[id]?.parent
  const x = inside ? Math.max(GROUP_PAD.side, position.x) : position.x
  const y = inside ? Math.max(GROUP_PAD.top, position.y) : position.y
  return {
    t: 'node.put',
    node: { id, x: Math.round(x - slot.x), y: Math.round(y - slot.y), pinned: true },
  }
}
