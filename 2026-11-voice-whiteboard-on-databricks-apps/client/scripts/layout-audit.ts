// Audit the automatic layout on saved boards.
//
//   bun scripts/layout-audit.ts <board.json> [more.json ...]
//
// A board file is what GET /api/boards/<id> returns (or a file from .data/boards).
// For each board this runs the same layout the app runs and checks what a person
// would check by eye: no connector passes through a shape, no two connectors share
// a track, no shapes overlap, every connector starts and ends on its shape's edge,
// and no connector cuts through a container it has nothing to do with. Hand-made
// positions are ignored: this is about what the automatic layout produces.

import fs from 'node:fs'
import ELK from 'elkjs/lib/elk.bundled.js'
import type { Doc } from '../src/lib/doc'
import { absoluteRect, computeLayout, setLayoutEngine, type Point, type Rect } from '../src/lib/layout'
import type { BEdge, BNode } from '../src/lib/types'

setLayoutEngine(new ELK())

const REFERENCE = { w: 1480, h: 850 } // a typical canvas, for the zoom-to-fit figure
let failures = 0

for (const file of process.argv.slice(2)) {
  const board = JSON.parse(fs.readFileSync(file, 'utf8')) as { title: string; direction: Doc['direction']; nodes: BNode[]; edges: BEdge[] }
  if (!board.nodes.length) continue
  const doc: Doc = {
    title: board.title,
    direction: board.direction,
    nodes: Object.fromEntries(board.nodes.map((n) => [n.id, { ...n, pinned: false, x: null, y: null, w: null, h: null }])),
    order: board.nodes.map((n) => n.id),
    edges: Object.fromEntries(board.edges.map((e) => [e.id, e])),
  }
  const started = performance.now()
  const layout = await computeLayout(doc)
  const ms = performance.now() - started

  const abs: Record<string, Rect> = {}
  for (const id of doc.order) {
    const r = absoluteRect(doc, layout.rects, id)
    if (r) abs[id] = r
  }
  const shapes = doc.order.filter((id) => doc.nodes[id].type === 'shape' && abs[id])
  const groups = doc.order.filter((id) => doc.nodes[id].type === 'group' && abs[id])
  const ancestors = (id: string) => {
    const out: string[] = []
    for (let c = doc.nodes[id]?.parent; c; c = doc.nodes[c]?.parent ?? null) out.push(c)
    return out
  }
  const onBorder = (p: Point, b: Rect) =>
    p.x >= b.x - 1 && p.x <= b.x + b.w + 1 && p.y >= b.y - 1 && p.y <= b.y + b.h + 1 &&
    (Math.abs(p.x - b.x) < 1.5 || Math.abs(p.x - b.x - b.w) < 1.5 || Math.abs(p.y - b.y) < 1.5 || Math.abs(p.y - b.y - b.h) < 1.5)

  let throughShape = 0
  let badAttach = 0
  let throughContainer = 0
  const segments: { edge: string; x1: number; y1: number; x2: number; y2: number }[] = []
  for (const e of Object.values(doc.edges)) {
    const route = layout.routes[e.id]
    if (!route) continue
    if (!onBorder(route.points[0], abs[e.source]) || !onBorder(route.points.at(-1)!, abs[e.target])) badAttach++
    const own = new Set([e.source, e.target, ...ancestors(e.source), ...ancestors(e.target)])
    const crossed = new Set<string>()
    for (let i = 0; i + 1 < route.points.length; i++) {
      const a = route.points[i]
      const b = route.points[i + 1]
      if (Math.abs(a.x - b.x) > 0.5 && Math.abs(a.y - b.y) > 0.5) badAttach++ // a diagonal run
      segments.push({ edge: e.id, x1: Math.min(a.x, b.x), y1: Math.min(a.y, b.y), x2: Math.max(a.x, b.x), y2: Math.max(a.y, b.y) })
      const hits = (s: Rect) =>
        Math.max(a.x, b.x) > s.x + 1 && Math.min(a.x, b.x) < s.x + s.w - 1 && Math.max(a.y, b.y) > s.y + 1 && Math.min(a.y, b.y) < s.y + s.h - 1
      for (const id of shapes) if (id !== e.source && id !== e.target && hits(abs[id])) throughShape++
      for (const id of groups) if (!own.has(id) && hits(abs[id])) crossed.add(id)
    }
    throughContainer += crossed.size
  }
  let sharedTrack = 0
  for (let i = 0; i < segments.length; i++) {
    for (let j = i + 1; j < segments.length; j++) {
      const a = segments[i]
      const b = segments[j]
      if (a.edge === b.edge) continue
      const vertical = a.x1 === a.x2 && b.x1 === b.x2 && Math.abs(a.x1 - b.x1) < 3 && Math.min(a.y2, b.y2) - Math.max(a.y1, b.y1) > 12
      const horizontal = a.y1 === a.y2 && b.y1 === b.y2 && Math.abs(a.y1 - b.y1) < 3 && Math.min(a.x2, b.x2) - Math.max(a.x1, b.x1) > 12
      if (vertical || horizontal) sharedTrack++
    }
  }
  let overlap = 0
  for (let i = 0; i < shapes.length; i++) {
    for (let j = i + 1; j < shapes.length; j++) {
      const a = abs[shapes[i]]
      const b = abs[shapes[j]]
      if (a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y) overlap++
    }
  }
  const tops = doc.order.filter((id) => !doc.nodes[id].parent && abs[id]).map((id) => abs[id])
  const width = Math.max(...tops.map((r) => r.x + r.w)) - Math.min(...tops.map((r) => r.x))
  const height = Math.max(...tops.map((r) => r.y + r.h)) - Math.min(...tops.map((r) => r.y))
  const unrouted = Object.keys(doc.edges).length - Object.keys(layout.routes).length
  const problems = throughShape + sharedTrack + overlap + badAttach + throughContainer + unrouted
  if (problems) failures++
  console.log(
    `${problems ? 'FAIL' : 'ok  '} ${board.title.slice(0, 36).padEnd(36)} ${shapes.length} shapes, ${groups.length} containers, ${Object.keys(doc.edges).length} connectors` +
      ` | ${Math.round(width)}x${Math.round(height)}, fits at ${Math.round(Math.min(1.1, REFERENCE.w / width, REFERENCE.h / height) * 100)}%, ${ms.toFixed(0)} ms` +
      ` | through shape ${throughShape}, shared track ${sharedTrack}, overlap ${overlap}, bad attach ${badAttach}, through other container ${throughContainer}, unrouted ${unrouted}`,
  )
}
console.log(failures ? `${failures} board(s) failed` : 'all boards pass')
process.exit(failures ? 1 : 0)
