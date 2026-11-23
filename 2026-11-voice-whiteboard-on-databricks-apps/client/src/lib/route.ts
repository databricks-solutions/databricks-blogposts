// Choosing which sides of two nodes a connector attaches to.
//
// Connectors are elbow paths between node sides. The obvious pair of sides
// (the ones facing each other) sometimes sends the path straight through
// another node, so every side pairing is scored and the cleanest one wins:
// no node crossings first, then few bends, then short.

import { Position, getSmoothStepPath } from '@xyflow/react'
import type { Rect } from './layout'
import type { Direction } from './types'

export type Sides = [Position, Position]

const SIDES = [Position.Right, Position.Bottom, Position.Left, Position.Top]
const OFFSET = 16

export function sidePoint(r: Rect, side: Position): { x: number; y: number } {
  switch (side) {
    case Position.Left:
      return { x: r.x, y: r.y + r.h / 2 }
    case Position.Right:
      return { x: r.x + r.w, y: r.y + r.h / 2 }
    case Position.Top:
      return { x: r.x + r.w / 2, y: r.y }
    default:
      return { x: r.x + r.w / 2, y: r.y + r.h }
  }
}

/** Corner points of the elbow path React Flow would draw for this pairing. */
function polyline(a: Rect, b: Rect, sp: Position, tp: Position): number[] {
  const s = sidePoint(a, sp)
  const t = sidePoint(b, tp)
  const [path] = getSmoothStepPath({
    sourceX: s.x,
    sourceY: s.y,
    sourcePosition: sp,
    targetX: t.x,
    targetY: t.y,
    targetPosition: tp,
    borderRadius: 0,
    offset: OFFSET,
  })
  return (path.match(/-?\d+(?:\.\d+)?/g) ?? []).map(Number)
}

/** Does the axis-aligned segment pass through the rectangle (grown by `pad`)? */
function hits(x1: number, y1: number, x2: number, y2: number, r: Rect, pad: number): boolean {
  const l = r.x - pad
  const t = r.y - pad
  const rgt = r.x + r.w + pad
  const btm = r.y + r.h + pad
  return (
    Math.max(x1, x2) > l && Math.min(x1, x2) < rgt && Math.max(y1, y2) > t && Math.min(y1, y2) < btm
  )
}

/**
 * Pick attachment sides for a connector from `a` to `b`.
 * `obstacles` are the other shapes on the board, in the same coordinate space.
 */
export function chooseSides(a: Rect, b: Rect, obstacles: Rect[], flow: Direction): Sides {
  const natural: Sides =
    flow === 'DOWN' ? [Position.Bottom, Position.Top] : [Position.Right, Position.Left]
  let best: Sides = natural
  let bestScore = Infinity
  for (const sp of SIDES) {
    for (const tp of SIDES) {
      const pts = polyline(a, b, sp, tp)
      if (pts.length < 4) continue
      let score = 0
      let length = 0
      const segments = pts.length / 2 - 1
      for (let i = 0; i < segments; i++) {
        const x1 = pts[i * 2]
        const y1 = pts[i * 2 + 1]
        const x2 = pts[i * 2 + 2]
        const y2 = pts[i * 2 + 3]
        length += Math.abs(x2 - x1) + Math.abs(y2 - y1)
        for (const o of obstacles) if (hits(x1, y1, x2, y2, o, 6)) score += 10000
        // A path may leave its own node and enter its target, but not cut back through either.
        if (i > 0 && hits(x1, y1, x2, y2, a, -2)) score += 10000
        if (i < segments - 1 && hits(x1, y1, x2, y2, b, -2)) score += 10000
      }
      score += length + (segments - 1) * 28
      // Mild pull toward the board's flow, so equal options read consistently.
      if (sp === natural[0]) score -= 24
      if (tp === natural[1]) score -= 24
      if (score < bestScore) {
        bestScore = score
        best = [sp, tp]
      }
    }
  }
  return best
}
