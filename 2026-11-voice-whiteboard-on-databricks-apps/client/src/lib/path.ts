// Turning a routed connector (a list of corner points) into what gets drawn.

import type { Point, Rect } from './layout'

/** Drop repeated points and points that sit on a straight run between their neighbours. */
export function simplify(points: Point[]): Point[] {
  const out: Point[] = []
  for (const p of points) {
    const last = out.at(-1)
    if (last && Math.abs(last.x - p.x) < 0.5 && Math.abs(last.y - p.y) < 0.5) continue
    const prev = out.at(-2)
    if (
      prev &&
      last &&
      ((Math.abs(prev.x - last.x) < 0.5 && Math.abs(last.x - p.x) < 0.5) ||
        (Math.abs(prev.y - last.y) < 0.5 && Math.abs(last.y - p.y) < 0.5))
    ) {
      out[out.length - 1] = p
      continue
    }
    out.push(p)
  }
  return out
}

/** An SVG path through the points with rounded corners. */
export function roundedPath(points: Point[], radius = 10): string {
  if (points.length < 2) return ''
  let d = `M ${points[0].x} ${points[0].y}`
  for (let i = 1; i < points.length - 1; i++) {
    const a = points[i - 1]
    const b = points[i]
    const c = points[i + 1]
    const inLen = Math.hypot(b.x - a.x, b.y - a.y)
    const outLen = Math.hypot(c.x - b.x, c.y - b.y)
    const r = Math.min(radius, inLen / 2, outLen / 2)
    if (r < 1) {
      d += ` L ${b.x} ${b.y}`
      continue
    }
    const from = { x: b.x + ((a.x - b.x) / inLen) * r, y: b.y + ((a.y - b.y) / inLen) * r }
    const to = { x: b.x + ((c.x - b.x) / outLen) * r, y: b.y + ((c.y - b.y) / outLen) * r }
    d += ` L ${from.x} ${from.y} Q ${b.x} ${b.y} ${to.x} ${to.y}`
  }
  const end = points.at(-1)!
  return `${d} L ${end.x} ${end.y}`
}

/** Middle of the longest straight run: a sensible place for a label with no reserved spot. */
export function midpoint(points: Point[]): Point {
  let best = { x: points[0].x, y: points[0].y }
  let longest = -1
  for (let i = 0; i + 1 < points.length; i++) {
    const len = Math.abs(points[i + 1].x - points[i].x) + Math.abs(points[i + 1].y - points[i].y)
    if (len > longest) {
      longest = len
      best = { x: (points[i].x + points[i + 1].x) / 2, y: (points[i].y + points[i + 1].y) / 2 }
    }
  }
  return best
}

/**
 * The layout attaches connectors to a node's bounding box. A diamond only
 * touches its box at the four tips, so slide an end point along its final
 * segment until it meets the diamond's edge.
 */
export function meetDiamond(end: Point, next: Point, box: Rect): Point {
  const cx = box.x + box.w / 2
  const cy = box.y + box.h / 2
  if (Math.abs(end.x - next.x) < 0.5) {
    // Vertical run: how far the slanted edge sits from the tip at this x.
    const reach = (1 - Math.min(1, Math.abs(end.x - cx) / (box.w / 2))) * (box.h / 2)
    return { x: end.x, y: end.y < cy ? cy - reach : cy + reach }
  }
  const reach = (1 - Math.min(1, Math.abs(end.y - cy) / (box.h / 2))) * (box.w / 2)
  return { x: end.x < cx ? cx - reach : cx + reach, y: end.y }
}
