import {
  BaseEdge,
  EdgeLabelRenderer,
  Position,
  getSmoothStepPath,
  useInternalNode,
  type Edge,
  type EdgeProps,
  type InternalNode,
} from '@xyflow/react'
import { memo, useEffect, useRef, useState } from 'react'
import type { Point, Rect } from '@/lib/layout'
import { midpoint, roundedPath } from '@/lib/path'
import { sidePoint, type Sides } from '@/lib/route'
import { useSession } from '@/lib/session'
import type { BEdge } from '@/lib/types'
import { useUi } from '@/lib/ui'

export interface EdgeData extends Record<string, unknown> {
  edge: BEdge
  /**
   * The path the layout engine routed for this connector, in canvas coordinates.
   * Null while either end is somewhere other than its automatic slot (being
   * dragged, hand-placed, or still gliding): then `sides` is used instead.
   */
  route: Point[] | null
  /** Where the layout reserved room for the label. */
  labelAt: Point | null
  /** Fallback: which side of each end a simple elbow attaches to (chosen in lib/route). */
  sides: Sides | null
}
export type BoardEdge = Edge<EdgeData, 'floating'>

function rect(n: InternalNode): Rect {
  const { x, y } = n.internals.positionAbsolute
  return { x, y, w: n.measured.width ?? n.width ?? 0, h: n.measured.height ?? n.height ?? 0 }
}

function EdgeLabelEditor({
  value,
  onDone,
}: {
  value: string
  onDone: (next: string | null) => void
}) {
  const ref = useRef<HTMLInputElement>(null)
  const [text, setText] = useState(value)
  const settled = useRef(false)
  useEffect(() => ref.current?.select(), [])
  const finish = (next: string | null) => {
    if (settled.current) return
    settled.current = true
    onDone(next)
  }
  return (
    <input
      ref={ref}
      value={text}
      aria-label="Connector label"
      name="connector-label"
      autoComplete="off"
      placeholder="Label…"
      maxLength={60}
      size={Math.max(6, text.length)}
      onChange={(e) => setText(e.target.value)}
      onBlur={() => finish(text)}
      onKeyDown={(e) => {
        e.stopPropagation()
        if (e.key === 'Enter') finish(text)
        if (e.key === 'Escape') finish(null)
      }}
      className="nodrag nopan rounded border border-blue-600 bg-white px-1.5 py-0.5 text-center text-[11.5px] text-ink outline-none"
    />
  )
}

// Solid connectors carry a pulse of data: a bright head with a fading tail that glides
// along the path (the "laser" flow style of the Solution Builder platform diagram). Dashed
// connectors mean optional or planned, so they stay still.
const LAVA = '#FF5F46'
const PULSE_PERIOD = 240 // dash period in path units: the head, then a long empty gap
const PULSE_DUR = 2.6
const PULSE_TRAIL: [len: number, width: number, opacity: number, lag: number][] = [
  [8, 1.3, 0.6, 0], // head: short, thin, muted
  [12, 0.9, 0.22, 6], // tail
]

function FlowPulse({ id, path }: { id: string; path: string }) {
  // Each connector starts its pulse at a different point, so a board does not pulse in step.
  let h = 0
  for (const c of id) h = (h * 31 + c.charCodeAt(0)) >>> 0
  const begin = -((h % 26) / 10)
  return (
    <g className="wb-comet" style={{ pointerEvents: 'none' }}>
      {PULSE_TRAIL.map(([len, width, opacity, lag], i) => (
        <path
          key={i}
          d={path}
          fill="none"
          stroke={LAVA}
          strokeWidth={width}
          strokeLinecap="round"
          strokeDasharray={`${len} ${PULSE_PERIOD - len}`}
          opacity={opacity}
        >
          <animate
            attributeName="stroke-dashoffset"
            values={`${PULSE_PERIOD + lag};${lag}`}
            dur={`${PULSE_DUR}s`}
            begin={`${begin}s`}
            repeatCount="indefinite"
          />
        </path>
      ))}
    </g>
  )
}

export const FloatingEdge = memo(function FloatingEdge({
  id,
  source,
  target,
  markerEnd,
  data,
  selected,
}: EdgeProps<BoardEdge>) {
  const s = useInternalNode(source)
  const t = useInternalNode(target)
  const editing = useUi((u) => u.editingEdge === id)
  const editEdge = useUi((u) => u.editEdge)
  const commit = useSession((x) => x.commit)
  if (!s || !t || !data) return null

  let path: string
  let labelX: number
  let labelY: number
  if (data.route) {
    path = roundedPath(data.route)
    const at = data.labelAt ?? midpoint(data.route)
    labelX = at.x
    labelY = at.y
  } else {
    const [sp, tp] = data.sides ?? [Position.Right, Position.Left]
    const from = sidePoint(rect(s), sp)
    const to = sidePoint(rect(t), tp)
    ;[path, labelX, labelY] = getSmoothStepPath({
      sourceX: from.x,
      sourceY: from.y,
      sourcePosition: sp,
      targetX: to.x,
      targetY: to.y,
      targetPosition: tp,
      borderRadius: 12,
      // Half the gap at most, so two close nodes get a straight run instead of a kink.
      offset: Math.max(
        4,
        Math.min(16, (Math.abs(to.x - from.x) + Math.abs(to.y - from.y)) / 2 - 4),
      ),
    })
  }
  const { edge } = data
  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        interactionWidth={18}
        // A solid connector draws itself in when it first appears (see .wb-draw in index.css).
        className={edge.dashed ? undefined : 'wb-draw'}
        pathLength={edge.dashed ? undefined : 1}
        style={{ strokeDasharray: edge.dashed ? '6 5' : undefined }}
      />
      {!edge.dashed && <FlowPulse id={id} path={path} />}
      {(edge.label || editing) && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan pointer-events-auto absolute"
            style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}
            onDoubleClick={() => editEdge(id)}
          >
            {editing ? (
              <EdgeLabelEditor
                value={edge.label}
                onDone={(next) => {
                  editEdge(null)
                  if (next !== null && next.trim() !== edge.label) {
                    commit([{ t: 'edge.put', edge: { id, label: next.trim() } }])
                  }
                }}
              />
            ) : (
              <span
                className={`rounded bg-oat px-1.5 py-0.5 text-[11.5px] leading-none ${
                  selected ? 'text-blue-700' : 'text-nav'
                }`}
              >
                {edge.label}
              </span>
            )}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
})

export const edgeTypes = { floating: FloatingEdge }
