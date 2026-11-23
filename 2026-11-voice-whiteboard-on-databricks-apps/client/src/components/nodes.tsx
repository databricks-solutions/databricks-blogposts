import * as DropdownMenu from '@radix-ui/react-dropdown-menu'
import {
  Handle,
  NodeResizer,
  NodeToolbar,
  Position,
  useConnection,
  type Node,
  type NodeProps,
  type ResizeParams,
} from '@xyflow/react'
import { Check, ChevronDown, Pin, PinOff, Trash2 } from 'lucide-react'
import { memo, useEffect, useRef, useState } from 'react'
import { KIND_SPECS, SWATCHES, artFor, swatchFor } from '@/lib/kinds'
import { ArtIcon } from '@/lib/icons'
import { hasArt } from '@/lib/art'
import { live } from '@/lib/live'
import { useSession } from '@/lib/session'
import { COLORS, KINDS, type BNode } from '@/lib/types'
import { useUi } from '@/lib/ui'

export interface NodeData extends Record<string, unknown> {
  node: BNode
  /** Timestamp of the AI's last change to this node (0 if none recently). */
  flash: number
  /** Colour and name of a collaborator who has this node selected. */
  peer: { color: string; name: string } | null
  soloSelected: boolean
}
export type BoardNode = Node<NodeData, 'shape' | 'frame'>

const HANDLES: { id: string; position: Position }[] = [
  { id: 't', position: Position.Top },
  { id: 'r', position: Position.Right },
  { id: 'b', position: Position.Bottom },
  { id: 'l', position: Position.Left },
]

function Handles() {
  // While a connector is being dragged, the whole node becomes a drop target, so
  // you can let go anywhere on a shape rather than aiming for a small dot.
  const connecting = useConnection((c) => c.inProgress)
  return (
    <>
      <Handle
        id="body"
        type="target"
        position={Position.Top}
        isConnectableStart={false}
        className="wb-body"
        style={{ pointerEvents: connecting ? 'all' : 'none' }}
      />
      {HANDLES.map((h) => (
        <Handle
          key={h.id}
          id={h.id}
          type="source"
          position={h.position}
          className={connecting ? 'wb-show' : ''}
        />
      ))}
    </>
  )
}

/** Inline label editor. Enter commits, Escape cancels, blur commits. */
function LabelEditor({
  value,
  onDone,
  className,
  placeholder,
}: {
  value: string
  onDone: (next: string | null) => void
  className: string
  placeholder: string
}) {
  const ref = useRef<HTMLInputElement>(null)
  const [text, setText] = useState(value)
  const settled = useRef(false)
  useEffect(() => {
    // Focus is the whole point of opening the editor.
    ref.current?.focus()
    ref.current?.select()
  }, [])
  const finish = (next: string | null) => {
    if (settled.current) return
    settled.current = true
    onDone(next)
  }
  return (
    <input
      ref={ref}
      value={text}
      aria-label="Label"
      name="label"
      autoComplete="off"
      spellCheck={false}
      placeholder={placeholder}
      maxLength={80}
      onChange={(e) => setText(e.target.value)}
      onBlur={() => finish(text)}
      onKeyDown={(e) => {
        e.stopPropagation()
        if (e.key === 'Enter') finish(text)
        if (e.key === 'Escape') finish(null)
      }}
      className={`nodrag nopan w-full min-w-0 rounded-sm bg-white/90 px-1 outline-2 outline-blue-600 ${className}`}
    />
  )
}

function useNodeActions(node: BNode) {
  const commit = useSession((s) => s.commit)
  const editing = useUi((s) => s.editingNode === node.id)
  const editNode = useUi((s) => s.editNode)
  return {
    editing,
    startEdit: () => editNode(node.id),
    finishEdit: (next: string | null) => {
      editNode(null)
      if (next !== null && next.trim() !== node.label) {
        commit([{ t: 'node.put', node: { id: node.id, label: next.trim(), touched: true } }])
      }
    },
    put: (patch: Partial<BNode>) => commit([{ t: 'node.put', node: { id: node.id, ...patch } }]),
    remove: () => commit([{ t: 'node.del', id: node.id }]),
  }
}

function useResize(node: BNode) {
  const commit = useSession((s) => s.commit)
  const hold = useUi((s) => s.hold)
  const release = useUi((s) => s.release)
  return {
    onResize: (_: unknown, p: ResizeParams) =>
      hold(node.id, { x: p.x, y: p.y, w: p.width, h: p.height }),
    onResizeEnd: (_: unknown, p: ResizeParams) => {
      const slot = live.auto[node.id]
      commit([
        {
          t: 'node.put',
          node: {
            id: node.id,
            x: Math.round(p.x - (slot?.x ?? 0)),
            y: Math.round(p.y - (slot?.y ?? 0)),
            w: Math.round(p.width),
            h: Math.round(p.height),
            pinned: true,
          },
        },
      ])
      requestAnimationFrame(() => release([node.id]))
    },
  }
}

function Toolbar({ node }: { node: BNode }) {
  const { put, remove } = useNodeActions(node)
  const spec = KIND_SPECS[node.kind]
  const item =
    'flex cursor-default items-center gap-2 rounded px-2 py-1.5 text-[13px] text-ink outline-none data-[highlighted]:bg-oat-medium'
  return (
    <div
      className="flex animate-rise items-center gap-0.5 rounded-lg border border-line bg-white p-1 shadow-float"
      role="toolbar"
      aria-label={`Options for ${node.label || 'this shape'}`}
    >
      {node.type === 'shape' && (
        <DropdownMenu.Root>
          <DropdownMenu.Trigger className="flex h-7 items-center gap-1.5 rounded-md px-2 text-[12.5px] text-nav hover:bg-oat-medium">
            <spec.icon size={14} aria-hidden />
            {spec.label}
            <ChevronDown size={12} aria-hidden />
          </DropdownMenu.Trigger>
          <DropdownMenu.Portal>
            <DropdownMenu.Content
              sideOffset={6}
              className="z-50 max-h-80 w-48 overflow-y-auto overscroll-contain rounded-lg border border-line bg-white p-1 shadow-float"
            >
              {KINDS.map((k) => {
                const s = KIND_SPECS[k]
                return (
                  <DropdownMenu.Item
                    key={k}
                    className={item}
                    onSelect={() => put({ kind: k, w: null, h: null })}
                  >
                    <s.icon size={14} className="text-body" aria-hidden />
                    <span className="flex-1">{s.label}</span>
                    {k === node.kind && <Check size={13} aria-hidden />}
                  </DropdownMenu.Item>
                )
              })}
            </DropdownMenu.Content>
          </DropdownMenu.Portal>
        </DropdownMenu.Root>
      )}
      <div className="mx-1 flex items-center gap-1" role="group" aria-label="Colour">
        {COLORS.map((c) => {
          const sw = SWATCHES[c]
          const current = (node.color ?? (node.type === 'group' ? 'navy' : spec.color)) === c
          return (
            <button
              key={c}
              type="button"
              aria-label={`${c} fill`}
              aria-pressed={current}
              onClick={() => put({ color: c })}
              className="size-[18px] rounded-full border transition-transform hover:scale-110"
              style={{
                background: sw.fill,
                borderColor: current ? sw.accent : sw.line,
                boxShadow: current ? `0 0 0 1.5px ${sw.accent}` : undefined,
              }}
            />
          )
        })}
      </div>
      {node.pinned && (
        <button
          type="button"
          aria-label="Return to automatic layout"
          title="Return to automatic layout"
          onClick={() => put({ x: null, y: null, w: null, h: null, pinned: false })}
          className="grid size-7 place-items-center rounded-md text-nav hover:bg-oat-medium"
        >
          <PinOff size={14} aria-hidden />
        </button>
      )}
      <button
        type="button"
        aria-label="Delete"
        title="Delete"
        onClick={remove}
        className="grid size-7 place-items-center rounded-md text-nav hover:bg-tint-lava hover:text-lava-700"
      >
        <Trash2 size={14} aria-hidden />
      </button>
    </div>
  )
}

function Rings({ data, selected, radius }: { data: NodeData; selected: boolean; radius: string }) {
  return (
    <>
      {data.flash > 0 && (
        // The AI's touch: a brief Lava outline that fades. Keyed so each change restarts it.
        <span
          key={data.flash}
          aria-hidden
          className={`pointer-events-none absolute -inset-[3px] animate-flash border-2 border-lava-500 ${radius}`}
        />
      )}
      {data.peer && !selected && (
        <>
          <span
            aria-hidden
            className={`pointer-events-none absolute -inset-[3px] border-2 ${radius}`}
            style={{ borderColor: data.peer.color }}
          />
          <span
            className="pointer-events-none absolute -top-[19px] left-0 rounded px-1 font-mono text-[9.5px] leading-[14px] text-white"
            style={{ background: data.peer.color }}
          >
            {data.peer.name}
          </span>
        </>
      )}
      {selected && (
        <span
          aria-hidden
          className={`pointer-events-none absolute -inset-[3px] border-2 border-blue-600 ${radius}`}
        />
      )}
    </>
  )
}

export const ShapeNode = memo(function ShapeNode({ data, selected }: NodeProps<BoardNode>) {
  const { node } = data
  const spec = KIND_SPECS[node.kind] ?? KIND_SPECS.service
  const sw = swatchFor(node)
  const { editing, startEdit, finishEdit } = useNodeActions(node)
  const resize = useResize(node)
  const Icon = spec.icon
  const art = artFor(node)

  const label = editing ? (
    <LabelEditor
      value={node.label}
      onDone={finishEdit}
      placeholder="Name…"
      className={`text-[13.5px] font-medium ${spec.shape === 'card' ? '' : 'text-center'}`}
    />
  ) : (
    <span
      className="line-clamp-2 break-words text-[13.5px] leading-[1.2] font-medium"
      style={{ color: sw.ink }}
    >
      {node.label || <span className="font-normal text-navy-400">Untitled</span>}
    </span>
  )

  let body
  if (spec.shape === 'diamond') {
    body = (
      <div className="relative grid size-full place-items-center">
        <svg
          className="absolute inset-0 size-full overflow-visible"
          preserveAspectRatio="none"
          viewBox="0 0 100 100"
          aria-hidden
        >
          <polygon
            points="50,1 99,50 50,99 1,50"
            fill={sw.fill}
            stroke={sw.line}
            strokeWidth="1.5"
            vectorEffect="non-scaling-stroke"
            strokeLinejoin="round"
          />
        </svg>
        <div className="relative w-[58%] min-w-0 text-center">{label}</div>
      </div>
    )
  } else if (spec.shape === 'pill') {
    body = (
      <div
        className="grid size-full place-items-center rounded-full border-[1.5px] px-4 text-center shadow-node"
        style={{ background: sw.fill, borderColor: sw.line }}
      >
        <div className="w-full min-w-0">{label}</div>
      </div>
    )
  } else if (spec.shape === 'note') {
    body = (
      <div
        className="size-full rounded-[3px] rounded-br-[14px] border px-3 py-2.5 shadow-node"
        style={{ background: sw.fill, borderColor: sw.line }}
      >
        {label}
        {node.detail && (
          <p className="mt-1 line-clamp-3 text-[11.5px] leading-snug" style={{ color: sw.accent }}>
            {node.detail}
          </p>
        )}
      </div>
    )
  } else {
    body = (
      <div
        className="flex size-full items-center gap-2.5 rounded-[10px] border px-3 shadow-node"
        style={{
          background: sw.fill,
          borderColor: sw.line,
          borderStyle: spec.dashed ? 'dashed' : 'solid',
        }}
      >
        {hasArt(art) ? (
          // The artwork sits on a white tile with a hairline in the swatch colour, the way
          // a product tile does on a platform diagram: the mark reads even on a tinted card.
          <span
            className="grid size-[38px] shrink-0 place-items-center rounded-lg bg-white"
            style={{ boxShadow: `inset 0 0 0 1px ${sw.line}` }}
          >
            <ArtIcon name={art} size={28} style={{ color: sw.ink }} />
          </span>
        ) : (
          <Icon
            size={17}
            strokeWidth={1.75}
            style={{ color: sw.accent }}
            className="shrink-0"
            aria-hidden
          />
        )}
        <div className="min-w-0 flex-1">
          {label}
          {node.detail && (
            <p
              className="mt-0.5 truncate font-mono text-[10.5px] leading-tight"
              style={{ color: sw.accent }}
            >
              {node.detail}
            </p>
          )}
        </div>
      </div>
    )
  }

  const radius =
    spec.shape === 'pill'
      ? 'rounded-full'
      : spec.shape === 'diamond'
        ? 'rounded-[4px]'
        : 'rounded-[13px]'
  return (
    <div className="relative size-full animate-pop" onDoubleClick={startEdit}>
      <NodeResizer
        isVisible={selected && data.soloSelected}
        minWidth={110}
        minHeight={40}
        {...resize}
      />
      <NodeToolbar
        isVisible={selected && data.soloSelected && !editing}
        position={Position.Top}
        offset={12}
      >
        <Toolbar node={node} />
      </NodeToolbar>
      {body}
      <Rings data={data} selected={selected} radius={radius} />
      {node.pinned && (
        <Pin
          size={10}
          className="absolute -top-1.5 -right-1.5 rotate-45 text-navy-400"
          aria-label="Placed by hand"
        />
      )}
      <Handles />
    </div>
  )
})

export const GroupNode = memo(function GroupNode({ data, selected }: NodeProps<BoardNode>) {
  const { node } = data
  const sw = swatchFor(node)
  const { editing, startEdit, finishEdit } = useNodeActions(node)
  const resize = useResize(node)
  return (
    <div className="relative size-full animate-pop">
      <NodeResizer
        isVisible={selected && data.soloSelected}
        minWidth={160}
        minHeight={90}
        {...resize}
      />
      <NodeToolbar
        isVisible={selected && data.soloSelected && !editing}
        position={Position.Top}
        offset={12}
      >
        <Toolbar node={node} />
      </NodeToolbar>
      <div
        className="size-full rounded-2xl border"
        style={{ background: `${sw.fill}73`, borderColor: sw.line }}
      >
        {/* The container's name is set in mono caps: the deck system's kicker, reused as a frame label. */}
        <div
          className="flex h-9 items-center px-4 font-mono text-[10.5px] font-medium tracking-[0.09em] uppercase"
          style={{ color: sw.accent }}
          onDoubleClick={startEdit}
        >
          {editing ? (
            <LabelEditor
              value={node.label}
              onDone={finishEdit}
              placeholder="Group name…"
              className="font-mono text-[10.5px] tracking-[0.09em] uppercase"
            />
          ) : (
            <span className="truncate">{node.label || 'Group'}</span>
          )}
        </div>
      </div>
      <Rings data={data} selected={selected} radius="rounded-[19px]" />
      <Handles />
    </div>
  )
})

// React Flow ships default chrome for a type named "group", so containers register as "frame".
export const nodeTypes = { shape: ShapeNode, frame: GroupNode }
