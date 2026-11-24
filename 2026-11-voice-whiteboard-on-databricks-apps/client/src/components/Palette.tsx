import { Redo2, SquareDashed, Undo2 } from 'lucide-react'
import { KIND_SPECS } from '@/lib/kinds'
import { useSession } from '@/lib/session'
import { KINDS, type Kind } from '@/lib/types'
import { useAddNode } from './Canvas'
import { Tip } from './Tip'

/** Shapes to click or drag onto the canvas, plus undo and redo. */
export function Palette() {
  const addNode = useAddNode()
  const undo = useSession((s) => s.undo)
  const redo = useSession((s) => s.redo)
  const canUndo = useSession((s) => s.undoStack.length > 0)
  const canRedo = useSession((s) => s.redoStack.length > 0)

  const tile = (kind: Kind | 'group', label: string, icon: React.ReactNode) => (
    <Tip key={kind} label={label} side="right">
      <button
        type="button"
        aria-label={`Add ${label.toLowerCase()}`}
        draggable
        onDragStart={(e) => {
          e.dataTransfer.setData('application/x-wb-kind', kind)
          e.dataTransfer.effectAllowed = 'copy'
        }}
        onClick={() => addNode(kind)}
        className="grid size-8 cursor-grab place-items-center rounded-md text-nav transition-colors hover:bg-oat-medium hover:text-ink active:cursor-grabbing active:bg-line"
      >
        {icon}
      </button>
    </Tip>
  )

  return (
    <div
      className="absolute top-4 left-4 z-10 rounded-xl border border-line bg-white p-1.5 shadow-float"
      role="toolbar"
      aria-label="Shapes"
      aria-orientation="vertical"
    >
      <div className="grid grid-cols-2 gap-0.5">
        {KINDS.map((k) => {
          const spec = KIND_SPECS[k]
          return tile(k, spec.label, <spec.icon size={16} strokeWidth={1.75} aria-hidden />)
        })}
        {tile('group', 'Container', <SquareDashed size={16} strokeWidth={1.75} aria-hidden />)}
      </div>
      <div className="my-1.5 h-px bg-line" />
      <div className="grid grid-cols-2 gap-0.5">
        <Tip label="Undo" keys="⌘Z" side="right">
          <button
            type="button"
            aria-label="Undo"
            disabled={!canUndo}
            onClick={undo}
            className="grid size-8 place-items-center rounded-md text-nav hover:bg-oat-medium disabled:pointer-events-none disabled:opacity-35"
          >
            <Undo2 size={16} strokeWidth={1.75} aria-hidden />
          </button>
        </Tip>
        <Tip label="Redo" keys="⇧⌘Z" side="right">
          <button
            type="button"
            aria-label="Redo"
            disabled={!canRedo}
            onClick={redo}
            className="grid size-8 place-items-center rounded-md text-nav hover:bg-oat-medium disabled:pointer-events-none disabled:opacity-35"
          >
            <Redo2 size={16} strokeWidth={1.75} aria-hidden />
          </button>
        </Tip>
      </div>
    </div>
  )
}
