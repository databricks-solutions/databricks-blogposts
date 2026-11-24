import {
  ArrowDownFromLine,
  ArrowRightFromLine,
  CloudAlert,
  CloudCheck,
  CloudUpload,
  LayoutGrid,
  Mic,
  PanelRight,
  Sparkles,
} from 'lucide-react'
import { useEffect, useState } from 'react'
import { useSession } from '@/lib/session'
import { useUi } from '@/lib/ui'
import { SharePanel } from './SharePanel'
import { IconButton, Tip } from './Tip'

export function Mark() {
  return (
    <svg width="22" height="22" viewBox="0 0 32 32" aria-hidden>
      <rect width="32" height="32" rx="7" fill="#0B2026" />
      <rect x="7" y="8" width="8" height="6" rx="1.5" fill="#F9F7F4" />
      <rect x="17" y="18" width="8" height="6" rx="1.5" fill="#FF3621" />
      <path
        d="M15 11h4a2 2 0 0 1 2 2v5"
        fill="none"
        stroke="#C4CCD6"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
    </svg>
  )
}

export const initials = (name: string) =>
  name
    .split(/\s+/)
    .map((p) => p[0])
    .slice(0, 2)
    .join('')
    .toUpperCase()

function Title() {
  const title = useSession((s) => s.doc.title)
  const commit = useSession((s) => s.commit)
  const [draft, setDraft] = useState(title)
  const [focused, setFocused] = useState(false)
  useEffect(() => {
    if (!focused) setDraft(title)
  }, [title, focused])
  const save = () => {
    setFocused(false)
    const next = draft.trim()
    if (next && next !== title) commit([{ t: 'board.set', patch: { title: next } }])
    else setDraft(title)
  }
  return (
    <input
      value={draft}
      aria-label="Board title"
      name="board-title"
      autoComplete="off"
      maxLength={80}
      onFocus={() => setFocused(true)}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={save}
      onKeyDown={(e) => {
        if (e.key === 'Enter') e.currentTarget.blur()
        if (e.key === 'Escape') {
          setDraft(title)
          e.currentTarget.blur()
        }
      }}
      className="min-w-0 flex-1 truncate rounded-md border border-transparent bg-transparent px-2 py-1 text-[15px] font-semibold text-ink hover:border-line focus:border-line focus:bg-white md:max-w-md"
    />
  )
}

function People() {
  const peers = useSession((s) => s.peers)
  const me = useSession((s) => s.me)
  const holder = useSession((s) => s.mic.holder?.id)
  const shown = peers.slice(0, 6)
  return (
    <div className="flex items-center" role="list" aria-label="People on this board">
      {shown.map((p) => (
        <Tip
          key={p.id}
          label={`${p.name}${p.id === me?.id ? ' (you)' : ''}${p.id === holder ? ' has the mic' : ''}`}
        >
          <div
            role="listitem"
            className="relative -ml-1.5 grid size-7 place-items-center rounded-full border-2 border-oat text-[10.5px] font-semibold text-white first:ml-0"
            style={{ background: p.color }}
          >
            {initials(p.name)}
            {p.id === holder && (
              <span className="absolute -right-1 -bottom-1 grid size-3.5 place-items-center rounded-full bg-lava-600 text-navy-900 ring-2 ring-oat">
                <Mic size={8} strokeWidth={3} aria-hidden />
              </span>
            )}
          </div>
        </Tip>
      ))}
      {peers.length > shown.length && (
        <span className="ml-1.5 font-mono text-[11px] text-body">
          +{peers.length - shown.length}
        </span>
      )}
    </div>
  )
}

const clock = new Intl.DateTimeFormat(undefined, {
  hour: 'numeric',
  minute: '2-digit',
  second: '2-digit',
})

/** Autosave: whether what is on screen has reached the database yet. */
function SaveIndicator() {
  const save = useSession((s) => s.save)
  const status = useSession((s) => s.status)
  if (status !== 'open') return null
  const view =
    save.state === 'saving'
      ? { icon: <CloudUpload size={14} aria-hidden />, text: 'Saving…', tone: 'text-body' }
      : save.state === 'error'
        ? {
            icon: <CloudAlert size={14} aria-hidden />,
            text: 'Not saved. Retrying…',
            tone: 'text-maroon-600',
          }
        : { icon: <CloudCheck size={14} aria-hidden />, text: 'Saved', tone: 'text-body' }
  const detail =
    save.state === 'error'
      ? 'The database could not be reached. Your changes are safe on screen and will be saved as soon as it responds.'
      : save.at
        ? `Every change is saved automatically. Last saved at ${clock.format(save.at * 1000)}.`
        : 'Every change is saved automatically.'
  return (
    <Tip label={detail}>
      <span
        className={`flex shrink-0 items-center gap-1.5 px-1 font-mono text-[11px] whitespace-nowrap ${view.tone}`}
        role="status"
        aria-live="polite"
        tabIndex={0}
      >
        {view.icon}
        {view.text}
      </span>
    </Tip>
  )
}

export function TopBar({ onHome }: { onHome: () => void }) {
  const direction = useSession((s) => s.doc.direction)
  const status = useSession((s) => s.status)
  const phase = useSession((s) => s.ai.phase)
  const hasPins = useSession((s) => Object.values(s.doc.nodes).some((n) => n.pinned))
  const nodeCount = useSession((s) => s.doc.order.length)
  const commit = useSession((s) => s.commit)
  const send = useSession((s) => s.send)
  const transcriptOpen = useUi((s) => s.transcriptOpen)
  const toggleTranscript = useUi((s) => s.toggleTranscript)

  return (
    <header className="flex h-12 shrink-0 items-center gap-2 border-b border-line bg-oat px-3">
      <Tip label="Your boards">
        <button
          type="button"
          aria-label="Your boards"
          onClick={onHome}
          className="grid size-8 place-items-center rounded-md hover:bg-oat-medium"
        >
          <Mark />
        </button>
      </Tip>
      <Title />
      <SaveIndicator />
      {status === 'reconnecting' && (
        <span
          className="rounded-full bg-tint-yellow px-2.5 py-0.5 font-mono text-[11px] text-yellow-800"
          role="status"
        >
          Reconnecting…
        </span>
      )}
      <div className="ml-auto flex items-center gap-1">
        <IconButton
          label={
            direction === 'RIGHT'
              ? 'Flow: left to right. Switch to top to bottom'
              : 'Flow: top to bottom. Switch to left to right'
          }
          onClick={() =>
            commit([
              { t: 'board.set', patch: { direction: direction === 'RIGHT' ? 'DOWN' : 'RIGHT' } },
            ])
          }
        >
          {direction === 'RIGHT' ? (
            <ArrowRightFromLine size={16} aria-hidden />
          ) : (
            <ArrowDownFromLine size={16} aria-hidden />
          )}
        </IconButton>
        <IconButton
          label="Re-flow: return every shape to the automatic layout"
          disabled={!hasPins}
          onClick={() => commit([{ t: 'board.reflow' }])}
        >
          <LayoutGrid size={16} aria-hidden />
        </IconButton>
        <Tip label="Reset the layout and have the AI reorganise the diagram">
          <button
            type="button"
            disabled={phase === 'refining' || nodeCount < 2}
            onClick={() => {
              // Two things people mean by "tidy": put everything back in the clean
              // automatic layout, and have the AI rethink structure and connectors.
              if (hasPins) commit([{ t: 'board.reflow' }])
              send({ type: 'refine' })
            }}
            className="flex h-8 items-center gap-1.5 rounded-md px-2.5 text-[13px] font-medium text-nav hover:bg-oat-medium hover:text-ink disabled:pointer-events-none disabled:opacity-40"
          >
            <Sparkles size={15} aria-hidden />
            {phase === 'refining' ? 'Tidying…' : 'Tidy up'}
          </button>
        </Tip>
        <div className="mx-1.5 h-5 w-px bg-line" />
        <People />
        <SharePanel />
        <IconButton
          label={transcriptOpen ? 'Hide transcript' : 'Show transcript'}
          active={transcriptOpen}
          onClick={toggleTranscript}
        >
          <PanelRight size={16} aria-hidden />
        </IconButton>
      </div>
    </header>
  )
}
