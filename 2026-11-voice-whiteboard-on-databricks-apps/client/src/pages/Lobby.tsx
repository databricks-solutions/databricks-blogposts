import { ArrowRight, Globe, Mic, Plus, Trash2 } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'
import { initials, Mark } from '@/components/TopBar'
import { api } from '@/lib/api'
import type { BoardList, BoardSummary } from '@/lib/types'

const relative = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })
function ago(epochSeconds: number): string {
  const diff = epochSeconds - Date.now() / 1000
  const abs = Math.abs(diff)
  if (abs < 60) return 'just now'
  if (abs < 3600) return relative.format(Math.round(diff / 60), 'minute')
  if (abs < 86400) return relative.format(Math.round(diff / 3600), 'hour')
  return relative.format(Math.round(diff / 86400), 'day')
}

function Row({
  board,
  onOpen,
  onDelete,
}: {
  board: BoardSummary
  onOpen: (id: string) => void
  onDelete?: (board: BoardSummary) => Promise<void>
}) {
  // Deleting is permanent and affects everyone the board is shared with, so it takes two clicks.
  const [confirming, setConfirming] = useState(false)
  const [deleting, setDeleting] = useState(false)
  return (
    <li className="group relative flex h-16 items-center border-b border-line">
      <a
        href={`/b/${board.id}${location.search}`}
        onClick={(e) => {
          if (e.metaKey || e.ctrlKey || e.shiftKey) return
          e.preventDefault()
          onOpen(board.id)
        }}
        className="-mx-3 flex h-full min-w-0 flex-1 items-center gap-4 rounded-lg px-3 hover:bg-oat-medium"
      >
        <div className="min-w-0 flex-1">
          <p className="flex items-center gap-2 text-[15px] font-semibold text-ink">
            <span className="truncate">{board.title}</span>
            {board.live && (
              <span className="flex shrink-0 items-center gap-1 rounded-full bg-tint-lava px-2 py-0.5 font-mono text-[10px] font-medium text-lava-700 uppercase">
                <Mic size={10} aria-hidden />
                Live
              </span>
            )}
            {board.link === 'workspace' && (
              <span
                title="Anyone in this workspace with the link can open it"
                className="shrink-0 text-navy-400"
              >
                <Globe size={13} aria-label="Open to the workspace by link" />
              </span>
            )}
          </p>
          <p className="tabular mt-0.5 truncate font-mono text-[11.5px] text-body">
            {board.nodes} {board.nodes === 1 ? 'shape' : 'shapes'}
            {!board.mine && ` · ${board.owner || board.owner_email}`} · {ago(board.updated)}
          </p>
        </div>
        <div className="flex shrink-0 items-center">
          {board.people.slice(0, 5).map((p, i) => (
            <span
              key={i}
              title={p.name}
              className="-ml-1.5 grid size-6 place-items-center rounded-full border-2 border-oat text-[9.5px] font-semibold text-white first:ml-0 group-hover:border-oat-medium"
              style={{ background: p.color }}
            >
              {initials(p.name)}
            </span>
          ))}
        </div>
        <ArrowRight
          size={16}
          className="shrink-0 text-navy-400 transition-transform group-hover:translate-x-0.5 group-hover:text-ink"
          aria-hidden
        />
      </a>
      {onDelete &&
        (confirming ? (
          <span className="ml-3 flex shrink-0 items-center gap-1.5">
            <button
              type="button"
              disabled={deleting}
              onClick={async () => {
                setDeleting(true)
                await onDelete(board)
                setDeleting(false)
                setConfirming(false)
              }}
              className="h-8 rounded-md bg-maroon-600 px-2.5 text-[12.5px] font-medium text-white hover:bg-maroon-700 disabled:opacity-60"
            >
              {deleting ? 'Deleting…' : 'Delete for everyone'}
            </button>
            <button
              type="button"
              onClick={() => setConfirming(false)}
              className="h-8 rounded-md px-2 text-[12.5px] text-nav hover:bg-oat-medium"
            >
              Keep
            </button>
          </span>
        ) : (
          <button
            type="button"
            aria-label={`Delete ${board.title}`}
            onClick={() => setConfirming(true)}
            className="ml-3 grid size-8 shrink-0 place-items-center rounded-md text-navy-400 opacity-0 group-focus-within:opacity-100 group-hover:opacity-100 hover:bg-tint-lava hover:text-lava-700 focus-visible:opacity-100"
          >
            <Trash2 size={15} aria-hidden />
          </button>
        ))}
    </li>
  )
}

function Section({
  id,
  title,
  boards,
  empty,
  onOpen,
  onDelete,
}: {
  id: string
  title: string
  boards: BoardSummary[] | null
  empty: string
  onOpen: (id: string) => void
  onDelete?: (board: BoardSummary) => Promise<void>
}) {
  return (
    <section className="mt-12" aria-labelledby={id}>
      <h2
        id={id}
        className="flex items-baseline gap-2 font-mono text-[11px] font-medium tracking-[0.12em] text-nav uppercase"
      >
        {title}
        {boards && boards.length > 0 && (
          <span className="tabular text-navy-400">{boards.length}</span>
        )}
      </h2>
      <div className="mt-3 h-px bg-line" />
      {boards === null ? (
        <ul aria-label="Loading boards…">
          {[0, 1].map((i) => (
            <li key={i} className="flex h-16 items-center border-b border-line">
              <div className="h-4 w-48 animate-shimmer rounded bg-[linear-gradient(90deg,#EEEDE9_25%,#F9F7F4_50%,#EEEDE9_75%)] bg-[length:200%_100%]" />
            </li>
          ))}
        </ul>
      ) : boards.length === 0 ? (
        <p className="py-6 text-[14.5px] text-body">{empty}</p>
      ) : (
        <ul>
          {boards.map((b) => (
            <Row key={b.id} board={b} onOpen={onOpen} onDelete={onDelete} />
          ))}
        </ul>
      )}
    </section>
  )
}

export function Lobby({ onOpen }: { onOpen: (id: string) => void }) {
  const [boards, setBoards] = useState<BoardList | null>(null)
  const [me, setMe] = useState<{ name: string; email: string } | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  const load = useCallback(
    () =>
      api
        .boards()
        .then((b) => {
          setBoards(b)
          setError(null)
        })
        .catch(() => setError('Could not load your boards. Check your connection and reload.')),
    [],
  )

  useEffect(() => {
    document.title = 'Voice Whiteboard'
    void load()
    void api
      .me()
      .then(setMe)
      .catch(() => undefined)
    const timer = window.setInterval(load, 5000)
    return () => window.clearInterval(timer)
  }, [load])

  const create = async () => {
    setCreating(true)
    try {
      onOpen((await api.create()).id)
    } catch {
      setError('Could not create a board. Try again.')
      setCreating(false)
    }
  }

  const remove = async (board: BoardSummary) => {
    try {
      await api.remove(board.id)
      await load()
    } catch (e) {
      setError(
        e instanceof Error
          ? `Could not delete “${board.title}”: ${e.message}`
          : 'Could not delete the board.',
      )
    }
  }

  return (
    <div className="min-h-full">
      <header className="mx-auto flex h-14 max-w-5xl items-center gap-2.5 px-6">
        <Mark />
        <span className="text-[15px] font-semibold text-ink">Voice Whiteboard</span>
        {me && (
          <span className="ml-auto truncate font-mono text-[11.5px] text-body" title={me.email}>
            {me.name}
          </span>
        )}
      </header>

      <main className="mx-auto max-w-5xl px-6 pt-14 pb-24">
        <p className="font-mono text-[12px] tracking-[0.12em] text-lava-700 uppercase">
          Live diagramming
        </p>
        <h1 className="mt-3 max-w-2xl text-[44px] leading-[1.08] font-bold tracking-[-0.01em] text-ink max-sm:text-[34px]">
          Say it. <span className="text-lava-600">See it drawn.</span>
        </h1>
        <p className="mt-5 max-w-xl text-[17px] leading-relaxed text-body">
          Talk through an architecture or a process and the diagram draws itself while you speak.
          Your boards are private until you share them, and every change is saved as you go.
        </p>
        <button
          type="button"
          onClick={create}
          disabled={creating}
          className="mt-8 flex h-11 items-center gap-2 rounded-xl bg-navy-900 pr-5 pl-4 text-[15px] font-semibold text-white hover:bg-navy-800 active:bg-navy-700 disabled:opacity-60"
        >
          <Plus size={18} aria-hidden />
          {creating ? 'Creating…' : 'New board'}
        </button>

        {error && (
          <p
            className="mt-8 rounded-lg bg-tint-maroon px-4 py-3 text-[14px] text-maroon-700"
            role="alert"
          >
            {error}
          </p>
        )}
        <Section
          id="my-boards"
          title="My boards"
          boards={boards ? boards.mine : null}
          empty="You have no boards yet. Create one, then use Share to bring others in."
          onOpen={onOpen}
          onDelete={remove}
        />
        <Section
          id="shared-boards"
          title="Shared with me"
          boards={boards ? boards.shared : null}
          empty="Boards other people share with you will appear here."
          onOpen={onOpen}
        />
      </main>
    </div>
  )
}
