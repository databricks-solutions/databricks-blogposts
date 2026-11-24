import { ReactFlowProvider } from '@xyflow/react'
import { useEffect, useState } from 'react'
import { Canvas } from '@/components/Canvas'
import { MicDock } from '@/components/MicDock'
import { Notices } from '@/components/Notices'
import { Palette } from '@/components/Palette'
import { TopBar } from '@/components/TopBar'
import { TranscriptPanel } from '@/components/TranscriptPanel'
import { api } from '@/lib/api'
import { useSession } from '@/lib/session'
import { useUi } from '@/lib/ui'

/** Shown when the board exists but the signed-in person is not on its share list. */
function NoAccess({ owner, onHome }: { owner: string; onHome: () => void }) {
  const [email, setEmail] = useState('')
  useEffect(() => {
    void api
      .me()
      .then((m) => setEmail(m.email))
      .catch(() => undefined)
  }, [])
  return (
    <main className="grid h-full place-items-center px-6">
      <div className="max-w-sm text-center">
        <p className="font-mono text-[11px] tracking-[0.12em] text-lava-700 uppercase">No access</p>
        <h1 className="mt-2 text-2xl font-bold text-ink">
          This board has not been shared with you.
        </h1>
        <p className="mt-3 text-[15px] leading-relaxed break-words text-body">
          Ask {owner || 'its owner'} to share it with{' '}
          {email ? <span className="font-medium text-ink">{email}</span> : 'you'}, or to open its
          link to the workspace.
        </p>
        <button
          type="button"
          onClick={onHome}
          className="mt-6 h-10 rounded-lg bg-navy-900 px-4 text-[14px] font-semibold text-white hover:bg-navy-800"
        >
          Go to my boards
        </button>
      </div>
    </main>
  )
}

export function BoardPage({ boardId, onHome }: { boardId: string; onHome: () => void }) {
  const connect = useSession((s) => s.connect)
  const disconnect = useSession((s) => s.disconnect)
  const status = useSession((s) => s.status)
  const deniedBy = useSession((s) => s.deniedBy)
  const title = useSession((s) => s.doc.title)
  const transcriptOpen = useUi((s) => s.transcriptOpen)

  useEffect(() => {
    connect(boardId)
    return disconnect
  }, [boardId, connect, disconnect])

  useEffect(() => {
    document.title = `${title} · Voice Whiteboard`
  }, [title])

  if (status === 'denied') return <NoAccess owner={deniedBy} onHome={onHome} />

  if (status === 'gone') {
    return (
      <main className="grid h-full place-items-center px-6">
        <div className="max-w-sm text-center">
          <p className="font-mono text-[11px] tracking-[0.12em] text-lava-700 uppercase">
            Board not found
          </p>
          <h1 className="mt-2 text-2xl font-bold text-ink">This board no longer exists.</h1>
          <p className="mt-3 text-[15px] text-body">
            It may have been deleted by its owner. Start a new one from your boards.
          </p>
          <button
            type="button"
            onClick={onHome}
            className="mt-6 h-10 rounded-lg bg-navy-900 px-4 text-[14px] font-semibold text-white hover:bg-navy-800"
          >
            Go to my boards
          </button>
        </div>
      </main>
    )
  }

  return (
    <ReactFlowProvider>
      <div className="flex h-full flex-col">
        <TopBar onHome={onHome} />
        <div className="flex min-h-0 flex-1">
          <main className="relative min-w-0 flex-1">
            <Canvas />
            <Palette />
            <MicDock />
            <Notices />
            {status === 'connecting' && (
              <div className="absolute inset-0 z-40 grid place-items-center bg-oat" role="status">
                <p className="font-mono text-[12px] tracking-wide text-body">Opening the board…</p>
              </div>
            )}
          </main>
          {transcriptOpen && <TranscriptPanel />}
        </div>
      </div>
    </ReactFlowProvider>
  )
}
