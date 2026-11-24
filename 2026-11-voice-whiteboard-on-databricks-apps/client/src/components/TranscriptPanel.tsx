import { Keyboard, X } from 'lucide-react'
import { useEffect, useRef } from 'react'
import { useSession } from '@/lib/session'
import type { Segment } from '@/lib/types'
import { useUi } from '@/lib/ui'

const secs = new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
})
const clock = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit' })

/** How long it took from the end of the utterance to the first shape appearing. */
function latency(seg: Segment): string | null {
  const first = seg.ms.shape ?? seg.ms.first
  if (first == null) return null
  return `${secs.format((seg.ms.stt ?? 0) + first)} s`
}

function Row({ seg, showSpeaker }: { seg: Segment; showSpeaker: boolean }) {
  const skipped = seg.state === 'skipped'
  const failed = seg.state === 'failed'
  return (
    <li className="animate-rise px-4 py-1.5">
      {showSpeaker && (
        <p className="mt-2 mb-1 flex items-center gap-1.5 font-mono text-[10.5px] tracking-wide text-body uppercase">
          <span className="size-1.5 rounded-full" style={{ background: seg.color }} aria-hidden />
          {seg.user}
          <span className="text-navy-400 normal-case">{clock.format(seg.t * 1000)}</span>
        </p>
      )}
      {seg.state === 'transcribing' ? (
        <div
          className="h-4 w-3/5 animate-shimmer rounded bg-[linear-gradient(90deg,#EEEDE9_25%,#F9F7F4_50%,#EEEDE9_75%)] bg-[length:200%_100%]"
          role="status"
          aria-label="Transcribing…"
        />
      ) : failed ? (
        <p className="text-[13px] text-maroon-600">
          Couldn’t transcribe that. Say it again or type it.
        </p>
      ) : (
        <p
          className={`text-[13.5px] leading-snug break-words ${skipped ? 'text-navy-400' : 'text-ink'}`}
        >
          {seg.source === 'typed' && (
            <Keyboard
              size={12}
              className="mr-1 inline -translate-y-px text-navy-400"
              aria-label="Typed"
            />
          )}
          {seg.text}
          <span className="tabular ml-2 font-mono text-[10px] whitespace-nowrap text-navy-400">
            {skipped
              ? 'not drawn'
              : seg.state === 'drawn'
                ? seg.ops
                  ? latency(seg)
                  : 'no change'
                : '…'}
          </span>
        </p>
      )}
    </li>
  )
}

export function TranscriptPanel() {
  const transcript = useSession((s) => s.transcript)
  const phase = useSession((s) => s.ai.phase)
  const refine = useSession((s) => s.ai.refine)
  const models = useSession((s) => s.models)
  const toggle = useUi((s) => s.toggleTranscript)
  const scroller = useRef<HTMLDivElement>(null)
  const stick = useRef(true)

  useEffect(() => {
    const el = scroller.current
    if (el && stick.current) el.scrollTop = el.scrollHeight
  }, [transcript])

  const short = (m: string) =>
    m
      .replace('databricks-claude-', '')
      .replace(/-(\d)-(\d)$/, ' $1.$2')
      .replace(/-(\d)$/, ' $1')
  return (
    <aside
      className="flex w-[320px] shrink-0 flex-col border-l border-line bg-white max-md:hidden"
      aria-label="Transcript"
    >
      <div className="flex h-11 shrink-0 items-center justify-between border-b border-line pr-2 pl-4">
        <h2 className="font-mono text-[11px] font-medium tracking-[0.1em] text-nav uppercase">
          Transcript
        </h2>
        <button
          type="button"
          aria-label="Hide transcript"
          onClick={toggle}
          className="grid size-7 place-items-center rounded-md text-body hover:bg-oat-medium hover:text-ink"
        >
          <X size={15} aria-hidden />
        </button>
      </div>
      <div
        ref={scroller}
        className="min-h-0 flex-1 overflow-y-auto overscroll-contain py-2"
        onScroll={(e) => {
          const el = e.currentTarget
          stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 48
        }}
      >
        {transcript.length === 0 ? (
          <p className="px-4 py-6 text-[13.5px] leading-relaxed text-body">
            What is said shows up here, with how long each phrase took to reach the board.
          </p>
        ) : (
          <ol>
            {transcript.map((seg, i) => (
              <Row
                key={seg.id}
                seg={seg}
                showSpeaker={i === 0 || transcript[i - 1].user !== seg.user}
              />
            ))}
          </ol>
        )}
      </div>
      <div
        className="shrink-0 border-t border-line px-4 py-2.5 font-mono text-[10.5px] leading-relaxed text-body"
        role="status"
      >
        <p className="flex items-center gap-1.5">
          <span
            className={`size-1.5 rounded-full ${phase === 'idle' ? 'bg-navy-300' : 'animate-pulse bg-lava-600'}`}
            aria-hidden
          />
          {phase === 'drawing'
            ? `Drawing · ${short(models.fast)}`
            : phase === 'refining'
              ? `Tidying · ${short(models.refine)}`
              : refine?.total
                ? `Last tidy-up ${secs.format(refine.total)} s · ${refine.ops} ${refine.ops === 1 ? 'change' : 'changes'}`
                : 'Idle'}
        </p>
      </div>
    </aside>
  )
}
