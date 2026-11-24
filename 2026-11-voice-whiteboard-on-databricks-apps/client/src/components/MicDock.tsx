import { ArrowUp, Hand, Mic, Square } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import { MicCapture } from '@/lib/audio'
import { iHoldMic, useSession } from '@/lib/session'

const BARS = 28

/** Scrolling level meter. Drawn straight to the DOM so audio frames never re-render React. */
function Meter({ level }: { level: React.RefObject<number> }) {
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const history = new Array<number>(BARS).fill(0)
    let raf = 0
    let last = 0
    const draw = (now: number) => {
      raf = requestAnimationFrame(draw)
      if (now - last < 45) return
      last = now
      history.push(Math.min(1, Math.sqrt(level.current) * 3.2))
      history.shift()
      const bars = ref.current?.children
      if (!bars) return
      for (let i = 0; i < BARS; i++) {
        ;(bars[i] as HTMLElement).style.transform = `scaleY(${Math.max(0.08, history[i])})`
      }
    }
    raf = requestAnimationFrame(draw)
    return () => cancelAnimationFrame(raf)
  }, [level])
  return (
    <div ref={ref} className="flex h-7 items-center gap-[2px]" aria-hidden>
      {Array.from({ length: BARS }, (_, i) => (
        <span
          key={i}
          className="h-full w-[2.5px] origin-center rounded-full bg-lava-600"
          style={{ transform: 'scaleY(0.08)' }}
        />
      ))}
    </div>
  )
}

function SayBox({ placeholder }: { placeholder: string }) {
  const send = useSession((s) => s.send)
  const [text, setText] = useState('')
  const submit = () => {
    const t = text.trim()
    if (!t) return
    send({ type: 'say', text: t })
    setText('')
  }
  return (
    <form
      className="flex min-w-0 flex-1 items-center gap-1.5"
      onSubmit={(e) => {
        e.preventDefault()
        submit()
      }}
    >
      <input
        value={text}
        onChange={(e) => setText(e.target.value)}
        aria-label="Describe what to draw"
        name="say"
        autoComplete="off"
        placeholder={placeholder}
        className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-oat px-3 text-[14px] text-ink placeholder:text-navy-400 focus:border-navy-400 focus:bg-white"
      />
      <button
        type="submit"
        aria-label="Send"
        disabled={!text.trim()}
        className="grid size-9 shrink-0 place-items-center rounded-lg bg-navy-900 text-white hover:bg-navy-800 disabled:pointer-events-none disabled:opacity-30"
      >
        <ArrowUp size={16} aria-hidden />
      </button>
    </form>
  )
}

export function MicDock() {
  const mine = useSession(iHoldMic)
  const holder = useSession((s) => s.mic.holder)
  const speaking = useSession((s) => s.mic.speaking)
  const phase = useSession((s) => s.ai.phase)
  const stt = useSession((s) => s.stt)
  const status = useSession((s) => s.status)
  const transcribing = useSession((s) => s.transcript.some((x) => x.state === 'transcribing'))
  const send = useSession((s) => s.send)
  const sendAudio = useSession((s) => s.sendAudio)
  const notify = useSession((s) => s.notify)

  const capture = useRef<MicCapture | null>(null)
  const level = useRef(0)
  const [iSpeak, setISpeak] = useState(false)
  const [starting, setStarting] = useState(false)
  const [micError, setMicError] = useState(false)

  // Holding the mic lock and capturing audio go together: capture starts when
  // the server grants the lock and stops the moment it is lost for any reason.
  useEffect(() => {
    if (!mine) {
      capture.current?.stop()
      capture.current = null
      setISpeak(false)
      return
    }
    if (capture.current) return
    const cap = new MicCapture({
      onLevel: (rms) => (level.current = rms),
      onSpeaking: (on) => {
        setISpeak(on)
        useSession.getState().send({ type: 'vad', speaking: on })
      },
      onSegment: (frame) => sendAudio(frame),
    })
    capture.current = cap
    setStarting(true)
    cap
      .start()
      .then(() => setMicError(false))
      .catch(() => {
        // No microphone (or permission denied): keep the lock so typing still works.
        setMicError(true)
        capture.current = null
        notify('info', 'No microphone available. You have the floor: type what to draw instead.')
      })
      .finally(() => setStarting(false))
  }, [mine, sendAudio, notify])

  useEffect(() => () => capture.current?.stop(), [])

  const offline = status !== 'open'
  const warming = stt.state === 'warming'
  const sttDown = stt.state === 'error'

  let line: string
  if (phase === 'refining') line = 'Tidying the diagram…'
  else if (phase === 'drawing') line = 'Drawing…'
  else if (transcribing) line = 'Transcribing…'
  else if (mine)
    line = starting
      ? 'Starting the microphone…'
      : micError
        ? 'Type what to draw'
        : iSpeak
          ? 'Listening…'
          : 'Go ahead, I’m listening'
  else if (holder) line = speaking ? `${holder.name} is talking` : `${holder.name} has the mic`
  else line = 'Nobody is talking'

  return (
    <div className="pointer-events-none absolute inset-x-0 bottom-4 z-10 flex justify-center px-4">
      <div className="pointer-events-auto w-full max-w-[560px] rounded-2xl border border-line bg-white p-2 shadow-float">
        {(warming || sttDown) && (
          <p
            className="mb-2 rounded-lg bg-tint-yellow px-3 py-1.5 text-[12.5px] text-yellow-800"
            role="status"
          >
            {warming
              ? `${stt.detail || 'Waking the speech model'}… Voice will work in a few minutes; typing works now.`
              : `Voice is unavailable: ${stt.detail}. Typing still works.`}
          </p>
        )}
        <div className="flex items-center gap-2">
          {mine ? (
            <>
              <button
                type="button"
                onClick={() => send({ type: 'mic.release' })}
                aria-label="Stop and release the mic"
                className="relative grid size-11 shrink-0 place-items-center rounded-xl bg-lava-600 text-navy-900 hover:bg-lava-500 active:bg-lava-700"
              >
                <Square size={16} fill="currentColor" aria-hidden />
              </button>
              <div className="flex w-[132px] shrink-0 flex-col justify-center max-sm:hidden">
                {micError ? <span className="h-7" /> : <Meter level={level} />}
                <span className="truncate font-mono text-[10.5px] text-body" aria-live="polite">
                  {line}
                </span>
              </div>
              <SayBox placeholder="…or type what to draw" />
            </>
          ) : holder ? (
            <>
              <div
                className="relative grid size-11 shrink-0 place-items-center rounded-xl text-white"
                style={{ background: holder.color }}
                aria-hidden
              >
                <Mic size={18} />
                {speaking && (
                  <span
                    className="absolute inset-0 animate-ping rounded-xl opacity-30"
                    style={{ background: holder.color }}
                  />
                )}
              </div>
              <div className="min-w-0 flex-1">
                <p className="truncate text-[14px] font-medium text-ink" aria-live="polite">
                  {line}
                </p>
                <p className="truncate text-[12px] text-body">
                  One person talks at a time. You can still edit the board.
                </p>
              </div>
              <button
                type="button"
                onClick={() => {
                  send({ type: 'mic.ask' })
                  notify('info', `Asked ${holder.name} for the mic.`)
                }}
                className="flex h-9 shrink-0 items-center gap-1.5 rounded-lg border border-line px-3 text-[13px] font-medium text-nav hover:bg-oat-medium"
              >
                <Hand size={15} aria-hidden />
                Ask for the mic
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                disabled={offline}
                onClick={() => send({ type: 'mic.request' })}
                className="flex h-11 shrink-0 items-center gap-2 rounded-xl bg-navy-900 pr-4 pl-3.5 text-[14px] font-semibold text-white hover:bg-navy-800 active:bg-navy-700 disabled:opacity-40"
              >
                <Mic size={18} aria-hidden />
                Start talking
              </button>
              <SayBox placeholder="…or type what to draw, e.g. “a web app calls an orders API”" />
            </>
          )}
        </div>
      </div>
    </div>
  )
}
