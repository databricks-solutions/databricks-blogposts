import { X } from 'lucide-react'
import { useSession } from '@/lib/session'

/** Transient messages (mic hand-offs, errors). Announced politely to screen readers. */
export function Notices() {
  const notices = useSession((s) => s.notices)
  const dismiss = useSession((s) => s.dismiss)
  return (
    <div
      className="pointer-events-none absolute top-16 left-1/2 z-30 flex w-full max-w-md -translate-x-1/2 flex-col items-center gap-2 px-4"
      aria-live="polite"
    >
      {notices.map((n) => (
        <div
          key={n.id}
          className={`pointer-events-auto flex animate-rise items-center gap-3 rounded-xl py-2 pr-2 pl-4 text-[13.5px] shadow-float ${
            n.level === 'error' ? 'bg-maroon-700 text-white' : 'bg-navy-900 text-white'
          }`}
        >
          <span className="min-w-0 break-words">{n.text}</span>
          {n.action && (
            <button
              type="button"
              onClick={() => {
                n.action!.run()
                dismiss(n.id)
              }}
              className="shrink-0 rounded-md bg-white px-2.5 py-1 text-[12.5px] font-semibold text-navy-900 hover:bg-oat-medium"
            >
              {n.action.label}
            </button>
          )}
          <button
            type="button"
            aria-label="Dismiss"
            onClick={() => dismiss(n.id)}
            className="grid size-6 shrink-0 place-items-center rounded-md text-navy-300 hover:bg-white/10 hover:text-white"
          >
            <X size={14} aria-hidden />
          </button>
        </div>
      ))}
    </div>
  )
}
