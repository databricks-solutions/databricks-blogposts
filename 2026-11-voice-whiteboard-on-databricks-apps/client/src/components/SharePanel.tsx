import * as Popover from '@radix-ui/react-popover'
import { Check, Globe, Link2, Lock, UserPlus, X } from 'lucide-react'
import { useState } from 'react'
import { api } from '@/lib/api'
import { useSession } from '@/lib/session'
import { initials } from './TopBar'

const EMAIL = /^[^@\s,;<>]+@[^@\s,;<>]+$/

/** Who can open this board: invite people by email, or open the link to the workspace. */
export function SharePanel() {
  const boardId = useSession((s) => s.boardId)
  const access = useSession((s) => s.access)
  const me = useSession((s) => s.me)
  const [draft, setDraft] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [copied, setCopied] = useState(false)
  if (!boardId || !access) return null
  const owner = access.role === 'owner'

  const change = async (body: Parameters<typeof api.share>[1]) => {
    setBusy(true)
    setError(null)
    try {
      // The server also pushes the new sharing state to everyone on the board.
      useSession.setState({ access: await api.share(boardId, body) })
      return true
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not update sharing. Try again.')
      return false
    } finally {
      setBusy(false)
    }
  }

  const invite = async () => {
    const emails = draft
      .split(/[\s,;]+/)
      .map((e) => e.trim().toLowerCase())
      .filter(Boolean)
    if (!emails.length) return
    const bad = emails.find((e) => !EMAIL.test(e))
    if (bad) {
      setError(`“${bad}” is not an email address.`)
      return
    }
    if (await change({ add: emails })) setDraft('')
  }

  const copy = async () => {
    const link = `${location.origin}${location.pathname}`
    try {
      await navigator.clipboard.writeText(link)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1800)
    } catch {
      setError(`Copy this link: ${link}`)
    }
  }

  const row = 'flex items-center gap-2.5 py-1.5'
  const avatar =
    'grid size-7 shrink-0 place-items-center rounded-full bg-tint-navy text-[10.5px] font-semibold text-navy-800'
  return (
    <Popover.Root onOpenChange={() => setError(null)}>
      <Popover.Trigger asChild>
        <button
          type="button"
          className="ml-2 flex h-8 items-center gap-1.5 rounded-md bg-navy-900 px-3 text-[13px] font-medium text-white hover:bg-navy-800 active:bg-navy-700"
        >
          {access.link === 'workspace' ? (
            <Globe size={15} aria-hidden />
          ) : (
            <Lock size={14} aria-hidden />
          )}
          Share
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          align="end"
          sideOffset={8}
          className="z-50 w-[380px] max-w-[calc(100vw-24px)] animate-rise rounded-xl border border-line bg-white p-4 shadow-float"
        >
          <div className="flex items-start justify-between">
            <h2 className="text-[15px] font-semibold text-ink">Share this board</h2>
            <Popover.Close
              aria-label="Close"
              className="-mt-1 -mr-1 grid size-7 place-items-center rounded-md text-body hover:bg-oat-medium hover:text-ink"
            >
              <X size={15} aria-hidden />
            </Popover.Close>
          </div>

          {owner && (
            <form
              className="mt-3 flex gap-2"
              onSubmit={(e) => {
                e.preventDefault()
                void invite()
              }}
            >
              <input
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                type="text"
                inputMode="email"
                name="invite-email"
                autoComplete="off"
                spellCheck={false}
                aria-label="Email addresses to share with"
                aria-invalid={!!error}
                placeholder="name@company.com…"
                className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-oat px-3 text-[13.5px] text-ink placeholder:text-navy-400 focus:border-navy-400 focus:bg-white"
              />
              <button
                type="submit"
                disabled={busy || !draft.trim()}
                className="flex h-9 shrink-0 items-center gap-1.5 rounded-lg bg-navy-900 px-3 text-[13px] font-medium text-white hover:bg-navy-800 disabled:pointer-events-none disabled:opacity-40"
              >
                <UserPlus size={14} aria-hidden />
                {busy ? 'Sharing…' : 'Add'}
              </button>
            </form>
          )}
          {error && (
            <p className="mt-2 text-[12.5px] break-words text-maroon-600" role="alert">
              {error}
            </p>
          )}

          <h3 className="mt-4 font-mono text-[10.5px] font-medium tracking-[0.1em] text-nav uppercase">
            People with access
          </h3>
          <ul className="mt-1 max-h-52 overflow-y-auto overscroll-contain">
            <li className={row}>
              <span className={avatar}>{initials(access.owner.name || access.owner.email)}</span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[13.5px] text-ink">
                  {access.owner.name || access.owner.email}
                  {access.owner.email === me?.email && <span className="text-body"> (you)</span>}
                </span>
                <span className="block truncate text-[11.5px] text-body">{access.owner.email}</span>
              </span>
              <span className="font-mono text-[10.5px] text-body">Owner</span>
            </li>
            {access.people.map((p) => (
              <li key={p.email} className={row}>
                <span className={avatar}>{initials(p.name || p.email)}</span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-[13.5px] text-ink">
                    {p.name || p.email}
                    {p.email === me?.email && <span className="text-body"> (you)</span>}
                  </span>
                  <span className="block truncate text-[11.5px] text-body">
                    {p.name ? p.email : 'Has not opened it yet'}
                    {p.via === 'link' && ' · joined by link'}
                  </span>
                </span>
                {owner ? (
                  <button
                    type="button"
                    disabled={busy}
                    onClick={() => void change({ remove: [p.email] })}
                    aria-label={`Stop sharing with ${p.name || p.email}`}
                    className="rounded-md px-2 py-1 text-[12px] text-body hover:bg-tint-lava hover:text-lava-700 disabled:opacity-40"
                  >
                    Remove
                  </button>
                ) : (
                  <span className="font-mono text-[10.5px] text-body">Can edit</span>
                )}
              </li>
            ))}
          </ul>

          <h3 className="mt-4 font-mono text-[10.5px] font-medium tracking-[0.1em] text-nav uppercase">
            Link access
          </h3>
          <div className="mt-1.5 flex items-center gap-2.5">
            <span className="grid size-7 shrink-0 place-items-center rounded-full bg-oat-medium text-nav">
              {access.link === 'workspace' ? (
                <Globe size={14} aria-hidden />
              ) : (
                <Lock size={13} aria-hidden />
              )}
            </span>
            {owner ? (
              <select
                value={access.link}
                disabled={busy}
                aria-label="Who can open the link"
                onChange={(e) => void change({ link: e.target.value })}
                className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-white px-2 text-[13px] text-ink"
              >
                <option value="restricted">Restricted: only people added above</option>
                <option value="workspace">Anyone in this workspace with the link</option>
              </select>
            ) : (
              <span className="min-w-0 flex-1 text-[13px] text-ink">
                {access.link === 'workspace'
                  ? 'Anyone in this workspace with the link'
                  : 'Restricted: only people added above'}
              </span>
            )}
          </div>
          <p className="mt-2 text-[12px] leading-relaxed text-body">
            {access.link === 'workspace'
              ? 'Anyone signed in to this workspace who opens the link can edit, and the board then appears in their list.'
              : 'Only the people listed can open this board. Everyone with access can edit.'}
            {!owner && ' Only the owner can change sharing.'}
          </p>

          <button
            type="button"
            onClick={() => void copy()}
            className="mt-3 flex h-9 w-full items-center justify-center gap-1.5 rounded-lg border border-line text-[13px] font-medium text-nav hover:bg-oat-medium"
          >
            {copied ? <Check size={15} aria-hidden /> : <Link2 size={15} aria-hidden />}
            <span aria-live="polite">{copied ? 'Link copied' : 'Copy link'}</span>
          </button>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  )
}
