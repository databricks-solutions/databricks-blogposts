import { ChatBubbleLeftIcon as MessageSquare, PlusIcon as Plus } from "@heroicons/react/24/outline";
import { cn } from "@/lib/cn";
import { relativeTime } from "@/lib/time";
import { useChat } from "@/stores/chat";

export function Sidebar() {
  const { me, persona, sessions, sessionId, selectPersona, newConversation, openSession, sending } = useChat();
  const personas = me?.personas ?? [];

  return (
    <aside className="flex w-64 shrink-0 flex-col border-r border-oat-300 bg-oat-100">
      {personas.length > 0 && (
        <section className="border-b border-oat-300 p-3">
          <h2 className="mb-2 px-1 text-2xs font-semibold uppercase tracking-wider text-navy-400">Working as</h2>
          <ul className="space-y-1">
            {personas.map((p) => (
              <li key={p.id}>
                <button
                  type="button"
                  disabled={sending}
                  onClick={() => void selectPersona(p.id)}
                  aria-pressed={persona === p.id}
                  className={cn(
                    "flex w-full items-center gap-2.5 rounded-md px-2 py-1.5 text-left transition-colors disabled:opacity-60",
                    persona === p.id ? "bg-white shadow-card ring-1 ring-oat-300" : "hover:bg-oat-300/50",
                  )}
                >
                  <span
                    className={cn(
                      "flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-2xs font-semibold",
                      persona === p.id ? "bg-navy text-white" : "bg-oat-300 text-navy-600",
                    )}
                    aria-hidden
                  >
                    {p.name.split(" ").map((part) => part[0]).join("")}
                  </span>
                  <span className="min-w-0">
                    <span className="block text-[13px] font-medium leading-tight">{p.name}</span>
                    <span className="block truncate text-2xs text-navy-400">
                      {p.role} · Plant {p.plant} {p.plant_name}
                    </span>
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      )}
      <div className="p-3">
        <button
          type="button"
          onClick={newConversation}
          disabled={sending}
          className="flex w-full items-center justify-center gap-1.5 rounded-md border border-oat-400 bg-white py-1.5 text-[13px] font-medium hover:border-navy-400 disabled:opacity-60"
        >
          <Plus className="h-3.5 w-3.5" aria-hidden /> New conversation
        </button>
      </div>
      <nav className="min-h-0 flex-1 overflow-y-auto px-3 pb-3" aria-label="Recent conversations">
        <h2 className="mb-1.5 px-1 text-2xs font-semibold uppercase tracking-wider text-navy-400">Recent</h2>
        {sessions.length === 0 && <p className="px-1 text-xs text-navy-400">No conversations yet.</p>}
        <ul className="space-y-0.5">
          {sessions.map((s) => (
            <li key={s.id}>
              <button
                type="button"
                disabled={sending}
                onClick={() => void openSession(s.id)}
                className={cn(
                  "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-xs disabled:opacity-60",
                  s.id === sessionId ? "bg-white font-medium ring-1 ring-oat-300" : "text-navy-600 hover:bg-oat-300/50",
                )}
              >
                <MessageSquare className="mt-0.5 h-3 w-3 shrink-0 self-start text-navy-400" aria-hidden />
                <span className="min-w-0">
                  <span className={cn("line-clamp-2 leading-snug", !s.title && "font-mono text-2xs")}>
                    {s.title ?? s.id.slice(0, 13)}
                  </span>
                  <span className="block text-2xs text-navy-400">{relativeTime(s.updated)}</span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      </nav>
    </aside>
  );
}
