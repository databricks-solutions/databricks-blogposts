import { ArrowUpIcon as ArrowUp } from "@heroicons/react/24/outline";
import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from "react";
import { DEFAULT_PROMPTS, PROMPTS } from "@/lib/prompts";
import { useChat } from "@/stores/chat";
import { MessageBubble } from "./MessageBubble";

export function Chat() {
  const { me, persona, messages, send, sending } = useChat();
  const [draft, setDraft] = useState("");
  const scroller = useRef<HTMLDivElement>(null);
  const current = me?.personas.find((p) => p.id === persona);
  const prompts = (persona && PROMPTS[persona]) || DEFAULT_PROMPTS;

  useEffect(() => {
    scroller.current?.scrollTo({ top: scroller.current.scrollHeight, behavior: "smooth" });
  }, [messages]);

  const submit = (text: string) => {
    const value = text.trim();
    if (!value || sending) return;
    setDraft("");
    void send(value);
  };
  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    submit(draft);
  };
  const onKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      submit(draft);
    }
  };

  return (
    <main className="flex min-w-0 flex-1 flex-col bg-white">
      <div ref={scroller} className="min-h-0 flex-1 overflow-y-auto">
        {messages.length === 0 ? (
          <div className="mx-auto flex h-full max-w-2xl flex-col justify-center px-6">
            <p className="text-2xs font-semibold uppercase tracking-wider text-lava">New conversation</p>
            <h1 className="mt-1 text-2xl font-semibold tracking-tight">
              {current ? `What do you need, ${current.name.split(" ")[0]}?` : "What do you need?"}
            </h1>
            <p className="mt-1.5 text-sm text-navy-600">
              MaintBot queries the plant CMMS and remembers what you've taught it in earlier conversations. Each
              question below depends on something it learned before.
            </p>
            <div className="mt-5 grid gap-2 sm:grid-cols-2">
              {prompts.map((prompt) => (
                <button
                  key={prompt}
                  type="button"
                  onClick={() => submit(prompt)}
                  className="rounded-lg border border-oat-300 bg-oat-50 p-3 text-left text-[13px] leading-snug text-navy-800 transition-colors hover:border-lava/40 hover:bg-white"
                >
                  {prompt}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="mx-auto max-w-3xl space-y-6 px-6 py-6">
            {messages.map((message) => (
              <MessageBubble key={message.id} message={message} />
            ))}
          </div>
        )}
      </div>
      <form onSubmit={onSubmit} className="border-t border-oat-300 bg-white p-4">
        <div className="mx-auto flex max-w-3xl items-end gap-2 rounded-xl border border-oat-400 bg-white p-2 focus-within:border-navy-400">
          <label htmlFor="composer" className="sr-only">
            Ask MaintBot
          </label>
          <textarea
            id="composer"
            rows={1}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Ask about an asset, an alarm, a part, or a report…"
            className="max-h-40 min-h-[36px] flex-1 resize-none bg-transparent px-2 py-1.5 text-[14px] outline-none placeholder:text-navy-400"
          />
          <button
            type="submit"
            disabled={!draft.trim() || sending}
            aria-label="Send"
            className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-lava text-white transition-colors hover:bg-lava-600 disabled:bg-oat-300 disabled:text-navy-400"
          >
            <ArrowUp className="h-4 w-4" aria-hidden />
          </button>
        </div>
      </form>
    </main>
  );
}
