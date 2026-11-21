import { useState, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowUp, Loader2 } from "lucide-react";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";
import { JanusMark } from "@/components/Layout";
import { Button } from "@/components/ui";

interface Message {
  role: "user" | "assistant";
  content: string;
}

const STARTERS = [
  "Who holds the strongest position right now, and why?",
  "What would most likely change the outcome?",
  "Summarise the latest simulation in five sentences.",
];

/** Read an SSE body: events end at a blank line; a multi-line chunk arrives as several data: lines. */
async function* readSse(body: ReadableStream<Uint8Array>) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
    let end: number;
    while ((end = buffer.indexOf("\n\n")) !== -1) {
      const block = buffer.slice(0, end);
      buffer = buffer.slice(end + 2);
      const data = block
        .split("\n")
        .filter((l) => l.startsWith("data:"))
        .map((l) => l.slice(l.startsWith("data: ") ? 6 : 5))
        .join("\n");
      if (data === "[DONE]") return;
      if (data) yield data;
    }
  }
}

export default function ChatPanel({ matterId }: { matterId: string }) {
  const [messages, setMessages] = useState<Message[] | null>(null);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const endRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const history = useQuery({
    queryKey: ["chat", matterId],
    queryFn: () => api.getChatHistory(matterId),
  });

  // Earlier conversation from the server, until this session adds to it
  const shown: Message[] =
    messages ?? (history.data ?? []).map((m) => ({ role: m.role, content: m.content }));

  const lastContent = shown[shown.length - 1]?.content;
  useEffect(() => {
    endRef.current?.scrollIntoView({ block: "end" });
  }, [shown.length, lastContent]);

  const send = async (text: string) => {
    const userMsg = text.trim();
    if (!userMsg || loading) return;
    setInput("");
    setError(null);
    const base = [...shown, { role: "user" as const, content: userMsg }];
    setMessages(base);
    setLoading(true);

    try {
      const res = await fetch(`/api/chat/${matterId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: userMsg }),
      });
      if (!res.ok || !res.body) throw new Error(`${res.status}`);

      let fullText = "";
      for await (const chunk of readSse(res.body)) {
        fullText += chunk;
        setMessages([...base, { role: "assistant", content: fullText }]);
      }
    } catch {
      setMessages(base);
      setError("The assistant didn’t answer. Check that the model serving endpoint is up, then send again.");
      setInput(userMsg);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  return (
    <div className="flex min-h-[calc(100dvh-210px)] flex-col">
      <div className="flex-1 px-4 py-6 md:px-8" aria-live="polite">
        <div className="mx-auto max-w-3xl space-y-6">
          {history.isLoading && <p className="text-sm text-muted-foreground">Loading conversation…</p>}

          {!history.isLoading && shown.length === 0 && (
            <div>
              <h2 className="text-base font-semibold text-foreground">Ask about this matter</h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Answers draw on the matter’s description, its parties, the uploaded documents and the
                latest simulation.
              </p>
              <ul className="mt-4 space-y-2">
                {STARTERS.map((s) => (
                  <li key={s}>
                    <button
                      type="button"
                      onClick={() => send(s)}
                      className="w-full rounded-md border border-border bg-card px-4 py-2.5 text-left text-sm text-foreground transition-colors duration-100 hover:border-input"
                    >
                      {s}
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {shown.map((msg, i) =>
            msg.role === "user" ? (
              <div key={i} className="flex justify-end">
                <p className="max-w-[80%] rounded-lg bg-active px-4 py-2.5 text-sm whitespace-pre-wrap text-foreground">
                  {msg.content}
                </p>
              </div>
            ) : (
              <div key={i} className="flex gap-3">
                <JanusMark size={20} />
                <div className="min-w-0 flex-1">
                  <p className="text-xs font-medium text-muted-foreground">Janus</p>
                  <p className="mt-1 text-sm leading-6 whitespace-pre-wrap text-foreground">{msg.content}</p>
                </div>
              </div>
            ),
          )}

          {loading && shown[shown.length - 1]?.role === "user" && (
            <div className="flex items-center gap-3 text-sm text-muted-foreground">
              <JanusMark size={20} />
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
              Thinking…
            </div>
          )}
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <div ref={endRef} />
        </div>
      </div>

      <form
        className="sticky bottom-0 border-t border-border bg-background px-4 py-4 md:px-8"
        onSubmit={(e) => {
          e.preventDefault();
          send(input);
        }}
      >
        <div className="mx-auto flex max-w-3xl items-end gap-2 rounded-lg border border-input bg-white p-2 focus-within:outline-2 focus-within:outline-ring">
          <label htmlFor="chat-input" className="sr-only">
            Message
          </label>
          <textarea
            id="chat-input"
            ref={inputRef}
            rows={1}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
                e.preventDefault();
                send(input);
              }
            }}
            placeholder="Ask about this matter…"
            className={cn(
              "max-h-40 min-h-9 flex-1 resize-none bg-transparent px-2 py-2 text-sm outline-none focus-visible:outline-none",
              "field-sizing-content",
            )}
          />
          <Button type="submit" variant="primary" size="sm" disabled={!input.trim() || loading} aria-label="Send message">
            <ArrowUp className="h-4 w-4" aria-hidden="true" />
            Send
          </Button>
        </div>
        <p className="mx-auto mt-1.5 max-w-3xl text-xs text-muted-foreground">Enter to send · Shift+Enter for a new line</p>
      </form>
    </div>
  );
}
