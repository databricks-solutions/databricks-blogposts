import axios from "axios";
import type { Me, MemoryEntry, MemoryScope, PipelineStatus, SessionSummary, StreamEvent, WireMessage } from "@/types";

export const api = axios.create({ baseURL: "/api" });

const params = (persona: string | null) => (persona ? { persona } : {});

export const getMe = async () => (await api.get<Me>("/me")).data;

export const getMemories = async (persona: string | null, scope: MemoryScope) =>
  (await api.get<{ memories: MemoryEntry[] }>("/memories", { params: { ...params(persona), scope } })).data.memories;

export const deleteMemory = async (persona: string | null, scope: MemoryScope, path: string) =>
  (await api.delete<{ deleted: number }>("/memories", { params: { ...params(persona), scope, path } })).data;

export const getSessions = async (persona: string | null) =>
  (await api.get<{ sessions: SessionSummary[] }>("/sessions", { params: params(persona) })).data.sessions;

export const getSessionHistory = async (persona: string | null, id: string) =>
  (await api.get<{ items: { item_id: string; data: WireMessage }[] }>(`/sessions/${encodeURIComponent(id)}`, {
    params: params(persona),
  })).data.items.map((item) => item.data);

export const getPipelines = async () => (await api.get<Record<MemoryScope, PipelineStatus>>("/pipeline")).data;

/** Memory paths listed in a `recall` / `recall_team` tool result ("/path.md:" or "/path.md [session x]:"). */
export function recalledPaths(result: string): string[] {
  return result
    .split("\n")
    .filter((line) => line.startsWith("/") && line.trimEnd().endsWith(":"))
    .map((line) => line.trimEnd().slice(0, -1).replace(/ \[session [^\]]+\]$/, ""));
}

/**
 * Send one turn to the Agent Bricks invocation endpoint and stream its events.
 * The server derives the memory actor from the signed-in user; the client only names a persona.
 */
export async function streamTurn(
  sessionId: string,
  text: string,
  persona: string | null,
  onEvent: (event: StreamEvent) => void,
): Promise<void> {
  const response = await fetch("/api/invocations", {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
    body: JSON.stringify({
      id: crypto.randomUUID(),
      session_id: sessionId,
      stream: true,
      input: { messages: [{ role: "user", content: text }], ...(persona ? { persona } : {}) },
    }),
  });
  if (!response.ok || !response.body) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail ?? `Request failed (${response.status})`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let terminal: string | null = null;
  const handle = (frame: string) => {
    const data = frame
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trimStart())
      .join("\n");
    if (!data || data === "[DONE]") return;
    const event = JSON.parse(data) as StreamEvent;
    if (event.type === "run.completed" || event.type === "run.failed") terminal = event.type;
    onEvent(event);
  };
  for (;;) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });
    const frames = buffer.split("\n\n");
    buffer = frames.pop() ?? "";
    frames.forEach(handle);
    if (done) break;
  }
  if (buffer.trim()) handle(buffer);
  if (terminal === "run.failed") throw new Error("MaintBot couldn't finish that answer. Check the app logs.");
  if (terminal !== "run.completed") throw new Error("The connection dropped before the answer finished.");
}

/**
 * The visible text of a message's content. Reasoning models return a list of blocks (and, through
 * the AI Gateway, sometimes a JSON string of blocks); only `text` blocks are shown.
 */
export function contentText(content: unknown): string {
  let value = content;
  if (typeof value === "string" && value.trimStart().startsWith('[{"type"')) value = jsonBlocks(value);
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    return value
      .map((block) => (typeof block === "string" ? block : block?.type === "text" ? (block.text ?? "") : ""))
      .join("");
  }
  return "";
}

/** Split gateway content into blocks: leading JSON arrays of blocks, then any trailing plain text. */
function jsonBlocks(raw: string): unknown[] {
  const blocks: unknown[] = [];
  const text = raw.trim();
  let index = 0;
  while (text[index] === "[") {
    const end = arrayEnd(text, index);
    if (end < 0) break;
    try {
      const value = JSON.parse(text.slice(index, end + 1));
      blocks.push(...(Array.isArray(value) ? value : [value]));
    } catch {
      break;
    }
    index = end + 1;
    while (/\s/.test(text[index] ?? "")) index++;
  }
  if (index < text.length) blocks.push({ type: "text", text: text.slice(index) });
  return blocks;
}

/** Index of the bracket closing the array that opens at `start`, or -1. Brackets inside strings are skipped. */
function arrayEnd(text: string, start: number): number {
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
    } else if (ch === '"') inString = true;
    else if (ch === "[" || ch === "{") depth++;
    else if ((ch === "]" || ch === "}") && --depth === 0) return i;
  }
  return -1;
}
