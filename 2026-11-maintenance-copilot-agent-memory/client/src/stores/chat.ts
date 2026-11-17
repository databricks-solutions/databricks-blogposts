import { create } from "zustand";
import * as api from "@/lib/api";
import type { ChatMessage, Me, MemoryEntry, MemoryScope, PipelineStatus, SessionSummary, WireMessage } from "@/types";

const SCOPES: MemoryScope[] = ["team", "personal"];

/** Which memory a built-in memory tool touches, and whether it reads or writes. */
function memoryTool(name: string | null | undefined): { scope: MemoryScope; write: boolean } | null {
  if (!name) return null;
  const scope: MemoryScope = name.endsWith("_team") ? "team" : "personal";
  const base = name.replace(/_team$/, "");
  if (base === "recall") return { scope, write: false };
  if (base === "remember" || base === "forget") return { scope, write: true };
  return null;
}

/** Highlight key for a memory entry: `<scope>:<path>`. */
export const memoryKey = (scope: MemoryScope, path: string) => `${scope}:${path}`;

interface ChatState {
  me: Me | null;
  persona: string | null;
  sessions: SessionSummary[];
  sessionId: string;
  messages: ChatMessage[];
  memories: Record<MemoryScope, MemoryEntry[]>;
  memoriesLoading: boolean;
  /** Entries recalled or written during the latest turn, keyed by memoryKey(). */
  highlighted: Record<string, "recalled" | "written">;
  pipelines: Record<MemoryScope, PipelineStatus> | null;
  sending: boolean;
  init: () => Promise<void>;
  selectPersona: (persona: string | null) => Promise<void>;
  newConversation: () => void;
  openSession: (id: string) => Promise<void>;
  send: (text: string) => Promise<void>;
  refreshMemories: () => Promise<void>;
  forget: (scope: MemoryScope, path: string) => Promise<void>;
}

const newId = () => crypto.randomUUID();
const emptyMemories = (): Record<MemoryScope, MemoryEntry[]> => ({ team: [], personal: [] });
const assistant = (): ChatMessage => ({ id: newId(), role: "assistant", text: "", toolCalls: [] });
const fingerprint = (memories: Record<MemoryScope, MemoryEntry[]>) =>
  new Set(SCOPES.flatMap((scope) => memories[scope].map((m) => `${scope}\u0000${m.path}\u0000${m.content}`)));

/** Rebuild chat bubbles from checkpointed LangGraph messages. */
function fromHistory(items: WireMessage[]): ChatMessage[] {
  const out: ChatMessage[] = [];
  let current: ChatMessage | null = null;
  for (const item of items) {
    if (item.type === "human") {
      current = null;
      out.push({ id: item.id ?? newId(), role: "user", text: api.contentText(item.content), toolCalls: [] });
    } else if (item.type === "ai") {
      if (!current) {
        current = assistant();
        out.push(current);
      }
      current.toolCalls.push(...(item.tool_calls ?? []).map((call) => ({ ...call })));
      const text = api.contentText(item.content);
      if (text && !item.tool_calls?.length) current.text = text;
    } else if (item.type === "tool" && current) {
      const call = current.toolCalls.find((c) => c.id === item.tool_call_id);
      if (call) call.result = api.contentText(item.content);
    }
  }
  return out;
}

export const useChat = create<ChatState>((set, get) => {
  const patchLast = (patch: (message: ChatMessage) => ChatMessage) =>
    set((state) => ({ messages: [...state.messages.slice(0, -1), patch(state.messages[state.messages.length - 1])] }));

  const refreshSessions = async () => set({ sessions: await api.getSessions(get().persona) });

  return {
    me: null,
    persona: null,
    sessions: [],
    sessionId: newId(),
    messages: [],
    memories: emptyMemories(),
    memoriesLoading: true,
    highlighted: {},
    pipelines: null,
    sending: false,

    init: async () => {
      const me = await api.getMe();
      set({ me, persona: me.personas[0]?.id ?? null });
      await Promise.all([
        get().refreshMemories(),
        refreshSessions(),
        api.getPipelines().then((pipelines) => set({ pipelines })),
      ]);
    },

    selectPersona: async (persona) => {
      if (persona === get().persona) return;
      set({ persona, messages: [], sessionId: newId(), highlighted: {}, memories: emptyMemories(), memoriesLoading: true, sessions: [] });
      await Promise.all([get().refreshMemories(), refreshSessions()]);
    },

    newConversation: () => set({ messages: [], sessionId: newId(), highlighted: {} }),

    openSession: async (id) => {
      set({ sessionId: id, messages: [], highlighted: {} });
      const messages = fromHistory(await api.getSessionHistory(get().persona, id));
      // Show what the latest answer recalled, from its saved recall tool results.
      const last = [...messages].reverse().find((m) => m.role === "assistant");
      const highlighted: Record<string, "recalled"> = {};
      for (const call of last?.toolCalls ?? []) {
        const tool = memoryTool(call.name);
        if (tool && !tool.write && call.result) {
          for (const path of api.recalledPaths(call.result)) highlighted[memoryKey(tool.scope, path)] = "recalled";
        }
      }
      set({ messages, highlighted });
    },

    send: async (text) => {
      const { sessionId, persona } = get();
      const user: ChatMessage = { id: newId(), role: "user", text, toolCalls: [] };
      const before = fingerprint(get().memories);
      set((state) => ({ sending: true, highlighted: {}, messages: [...state.messages, user, { ...assistant(), pending: true }] }));
      let draft = "";
      try {
        await api.streamTurn(sessionId, text, persona, (event) => {
          if (event.type === "delta") {
            draft += api.contentText(event.content);
            patchLast((m) => ({ ...m, text: draft }));
          } else if (event.type === "message" && event.message.type === "ai") {
            const calls = event.message.tool_calls ?? [];
            if (calls.length) {
              draft = "";
              patchLast((m) => ({ ...m, text: "", toolCalls: [...m.toolCalls, ...calls.map((c) => ({ ...c }))] }));
            } else {
              patchLast((m) => ({ ...m, text: api.contentText(event.message.content) || draft }));
            }
          } else if (event.type === "message" && event.message.type === "tool") {
            const { tool_call_id: callId, content, name } = event.message;
            const result = api.contentText(content);
            patchLast((m) => ({ ...m, toolCalls: m.toolCalls.map((c) => (c.id === callId ? { ...c, result } : c)) }));
            const tool = memoryTool(name);
            if (tool && !tool.write) {
              const recalled = Object.fromEntries(api.recalledPaths(result).map((path) => [memoryKey(tool.scope, path), "recalled" as const]));
              set((state) => ({ highlighted: { ...state.highlighted, ...recalled } }));
            } else if (tool?.write) {
              void get().refreshMemories();
            }
          }
        });
        patchLast((m) => ({ ...m, pending: false }));
      } catch (error) {
        patchLast((m) => ({ ...m, pending: false, error: (error as Error).message }));
      } finally {
        set({ sending: false });
        await get().refreshMemories();
        const written = SCOPES.flatMap((scope) =>
          get().memories[scope].filter((m) => !before.has(`${scope}\u0000${m.path}\u0000${m.content}`)).map((m) => memoryKey(scope, m.path)),
        );
        if (written.length) {
          set((state) => ({ highlighted: { ...state.highlighted, ...Object.fromEntries(written.map((key) => [key, "written" as const])) } }));
        }
        void refreshSessions();
      }
    },

    refreshMemories: async () => {
      const persona = get().persona;
      const [team, personal] = await Promise.all(SCOPES.map((scope) => api.getMemories(persona, scope)));
      if (get().persona === persona) set({ memories: { team, personal }, memoriesLoading: false });
    },

    forget: async (scope, path) => {
      await api.deleteMemory(get().persona, scope, path);
      await get().refreshMemories();
    },
  };
});
