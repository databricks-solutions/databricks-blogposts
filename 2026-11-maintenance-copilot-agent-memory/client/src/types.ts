export interface Persona {
  id: string;
  name: string;
  role: string;
  plant: string;
  plant_name: string;
}

/** Personal memory belongs to one person; team memory is shared by everyone at their plant. */
export type MemoryScope = "personal" | "team";

export interface Me {
  user: string;
  plant: { code: string; name: string };
  personas: Persona[];
  model: string;
  memory_store: string | null;
  session_store: string | null;
  transcript_stores: Record<MemoryScope, string | null>;
}

export interface MemoryEntry {
  path: string;
  content: string;
  description: string | null;
  session_id: string | null;
  source: string | null;
  updated: string | null;
}

export interface SessionSummary {
  id: string;
  updated: string | null;
  title: string | null;
}

export interface PipelineStatus {
  configured: boolean;
  state?: string;
  started?: string;
  error?: string;
}

export interface ToolCall {
  id: string;
  name: string;
  args: Record<string, unknown>;
  result?: string;
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  text: string;
  toolCalls: ToolCall[];
  pending?: boolean;
  error?: string;
}

/** LangGraph message as serialized by `message.model_dump()` on the server. */
export interface WireMessage {
  id?: string | null;
  type: "human" | "ai" | "tool" | "system";
  content: unknown;
  name?: string | null;
  tool_calls?: { id: string; name: string; args: Record<string, unknown> }[];
  tool_call_id?: string;
}

export type StreamEvent =
  | { type: "run.started" | "run.completed" | "run.failed" }
  | { type: "delta"; content: unknown; id?: string }
  | { type: "message"; message: WireMessage }
  | { type: "interrupt"; id: string; value: unknown };
