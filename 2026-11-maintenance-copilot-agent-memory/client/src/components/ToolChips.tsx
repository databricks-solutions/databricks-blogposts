import { BookmarkIcon as BookmarkPlus, CpuChipIcon as Brain, CircleStackIcon as Database, BackspaceIcon as Eraser, TableCellsIcon as Table2, UserGroupIcon as Users } from "@heroicons/react/24/outline";
import { useState } from "react";
import { cn } from "@/lib/cn";
import type { ToolCall } from "@/types";

type Memory = "personal" | "team";

const TOOL_META: Record<string, { label: string; icon: typeof Database; memory?: Memory }> = {
  run_sql: { label: "Queried CMMS", icon: Database },
  list_tables: { label: "Listed tables", icon: Table2 },
  recall: { label: "Searched my memory", icon: Brain, memory: "personal" },
  remember: { label: "Saved to my memory", icon: BookmarkPlus, memory: "personal" },
  forget: { label: "Removed from my memory", icon: Eraser, memory: "personal" },
  list_memories: { label: "Listed my memory", icon: Brain, memory: "personal" },
  recall_team: { label: "Searched team memory", icon: Users, memory: "team" },
  remember_team: { label: "Saved to team memory", icon: BookmarkPlus, memory: "team" },
  forget_team: { label: "Removed from team memory", icon: Eraser, memory: "team" },
  list_memories_team: { label: "Listed team memory", icon: Users, memory: "team" },
};

interface ToolChipsProps {
  calls: ToolCall[];
}

export function ToolChips({ calls }: ToolChipsProps) {
  const [open, setOpen] = useState<string | null>(null);
  const openCall = calls.find((c) => c.id === open);
  if (!calls.length) return null;

  return (
    <div className="mb-2">
      <div className="flex flex-wrap gap-1.5">
        {calls.map((call) => {
          const meta = TOOL_META[call.name] ?? { label: call.name, icon: Database };
          const Icon = meta.icon;
          return (
            <button
              key={call.id}
              type="button"
              onClick={() => setOpen(open === call.id ? null : call.id)}
              aria-expanded={open === call.id}
              className={cn(
                "flex items-center gap-1 rounded-full px-2 py-0.5 text-2xs font-medium ring-1",
                meta.memory === "team" && "bg-lava-50 text-lava-600 ring-lava-100",
                meta.memory === "personal" && "bg-sky-50 text-sky ring-sky/20",
                !meta.memory && "bg-oat-100 text-navy-600 ring-oat-300",
                call.result === undefined && "animate-pulse",
              )}
            >
              <Icon className="h-3 w-3" aria-hidden />
              {meta.label}
            </button>
          );
        })}
      </div>
      {openCall && (
        <div className="mt-2 overflow-hidden rounded-md border border-oat-300 bg-white">
          <pre className="max-h-48 overflow-auto whitespace-pre-wrap bg-oat-50 p-2.5 font-mono text-2xs text-navy-800">
            {typeof openCall.args.query === "string" ? openCall.args.query : JSON.stringify(openCall.args, null, 2)}
          </pre>
          {openCall.result && (
            <pre className="max-h-40 overflow-auto whitespace-pre-wrap border-t border-oat-300 p-2.5 font-mono text-2xs text-navy-600">
              {openCall.result.length > 1500 ? `${openCall.result.slice(0, 1500)}…` : openCall.result}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}
