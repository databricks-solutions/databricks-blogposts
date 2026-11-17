import { WrenchIcon as Wrench } from "@heroicons/react/24/outline";
import type { Me } from "@/types";

interface TopBarProps {
  me: Me | null;
}

export function TopBar({ me }: TopBarProps) {
  return (
    <header className="flex h-12 shrink-0 items-center justify-between border-b border-oat-300 bg-white px-4">
      <div className="flex items-center gap-3">
        <div className="flex h-7 w-7 items-center justify-center rounded-md bg-lava text-white">
          <Wrench className="h-4 w-4" aria-hidden />
        </div>
        <div className="flex items-baseline gap-2">
          <span className="text-[15px] font-semibold tracking-tight">MaintBot</span>
          <span className="text-xs text-navy-400">Cascade Fabrication · Maintenance copilot</span>
        </div>
      </div>
      <div className="flex items-center gap-4 text-xs text-navy-600">
        {me?.model && (
          <span className="rounded border border-oat-300 bg-oat-50 px-2 py-0.5 font-mono text-2xs text-navy-600">
            {me.model.replace("system.ai.", "")}
          </span>
        )}
        <span className="flex items-center gap-1.5">
          <span className="h-1.5 w-1.5 animate-pulse-dot rounded-full bg-moss" aria-hidden />
          Agent Memory
          <span className="font-mono text-2xs text-navy-400">{me?.memory_store ?? "…"}</span>
        </span>
        <span className="border-l border-oat-300 pl-4 text-navy-600">{me?.user ?? ""}</span>
      </div>
    </header>
  );
}
