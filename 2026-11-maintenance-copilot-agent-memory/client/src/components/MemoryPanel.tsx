import anime from "animejs";
import { CpuChipIcon as Brain, ChevronDownIcon as ChevronDown, MoonIcon as MoonStar, TrashIcon as Trash2, UserGroupIcon as Users } from "@heroicons/react/24/outline";
import { useEffect, useMemo, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { cn } from "@/lib/cn";
import { relativeTime } from "@/lib/time";
import { memoryKey, useChat } from "@/stores/chat";
import type { MemoryEntry, MemoryScope, PipelineStatus } from "@/types";

const distilled = (source: string | null) => !!source && /pipeline|dream|distill/i.test(source);

interface MemoryCardProps {
  memory: MemoryEntry;
  highlight?: "recalled" | "written";
  onForget: (path: string) => void;
}

function MemoryCard({ memory, highlight, onForget }: MemoryCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const fromPipeline = distilled(memory.source);

  return (
    <li
      className={cn(
        "memory-card rounded-lg border bg-white p-2.5 shadow-card transition-shadow",
        highlight === "recalled" && "border-lava/50 ring-2 ring-lava/15",
        highlight === "written" && "border-moss/50 ring-2 ring-moss/15",
        !highlight && "border-oat-300",
      )}
    >
      <div className="flex items-start gap-2">
        <button type="button" onClick={() => setExpanded(!expanded)} aria-expanded={expanded} className="min-w-0 flex-1 text-left">
          <span className="flex items-center gap-1.5">
            <span className="truncate font-mono text-2xs text-navy-800">{memory.path}</span>
            <ChevronDown className={cn("h-3 w-3 shrink-0 text-navy-400 transition-transform", expanded && "rotate-180")} aria-hidden />
          </span>
          <div
            className={cn(
              "prose prose-sm mt-1 max-w-none text-xs leading-relaxed text-navy-600 prose-p:my-0.5 prose-ul:my-0.5 prose-li:my-0 prose-headings:hidden prose-strong:text-navy-800 prose-code:font-mono prose-code:text-[11px] prose-code:before:content-none prose-code:after:content-none",
              !expanded && "line-clamp-3",
            )}
          >
            <ReactMarkdown remarkPlugins={[remarkGfm]} skipHtml>
              {memory.content}
            </ReactMarkdown>
          </div>
        </button>
        <button
          type="button"
          onClick={() => (confirming ? onForget(memory.path) : setConfirming(true))}
          onBlur={() => setConfirming(false)}
          aria-label={confirming ? `Confirm forgetting ${memory.path}` : `Forget ${memory.path}`}
          className={cn("shrink-0 rounded p-1 text-navy-400 hover:bg-oat-100 hover:text-lava", confirming && "bg-lava-50 text-lava")}
        >
          <Trash2 className="h-3.5 w-3.5" aria-hidden />
        </button>
      </div>
      <div className="mt-2 flex items-center gap-2 whitespace-nowrap text-2xs">
        <span className={cn("rounded px-1.5 py-px font-medium", fromPipeline ? "bg-sky-50 text-sky" : "bg-moss-50 text-moss")}>
          {fromPipeline ? "Distilled by pipeline" : "Saved by agent"}
        </span>
        {highlight && (
          <span className={cn("font-medium", highlight === "recalled" ? "text-lava-600" : "text-moss")}>
            {highlight === "recalled" ? "Recalled" : "Saved this turn"}
          </span>
        )}
        {confirming ? (
          <span className="ml-auto text-lava-600">Click again to forget</span>
        ) : (
          <span className="ml-auto truncate font-mono text-navy-400">{relativeTime(memory.updated)}</span>
        )}
      </div>
    </li>
  );
}

interface PipelineLineProps {
  pipeline?: PipelineStatus;
}

function PipelineLine({ pipeline }: PipelineLineProps) {
  if (!pipeline?.configured) return null;
  const running = pipeline.state === "RUNNING" || pipeline.state === "PENDING";
  return (
    <p className="mt-1.5 flex items-center gap-1.5 whitespace-nowrap text-2xs text-navy-400">
      <MoonStar className="h-3 w-3 text-sky" aria-hidden />
      <span className={cn("h-1.5 w-1.5 rounded-full", running ? "animate-pulse-dot bg-sky" : pipeline.state === "SUCCEEDED" ? "bg-moss" : "bg-navy-200")} aria-hidden />
      <span className="truncate">
        Pipeline {running ? "distilling now" : pipeline.state ? `last run ${pipeline.state.toLowerCase()}` : "has no runs yet"}
      </span>
      <span className="ml-auto shrink-0 font-mono">{relativeTime(pipeline.started)}</span>
    </p>
  );
}

interface MemorySectionProps {
  scope: MemoryScope;
  title: string;
  subtitle: string;
  empty: string;
}

function MemorySection({ scope, title, subtitle, empty }: MemorySectionProps) {
  const { memories, memoriesLoading, highlighted, forget, pipelines } = useChat();
  const entries = useMemo(() => [...memories[scope]].sort((a, b) => a.path.localeCompare(b.path)), [memories, scope]);
  const Icon = scope === "team" ? Users : Brain;

  return (
    <section className="border-b border-oat-300 last:border-b-0">
      <div className="sticky top-0 z-10 border-b border-oat-300 bg-white px-4 py-2.5">
        <div className="flex items-center gap-2">
          <Icon className={cn("h-4 w-4", scope === "team" ? "text-lava" : "text-sky")} aria-hidden />
          <h2 className="text-[13px] font-semibold">{title}</h2>
          <span className="ml-auto rounded bg-oat-100 px-1.5 font-mono text-2xs text-navy-600">{entries.length}</span>
        </div>
        <p className="mt-0.5 text-2xs leading-relaxed text-navy-400">{subtitle}</p>
        <PipelineLine pipeline={pipelines?.[scope]} />
      </div>
      <div className="space-y-2 p-3">
        {memoriesLoading &&
          [0, 1].map((i) => (
            <div key={i} className="space-y-2 rounded-lg border border-oat-300 bg-white p-2.5">
              <div className="skeleton h-2.5 w-1/3" />
              <div className="skeleton h-2.5 w-full" />
            </div>
          ))}
        {!memoriesLoading && entries.length === 0 && <p className="px-1 text-xs text-navy-400">{empty}</p>}
        <ul className="space-y-2">
          {entries.map((memory) => (
            <MemoryCard
              key={memory.path + (memory.session_id ?? "")}
              memory={memory}
              highlight={highlighted[memoryKey(scope, memory.path)]}
              onForget={(path) => void forget(scope, path)}
            />
          ))}
        </ul>
      </div>
    </section>
  );
}

export function MemoryPanel() {
  const { me, persona, memoriesLoading } = useChat();
  const list = useRef<HTMLDivElement>(null);
  const current = me?.personas.find((p) => p.id === persona);
  const plant = current ? `Plant ${current.plant} · ${current.plant_name}` : me ? `Plant ${me.plant.code} · ${me.plant.name}` : "Plant";
  const firstName = current?.name.split(" ")[0];

  useEffect(() => {
    if (!list.current || memoriesLoading) return;
    anime({
      targets: list.current.querySelectorAll(".memory-card"),
      opacity: [0, 1],
      translateY: [6, 0],
      delay: anime.stagger(35),
      duration: 320,
      easing: "easeOutQuad",
    });
  }, [persona, memoriesLoading]);

  return (
    <aside ref={list} className="w-[360px] shrink-0 overflow-y-auto border-l border-oat-300 bg-oat-100" aria-label="What MaintBot remembers">
      <MemorySection
        scope="team"
        title={`Team memory · ${plant}`}
        subtitle="Shared by everyone at this plant: equipment quirks, fixes, data definitions, parts, and contacts."
        empty="Nothing yet. What technicians explain on shift is distilled here overnight."
      />
      <MemorySection
        scope="personal"
        title={firstName ? `${firstName}'s memory` : "Your memory"}
        subtitle="Only for this person: units, formats, and the reports they ask for."
        empty="Nothing yet. Preferences this person states are kept here."
      />
    </aside>
  );
}
