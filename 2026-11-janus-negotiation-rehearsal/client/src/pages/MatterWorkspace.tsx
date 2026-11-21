import { useState } from "react";
import { useParams, Link, useNavigate, useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ChevronRight, Pencil, Trash2 } from "lucide-react";
import { api } from "@/lib/api";
import { formatDate } from "@/lib/format";
import { cn } from "@/lib/utils";
import ConfirmDialog from "@/components/ConfirmDialog";
import MatterDialog, { type MatterFields } from "@/components/MatterDialog";
import { Button, EmptyState, StatusPill } from "@/components/ui";
import PartyEditor from "./PartyEditor";
import DocumentManager from "./DocumentManager";
import ExtractionReview from "./ExtractionReview";
import SimulationStudio from "./SimulationStudio";
import ChatPanel from "./ChatPanel";

const TABS = [
  { id: "parties", label: "Parties" },
  { id: "documents", label: "Documents" },
  { id: "extraction", label: "Extraction" },
  { id: "simulation", label: "Simulation" },
  { id: "chat", label: "Chat" },
] as const;

type TabId = (typeof TABS)[number]["id"];

export default function MatterWorkspace() {
  const { id: matterId } = useParams<{ id: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get("tab");
  const activeTab: TabId = TABS.some((t) => t.id === tabParam) ? (tabParam as TabId) : "parties";
  const setActiveTab = (id: TabId) => setSearchParams(id === "parties" ? {} : { tab: id }, { replace: true });

  const [editing, setEditing] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const {
    data: matter,
    isLoading,
    isError,
  } = useQuery({
    queryKey: ["matter", matterId],
    queryFn: () => api.getMatter(matterId!),
    enabled: !!matterId,
  });

  const updateMut = useMutation({
    mutationFn: (fields: MatterFields) => api.updateMatter(matterId!, { ...fields }),
    onSuccess: (updated) => {
      queryClient.setQueryData(["matter", matterId], updated);
      queryClient.invalidateQueries({ queryKey: ["matters"] });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    mutationFn: () => api.deleteMatter(matterId!),
    onSuccess: () => {
      queryClient.removeQueries({ queryKey: ["matter", matterId] });
      queryClient.invalidateQueries({ queryKey: ["matters"] });
      navigate("/");
    },
  });

  if (isLoading)
    return (
      <div className="px-4 py-6 md:px-8 md:py-8" aria-busy="true">
        <div className="h-4 w-24 rounded-sm bg-raised" />
        <div className="mt-4 h-7 w-96 max-w-full rounded-sm bg-raised" />
        <div className="mt-3 h-4 w-64 rounded-sm bg-raised" />
      </div>
    );
  if (isError || !matter)
    return (
      <div className="px-4 py-6 md:px-8 md:py-8">
        <EmptyState
          action={
            <Link to="/" className="text-sm font-medium text-foreground underline underline-offset-4">
              Back to Matters
            </Link>
          }
        >
          This matter could not be found. It may have been deleted.
        </EmptyState>
      </div>
    );

  return (
    <div className="flex min-h-full flex-col">
      <header className="px-4 pt-5 md:px-8 md:pt-6">
        <nav aria-label="Breadcrumb" className="flex items-center gap-1 text-sm text-muted-foreground">
          <Link to="/" className="rounded-sm hover:text-foreground">
            Matters
          </Link>
          <ChevronRight className="h-3.5 w-3.5" aria-hidden="true" />
          <span className="truncate" aria-current="page">
            {matter.title}
          </span>
        </nav>

        <div className="mt-3 flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div className="min-w-0">
            <h1 className="text-2xl font-semibold text-foreground">{matter.title}</h1>
            <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted-foreground">
              <StatusPill status={matter.status} />
              <span className="capitalize">{matter.matter_type}</span>
              <span>
                Created <span className="num">{formatDate(matter.created_at)}</span>
              </span>
            </div>
            {matter.domain_context && (
              <p className="mt-3 line-clamp-2 max-w-[72ch] text-sm text-muted-foreground" title={matter.domain_context}>
                {matter.domain_context}
              </p>
            )}
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <Button size="sm" onClick={() => setEditing(true)}>
              <Pencil className="h-3.5 w-3.5" aria-hidden="true" />
              Edit Matter
            </Button>
            <Button size="sm" variant="ghost" className="hover:text-destructive" onClick={() => setConfirmDelete(true)}>
              <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
              Delete
            </Button>
          </div>
        </div>

        <div
          role="tablist"
          aria-label="Matter sections"
          className="-mx-4 mt-5 flex gap-1 overflow-x-auto border-b border-border px-4 md:-mx-8 md:px-8"
        >
          {TABS.map((tab) => {
            const selected = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                type="button"
                role="tab"
                id={`tab-${tab.id}`}
                aria-selected={selected}
                aria-controls="matter-tabpanel"
                onClick={() => setActiveTab(tab.id)}
                className={cn(
                  "-mb-px h-10 shrink-0 border-b-2 px-3 text-sm transition-colors duration-100",
                  selected
                    ? "border-accent font-medium text-foreground"
                    : "border-transparent text-muted-foreground hover:text-foreground",
                )}
              >
                {tab.label}
              </button>
            );
          })}
        </div>
      </header>

      {editing && (
        <MatterDialog
          heading="Edit Matter"
          initial={{
            title: matter.title,
            matter_type: matter.matter_type,
            status: matter.status,
            domain_context: matter.domain_context ?? "",
          }}
          submitLabel="Save Changes"
          pendingLabel="Saving…"
          pending={updateMut.isPending}
          error={updateMut.error?.message}
          onSubmit={(fields) => updateMut.mutate(fields)}
          onCancel={() => {
            setEditing(false);
            updateMut.reset();
          }}
        />
      )}

      {confirmDelete && (
        <ConfirmDialog
          title="Delete matter?"
          message={
            <>
              <p>
                <span className="font-medium text-foreground">{matter.title}</span> will be
                permanently deleted, with its parties, documents, extracted issues, evidence and
                timeline, chat history, and every simulation run.
              </p>
              <p className="mt-2">This cannot be undone. Audit log entries are kept.</p>
            </>
          }
          confirmLabel="Delete Matter"
          pending={deleteMut.isPending}
          error={deleteMut.error?.message}
          onConfirm={() => deleteMut.mutate()}
          onCancel={() => {
            setConfirmDelete(false);
            deleteMut.reset();
          }}
        />
      )}

      <div id="matter-tabpanel" role="tabpanel" aria-labelledby={`tab-${activeTab}`} className="flex-1">
        {activeTab === "parties" && <PartyEditor matterId={matterId!} />}
        {activeTab === "documents" && <DocumentManager matterId={matterId!} />}
        {activeTab === "extraction" && <ExtractionReview matterId={matterId!} />}
        {activeTab === "simulation" && (
          <SimulationStudio matterId={matterId!} onGoToParties={() => setActiveTab("parties")} />
        )}
        {activeTab === "chat" && <ChatPanel matterId={matterId!} />}
      </div>
    </div>
  );
}
