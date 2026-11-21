import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Plus, Trash2 } from "lucide-react";
import { api, type MatterSummary } from "@/lib/api";
import { formatDate, plural } from "@/lib/format";
import ConfirmDialog from "@/components/ConfirmDialog";
import MatterDialog, { type MatterFields } from "@/components/MatterDialog";
import { Button, Card, EmptyState, IconButton, StatusPill } from "@/components/ui";

// Matter | Status | Parties | Documents | Created | action
const ROW_GRID = "md:grid md:grid-cols-[minmax(0,1fr)_112px_80px_96px_120px_40px] md:items-center md:gap-4";

export default function Dashboard() {
  const queryClient = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [toDelete, setToDelete] = useState<MatterSummary | null>(null);

  const {
    data: matters = [],
    isLoading,
    isError,
    refetch,
  } = useQuery({
    queryKey: ["matters"],
    queryFn: api.listMatters,
  });

  const createMutation = useMutation({
    mutationFn: (fields: MatterFields) => api.createMatter(fields),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["matters"] });
      setShowCreate(false);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (matterId: string) => api.deleteMatter(matterId),
    onSuccess: (_, matterId) => {
      queryClient.setQueryData<MatterSummary[]>(["matters"], (old = []) =>
        old.filter((m) => m.matter_id !== matterId),
      );
      queryClient.invalidateQueries({ queryKey: ["matters"] });
      setToDelete(null);
    },
  });

  const totalParties = matters.reduce((s, m) => s + m.party_count, 0);
  const totalDocs = matters.reduce((s, m) => s + m.document_count, 0);

  return (
    <div className="px-4 py-6 md:px-8 md:py-8">
      <div className="mb-6 flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Matters</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {isLoading
              ? "Loading matters…"
              : `${plural(matters.length, "matter")} · ${plural(totalParties, "party", "parties")} · ${plural(totalDocs, "document")}`}
          </p>
        </div>
        <Button variant="primary" onClick={() => setShowCreate(true)}>
          <Plus className="h-4 w-4" aria-hidden="true" />
          New Matter
        </Button>
      </div>

      {showCreate && (
        <MatterDialog
          heading="New Matter"
          initial={{ title: "", matter_type: "negotiation", domain_context: "" }}
          submitLabel="Create Matter"
          pendingLabel="Creating…"
          pending={createMutation.isPending}
          error={createMutation.error?.message}
          onSubmit={(fields) => createMutation.mutate(fields)}
          onCancel={() => {
            setShowCreate(false);
            createMutation.reset();
          }}
        />
      )}

      {toDelete && (
        <ConfirmDialog
          title="Delete matter?"
          message={
            <>
              <p>
                <span className="font-medium text-foreground">{toDelete.title}</span> will be
                permanently deleted, with its {plural(toDelete.party_count, "party", "parties")},{" "}
                {plural(toDelete.document_count, "document")}, extracted issues, evidence and
                timeline, chat history, and every simulation run.
              </p>
              <p className="mt-2">This cannot be undone. Audit log entries are kept.</p>
            </>
          }
          confirmLabel="Delete Matter"
          pending={deleteMutation.isPending}
          error={deleteMutation.error?.message}
          onConfirm={() => deleteMutation.mutate(toDelete.matter_id)}
          onCancel={() => {
            setToDelete(null);
            deleteMutation.reset();
          }}
        />
      )}

      {isError ? (
        <EmptyState
          action={
            <Button size="sm" onClick={() => refetch()}>
              Retry
            </Button>
          }
        >
          Couldn’t load matters. Check that the SQL warehouse is running, then retry.
        </EmptyState>
      ) : !isLoading && matters.length === 0 ? (
        <EmptyState
          action={
            <Button variant="primary" size="sm" onClick={() => setShowCreate(true)}>
              <Plus className="h-4 w-4" aria-hidden="true" />
              New Matter
            </Button>
          }
        >
          No matters yet. Create one, add the parties and their BATNAs, then run a simulation.
        </EmptyState>
      ) : (
        <Card>
          <div
            className={`hidden h-10 border-b border-border px-5 text-xs text-muted-foreground ${ROW_GRID}`}
            aria-hidden="true"
          >
            <span>Matter</span>
            <span>Status</span>
            <span className="text-right">Parties</span>
            <span className="text-right">Documents</span>
            <span className="text-right">Created</span>
            <span />
          </div>
          <ul>
            {isLoading
              ? Array.from({ length: 4 }, (_, i) => (
                  <li key={i} className="flex h-[60px] items-center border-b border-rule px-5 last:border-b-0">
                    <span className="h-4 w-64 rounded-sm bg-raised" />
                  </li>
                ))
              : matters.map((m) => (
                  <li
                    key={m.matter_id}
                    className={`group relative border-b border-rule px-5 py-3 last:border-b-0 hover:bg-raised/60 ${ROW_GRID}`}
                  >
                    <div className="min-w-0 pr-10 md:pr-0">
                      <Link
                        to={`/matter/${m.matter_id}`}
                        className="block truncate font-medium text-foreground after:absolute after:inset-0 after:content-['']"
                      >
                        {m.title}
                      </Link>
                      <p className="mt-0.5 text-sm text-muted-foreground capitalize">
                        {m.matter_type}
                        <span className="md:hidden">
                          {" "}
                          · {m.status} · {plural(m.party_count, "party", "parties")} ·{" "}
                          {plural(m.document_count, "doc")}
                        </span>
                      </p>
                    </div>
                    <div className="hidden md:block">
                      <StatusPill status={m.status} />
                    </div>
                    <span className="num hidden text-right md:block">{m.party_count}</span>
                    <span className="num hidden text-right md:block">{m.document_count}</span>
                    <span className="num hidden text-right text-muted-foreground md:block">
                      {formatDate(m.created_at)}
                    </span>
                    <div className="absolute top-3 right-3 md:static md:flex md:justify-end">
                      <IconButton
                        label={`Delete ${m.title}`}
                        tone="danger"
                        className="relative z-10"
                        onClick={() => setToDelete(m)}
                      >
                        <Trash2 className="h-4 w-4" aria-hidden="true" />
                      </IconButton>
                    </div>
                  </li>
                ))}
          </ul>
        </Card>
      )}
    </div>
  );
}
