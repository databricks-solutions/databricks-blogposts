import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Trash2 } from "lucide-react";
import { api, type Issue, type EvidenceItem, type TimelineEvent } from "@/lib/api";
import { formatDate } from "@/lib/format";
import ConfirmDialog from "@/components/ConfirmDialog";
import { Card, CardHeader, EmptyState, IconButton, Pill, type PillTone } from "@/components/ui";

// Strength is a rating, not a status: one neutral family, darker for stronger evidence
const STRENGTH_TONES: Record<string, PillTone> = {
  strong: "neutral",
  moderate: "plain",
  weak: "plain",
  circumstantial: "plain",
};

type Pending = { kind: "issue" | "evidence" | "event"; id: string; title: string } | null;

function Row({ children, onDelete, label }: { children: React.ReactNode; onDelete: () => void; label: string }) {
  return (
    <li className="group flex items-start gap-2 border-b border-rule px-5 py-3 last:border-b-0">
      <div className="min-w-0 flex-1">{children}</div>
      <IconButton
        label={`Delete ${label}`}
        tone="danger"
        className="-mt-1 -mr-2 opacity-60 group-hover:opacity-100 focus-visible:opacity-100"
        onClick={onDelete}
      >
        <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
      </IconButton>
    </li>
  );
}

export default function ExtractionReview({ matterId }: { matterId: string }) {
  const queryClient = useQueryClient();
  const [pending, setPending] = useState<Pending>(null);

  const { data: issues = [], isLoading: l1 } = useQuery({
    queryKey: ["issues", matterId],
    queryFn: () => api.listIssues(matterId),
  });
  const { data: evidence = [], isLoading: l2 } = useQuery({
    queryKey: ["evidence", matterId],
    queryFn: () => api.listEvidence(matterId),
  });
  const { data: timeline = [], isLoading: l3 } = useQuery({
    queryKey: ["timeline", matterId],
    queryFn: () => api.listTimeline(matterId),
  });

  const deleteMut = useMutation({
    mutationFn: (p: NonNullable<Pending>) =>
      p.kind === "issue"
        ? api.deleteIssue(matterId, p.id)
        : p.kind === "evidence"
          ? api.deleteEvidence(matterId, p.id)
          : api.deleteTimelineEvent(matterId, p.id),
    onSuccess: (_, p) => {
      const key = p.kind === "issue" ? "issues" : p.kind === "evidence" ? "evidence" : "timeline";
      queryClient.invalidateQueries({ queryKey: [key, matterId] });
      setPending(null);
    },
  });

  const loading = l1 || l2 || l3;
  const isEmpty = !loading && issues.length === 0 && evidence.length === 0 && timeline.length === 0;

  return (
    <div className="px-4 py-6 md:px-8">
      {pending && (
        <ConfirmDialog
          title={`Delete ${pending.kind === "event" ? "timeline event" : pending.kind}?`}
          message={
            <p>
              <span className="font-medium text-foreground">{pending.title}</span> will be removed
              from this matter.
              {pending.kind !== "event" && " Party agents in later simulations will no longer see it."}
            </p>
          }
          pending={deleteMut.isPending}
          error={deleteMut.error?.message}
          onConfirm={() => deleteMut.mutate(pending)}
          onCancel={() => {
            setPending(null);
            deleteMut.reset();
          }}
        />
      )}

      {isEmpty ? (
        <EmptyState>
          Nothing extracted yet. Upload a document and process it; its issues, evidence and timeline
          appear here.
        </EmptyState>
      ) : (
        <div className="grid gap-4 lg:grid-cols-3">
          <Card aria-labelledby="issues-h" className="flex flex-col">
            <CardHeader id="issues-h" title="Issues" meta={`${issues.length} in dispute, most important first`} />
            <ul className="flex-1 border-t border-border">
              {issues.map((issue: Issue) => (
                <Row
                  key={issue.issue_id}
                  label={issue.title}
                  onDelete={() => setPending({ kind: "issue", id: issue.issue_id, title: issue.title })}
                >
                  <div className="flex items-start justify-between gap-3">
                    <p className="font-medium text-foreground">{issue.title}</p>
                    <span className="flex flex-none items-center gap-2 pt-0.5">
                      <span className="h-1 w-10 rounded-full bg-active" aria-hidden="true">
                        <span
                          className="block h-full rounded-full bg-[#618794]"
                          style={{ width: `${issue.importance * 100}%` }}
                        />
                      </span>
                      <span className="num w-6 text-right text-sm">{Math.round(issue.importance * 100)}</span>
                    </span>
                  </div>
                  {issue.description && <p className="mt-1 text-sm text-muted-foreground">{issue.description}</p>}
                  {issue.category && (
                    <p className="mt-1.5 text-xs text-muted-foreground capitalize">{issue.category}</p>
                  )}
                </Row>
              ))}
            </ul>
            <p className="border-t border-border px-5 py-2.5 text-xs text-muted-foreground">
              Importance is scored 0–100 by the extraction model.
            </p>
          </Card>

          <Card aria-labelledby="evidence-h" className="flex flex-col">
            <CardHeader id="evidence-h" title="Evidence" meta={`${evidence.length} items`} />
            <ul className="flex-1 border-t border-border">
              {evidence.map((ev: EvidenceItem) => (
                <Row
                  key={ev.evidence_id}
                  label={ev.title}
                  onDelete={() => setPending({ kind: "evidence", id: ev.evidence_id, title: ev.title })}
                >
                  <div className="flex items-start justify-between gap-3">
                    <p className="font-medium text-foreground">{ev.title}</p>
                    <Pill tone={STRENGTH_TONES[ev.strength] ?? "plain"} className="capitalize">
                      {ev.strength}
                    </Pill>
                  </div>
                  {ev.description && <p className="mt-1 text-sm text-muted-foreground">{ev.description}</p>}
                </Row>
              ))}
            </ul>
            <p className="border-t border-border px-5 py-2.5 text-xs text-muted-foreground">
              Strength is rated by the extraction model from the source document.
            </p>
          </Card>

          <Card aria-labelledby="timeline-h" className="flex flex-col">
            <CardHeader id="timeline-h" title="Timeline" meta={`${timeline.length} events, oldest first`} />
            <ol className="flex-1 border-t border-border">
              {timeline.map((ev: TimelineEvent) => (
                <Row
                  key={ev.event_id}
                  label={ev.title}
                  onDelete={() => setPending({ kind: "event", id: ev.event_id, title: ev.title })}
                >
                  <div className="grid grid-cols-[112px_minmax(0,1fr)] gap-3">
                    <span className="num pt-px text-sm text-muted-foreground">
                      {ev.event_date ? formatDate(ev.event_date) : "—"}
                    </span>
                    <div>
                      <p className="font-medium text-foreground">{ev.title}</p>
                      {ev.description && <p className="mt-0.5 text-sm text-muted-foreground">{ev.description}</p>}
                    </div>
                  </div>
                </Row>
              ))}
            </ol>
            <p className="border-t border-border px-5 py-2.5 text-xs text-muted-foreground">
              Dates as stated in the source documents.
            </p>
          </Card>
        </div>
      )}
    </div>
  );
}
