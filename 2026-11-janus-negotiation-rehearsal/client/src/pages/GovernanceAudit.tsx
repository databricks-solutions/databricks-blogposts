import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { formatDateTime } from "@/lib/format";
import { Button, Card, CardHeader, EmptyState } from "@/components/ui";

interface AuditEntry {
  log_id: string;
  matter_id: string | null;
  action: string;
  entity_type: string | null;
  entity_id: string | null;
  details: unknown;
  timestamp: string;
}

function details(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "string") return value;
  return Object.entries(value as Record<string, unknown>)
    .map(([k, v]) => `${k.replace(/_/g, " ")} ${typeof v === "object" ? JSON.stringify(v) : String(v)}`)
    .join(" · ");
}

export default function GovernanceAudit() {
  const {
    data: logs = [],
    isLoading,
    isError,
    refetch,
  } = useQuery<AuditEntry[]>({
    queryKey: ["audit-logs"],
    queryFn: async () => {
      const res = await fetch("/api/governance/audit-log");
      if (!res.ok) throw new Error(`${res.status}`);
      return res.json();
    },
  });

  return (
    <div className="px-4 py-6 md:px-8 md:py-8">
      <h1 className="text-2xl font-semibold text-foreground">Governance</h1>
      <p className="mt-1 max-w-[72ch] text-sm text-muted-foreground">
        Matters, documents and simulation results live in Unity Catalog, so table and volume access
        follows its grants. Actions recorded in the audit log appear below, newest first.
      </p>

      <div className="mt-6">
        {isError ? (
          <EmptyState
            action={
              <Button size="sm" onClick={() => refetch()}>
                Retry
              </Button>
            }
          >
            Couldn’t load the audit log. Check that the SQL warehouse is running, then retry.
          </EmptyState>
        ) : (
          <Card>
            <CardHeader
              title="Audit log"
              meta={
                isLoading
                  ? "Loading…"
                  : logs.length >= 100
                    ? "The latest 100 entries"
                    : `${logs.length.toLocaleString()} ${logs.length === 1 ? "entry" : "entries"}`
              }
            />
            {!isLoading && logs.length === 0 ? (
              <p className="border-t border-border px-5 py-6 text-sm text-muted-foreground">
                No entries yet. Rows written to the <span className="num">audit_logs</span> table appear here.
              </p>
            ) : (
              <div className="overflow-x-auto border-t border-border">
                <table className="w-full min-w-[720px] text-sm">
                  <thead>
                    <tr className="h-10 border-b border-border text-left text-xs text-muted-foreground">
                      <th scope="col" className="w-44 px-5 font-normal">
                        Time
                      </th>
                      <th scope="col" className="w-48 px-3 font-normal">
                        Action
                      </th>
                      <th scope="col" className="w-36 px-3 font-normal">
                        Object
                      </th>
                      <th scope="col" className="px-3 font-normal">
                        Details
                      </th>
                      <th scope="col" className="w-32 px-5 font-normal">
                        Matter
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {logs.map((log) => (
                      <tr key={log.log_id} className="h-11 border-b border-rule last:border-b-0">
                        <td className="num px-5 text-muted-foreground">{formatDateTime(log.timestamp)}</td>
                        <td className="px-3 text-foreground">{log.action.replace(/_/g, " ")}</td>
                        <td className="px-3 text-muted-foreground">{(log.entity_type || "—").replace(/_/g, " ")}</td>
                        <td className="max-w-md truncate px-3 text-muted-foreground" title={details(log.details)}>
                          {details(log.details)}
                        </td>
                        <td className="px-5">
                          {log.matter_id ? (
                            <Link to={`/matter/${log.matter_id}`} className="num text-foreground hover:underline">
                              {log.matter_id.slice(0, 8)}…
                            </Link>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        )}
      </div>
    </div>
  );
}
