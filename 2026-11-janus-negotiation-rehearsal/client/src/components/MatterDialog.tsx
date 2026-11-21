import { useEffect, useState } from "react";
import { Button } from "@/components/ui";
import { inputClass, labelClass, selectClass, textareaClass } from "@/lib/styles";

const MATTER_TYPES = ["negotiation", "dispute", "mediation", "arbitration", "deal", "custom"];
const MATTER_STATUSES = ["draft", "active", "simulating", "completed", "archived"];

export interface MatterFields {
  title: string;
  matter_type: string;
  status?: string;
  domain_context: string;
}

/** Create or edit a matter. Status is only shown when editing (`initial.status` set). */
export default function MatterDialog({
  heading,
  initial,
  submitLabel,
  pendingLabel,
  pending,
  error,
  onSubmit,
  onCancel,
}: {
  heading: string;
  initial: MatterFields;
  submitLabel: string;
  pendingLabel: string;
  pending: boolean;
  error?: string | null;
  onSubmit: (fields: MatterFields) => void;
  onCancel: () => void;
}) {
  const [fields, setFields] = useState<MatterFields>(initial);
  const set = (k: keyof MatterFields, v: string) => setFields({ ...fields, [k]: v });
  const editing = initial.status !== undefined;

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && !pending && onCancel();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onCancel, pending]);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center overscroll-contain bg-[#0b2026]/40 p-4">
      <form
        role="dialog"
        aria-modal="true"
        aria-labelledby="matter-dialog-title"
        className="w-full max-w-lg rounded-lg bg-card p-6 shadow-[0_12px_32px_rgba(11,32,38,0.18)]"
        onSubmit={(e) => {
          e.preventDefault();
          if (fields.title.trim()) onSubmit({ ...fields, title: fields.title.trim() });
        }}
      >
        <h2 id="matter-dialog-title" className="text-lg font-semibold text-foreground">
          {heading}
        </h2>
        <div className="mt-5 space-y-4">
          <div>
            <label htmlFor="matter-title" className={labelClass}>
              Title
            </label>
            <input
              id="matter-title"
              name="title"
              autoComplete="off"
              autoFocus
              value={fields.title}
              onChange={(e) => set("title", e.target.value)}
              className={inputClass}
              placeholder="e.g. Northwind v. Apex: MSA termination…"
            />
          </div>
          <div className={editing ? "grid grid-cols-2 gap-4" : ""}>
            <div>
              <label htmlFor="matter-type" className={labelClass}>
                Type
              </label>
              <select
                id="matter-type"
                name="matter_type"
                value={fields.matter_type}
                onChange={(e) => set("matter_type", e.target.value)}
                className={selectClass}
              >
                {MATTER_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            {editing && (
              <div>
                <label htmlFor="matter-status" className={labelClass}>
                  Status
                </label>
                <select
                  id="matter-status"
                  name="status"
                  value={fields.status}
                  onChange={(e) => set("status", e.target.value)}
                  className={selectClass}
                >
                  {MATTER_STATUSES.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </div>
          <div>
            <label htmlFor="matter-context" className={labelClass}>
              Description
            </label>
            <textarea
              id="matter-context"
              name="domain_context"
              autoComplete="off"
              value={fields.domain_context}
              onChange={(e) => set("domain_context", e.target.value)}
              rows={5}
              className={textareaClass}
              placeholder="Who the parties are, what is in dispute, and what each side wants…"
            />
            <p className="mt-1.5 text-xs text-muted-foreground">
              The party agents, the chat assistant and document extraction read this as
              background.
            </p>
          </div>
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
        </div>
        <div className="mt-6 flex justify-end gap-2">
          <Button variant="ghost" onClick={onCancel}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={!fields.title.trim() || pending}>
            {pending ? pendingLabel : submitLabel}
          </Button>
        </div>
      </form>
    </div>
  );
}
