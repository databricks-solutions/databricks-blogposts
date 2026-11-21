import { useId, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Pencil, Plus, Trash2, X } from "lucide-react";
import { api, type Party } from "@/lib/api";
import { partyHue } from "@/lib/parties";
import { plural } from "@/lib/format";
import { inputClass, labelClass, selectClass, textareaClass } from "@/lib/styles";
import ConfirmDialog from "@/components/ConfirmDialog";
import { Button, Card, EmptyState, IconButton, PartyDisc, Pill } from "@/components/ui";

const STANCES = ["aggressive", "neutral", "cooperative", "defensive"];

interface PartyFields {
  name: string;
  role: string;
  stance: string;
  risk_tolerance: number;
  batna: string;
  objectives: string[];
  constraints: string[];
}

const EMPTY_PARTY: PartyFields = {
  name: "",
  role: "",
  stance: "neutral",
  risk_tolerance: 0.5,
  batna: "",
  objectives: [],
  constraints: [],
};

function fieldsOf(p: Party): PartyFields {
  return {
    name: p.name,
    role: p.role ?? "",
    stance: p.stance,
    risk_tolerance: p.risk_tolerance,
    batna: p.batna ?? "",
    objectives: p.objectives,
    constraints: p.constraints,
  };
}

/** A list of short strings: type and press Enter (or Add) to append; each item has its own remove button. */
function ListInput({
  label,
  placeholder,
  items,
  onChange,
}: {
  label: string;
  placeholder: string;
  items: string[];
  onChange: (items: string[]) => void;
}) {
  const id = useId();
  const [draft, setDraft] = useState("");
  const add = () => {
    if (draft.trim()) {
      onChange([...items, draft.trim()]);
      setDraft("");
    }
  };
  return (
    <div className="sm:col-span-2">
      <label htmlFor={id} className={labelClass}>
        {label}
      </label>
      {items.length > 0 && (
        <ul className="mb-2 space-y-1">
          {items.map((o, i) => (
            <li key={i} className="flex items-start justify-between gap-2 rounded-md bg-raised py-1.5 pr-1 pl-3 text-sm">
              <span className="pt-0.5">{o}</span>
              <IconButton
                label={`Remove “${o}”`}
                className="h-7 w-7"
                onClick={() => onChange(items.filter((_, j) => j !== i))}
              >
                <X className="h-3.5 w-3.5" aria-hidden="true" />
              </IconButton>
            </li>
          ))}
        </ul>
      )}
      <div className="flex gap-2">
        <input
          id={id}
          autoComplete="off"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              add();
            }
          }}
          className={inputClass}
          placeholder={placeholder}
        />
        <Button onClick={add} disabled={!draft.trim()}>
          Add
        </Button>
      </div>
    </div>
  );
}

function PartyForm({
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
  initial: PartyFields;
  submitLabel: string;
  pendingLabel: string;
  pending: boolean;
  error?: string | null;
  onSubmit: (fields: PartyFields) => void;
  onCancel: () => void;
}) {
  const id = useId();
  const [f, setF] = useState<PartyFields>(initial);
  const set = <K extends keyof PartyFields>(k: K, v: PartyFields[K]) => setF({ ...f, [k]: v });

  return (
    <Card className="border-input p-5">
      <form
        aria-label={heading}
        onSubmit={(e) => {
          e.preventDefault();
          if (f.name.trim()) onSubmit({ ...f, name: f.name.trim() });
        }}
      >
        <h3 className="mb-4 text-base font-semibold">{heading}</h3>
        <div className="grid gap-4 sm:grid-cols-2">
          <div>
            <label htmlFor={`${id}-name`} className={labelClass}>
              Name
            </label>
            <input
              id={`${id}-name`}
              name="name"
              autoComplete="off"
              value={f.name}
              onChange={(e) => set("name", e.target.value)}
              className={inputClass}
              placeholder="e.g. Northwind Logistics…"
            />
          </div>
          <div>
            <label htmlFor={`${id}-role`} className={labelClass}>
              Role
            </label>
            <input
              id={`${id}-role`}
              name="role"
              autoComplete="off"
              value={f.role}
              onChange={(e) => set("role", e.target.value)}
              className={inputClass}
              placeholder="e.g. Claimant, buyer, insurer…"
            />
          </div>
          <div>
            <label htmlFor={`${id}-stance`} className={labelClass}>
              Stance
            </label>
            <select
              id={`${id}-stance`}
              name="stance"
              value={f.stance}
              onChange={(e) => set("stance", e.target.value)}
              className={selectClass}
            >
              {STANCES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label htmlFor={`${id}-risk`} className={labelClass}>
              Risk tolerance <span className="num font-normal text-muted-foreground">{f.risk_tolerance.toFixed(2)}</span>
            </label>
            <input
              id={`${id}-risk`}
              name="risk_tolerance"
              type="range"
              min="0"
              max="1"
              step="0.05"
              value={f.risk_tolerance}
              onChange={(e) => set("risk_tolerance", parseFloat(e.target.value))}
              className="h-9 w-full accent-[#1b3139]"
            />
          </div>
          <div className="sm:col-span-2">
            <label htmlFor={`${id}-batna`} className={labelClass}>
              BATNA <span className="font-normal text-muted-foreground">· best alternative if talks fail</span>
            </label>
            <textarea
              id={`${id}-batna`}
              name="batna"
              autoComplete="off"
              value={f.batna}
              onChange={(e) => set("batna", e.target.value)}
              rows={2}
              className={textareaClass}
              placeholder="e.g. Arbitration under §14 and move volume to a second carrier…"
            />
          </div>
          <ListInput
            label="Objectives"
            placeholder="Add an objective, then press Enter…"
            items={f.objectives}
            onChange={(v) => set("objectives", v)}
          />
          <ListInput
            label="Constraints"
            placeholder="Add a constraint, then press Enter…"
            items={f.constraints}
            onChange={(v) => set("constraints", v)}
          />
        </div>
        {error && (
          <p role="alert" className="mt-3 text-sm text-destructive">
            {error}
          </p>
        )}
        <div className="mt-5 flex justify-end gap-2">
          <Button variant="ghost" onClick={onCancel}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={!f.name.trim() || pending}>
            {pending ? pendingLabel : submitLabel}
          </Button>
        </div>
      </form>
    </Card>
  );
}

function Detail({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[112px_minmax(0,1fr)] gap-3 py-2.5">
      <dt className="text-sm text-muted-foreground">{label}</dt>
      <dd className="text-sm text-foreground">{children}</dd>
    </div>
  );
}

function PartyCard({
  party,
  index,
  onEdit,
  onDelete,
}: {
  party: Party;
  index: number;
  onEdit: () => void;
  onDelete: () => void;
}) {
  const hue = partyHue(index);
  return (
    <Card className="flex flex-col p-5">
      <div className="flex items-start gap-3">
        <PartyDisc name={party.name} index={index} />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-base font-semibold text-foreground">{party.name}</h3>
            {party.source === "auto_detected" && <Pill tone="info">From document</Pill>}
          </div>
          {party.role && <p className="mt-0.5 text-sm text-muted-foreground">{party.role}</p>}
        </div>
        <div className="-mt-1 -mr-2 flex">
          <IconButton label={`Edit ${party.name}`} onClick={onEdit}>
            <Pencil className="h-4 w-4" aria-hidden="true" />
          </IconButton>
          <IconButton label={`Delete ${party.name}`} tone="danger" onClick={onDelete}>
            <Trash2 className="h-4 w-4" aria-hidden="true" />
          </IconButton>
        </div>
      </div>

      <dl className="mt-3 divide-y divide-rule border-t border-rule">
        <Detail label="Stance">
          <span className="capitalize">{party.stance}</span>
        </Detail>
        <Detail label="Risk tolerance">
          <span className="flex items-center gap-3">
            <span className="num w-9">{party.risk_tolerance.toFixed(2)}</span>
            <span className="h-1.5 max-w-40 flex-1 rounded-full bg-active" aria-hidden="true">
              <span
                className="block h-full rounded-full"
                style={{ width: `${party.risk_tolerance * 100}%`, backgroundColor: hue.line }}
              />
            </span>
          </span>
        </Detail>
        <Detail label="BATNA">{party.batna || <span className="text-muted-foreground">—</span>}</Detail>
        <Detail label="Objectives">
          {party.objectives.length ? (
            <ul className="list-disc space-y-1 pl-4 marker:text-muted-foreground">
              {party.objectives.map((o, i) => (
                <li key={i}>{o}</li>
              ))}
            </ul>
          ) : (
            <span className="text-muted-foreground">—</span>
          )}
        </Detail>
        <Detail label="Constraints">
          {party.constraints.length ? (
            <ul className="list-disc space-y-1 pl-4 marker:text-muted-foreground">
              {party.constraints.map((c, i) => (
                <li key={i}>{c}</li>
              ))}
            </ul>
          ) : (
            <span className="text-muted-foreground">—</span>
          )}
        </Detail>
      </dl>
    </Card>
  );
}

export default function PartyEditor({ matterId }: { matterId: string }) {
  const queryClient = useQueryClient();
  const [showAdd, setShowAdd] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [toDelete, setToDelete] = useState<Party | null>(null);

  const { data: parties = [], isLoading } = useQuery({
    queryKey: ["parties", matterId],
    queryFn: () => api.listParties(matterId),
  });

  const refresh = () => {
    queryClient.invalidateQueries({ queryKey: ["parties", matterId] });
    queryClient.invalidateQueries({ queryKey: ["matters"] });
  };

  const createMut = useMutation({
    mutationFn: (fields: PartyFields) => api.createParty(matterId, { ...fields }),
    onSuccess: (created) => {
      queryClient.setQueryData<Party[]>(["parties", matterId], (old = []) => [...old, created]);
      refresh();
      setShowAdd(false);
    },
  });

  const updateMut = useMutation({
    mutationFn: ({ partyId, fields }: { partyId: string; fields: PartyFields }) =>
      api.updateParty(matterId, partyId, { ...fields }),
    onSuccess: (updated) => {
      queryClient.setQueryData<Party[]>(["parties", matterId], (old = []) =>
        old.map((p) => (p.party_id === updated.party_id ? updated : p)),
      );
      refresh();
      setEditingId(null);
    },
  });

  const deleteMut = useMutation({
    mutationFn: (partyId: string) => api.deleteParty(matterId, partyId),
    onSuccess: (_, partyId) => {
      queryClient.setQueryData<Party[]>(["parties", matterId], (old = []) =>
        old.filter((p) => p.party_id !== partyId),
      );
      refresh();
      setToDelete(null);
    },
  });

  return (
    <div className="px-4 py-6 md:px-8">
      <div className="mb-4 flex items-center justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold text-foreground">Parties</h2>
          <p className="mt-0.5 text-sm text-muted-foreground">
            {isLoading
              ? "Loading parties…"
              : parties.length < 2
                ? `${plural(parties.length, "party", "parties")}. A simulation needs at least 2.`
                : `${plural(parties.length, "party", "parties")}. Each one is played by its own agent.`}
          </p>
        </div>
        <Button variant="primary" onClick={() => setShowAdd(true)} disabled={showAdd}>
          <Plus className="h-4 w-4" aria-hidden="true" />
          Add Party
        </Button>
      </div>

      {showAdd && (
        <div className="mb-4">
          <PartyForm
            heading="New party"
            initial={EMPTY_PARTY}
            submitLabel="Add Party"
            pendingLabel="Adding…"
            pending={createMut.isPending}
            error={createMut.error?.message}
            onSubmit={(fields) => createMut.mutate(fields)}
            onCancel={() => {
              setShowAdd(false);
              createMut.reset();
            }}
          />
        </div>
      )}

      {toDelete && (
        <ConfirmDialog
          title="Delete party?"
          message={
            <p>
              <span className="font-medium text-foreground">{toDelete.name}</span> will be removed
              from this matter. Its arguments will also stop appearing in past simulation runs.
              Edit the party instead if you only want to change its details.
            </p>
          }
          confirmLabel="Delete Party"
          pending={deleteMut.isPending}
          error={deleteMut.error?.message}
          onConfirm={() => deleteMut.mutate(toDelete.party_id)}
          onCancel={() => {
            setToDelete(null);
            deleteMut.reset();
          }}
        />
      )}

      {!isLoading && parties.length === 0 && !showAdd ? (
        <EmptyState
          action={
            <Button variant="primary" size="sm" onClick={() => setShowAdd(true)}>
              <Plus className="h-4 w-4" aria-hidden="true" />
              Add Party
            </Button>
          }
        >
          No parties yet. Add them by hand, or process a document and Janus will propose them.
        </EmptyState>
      ) : (
        <div className={`grid gap-4 ${parties.length === 3 || parties.length >= 5 ? "lg:grid-cols-2 xl:grid-cols-3" : "lg:grid-cols-2"}`}>
          {parties.map((p: Party, i: number) =>
            editingId === p.party_id ? (
              <PartyForm
                key={p.party_id}
                heading={`Edit ${p.name}`}
                initial={fieldsOf(p)}
                submitLabel="Save Changes"
                pendingLabel="Saving…"
                pending={updateMut.isPending}
                error={updateMut.error?.message}
                onSubmit={(fields) => updateMut.mutate({ partyId: p.party_id, fields })}
                onCancel={() => {
                  setEditingId(null);
                  updateMut.reset();
                }}
              />
            ) : (
              <PartyCard
                key={p.party_id}
                party={p}
                index={i}
                onEdit={() => {
                  updateMut.reset();
                  setEditingId(p.party_id);
                }}
                onDelete={() => setToDelete(p)}
              />
            ),
          )}
        </div>
      )}
    </div>
  );
}
