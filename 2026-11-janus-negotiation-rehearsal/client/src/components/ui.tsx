import { forwardRef } from "react";
import { cn } from "@/lib/utils";
import { partyHue } from "@/lib/parties";

type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";
type ButtonSize = "sm" | "md";

const BUTTON_VARIANTS: Record<ButtonVariant, string> = {
  // The one filled button per screen: Navy 800, never the accent
  primary: "bg-primary text-primary-foreground hover:bg-[#143d4a] active:bg-[#0b2026]",
  secondary: "bg-white text-foreground border border-input hover:bg-raised active:bg-active",
  ghost: "text-muted-foreground hover:text-foreground hover:bg-active active:bg-[#e4e2dd]",
  danger: "bg-white text-destructive border border-[#d69ea8] hover:bg-negative-bg",
};

const BUTTON_SIZES: Record<ButtonSize, string> = {
  sm: "h-8 px-2.5 text-sm gap-1.5",
  md: "h-9 px-3.5 text-sm gap-2",
};

export const Button = forwardRef<
  HTMLButtonElement,
  React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: ButtonVariant; size?: ButtonSize }
>(function Button({ variant = "secondary", size = "md", className, type = "button", ...props }, ref) {
  return (
    <button
      ref={ref}
      type={type}
      className={cn(
        "inline-flex items-center justify-center rounded-md font-medium whitespace-nowrap transition-colors duration-100 disabled:opacity-50 disabled:cursor-not-allowed",
        BUTTON_VARIANTS[variant],
        BUTTON_SIZES[size],
        className,
      )}
      {...props}
    />
  );
});

/** A square icon-only button. `label` is required: it is the accessible name and the tooltip. */
export function IconButton({
  label,
  className,
  tone = "default",
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & { label: string; tone?: "default" | "danger" }) {
  return (
    <button
      type="button"
      aria-label={label}
      title={label}
      className={cn(
        "inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors duration-100 hover:bg-active disabled:opacity-50",
        tone === "danger" ? "hover:text-destructive" : "hover:text-foreground",
        className,
      )}
      {...props}
    />
  );
}

export type PillTone = "positive" | "warning" | "negative" | "info" | "neutral" | "plain";

const PILL_TONES: Record<PillTone, string> = {
  positive: "bg-positive-bg text-positive-ink",
  warning: "bg-warning-bg text-warning-ink",
  negative: "bg-negative-bg text-negative-ink",
  info: "bg-info-bg text-info-ink",
  neutral: "bg-neutral-bg text-neutral-ink",
  plain: "bg-active text-nav",
};

export function Pill({
  tone = "plain",
  className,
  children,
}: {
  tone?: PillTone;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <span
      className={cn(
        "inline-flex h-5 flex-none items-center rounded-sm px-1.5 text-xs font-medium whitespace-nowrap",
        PILL_TONES[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

const STATUS_TONES: Record<string, PillTone> = {
  draft: "plain",
  active: "info",
  simulating: "warning",
  running: "warning",
  pending: "plain",
  completed: "positive",
  converged: "positive",
  archived: "neutral",
  cancelled: "neutral",
  failed: "negative",
};

export function StatusPill({ status }: { status: string }) {
  return (
    <Pill tone={STATUS_TONES[status] ?? "plain"} className="capitalize">
      {status}
    </Pill>
  );
}

/** A party's stable identity: its initial on its own pale tint. Identity, not data. */
export function PartyDisc({
  name,
  index,
  size = "md",
}: {
  name: string;
  index: number;
  size?: "sm" | "md";
}) {
  const hue = partyHue(index);
  return (
    <span
      aria-hidden="true"
      className={cn(
        "inline-flex flex-none items-center justify-center rounded-full font-semibold",
        size === "sm" ? "h-5 w-5 text-[11px]" : "h-8 w-8 text-sm",
      )}
      style={{ backgroundColor: hue.tint, color: hue.ink, boxShadow: `inset 0 0 0 1.5px ${hue.line}` }}
    >
      {name.trim().charAt(0).toUpperCase() || "?"}
    </span>
  );
}

/** The mark of a party's chart line, for legends: the same colour and stroke as the line itself. */
export function LineSwatch({ color }: { color: string }) {
  return (
    <span aria-hidden="true" className="inline-block h-0.5 w-3 flex-none rounded-full" style={{ backgroundColor: color }} />
  );
}

export function Card({
  className,
  children,
  ...props
}: React.HTMLAttributes<HTMLElement> & { children: React.ReactNode }) {
  return (
    <section className={cn("rounded-lg border border-border bg-card", className)} {...props}>
      {children}
    </section>
  );
}

/** A card's header row: title on the left, an optional note or action on the right. */
export function CardHeader({
  title,
  meta,
  action,
  id,
}: {
  title: React.ReactNode;
  meta?: React.ReactNode;
  action?: React.ReactNode;
  id?: string;
}) {
  return (
    <div className="flex min-h-12 items-center justify-between gap-4 px-5 pt-4 pb-3">
      <div className="min-w-0">
        <h2 id={id} className="text-base font-semibold text-foreground">
          {title}
        </h2>
        {meta && <p className="mt-0.5 text-sm text-muted-foreground">{meta}</p>}
      </div>
      {action && <div className="flex shrink-0 items-center gap-2">{action}</div>}
    </div>
  );
}

/** An empty block keeps its frame: one sentence, and the next step if there is one. */
export function EmptyState({
  children,
  action,
  className,
}: {
  children: React.ReactNode;
  action?: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-col items-start gap-3 rounded-lg border border-dashed border-input/60 bg-card px-5 py-6 text-sm text-muted-foreground sm:flex-row sm:items-center sm:justify-between",
        className,
      )}
    >
      <p>{children}</p>
      {action}
    </div>
  );
}
