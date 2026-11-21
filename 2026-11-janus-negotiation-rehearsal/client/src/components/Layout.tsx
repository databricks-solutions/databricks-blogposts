import { useEffect, useState } from "react";
import { Link, NavLink, useLocation } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { FolderOpen, Menu, ShieldCheck, X } from "lucide-react";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

/** Janus looks both ways: two faces turned apart, on the Lava square of the Databricks lockup. */
export function JanusMark({ size = 24 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" className="flex-none">
      <rect width="24" height="24" rx="5" fill="#FF3621" />
      <path
        d="M10 6.5 5.5 12l4.5 5.5M14 6.5l4.5 5.5-4.5 5.5"
        fill="none"
        stroke="#0B2026"
        strokeWidth="2.25"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

const NAV_ITEMS = [
  { to: "/", label: "Matters", icon: FolderOpen, end: true },
  { to: "/governance", label: "Governance", icon: ShieldCheck, end: false },
];

function Wordmark() {
  return (
    <Link to="/" className="flex items-center gap-2.5 rounded-md">
      <JanusMark />
      <span className="leading-tight">
        <span className="block text-base font-bold text-foreground">Janus</span>
        <span className="block text-xs text-muted-foreground">Negotiation rehearsal</span>
      </span>
    </Link>
  );
}

function NavContent({ onNavigate }: { onNavigate?: () => void }) {
  const location = useLocation();
  const { data: matters = [] } = useQuery({ queryKey: ["matters"], queryFn: api.listMatters });
  const recent = matters.slice(0, 6);

  const rowClass = (active: boolean) =>
    cn(
      "flex h-8 items-center gap-2.5 rounded-md px-2.5 text-sm transition-colors duration-100",
      active ? "bg-active font-medium text-foreground" : "text-nav hover:bg-active/70",
    );

  return (
    <>
      <nav aria-label="Main" className="space-y-0.5">
        {NAV_ITEMS.map(({ to, label, icon: Icon, end }) => {
          const active = end
            ? location.pathname === to || location.pathname.startsWith("/matter/")
            : location.pathname.startsWith(to);
          return (
            <NavLink key={to} to={to} end={end} onClick={onNavigate} className={rowClass(active)}>
              <Icon className="h-4 w-4 text-muted-foreground" strokeWidth={1.75} aria-hidden="true" />
              {label}
            </NavLink>
          );
        })}
      </nav>

      {recent.length > 0 && (
        <div className="mt-7">
          <p className="mb-1.5 px-2.5 text-xs text-muted-foreground">Recent matters</p>
          <ul className="space-y-0.5">
            {recent.map((m) => {
              const active = location.pathname === `/matter/${m.matter_id}`;
              return (
                <li key={m.matter_id}>
                  <Link
                    to={`/matter/${m.matter_id}`}
                    onClick={onNavigate}
                    title={m.title}
                    className={cn(rowClass(active), "pl-[34px]")}
                  >
                    <span className="truncate">{m.title}</span>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </>
  );
}

export default function Layout({ children }: { children: React.ReactNode }) {
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    if (!menuOpen) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setMenuOpen(false);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [menuOpen]);

  return (
    <div className="flex h-dvh flex-col bg-background md:flex-row">
      <a
        href="#main"
        className="sr-only z-50 rounded-md bg-primary px-3 py-2 text-sm text-primary-foreground focus:not-sr-only focus:fixed focus:top-2 focus:left-2"
      >
        Skip to content
      </a>

      {/* Desktop and tablet: a labelled sidebar on the page ground */}
      <aside className="hidden w-60 flex-none flex-col border-r border-border md:flex">
        <div className="px-5 pt-5 pb-6">
          <Wordmark />
        </div>
        <div className="flex-1 overflow-y-auto px-3">
          <NavContent />
        </div>
        <div className="px-5 py-4 text-xs text-muted-foreground">
          A Databricks App <span className="num">· v0.1.0</span>
        </div>
      </aside>

      {/* Phone: the sidebar folds into a top bar and a full-screen sheet */}
      <header className="flex h-12 flex-none items-center justify-between border-b border-border px-4 md:hidden">
        <Wordmark />
        <button
          type="button"
          onClick={() => setMenuOpen(true)}
          aria-label="Open navigation"
          aria-expanded={menuOpen}
          className="-mr-2 inline-flex h-11 w-11 items-center justify-center rounded-md text-nav hover:bg-active"
        >
          <Menu className="h-5 w-5" aria-hidden="true" />
        </button>
      </header>
      {menuOpen && (
        <div
          role="dialog"
          aria-modal="true"
          aria-label="Navigation"
          className="fixed inset-0 z-40 flex flex-col overscroll-contain bg-background md:hidden"
        >
          <div className="flex h-12 flex-none items-center justify-between border-b border-border px-4">
            <Wordmark />
            <button
              type="button"
              onClick={() => setMenuOpen(false)}
              aria-label="Close navigation"
              className="-mr-2 inline-flex h-11 w-11 items-center justify-center rounded-md text-nav hover:bg-active"
            >
              <X className="h-5 w-5" aria-hidden="true" />
            </button>
          </div>
          <div className="flex-1 overflow-y-auto px-3 py-4">
            <NavContent onNavigate={() => setMenuOpen(false)} />
          </div>
        </div>
      )}

      <main id="main" className="min-w-0 flex-1 overflow-y-auto">
        {children}
      </main>
    </div>
  );
}
