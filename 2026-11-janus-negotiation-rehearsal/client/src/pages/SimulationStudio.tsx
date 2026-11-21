import { useState, useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ChevronDown,
  ChevronRight,
  Loader2,
  Play,
  Square,
  Trash2,
} from "lucide-react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { api } from "@/lib/api";
import { partyHue } from "@/lib/parties";
import { formatDateTime, pct, plural } from "@/lib/format";
import { cn } from "@/lib/utils";
import ConfirmDialog from "@/components/ConfirmDialog";
import {
  Button,
  Card,
  CardHeader,
  EmptyState,
  IconButton,
  LineSwatch,
  PartyDisc,
  Pill,
} from "@/components/ui";
import {
  useSimulationStream,
  type NoveltyPoint,
} from "@/hooks/useSimulationStream";
import type { ArgumentCluster, Simulation, SimulationResults } from "@/lib/api";

const RESPONSE_TYPE_LABELS: Record<string, string> = {
  opener: "Opens",
  counter_argument: "Counters",
  supporting_argument: "Supports",
  additional_info: "Adds information",
  concession: "Concedes",
  pass: "Passes",
};

// Chart chrome: recessive grid and ticks in the brand's text grey
const AXIS = {
  fontSize: 12,
  fill: "#5A6F77",
  fontFamily: "DM Mono, ui-monospace, monospace",
};
const GRID = "#ECEAE5";
const INK = "#0B2026";
const QUIET = "#90A5B1"; // Navy 400: secondary series and reference marks

// Win probability chart: the plot is the chart's height minus its top margin and x-axis band
const WIN_MARGIN_TOP = 12;
const WIN_X_AXIS = 28;
const LABEL_GAP_PX = 15;
const LABEL_MAX_CHARS = 22;

interface SSEEvent {
  seq: number;
  event: string;
  data: Record<string, unknown>;
}

/** Convert /results response into synthetic SSE-style events for the UI */
function resultsToEvents(results: SimulationResults): {
  events: SSEEvent[];
  noveltyHistory: NoveltyPoint[];
} {
  const events: SSEEvent[] = [];
  const noveltyHistory: NoveltyPoint[] = [];
  let seq = 0;

  // Scoring events from scoring_history
  for (const snap of results.scoring_history) {
    events.push({
      seq: seq++,
      event: "scoring_update",
      data: {
        round_number: snap.round_number,
        branch_index: 0,
        party_scores: snap.party_scores,
        win_probabilities: snap.win_probabilities,
        leverage_scores: snap.leverage_scores,
        convergence_metrics: snap.convergence_metrics,
        leverage_explanations: snap.leverage_explanations || {},
        financial_positions: snap.financial_positions || {},
      },
    });

    // Novelty from convergence_metrics
    const cm = snap.convergence_metrics as Record<string, unknown>;
    if (cm && cm.global_novelty !== undefined) {
      noveltyHistory.push({
        round: snap.round_number,
        branchIndex: 0,
        globalNovelty: cm.global_novelty as number,
        perPartyNovelty: {},
        threadCount: (cm.thread_count as number) || 1,
        novelCount: (cm.novel_count as number) || 0,
        totalCount: (cm.total_count as number) || 0,
      });
    }
  }

  // Argument events from rounds data
  for (const [, roundArgs] of Object.entries(results.rounds)) {
    for (const arg of roundArgs as Record<string, unknown>[]) {
      const content = arg.content as Record<string, unknown> | string;
      let mainClaim = "";
      let strategyType = "";
      let financialAsk = "N/A";
      let financialOffer = "N/A";
      let estimatedValue = "N/A";

      if (typeof content === "object" && content) {
        mainClaim = (content.main_claim as string) || "";
        strategyType = (content.strategy_type as string) || "";
        financialAsk = (content.financial_ask as string) || "N/A";
        financialOffer = (content.financial_offer as string) || "N/A";
        estimatedValue = (content.estimated_total_value as string) || "N/A";
      } else if (typeof content === "string") {
        try {
          const p = JSON.parse(content);
          mainClaim = p.main_claim || "";
          strategyType = p.strategy_type || "";
          financialAsk = p.financial_ask || "N/A";
          financialOffer = p.financial_offer || "N/A";
          estimatedValue = p.estimated_total_value || "N/A";
        } catch {
          mainClaim = content.slice(0, 200);
        }
      }

      events.push({
        seq: seq++,
        event: "party_argument",
        data: {
          round_number: arg.round_number as number,
          branch_index: 0,
          party_id: arg.party_id as string,
          party_name: arg.party_name as string,
          argument_id: arg.argument_id as string,
          strategy_type: (arg.strategy_type as string) || strategyType,
          main_claim: mainClaim,
          response_type: (arg.response_type as string) || "opener",
          response_to_argument_id:
            (arg.response_to_argument_id as string) || null,
          thread_topic: "",
          financial_ask: financialAsk,
          financial_offer: financialOffer,
          estimated_total_value: estimatedValue,
        },
      });
    }
  }

  // Judge evaluations
  for (const ev of results.evaluations) {
    events.push({
      seq: seq++,
      event: "judge_evaluation",
      data: {
        round_number: 0,
        branch_index: 0,
        argument_id: ev.argument_id as string,
        scores: {
          evidence_support: ev.evidence_support as number,
          logical_coherence: ev.logical_coherence as number,
          novelty: ev.novelty as number,
          strategic_impact: ev.strategic_impact as number,
          overall: ev.overall_score as number,
        },
        reasoning: ev.reasoning as string,
      },
    });
  }

  // Terminal event
  events.push({
    seq: seq++,
    event: results.status === "converged" ? "converged" : "completed",
    data: {
      total_rounds: results.total_rounds,
      final_probabilities: results.final_probabilities,
      final_leverage: results.final_leverage,
    },
  });

  return { events, noveltyHistory };
}

interface PartyInfo {
  name: string;
  index: number;
  color: string;
}

interface Scoring {
  round_number: number;
  win_probabilities: Record<string, number>;
  leverage_scores: Record<string, number>;
  convergence_metrics: Record<string, number>;
  leverage_explanations?: Record<string, string>;
  std_win_probabilities?: Record<string, number>;
  financial_positions?: Record<
    string,
    {
      financial_ask: string;
      financial_offer: string;
      estimated_total_value: string;
    }
  >;
}

function runLabel(sim: Simulation): string {
  return `${formatDateTime(sim.created_at)} · ${sim.status} · ${plural(sim.total_rounds, "round")}`;
}

/** A plain tooltip: the round, then one row per series with its value. */
function ChartTooltip({
  active,
  label,
  payload,
  unit = "",
  digits = 1,
}: {
  active?: boolean;
  label?: string | number;
  payload?: {
    name?: string;
    value?: number;
    color?: string;
    dataKey?: string | number;
  }[];
  unit?: string;
  digits?: number;
}) {
  if (!active || !payload?.length) return null;
  const rows = [...payload].sort((a, b) => (b.value ?? 0) - (a.value ?? 0));
  return (
    <div className="rounded-md border border-border bg-white px-3 py-2 text-xs shadow-[0_4px_12px_rgba(11,32,38,0.10)]">
      <p className="mb-1 text-muted-foreground">Round {label}</p>
      {rows.map((r) => (
        <p
          key={String(r.dataKey)}
          className="flex items-center justify-between gap-4"
        >
          <span className="flex items-center gap-1.5 text-foreground">
            <LineSwatch color={r.color ?? INK} />
            {r.name}
          </span>
          <span className="num text-foreground">
            {typeof r.value === "number" ? r.value.toFixed(digits) : "—"}
            {unit}
          </span>
        </p>
      ))}
    </div>
  );
}

/** Push end labels apart (in value units) so names never sit on top of each other. */
function spreadLabels(
  finals: { key: string; value: number }[],
  gap: number,
  min: number,
  max: number,
) {
  const sorted = [...finals].sort((a, b) => b.value - a.value);
  const placed: { key: string; value: number }[] = [];
  for (const f of sorted) {
    const prev = placed[placed.length - 1];
    placed.push({
      key: f.key,
      value: prev
        ? Math.min(f.value, prev.value - gap)
        : Math.min(f.value, max),
    });
  }
  const overflow = min - (placed[placed.length - 1]?.value ?? min);
  if (overflow > 0) for (const p of placed) p.value += overflow;
  return Object.fromEntries(placed.map((p) => [p.key, p.value]));
}

function WinProbabilityChart({
  data,
  series,
}: {
  data: Record<string, number>[];
  series: { key: string; name: string; color: string }[];
}) {
  const [height, setHeight] = useState(300);
  const plotHeight = Math.max(80, height - WIN_MARGIN_TOP - WIN_X_AXIS);
  const last = data[data.length - 1] ?? {};
  const gap = (LABEL_GAP_PX / plotHeight) * 100;
  const labelAt = spreadLabels(
    series.map((s) => ({ key: s.key, value: last[s.key] ?? 0 })),
    gap,
    2,
    98,
  );
  const lastIndex = data.length - 1;
  const pxPerUnit = plotHeight / 100;

  return (
    <ResponsiveContainer
      width="100%"
      height="100%"
      onResize={(_w: number, h: number) => setHeight(h)}
    >
      <LineChart
        data={data}
        margin={{ top: WIN_MARGIN_TOP, right: 156, bottom: 0, left: 0 }}
      >
        <CartesianGrid vertical={false} stroke={GRID} />
        <XAxis
          dataKey="round"
          tick={AXIS}
          tickLine={false}
          axisLine={{ stroke: "#C4CCD6" }}
          height={WIN_X_AXIS}
          interval="preserveStartEnd"
          minTickGap={28}
        />
        <YAxis
          domain={[0, 100]}
          ticks={[0, 25, 50, 75, 100]}
          tick={AXIS}
          tickLine={false}
          axisLine={false}
          width={36}
          tickFormatter={(v: number) => `${v}%`}
        />
        <Tooltip
          content={<ChartTooltip unit="%" />}
          cursor={{ stroke: "#C4CCD6" }}
        />
        {series.map((s) => (
          <Line
            key={s.key}
            type="linear"
            dataKey={s.key}
            name={s.name}
            stroke={s.color}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, strokeWidth: 2, stroke: "#fff" }}
            isAnimationActive={false}
            label={(props: {
              x?: number | string;
              y?: number | string;
              index?: number;
              value?: unknown;
            }) => {
              if (
                props.index !== lastIndex ||
                typeof props.x !== "number" ||
                typeof props.y !== "number"
              )
                return <g key={`${s.key}-${props.index}`} />;
              const dy = ((last[s.key] ?? 0) - labelAt[s.key]) * pxPerUnit;
              return (
                <text
                  key={`${s.key}-label`}
                  x={props.x + 8}
                  y={props.y + dy}
                  dy={4}
                  fontSize={12}
                  fill={INK}
                  fontFamily="DM Sans, system-ui, sans-serif"
                >
                  {s.name.length > LABEL_MAX_CHARS
                    ? `${s.name.slice(0, LABEL_MAX_CHARS - 1)}…`
                    : s.name}
                </text>
              );
            }}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

/** Cumulative arguments made against distinct arguments found: the flat line is saturation. */
function SaturationChart({
  data,
  lastNewRound,
}: {
  data: { round: number; total: number; distinct: number }[];
  lastNewRound: number | null;
}) {
  return (
    <ResponsiveContainer width="100%" height={168}>
      <LineChart
        data={data}
        margin={{ top: 16, right: 12, bottom: 0, left: 0 }}
      >
        <CartesianGrid vertical={false} stroke={GRID} />
        <XAxis
          dataKey="round"
          tick={AXIS}
          tickLine={false}
          axisLine={{ stroke: "#C4CCD6" }}
          height={24}
          interval="preserveStartEnd"
          minTickGap={24}
        />
        <YAxis
          tick={AXIS}
          tickLine={false}
          axisLine={false}
          width={36}
          allowDecimals={false}
        />
        <Tooltip
          content={<ChartTooltip digits={0} />}
          cursor={{ stroke: "#C4CCD6" }}
        />
        {lastNewRound !== null && (
          <ReferenceLine
            x={lastNewRound}
            stroke={QUIET}
            strokeDasharray="3 3"
            label={{
              value: `Last new, round ${lastNewRound}`,
              position: "insideTopLeft",
              fontSize: 11,
              fill: "#5A6F77",
              dy: -14,
              dx: -4,
            }}
          />
        )}
        <Line
          type="linear"
          dataKey="total"
          name="Arguments made"
          stroke={QUIET}
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
        <Line
          type="linear"
          dataKey="distinct"
          name="New arguments"
          stroke={INK}
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}

function NoveltyChart({
  data,
  threshold,
}: {
  data: { round: number; novelty: number }[];
  threshold: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={132}>
      <LineChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
        <CartesianGrid vertical={false} stroke={GRID} />
        <XAxis
          dataKey="round"
          tick={AXIS}
          tickLine={false}
          axisLine={{ stroke: "#C4CCD6" }}
          height={24}
          interval="preserveStartEnd"
          minTickGap={24}
        />
        <YAxis
          domain={[0, 100]}
          ticks={[0, 50, 100]}
          tick={AXIS}
          tickLine={false}
          axisLine={false}
          width={36}
          tickFormatter={(v: number) => `${v}%`}
        />
        <Tooltip
          content={<ChartTooltip unit="%" />}
          cursor={{ stroke: "#C4CCD6" }}
        />
        <ReferenceLine
          y={threshold * 100}
          stroke={QUIET}
          strokeDasharray="3 3"
        />
        <Line
          type="linear"
          dataKey="novelty"
          name="Novelty"
          stroke={INK}
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}

export default function SimulationStudio({
  matterId,
  onGoToParties,
}: {
  matterId: string;
  onGoToParties?: () => void;
}) {
  const queryClient = useQueryClient();
  // A run started here streams live; otherwise the view shows the run picked, or the latest finished one
  const [liveId, setLiveId] = useState<string | null>(null);
  const [chosenId, setChosenId] = useState<string | null>(null);
  const [maxRounds, setMaxRounds] = useState(100);
  const [numBranches, setNumBranches] = useState(1);
  const [autoStop, setAutoStop] = useState(true);
  const [expandedRound, setExpandedRound] = useState<number | null>(null);
  const [showAllThemes, setShowAllThemes] = useState(false);
  const [showAllRounds, setShowAllRounds] = useState(false);
  const [simToDelete, setSimToDelete] = useState<Simulation | null>(null);

  const { data: parties = [] } = useQuery({
    queryKey: ["parties", matterId],
    queryFn: () => api.listParties(matterId),
  });

  const { data: pastSimulations = [], isLoading: simsLoading } = useQuery({
    queryKey: ["simulations", matterId],
    queryFn: () => api.listSimulations(matterId),
  });

  const isLive = liveId !== null;
  const defaultId =
    (
      pastSimulations.find((s) =>
        ["completed", "converged"].includes(s.status),
      ) ?? pastSimulations[0]
    )?.simulation_id ?? null;
  const simId = liveId ?? chosenId ?? defaultId;
  const selectedSim = pastSimulations.find((s) => s.simulation_id === simId);

  // Live stream (only when running a new simulation)
  const stream = useSimulationStream(liveId);

  // A finished run, read back from the tables
  const results = useQuery({
    queryKey: ["simulation-results", simId],
    queryFn: () => api.getResults(simId!),
    enabled: !isLive && !!simId,
  });
  const loaded = useMemo(
    () => (results.data ? resultsToEvents(results.data) : null),
    [results.data],
  );
  const loadError = results.error ? results.error.message : null;

  // Determine active data source
  const events = isLive ? stream.events : (loaded?.events ?? []);
  const connected = isLive ? stream.connected : false;
  const finished = isLive ? stream.finished : !!loaded;
  const noveltyHistory = isLive
    ? stream.noveltyHistory
    : (loaded?.noveltyHistory ?? []);
  const currentRound = isLive ? stream.currentRound : 0;
  const completedBranches = isLive ? stream.completedBranches : 0;
  const configThreshold = (
    selectedSim?.config as Record<string, unknown> | undefined
  )?.novelty_threshold;

  // Fetch similarity data after simulation finishes
  const { data: similarity } = useQuery({
    queryKey: ["similarity", simId],
    queryFn: () => (simId ? api.getSimilarity(simId) : Promise.reject()),
    enabled: !!simId && finished,
  });

  const chooseRun = (id: string) => {
    setLiveId(null);
    setChosenId(id);
    setExpandedRound(null);
  };

  const runMut = useMutation({
    mutationFn: () =>
      api.runSimulation({
        matter_id: matterId,
        config: {
          max_rounds: maxRounds,
          num_branches: numBranches,
          auto_stop: autoStop,
        },
      }),
    onSuccess: (data) => {
      setLiveId(data.simulation_id);
      setChosenId(data.simulation_id);
      setExpandedRound(null);
      queryClient.invalidateQueries({ queryKey: ["simulations", matterId] });
    },
  });

  const cancelMut = useMutation({
    mutationFn: () => (simId ? api.cancelSimulation(simId) : Promise.resolve()),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["simulations", matterId] }),
  });

  const deleteSimMut = useMutation({
    mutationFn: (id: string) => api.deleteSimulation(id),
    onSuccess: (_, id) => {
      if (id === liveId) setLiveId(null);
      if (id === chosenId) setChosenId(null);
      setSimToDelete(null);
      queryClient.setQueryData<Simulation[]>(
        ["simulations", matterId],
        (old = []) => old.filter((s) => s.simulation_id !== id),
      );
      queryClient.removeQueries({ queryKey: ["simulation-results", id] });
      queryClient.invalidateQueries({ queryKey: ["simulations", matterId] });
    },
  });

  // Party identity: fixed order from the parties list, so a party keeps its colour on every screen
  const partyMap = useMemo(() => {
    const m: Record<string, PartyInfo> = {};
    parties.forEach((p, i) => {
      m[p.party_id] = { name: p.name, index: i, color: partyHue(i).line };
    });
    return m;
  }, [parties]);

  const scoringEvents = events.filter((e) => e.event === "scoring_update");
  const argumentEvents = events.filter((e) => e.event === "party_argument");
  const evalEvents = events.filter((e) => e.event === "judge_evaluation");

  // One point per round (the last update wins), keyed by party id
  const probChartData = (() => {
    const byRound = new Map<number, Record<string, number>>();
    for (const e of scoringEvents) {
      const d = e.data as {
        round_number: number;
        win_probabilities: Record<string, number>;
      };
      const row: Record<string, number> = { round: d.round_number };
      for (const [pid, prob] of Object.entries(d.win_probabilities))
        row[pid] = Math.round(prob * 1000) / 10;
      byRound.set(d.round_number, row);
    }
    return [...byRound.values()].sort((a, b) => a.round - b.round);
  })();

  // Per-round novelty (averaged across runs) and the cumulative argument counts behind saturation
  const noveltyByRound = (() => {
    const byRound = new Map<
      number,
      { novelty: number[]; novel: number; total: number }
    >();
    for (const n of noveltyHistory) {
      const r = byRound.get(n.round) ?? { novelty: [], novel: 0, total: 0 };
      r.novelty.push(n.globalNovelty);
      r.novel += n.novelCount || 0;
      r.total += n.totalCount || 0;
      byRound.set(n.round, r);
    }
    const rounds = [...byRound.entries()].sort(([a], [b]) => a - b);
    const out: {
      round: number;
      novelty: number;
      novel: number;
      total: number;
      distinct: number;
      made: number;
    }[] = [];
    let distinct = 0;
    let made = 0;
    for (const [round, v] of rounds) {
      distinct += v.novel;
      made += v.total;
      out.push({
        round,
        novelty:
          Math.round(
            (v.novelty.reduce((s, x) => s + x, 0) / v.novelty.length) * 1000,
          ) / 10,
        novel: v.novel,
        total: v.total,
        distinct,
        made,
      });
    }
    return out;
  })();

  const lastNewRound =
    [...noveltyByRound].reverse().find((r) => r.novel > 0)?.round ?? null;
  const totals = noveltyByRound[noveltyByRound.length - 1];
  const threshold =
    typeof configThreshold === "number" ? configThreshold : 0.15;

  const latestScoring = scoringEvents[scoringEvents.length - 1]
    ?.data as unknown as Scoring | undefined;

  const ranked = latestScoring
    ? Object.entries(latestScoring.win_probabilities)
        .map(([pid, prob]) => ({
          pid,
          prob,
          leverage: latestScoring.leverage_scores[pid] ?? 0,
          std: latestScoring.std_win_probabilities?.[pid],
          party: partyMap[pid] ?? { name: pid, index: 0, color: QUIET },
        }))
        .sort((a, b) => b.prob - a.prob)
    : [];
  const leader = ranked[0];
  const runnerUp = ranked[1];
  const maxLeverage = Math.max(0.0001, ...ranked.map((r) => r.leverage));

  const series = parties
    .filter((p) => probChartData.some((row) => row[p.party_id] !== undefined))
    .map((p) => ({
      key: p.party_id,
      name: p.name,
      color: partyMap[p.party_id].color,
    }));

  // Group arguments by round, then by thread
  const roundGroups: Record<number, typeof argumentEvents> = {};
  for (const ev of argumentEvents) {
    const rn = (ev.data as { round_number: number }).round_number;
    if (!roundGroups[rn]) roundGroups[rn] = [];
    roundGroups[rn].push(ev);
  }
  const roundNumbers = Object.keys(roundGroups)
    .map(Number)
    .sort((a, b) => a - b);

  function buildThreads(args: typeof argumentEvents) {
    const openers = args.filter(
      (a) => (a.data as { response_type: string }).response_type === "opener",
    );
    const responses = args.filter(
      (a) => (a.data as { response_type: string }).response_type !== "opener",
    );
    return openers.map((opener) => {
      const openerId = (opener.data as { argument_id: string }).argument_id;
      const threadResponses = responses.filter(
        (r) =>
          (r.data as { response_to_argument_id: string })
            .response_to_argument_id === openerId,
      );
      return { opener, responses: threadResponses };
    });
  }

  const isRunning = !!(isLive && simId && !finished);
  const converged = events.some((e) => e.event === "converged");
  const clusters = similarity?.clusters ?? [];
  const shownClusters = showAllThemes ? clusters : clusters.slice(0, 6);
  const shownRounds = showAllRounds ? roundNumbers : roundNumbers.slice(-8);

  const overallById = new Map<string, number>();
  for (const e of evalEvents) {
    const d = e.data as { argument_id: string; scores?: { overall?: number } };
    if (typeof d.scores?.overall === "number")
      overallById.set(d.argument_id, d.scores.overall);
  }

  const financials = latestScoring?.financial_positions ?? {};
  const explanations = latestScoring?.leverage_explanations ?? {};

  return (
    <div className="space-y-4 px-4 py-6 md:px-8">
      {simToDelete && (
        <ConfirmDialog
          title="Delete simulation?"
          message={
            <p>
              The {simToDelete.status} run from{" "}
              {formatDateTime(simToDelete.created_at)} (
              {plural(simToDelete.total_rounds, "round")}) will be permanently
              deleted, with its arguments, judge scores and scoring history.
              {["running", "pending"].includes(simToDelete.status) &&
                " It is still running and will be stopped first."}
            </p>
          }
          confirmLabel="Delete Simulation"
          pending={deleteSimMut.isPending}
          error={deleteSimMut.error?.message}
          onConfirm={() => deleteSimMut.mutate(simToDelete.simulation_id)}
          onCancel={() => {
            setSimToDelete(null);
            deleteSimMut.reset();
          }}
        />
      )}

      {/* Toolbar: the run on the left, a new run's settings and the one action on the right */}
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div className="flex min-w-0 items-end gap-2">
          {pastSimulations.length > 0 && (
            <>
              <div className="min-w-0">
                <label
                  htmlFor="run-select"
                  className="mb-1.5 block text-xs text-muted-foreground"
                >
                  Simulation run
                </label>
                <select
                  id="run-select"
                  value={simId ?? ""}
                  disabled={isRunning}
                  onChange={(e) => e.target.value && chooseRun(e.target.value)}
                  className="h-8 max-w-full min-w-0 rounded-md border border-input bg-white pr-8 pl-2.5 text-sm sm:w-[340px]"
                >
                  {!simId && <option value="">Choose a run…</option>}
                  {pastSimulations.map((s) => (
                    <option key={s.simulation_id} value={s.simulation_id}>
                      {runLabel(s)}
                    </option>
                  ))}
                </select>
              </div>
              {selectedSim && (
                <IconButton
                  label="Delete this simulation run"
                  tone="danger"
                  disabled={isRunning}
                  onClick={() => setSimToDelete(selectedSim)}
                >
                  <Trash2 className="h-4 w-4" aria-hidden="true" />
                </IconButton>
              )}
            </>
          )}
        </div>

        <div className="flex flex-wrap items-end gap-3">
          <div>
            <label
              htmlFor="max-rounds"
              className="mb-1.5 block text-xs text-muted-foreground"
            >
              {autoStop ? "Max rounds" : "Rounds"}
            </label>
            <input
              id="max-rounds"
              type="number"
              inputMode="numeric"
              min={1}
              max={10000}
              value={maxRounds}
              onChange={(e) => setMaxRounds(parseInt(e.target.value) || 100)}
              disabled={isRunning}
              className="num h-8 w-20 rounded-md border border-input bg-white px-2 text-right text-sm"
            />
          </div>
          <div>
            <label
              htmlFor="branches"
              className="mb-1.5 block text-xs text-muted-foreground"
            >
              Parallel runs
            </label>
            <input
              id="branches"
              type="number"
              inputMode="numeric"
              min={1}
              max={20}
              value={numBranches}
              onChange={(e) => setNumBranches(parseInt(e.target.value) || 1)}
              disabled={isRunning}
              className="num h-8 w-16 rounded-md border border-input bg-white px-2 text-right text-sm"
            />
          </div>
          <label className="flex h-8 cursor-pointer items-center gap-2 text-sm text-foreground">
            <button
              type="button"
              role="switch"
              aria-checked={autoStop}
              onClick={() => setAutoStop(!autoStop)}
              disabled={isRunning}
              className={cn(
                "relative inline-flex h-5 w-9 flex-none items-center rounded-full transition-colors duration-100 disabled:opacity-50",
                autoStop ? "bg-primary" : "bg-[#C4CCD6]",
              )}
            >
              <span
                className={cn(
                  "inline-block h-4 w-4 rounded-full bg-white transition-transform duration-100",
                  autoStop ? "translate-x-[18px]" : "translate-x-0.5",
                )}
              />
            </button>
            Stop when arguments run out
          </label>
          {isRunning ? (
            <Button
              size="sm"
              variant="danger"
              onClick={() => cancelMut.mutate()}
              disabled={cancelMut.isPending}
            >
              <Square className="h-3.5 w-3.5" aria-hidden="true" />
              Stop Run
            </Button>
          ) : (
            <Button
              size="sm"
              variant="primary"
              onClick={() => runMut.mutate()}
              disabled={runMut.isPending || parties.length < 2}
              title={
                parties.length < 2 ? "Add at least 2 parties first" : undefined
              }
            >
              {runMut.isPending ? (
                <Loader2
                  className="h-3.5 w-3.5 animate-spin"
                  aria-hidden="true"
                />
              ) : (
                <Play className="h-3.5 w-3.5" aria-hidden="true" />
              )}
              Run Simulation
            </Button>
          )}
        </div>
      </div>

      <div aria-live="polite">
        {runMut.isError && (
          <p role="alert" className="text-sm text-destructive">
            The run didn’t start: {runMut.error.message}
          </p>
        )}
        {loadError && (
          <p role="alert" className="text-sm text-destructive">
            Couldn’t load this run: {loadError}
          </p>
        )}
        {isRunning && (
          <p className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" />
            {numBranches > 1 ? (
              <>
                <span className="num">
                  {completedBranches} of {numBranches}
                </span>{" "}
                runs finished
              </>
            ) : (
              <>
                Round <span className="num">{currentRound}</span> of up to{" "}
                <span className="num">{maxRounds}</span>
              </>
            )}
            <span>·</span>
            {connected ? "Live" : "Reconnecting…"}
          </p>
        )}
      </div>

      {/* Nothing to show yet */}
      {!simId &&
        !simsLoading &&
        pastSimulations.length === 0 &&
        !isRunning &&
        (parties.length < 2 ? (
          <EmptyState
            action={
              onGoToParties && (
                <Button size="sm" onClick={onGoToParties}>
                  Go to Parties
                </Button>
              )
            }
          >
            A simulation needs at least 2 parties. This matter has{" "}
            {plural(parties.length, "party", "parties")}.
          </EmptyState>
        ) : (
          <EmptyState>
            No simulations yet. In a run, each party’s agent argues in rounds,
            an LLM judge scores every argument, and the scores become win
            probabilities. With auto-stop on, the run ends once new arguments
            stop appearing.
          </EmptyState>
        ))}

      {simId && !latestScoring && !isRunning && !loadError && (
        <p className="text-sm text-muted-foreground">Loading run…</p>
      )}
      {isRunning && !latestScoring && (
        <EmptyState>
          Waiting for the first round. The agents and the judge each take a few
          seconds per argument.
        </EmptyState>
      )}

      {latestScoring && leader && (
        <>
          {/* The answer: who is ahead, by how much, and whether the run settled */}
          <Card aria-labelledby="outcome-h">
            <div className="px-5 pt-4 pb-1">
              <h2
                id="outcome-h"
                className="text-base font-semibold text-foreground"
              >
                {finished ? "Outcome" : "Standing so far"}
              </h2>
              <p className="mt-0.5 text-sm text-muted-foreground">
                {leader.party.name} leads at{" "}
                <span className="num">{pct(leader.prob)}</span>
                {runnerUp && (
                  <>
                    ,{" "}
                    <span className="num">
                      {((leader.prob - runnerUp.prob) * 100).toFixed(1)}
                    </span>{" "}
                    points ahead of {runnerUp.party.name}
                  </>
                )}
                .{" "}
                {finished &&
                  (converged
                    ? `Converged after ${plural(latestScoring.round_number, "round")}${lastNewRound !== null ? `; the last new argument came in round ${lastNewRound}` : ""}.`
                    : `Stopped after ${plural(latestScoring.round_number, "round")} without converging.`)}
              </p>
            </div>
            <div
              className="grid divide-y divide-rule sm:divide-x sm:divide-y-0"
              style={{
                gridTemplateColumns: `repeat(auto-fit, minmax(180px, 1fr))`,
              }}
            >
              {ranked.map((r, i) => (
                <div key={r.pid} className="px-5 py-4">
                  <p className="flex items-center gap-2 text-sm text-foreground">
                    <PartyDisc
                      name={r.party.name}
                      index={r.party.index}
                      size="sm"
                    />
                    <span className="truncate">{r.party.name}</span>
                  </p>
                  <p
                    className={cn(
                      "num mt-2 text-foreground",
                      i === 0 ? "text-[32px] leading-9" : "text-2xl leading-9",
                    )}
                  >
                    {(r.prob * 100).toFixed(1)}
                    <span className="text-base text-muted-foreground">%</span>
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Win probability
                    {r.std !== undefined && (
                      <>
                        {" "}
                        ·{" "}
                        <span className="num">
                          ±{(r.std * 100).toFixed(1)}
                        </span>{" "}
                        across runs
                      </>
                    )}
                  </p>
                </div>
              ))}
            </div>
          </Card>

          <div className="grid gap-4 xl:grid-cols-3">
            <Card
              className="flex flex-col xl:col-span-2"
              aria-labelledby="winprob-h"
            >
              <CardHeader
                id="winprob-h"
                title="Win probability by round"
                meta="Softmax of each party’s decayed argument score"
                action={
                  <ul
                    className="hidden flex-wrap justify-end gap-x-4 gap-y-1 md:flex"
                    aria-label="Legend"
                  >
                    {series.map((s) => (
                      <li
                        key={s.key}
                        className="flex items-center gap-1.5 text-xs text-muted-foreground"
                      >
                        <LineSwatch color={s.color} />
                        {s.name}
                      </li>
                    ))}
                  </ul>
                }
              />
              <div className="h-[300px] px-3 pb-3 xl:h-auto xl:min-h-[300px] xl:flex-1">
                <WinProbabilityChart data={probChartData} series={series} />
              </div>
            </Card>

            <Card aria-labelledby="saturation-h">
              <CardHeader
                id="saturation-h"
                title="Have the arguments run out?"
                meta={
                  totals ? (
                    <>
                      <span className="num">{totals.distinct}</span> new of{" "}
                      <span className="num">{totals.made}</span> arguments made
                    </>
                  ) : (
                    "No novelty data for this run"
                  )
                }
              />
              {noveltyByRound.length > 0 && (
                <div className="px-3 pb-3">
                  <ul
                    className="flex gap-4 px-2 pb-1 text-xs text-muted-foreground"
                    aria-label="Legend"
                  >
                    <li className="flex items-center gap-1.5">
                      <LineSwatch color={INK} />
                      New arguments
                    </li>
                    <li className="flex items-center gap-1.5">
                      <LineSwatch color={QUIET} />
                      Arguments made
                    </li>
                  </ul>
                  <SaturationChart
                    data={noveltyByRound.map((r) => ({
                      round: r.round,
                      total: r.made,
                      distinct: r.distinct,
                    }))}
                    lastNewRound={lastNewRound}
                  />
                  <ul
                    className="mt-3 flex flex-wrap gap-x-4 gap-y-1 px-2 pb-1 text-xs text-muted-foreground"
                    aria-label="Legend"
                  >
                    <li className="flex items-center gap-1.5">
                      <LineSwatch color={INK} />
                      Novelty per round
                    </li>
                    <li className="flex items-center gap-1.5">
                      <span
                        aria-hidden="true"
                        className="inline-block w-3 border-t border-dashed border-[#90A5B1]"
                      />
                      Stop threshold,{" "}
                      <span className="num">
                        {Math.round(threshold * 100)}%
                      </span>
                    </li>
                  </ul>
                  <NoveltyChart data={noveltyByRound} threshold={threshold} />
                  <p className="mt-2 px-2 text-xs text-muted-foreground">
                    Auto-stop needs novelty under the line and every win
                    probability within <span className="num">0.5</span> points
                    for <span className="num">5</span> rounds in a row.
                  </p>
                </div>
              )}
            </Card>
          </div>

          {/* Positions: leverage, money on the table, and the judge's reading of each party */}
          <Card aria-labelledby="positions-h">
            <CardHeader
              id="positions-h"
              title="Positions"
              meta={`Leverage and stated terms after round ${latestScoring.round_number}`}
            />
            <div className="hidden h-9 grid-cols-[minmax(0,1.4fr)_minmax(0,1.2fr)_repeat(3,minmax(0,1fr))] items-center gap-4 border-y border-border px-5 text-xs text-muted-foreground lg:grid">
              <span>Party</span>
              <span>Leverage</span>
              <span className="text-right">Asking</span>
              <span className="text-right">Offering</span>
              <span className="text-right">Estimated value</span>
            </div>
            <ul className="border-t border-border lg:border-t-0">
              {ranked.map((r, i) => {
                const fin = financials[r.pid];
                const money = (v?: string) => (v && v !== "N/A" ? v : "—");
                return (
                  <li
                    key={r.pid}
                    className="border-b border-rule px-5 py-3.5 last:border-b-0"
                  >
                    <div className="grid grid-cols-2 items-center gap-x-4 gap-y-2 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1.2fr)_repeat(3,minmax(0,1fr))]">
                      <p className="col-span-2 flex min-w-0 items-center gap-2 font-medium text-foreground lg:col-span-1">
                        <PartyDisc
                          name={r.party.name}
                          index={r.party.index}
                          size="sm"
                        />
                        <span className="truncate">{r.party.name}</span>
                      </p>
                      <div className="col-span-2 flex items-center gap-3 lg:col-span-1">
                        <span className="num w-10 text-right text-sm">
                          {Math.round(r.leverage * 100)}
                        </span>
                        <span
                          className="h-1 flex-1 rounded-full bg-active"
                          aria-hidden="true"
                        >
                          <span
                            className="block h-full rounded-full"
                            style={{
                              width: `${(r.leverage / maxLeverage) * 88}%`,
                              backgroundColor: i === 0 ? INK : QUIET,
                            }}
                          />
                        </span>
                      </div>
                      {(
                        [
                          ["Asking", fin?.financial_ask],
                          ["Offering", fin?.financial_offer],
                          ["Estimated value", fin?.estimated_total_value],
                        ] as const
                      ).map(([label, v]) => (
                        <p key={label} className="text-sm lg:text-right">
                          <span className="text-muted-foreground lg:hidden">
                            {label}{" "}
                          </span>
                          <span
                            className={cn(
                              "num",
                              money(v) === "—" && "text-muted-foreground",
                            )}
                          >
                            {money(v)}
                          </span>
                        </p>
                      ))}
                    </div>
                    {explanations[r.pid] && (
                      <p className="mt-2 max-w-[90ch] text-sm text-muted-foreground lg:pl-7">
                        {explanations[r.pid]}
                      </p>
                    )}
                  </li>
                );
              })}
            </ul>
          </Card>

          {/* Argument themes: the distinct positions the run produced */}
          <Card aria-labelledby="themes-h">
            <CardHeader
              id="themes-h"
              title="Argument themes"
              meta={
                similarity
                  ? `${plural(clusters.length, "distinct theme")} across ${plural(argumentEvents.length, "argument")}`
                  : finished
                    ? "Grouping arguments by meaning…"
                    : "Shown when the run finishes"
              }
            />
            {clusters.length > 0 && (
              <>
                <ul className="grid border-t border-border lg:grid-cols-2">
                  {shownClusters.map((cluster: ArgumentCluster) => (
                    <li
                      key={cluster.cluster_id}
                      className="grid grid-cols-[40px_minmax(0,1fr)] gap-3 border-b border-rule px-5 py-3"
                    >
                      <span className="num pt-px text-right text-sm text-muted-foreground">
                        ×{cluster.size}
                      </span>
                      <div>
                        <p className="text-sm text-foreground">
                          {cluster.representative_claim}
                        </p>
                        <p className="mt-1.5 flex flex-wrap gap-1.5">
                          {cluster.parties.map((pName) => {
                            const info = Object.values(partyMap).find(
                              (v) => v.name === pName,
                            );
                            return (
                              <span
                                key={pName}
                                className="flex items-center gap-1 text-xs text-muted-foreground"
                              >
                                <PartyDisc
                                  name={pName}
                                  index={info?.index ?? 0}
                                  size="sm"
                                />
                                {pName}
                              </span>
                            );
                          })}
                        </p>
                      </div>
                    </li>
                  ))}
                </ul>
                {clusters.length > 6 && (
                  <button
                    type="button"
                    onClick={() => setShowAllThemes(!showAllThemes)}
                    className="w-full border-t border-border px-5 py-2.5 text-left text-sm font-medium text-foreground hover:bg-raised"
                  >
                    {showAllThemes
                      ? "Show fewer"
                      : `Show all ${clusters.length} themes`}
                  </button>
                )}
              </>
            )}
          </Card>

          {/* Rounds: one row each; open a round to read its threads and the judge's scores */}
          <Card aria-labelledby="rounds-h">
            <CardHeader
              id="rounds-h"
              title="Rounds"
              meta={
                roundNumbers.length
                  ? `${plural(roundNumbers.length, "round")}, ${plural(argumentEvents.length, "argument")}. Open a round to read it.`
                  : "Arguments appear here as rounds finish"
              }
            />
            {roundNumbers.length > 0 && (
              <>
                <div className="hidden h-9 grid-cols-[56px_80px_48px_72px_220px_minmax(0,1fr)_24px] items-center gap-4 border-y border-border px-5 text-xs text-muted-foreground lg:grid">
                  <span>Round</span>
                  <span className="text-right">Arguments</span>
                  <span className="text-right">New</span>
                  <span className="text-right">Novelty</span>
                  <span>Leading after the round</span>
                  <span>Highest-scored argument</span>
                  <span />
                </div>
                {roundNumbers.length > shownRounds.length && (
                  <button
                    type="button"
                    onClick={() => setShowAllRounds(true)}
                    className="w-full border-b border-rule px-5 py-2.5 text-left text-sm font-medium text-foreground hover:bg-raised"
                  >
                    Show the first{" "}
                    {plural(roundNumbers.length - shownRounds.length, "round")}
                  </button>
                )}
                <ul>
                  {shownRounds.map((roundNum) => {
                    const args = roundGroups[roundNum];
                    const expanded = expandedRound === roundNum;
                    const nov = noveltyByRound.find(
                      (n) => n.round === roundNum,
                    );
                    const probs = probChartData.find(
                      (p) => p.round === roundNum,
                    );
                    const lead = probs
                      ? Object.entries(probs)
                          .filter(([k]) => k !== "round")
                          .sort((a, b) => b[1] - a[1])[0]
                      : undefined;
                    const threads = expanded ? buildThreads(args) : [];
                    const best = args
                      .map((a) => a.data as unknown as ArgumentCardData)
                      .filter(
                        (a) =>
                          a.response_type !== "pass" &&
                          a.argument_id &&
                          overallById.has(a.argument_id),
                      )
                      .sort(
                        (a, b) =>
                          overallById.get(b.argument_id!)! -
                          overallById.get(a.argument_id!)!,
                      )[0];
                    return (
                      <li
                        key={roundNum}
                        className="border-b border-rule last:border-b-0"
                      >
                        <button
                          type="button"
                          aria-expanded={expanded}
                          onClick={() =>
                            setExpandedRound(expanded ? null : roundNum)
                          }
                          className="grid w-full grid-cols-[56px_minmax(0,1fr)_24px] items-center gap-4 px-5 py-2.5 text-left text-sm hover:bg-raised/60 lg:grid-cols-[56px_80px_48px_72px_220px_minmax(0,1fr)_24px]"
                        >
                          <span className="num text-foreground">
                            {roundNum}
                          </span>
                          <span className="num hidden text-right lg:block">
                            {args.length}
                          </span>
                          <span className="num hidden text-right lg:block">
                            {nov ? nov.novel : "—"}
                          </span>
                          <span className="num hidden text-right text-muted-foreground lg:block">
                            {nov ? `${nov.novelty.toFixed(0)}%` : "—"}
                          </span>
                          <span className="flex min-w-0 items-center gap-2 text-muted-foreground">
                            {lead && partyMap[lead[0]] ? (
                              <>
                                <PartyDisc
                                  name={partyMap[lead[0]].name}
                                  index={partyMap[lead[0]].index}
                                  size="sm"
                                />
                                <span className="truncate text-foreground">
                                  {partyMap[lead[0]].name}
                                </span>
                                <span className="num">
                                  {lead[1].toFixed(1)}%
                                </span>
                              </>
                            ) : (
                              <span>{plural(args.length, "argument")}</span>
                            )}
                          </span>
                          <span className="hidden min-w-0 items-center gap-2 lg:flex">
                            {best ? (
                              <>
                                <span className="num flex-none text-muted-foreground">
                                  {Math.round(
                                    overallById.get(best.argument_id!)! * 100,
                                  )}
                                </span>
                                <PartyDisc
                                  name={best.party_name}
                                  index={partyMap[best.party_id]?.index ?? 0}
                                  size="sm"
                                />
                                <span
                                  className="truncate text-foreground"
                                  title={best.main_claim ?? ""}
                                >
                                  {best.main_claim}
                                </span>
                              </>
                            ) : (
                              <span className="text-muted-foreground">—</span>
                            )}
                          </span>
                          {expanded ? (
                            <ChevronDown
                              className="h-4 w-4 text-muted-foreground"
                              aria-hidden="true"
                            />
                          ) : (
                            <ChevronRight
                              className="h-4 w-4 text-muted-foreground"
                              aria-hidden="true"
                            />
                          )}
                        </button>
                        {expanded && (
                          <div className="space-y-4 bg-raised/50 px-5 pt-1 pb-5">
                            {threads.length === 0 && (
                              <p className="text-sm text-muted-foreground">
                                No opening arguments recorded for this round.
                              </p>
                            )}
                            {threads.map((thread, ti) => {
                              const openerData = thread.opener
                                .data as unknown as ArgumentCardData & {
                                thread_topic: string;
                              };
                              const color =
                                partyMap[openerData.party_id]?.color ?? QUIET;
                              return (
                                <div
                                  key={ti}
                                  className="rounded-md border border-border bg-card"
                                >
                                  {openerData.thread_topic && (
                                    <p className="border-b border-rule px-4 py-2 text-xs text-muted-foreground">
                                      {openerData.thread_topic}
                                    </p>
                                  )}
                                  <div
                                    className="border-l-2 px-4 py-3"
                                    style={{ borderLeftColor: color }}
                                  >
                                    <ArgumentCard
                                      data={openerData}
                                      partyMap={partyMap}
                                      evalEvents={evalEvents}
                                    />
                                  </div>
                                  {thread.responses.length > 0 && (
                                    <ul className="border-t border-rule">
                                      {thread.responses.map((resp) => (
                                        <li
                                          key={resp.seq}
                                          className="border-b border-rule py-3 pr-4 pl-8 last:border-b-0"
                                        >
                                          <ArgumentCard
                                            data={
                                              resp.data as unknown as ArgumentCardData
                                            }
                                            partyMap={partyMap}
                                            evalEvents={evalEvents}
                                          />
                                        </li>
                                      ))}
                                    </ul>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        )}
                      </li>
                    );
                  })}
                </ul>
              </>
            )}
          </Card>
        </>
      )}
    </div>
  );
}

interface ArgumentCardData {
  party_name: string;
  party_id: string;
  strategy_type: string | null;
  main_claim: string | null;
  response_type: string;
  argument_id: string | null;
  financial_ask?: string;
  financial_offer?: string;
  estimated_total_value?: string;
}

const SCORE_LABELS: [string, string][] = [
  ["evidence_support", "Evidence"],
  ["logical_coherence", "Logic"],
  ["novelty", "Novelty"],
  ["strategic_impact", "Impact"],
];

function ArgumentCard({
  data,
  partyMap,
  evalEvents,
}: {
  data: ArgumentCardData;
  partyMap: Record<string, PartyInfo>;
  evalEvents: { seq: number; event: string; data: Record<string, unknown> }[];
}) {
  const info = partyMap[data.party_id];
  const verb =
    RESPONSE_TYPE_LABELS[data.response_type] ??
    data.response_type.replace(/_/g, " ");

  const header = (
    <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm">
      <PartyDisc name={data.party_name} index={info?.index ?? 0} size="sm" />
      <span className="font-medium text-foreground">{data.party_name}</span>
      <span className="text-muted-foreground">{verb}</span>
      {data.strategy_type && data.response_type !== "pass" && (
        <Pill>{data.strategy_type}</Pill>
      )}
    </p>
  );

  if (data.response_type === "pass") return header;

  const evalEv = data.argument_id
    ? evalEvents.find(
        (e) =>
          (e.data as { argument_id: string }).argument_id === data.argument_id,
      )
    : undefined;
  const scores = evalEv?.data as
    { scores: Record<string, number>; reasoning: string } | undefined;
  const money = [
    data.financial_ask && data.financial_ask !== "N/A"
      ? `Asks ${data.financial_ask}`
      : null,
    data.financial_offer && data.financial_offer !== "N/A"
      ? `Offers ${data.financial_offer}`
      : null,
  ].filter(Boolean);

  return (
    <div>
      {header}
      <p className="mt-1.5 text-sm leading-6 text-foreground">
        {data.main_claim}
      </p>
      {money.length > 0 && (
        <p className="num mt-1 text-xs text-muted-foreground">
          {money.join(" · ")}
        </p>
      )}
      {scores && (
        <div className="mt-2.5 flex flex-wrap items-baseline gap-x-4 gap-y-1 text-xs text-muted-foreground">
          {SCORE_LABELS.map(([key, label]) => (
            <span key={key}>
              {label}{" "}
              <span className="num text-foreground">
                {Math.round((scores.scores?.[key] || 0) * 100)}
              </span>
            </span>
          ))}
          <span className="font-medium text-foreground">
            Overall{" "}
            <span className="num">
              {Math.round((scores.scores?.overall || 0) * 100)}
            </span>
          </span>
        </div>
      )}
      {scores?.reasoning && (
        <p className="mt-1.5 text-xs leading-5 text-muted-foreground">
          {scores.reasoning}
        </p>
      )}
    </div>
  );
}
