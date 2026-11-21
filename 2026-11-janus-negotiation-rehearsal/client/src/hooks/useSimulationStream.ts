import { useState, useEffect, useCallback, useRef } from "react";

interface SSEEvent {
  seq: number;
  event: string;
  data: Record<string, unknown>;
}

export interface NoveltyPoint {
  round: number;
  branchIndex: number;
  globalNovelty: number;
  perPartyNovelty: Record<string, number>;
  threadCount: number;
  novelCount: number;
  totalCount: number;
}

export function useSimulationStream(simulationId: string | null) {
  const [events, setEvents] = useState<SSEEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const [finished, setFinished] = useState(false);
  const [noveltyHistory, setNoveltyHistory] = useState<NoveltyPoint[]>([]);
  const cursorRef = useRef(-1);
  const eventSourceRef = useRef<EventSource | null>(null);

  const connect = useCallback(() => {
    if (!simulationId) return;

    const url = `/api/simulation/${simulationId}/stream?starting_after=${cursorRef.current}`;
    const es = new EventSource(url);
    eventSourceRef.current = es;

    es.onopen = () => setConnected(true);
    es.onerror = () => {
      setConnected(false);
      es.close();
      // Auto-reconnect after 2s if not finished
      if (!finished) {
        setTimeout(() => connect(), 2000);
      }
    };

    const TERMINAL = ["completed", "converged", "error"];
    const EVENT_TYPES = [
      "simulation_start",
      "round_start",
      "party_argument",
      "judge_evaluation",
      "scoring_update",
      "novelty_update",
      "round_complete",
      "branch_progress",
      "completed",
      "converged",
      "error",
    ];

    for (const type of EVENT_TYPES) {
      es.addEventListener(type, (e: MessageEvent) => {
        const seq = parseInt(e.lastEventId || "0", 10);
        cursorRef.current = seq;
        const data = JSON.parse(e.data);
        setEvents((prev) => [...prev, { seq, event: type, data }]);

        // Track novelty over time
        if (type === "novelty_update") {
          setNoveltyHistory((prev) => [
            ...prev,
            {
              round: data.round_number as number,
              branchIndex: (data.branch_index as number) || 0,
              globalNovelty: data.global_novelty as number,
              perPartyNovelty: data.per_party_novelty as Record<string, number>,
              threadCount: data.thread_count as number,
              novelCount: (data.novel_count as number) || 0,
              totalCount: (data.total_count as number) || 0,
            },
          ]);
        }

        if (TERMINAL.includes(type)) {
          setFinished(true);
          es.close();
        }
      });
    }
  }, [simulationId, finished]);

  useEffect(() => {
    if (simulationId && !finished) {
      connect();
    }
    return () => {
      eventSourceRef.current?.close();
    };
  }, [simulationId, connect, finished]);

  const latestScoring = events
    .filter((e) => e.event === "scoring_update")
    .pop()?.data as
    | {
        win_probabilities: Record<string, number>;
        leverage_scores: Record<string, number>;
        leverage_explanations?: Record<string, string>;
        std_win_probabilities?: Record<string, number>;
        financial_positions?: Record<
          string,
          { financial_ask: string; financial_offer: string; estimated_total_value: string }
        >;
      }
    | undefined;

  const branchEvents = events.filter((e) => e.event === "branch_progress");
  const completedBranches = new Set(
    branchEvents
      .filter((e) => {
        const d = e.data as { branch_index: number };
        return d.branch_index !== undefined;
      })
      .map((e) => (e.data as { branch_index: number }).branch_index),
  ).size;

  const currentRound =
    events.filter((e) => e.event === "round_start").length || 0;

  return {
    events,
    connected,
    finished,
    latestScoring,
    currentRound,
    completedBranches,
    branchEvents,
    noveltyHistory,
  };
}
