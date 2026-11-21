const BASE = "/api";

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const opts: RequestInit = {
    method,
    headers: { "Content-Type": "application/json" },
  };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE}${path}`, opts);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

async function uploadFile<T>(path: string, file: File): Promise<T> {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(`${BASE}${path}`, { method: "POST", body: form });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

// Types
export interface MatterSummary {
  matter_id: string;
  title: string;
  matter_type: string;
  status: string;
  created_at: string;
  party_count: number;
  document_count: number;
}

export interface Matter {
  matter_id: string;
  title: string;
  matter_type: string;
  status: string;
  domain_context: string | null;
  created_at: string;
  updated_at: string | null;
}

export interface Party {
  party_id: string;
  matter_id: string;
  name: string;
  role: string | null;
  objectives: string[];
  constraints: string[];
  risk_tolerance: number;
  batna: string | null;
  stance: string;
  source: string;
  source_document_id: string | null;
  created_at: string;
}

export interface Document {
  document_id: string;
  matter_id: string;
  file_name: string;
  document_type: string | null;
  file_size: number | null;
  volume_path: string | null;
  uploaded_at: string;
  processed_at: string | null;
}

export interface DetectedParty {
  name: string;
  role: string;
  objectives: string[];
  constraints: string[];
  risk_tolerance: number;
  batna: string;
  stance: string;
}

export interface ProcessingResult {
  status: string;
  chunks: number;
  issues: number;
  evidence: number;
  timeline_events: number;
  detected_parties: DetectedParty[];
  created_party_ids: string[];
}

export interface Issue {
  issue_id: string;
  matter_id: string;
  title: string;
  description: string | null;
  category: string | null;
  affected_parties: string[];
  importance: number;
}

export interface EvidenceItem {
  evidence_id: string;
  matter_id: string;
  title: string;
  description: string | null;
  strength: string;
  supporting_party_ids: string[];
}

export interface TimelineEvent {
  event_id: string;
  matter_id: string;
  event_date: string | null;
  title: string;
  description: string | null;
  significance: number;
}

export interface ChatMessage {
  message_id: string;
  matter_id: string;
  role: "user" | "assistant";
  content: string;
  created_at: string;
}

export interface Simulation {
  simulation_id: string;
  matter_id: string;
  status: string;
  config: Record<string, unknown>;
  total_rounds: number;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
}

export interface ScoringSnapshot {
  snapshot_id: string;
  simulation_id: string;
  round_number: number;
  party_scores: Record<string, number>;
  win_probabilities: Record<string, number>;
  leverage_scores: Record<string, number>;
  convergence_metrics: Record<string, number>;
  leverage_explanations?: Record<string, string>;
  financial_positions?: Record<
    string,
    { financial_ask: string; financial_offer: string; estimated_total_value: string }
  >;
}

export interface ArgumentCluster {
  cluster_id: number;
  size: number;
  representative_claim: string;
  parties: string[];
  argument_ids: string[];
}

export interface SimilarityResult {
  arguments: Record<string, unknown>[];
  clusters: ArgumentCluster[];
  novelty_over_time: { round: number; avg_novelty: number }[];
}

export interface SimulationResults {
  simulation_id: string;
  status: string;
  config: Record<string, unknown>;
  total_rounds: number;
  scoring_history: ScoringSnapshot[];
  rounds: Record<string, Record<string, unknown>[]>;
  evaluations: Record<string, unknown>[];
  final_probabilities: Record<string, number>;
  final_leverage: Record<string, number>;
}

// API client
export const api = {
  // Matters
  listMatters: () => request<MatterSummary[]>("GET", "/matters"),
  getMatter: (id: string) => request<Matter>("GET", `/matters/${id}`),
  createMatter: (data: {
    title: string;
    matter_type?: string;
    domain_context?: string;
  }) => request<Matter>("POST", "/matters", data),
  updateMatter: (id: string, data: Record<string, unknown>) =>
    request<Matter>("PATCH", `/matters/${id}`, data),
  deleteMatter: (id: string) => request<void>("DELETE", `/matters/${id}`),

  // Parties
  listParties: (matterId: string) =>
    request<Party[]>("GET", `/matters/${matterId}/parties`),
  createParty: (matterId: string, data: Record<string, unknown>) =>
    request<Party>("POST", `/matters/${matterId}/parties`, data),
  updateParty: (
    matterId: string,
    partyId: string,
    data: Record<string, unknown>,
  ) => request<Party>("PATCH", `/matters/${matterId}/parties/${partyId}`, data),
  deleteParty: (matterId: string, partyId: string) =>
    request<void>("DELETE", `/matters/${matterId}/parties/${partyId}`),

  // Documents
  listDocuments: (matterId: string) =>
    request<Document[]>("GET", `/documents/${matterId}`),
  uploadDocument: (matterId: string, file: File) =>
    uploadFile<Document>(`/documents/${matterId}/upload`, file),
  processDocument: (matterId: string, docId: string) =>
    request<{ status: string; step?: string }>(
      "POST",
      `/documents/${matterId}/${docId}/process`,
    ),
  getProcessingStatus: (matterId: string, docId: string) =>
    request<{ status: string; step?: string; result?: ProcessingResult; error?: string }>(
      "GET",
      `/documents/${matterId}/${docId}/process/status`,
    ),
  deleteDocument: (matterId: string, docId: string) =>
    request<void>("DELETE", `/documents/${matterId}/${docId}`),

  // Extraction
  listIssues: (matterId: string) =>
    request<Issue[]>("GET", `/extraction/${matterId}/issues`),
  createIssue: (matterId: string, data: Record<string, unknown>) =>
    request<Issue>("POST", `/extraction/${matterId}/issues`, data),
  deleteIssue: (matterId: string, issueId: string) =>
    request<void>("DELETE", `/extraction/${matterId}/issues/${issueId}`),
  listEvidence: (matterId: string) =>
    request<EvidenceItem[]>("GET", `/extraction/${matterId}/evidence`),
  createEvidence: (matterId: string, data: Record<string, unknown>) =>
    request<EvidenceItem>("POST", `/extraction/${matterId}/evidence`, data),
  deleteEvidence: (matterId: string, evidenceId: string) =>
    request<void>("DELETE", `/extraction/${matterId}/evidence/${evidenceId}`),
  listTimeline: (matterId: string) =>
    request<TimelineEvent[]>("GET", `/extraction/${matterId}/timeline`),
  createTimelineEvent: (matterId: string, data: Record<string, unknown>) =>
    request<TimelineEvent>("POST", `/extraction/${matterId}/timeline`, data),
  deleteTimelineEvent: (matterId: string, eventId: string) =>
    request<void>("DELETE", `/extraction/${matterId}/timeline/${eventId}`),

  // Chat
  getChatHistory: (matterId: string) =>
    request<ChatMessage[]>("GET", `/chat/${matterId}/history`),

  // Simulation
  listSimulations: (matterId: string) =>
    request<Simulation[]>("GET", `/simulation/list?matter_id=${matterId}`),
  runSimulation: (data: { matter_id: string; config?: Record<string, unknown> }) =>
    request<{ simulation_id: string }>("POST", "/simulation/run", data),
  getSimulation: (simId: string) =>
    request<Simulation>("GET", `/simulation/${simId}`),
  getScoring: (simId: string) =>
    request<ScoringSnapshot[]>("GET", `/simulation/${simId}/scoring`),
  getRoundArguments: (simId: string, round: number) =>
    request<Record<string, unknown>[]>(
      "GET",
      `/simulation/${simId}/rounds/${round}/arguments`,
    ),
  cancelSimulation: (simId: string) =>
    request<void>("POST", `/simulation/${simId}/cancel`),
  deleteSimulation: (simId: string) =>
    request<void>("DELETE", `/simulation/${simId}`),
  getSimilarity: (simId: string) =>
    request<SimilarityResult>("GET", `/simulation/${simId}/similarity`),
  getResults: (simId: string) =>
    request<SimulationResults>("GET", `/simulation/${simId}/results`),
};
