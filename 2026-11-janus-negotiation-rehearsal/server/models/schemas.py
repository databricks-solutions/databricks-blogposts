from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# --- Enums ---


class MatterType(str, Enum):
    negotiation = "negotiation"
    dispute = "dispute"
    mediation = "mediation"
    arbitration = "arbitration"
    deal = "deal"
    custom = "custom"


class MatterStatus(str, Enum):
    draft = "draft"
    active = "active"
    simulating = "simulating"
    completed = "completed"
    archived = "archived"


class PartyStance(str, Enum):
    aggressive = "aggressive"
    neutral = "neutral"
    cooperative = "cooperative"
    defensive = "defensive"


class DocumentType(str, Enum):
    contract = "contract"
    correspondence = "correspondence"
    evidence = "evidence"
    legal_filing = "legal_filing"
    report = "report"
    other = "other"


class SimulationStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    converged = "converged"
    cancelled = "cancelled"
    failed = "failed"


class EvidenceStrength(str, Enum):
    strong = "strong"
    moderate = "moderate"
    weak = "weak"
    circumstantial = "circumstantial"


# --- Matter ---


class CreateMatterRequest(BaseModel):
    title: str
    matter_type: MatterType = MatterType.negotiation
    domain_context: str = ""


class UpdateMatterRequest(BaseModel):
    title: str | None = None
    matter_type: MatterType | None = None
    status: MatterStatus | None = None
    domain_context: str | None = None


class Matter(BaseModel):
    matter_id: str
    title: str
    matter_type: str
    status: str
    domain_context: str | None = None
    created_at: datetime
    updated_at: datetime | None = None


class MatterSummary(BaseModel):
    matter_id: str
    title: str
    matter_type: str
    status: str
    created_at: datetime
    party_count: int = 0
    document_count: int = 0


# --- Party ---


class CreatePartyRequest(BaseModel):
    name: str
    role: str = ""
    objectives: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    risk_tolerance: float = Field(default=0.5, ge=0.0, le=1.0)
    batna: str = ""
    stance: PartyStance = PartyStance.neutral


class UpdatePartyRequest(BaseModel):
    name: str | None = None
    role: str | None = None
    objectives: list[str] | None = None
    constraints: list[str] | None = None
    risk_tolerance: float | None = Field(default=None, ge=0.0, le=1.0)
    batna: str | None = None
    stance: PartyStance | None = None


class Party(BaseModel):
    party_id: str
    matter_id: str
    name: str
    role: str | None = None
    objectives: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    risk_tolerance: float = 0.5
    batna: str | None = None
    stance: str = "neutral"
    source: str = "manual"
    source_document_id: str | None = None
    created_at: datetime


# --- Document ---


class Document(BaseModel):
    document_id: str
    matter_id: str
    file_name: str
    document_type: str | None = None
    content_type: str | None = None
    file_size: int | None = None
    page_count: int | None = None
    volume_path: str | None = None
    uploaded_at: datetime
    processed_at: datetime | None = None


class DetectedParty(BaseModel):
    name: str
    role: str = ""
    objectives: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    risk_tolerance: float = 0.5
    batna: str = ""
    stance: str = "neutral"


class DocumentProcessingResult(BaseModel):
    status: str
    chunks: int = 0
    issues: int = 0
    evidence: int = 0
    timeline_events: int = 0
    detected_parties: list[DetectedParty] = Field(default_factory=list)
    created_party_ids: list[str] = Field(default_factory=list)


# --- Extraction entities ---


class Issue(BaseModel):
    issue_id: str
    matter_id: str
    title: str
    description: str | None = None
    category: str | None = None
    affected_parties: list[str] = Field(default_factory=list)
    importance: float = 0.5
    created_at: datetime


class CreateIssueRequest(BaseModel):
    title: str
    description: str = ""
    category: str = ""
    affected_parties: list[str] = Field(default_factory=list)
    importance: float = 0.5


class UpdateIssueRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    category: str | None = None
    affected_parties: list[str] | None = None
    importance: float | None = None


class EvidenceItem(BaseModel):
    evidence_id: str
    matter_id: str
    title: str
    description: str | None = None
    source_document_id: str | None = None
    strength: str = "moderate"
    supporting_party_ids: list[str] = Field(default_factory=list)
    created_at: datetime


class CreateEvidenceRequest(BaseModel):
    title: str
    description: str = ""
    source_document_id: str | None = None
    strength: EvidenceStrength = EvidenceStrength.moderate
    supporting_party_ids: list[str] = Field(default_factory=list)


class UpdateEvidenceRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    strength: EvidenceStrength | None = None
    supporting_party_ids: list[str] | None = None


class TimelineEvent(BaseModel):
    event_id: str
    matter_id: str
    event_date: str | None = None
    title: str
    description: str | None = None
    significance: float = 0.5
    created_at: datetime


class CreateTimelineEventRequest(BaseModel):
    event_date: str = ""
    title: str
    description: str = ""
    significance: float = 0.5


class UpdateTimelineEventRequest(BaseModel):
    event_date: str | None = None
    title: str | None = None
    description: str | None = None
    significance: float | None = None


# --- Simulation ---


class SimulationConfig(BaseModel):
    max_rounds: int = Field(default=100, ge=1, le=10000)
    num_branches: int = Field(default=1, ge=1, le=20)
    auto_stop: bool = Field(default=True)
    novelty_threshold: float = Field(default=0.15, ge=0.0, le=1.0)
    probability_movement_threshold: float = Field(default=0.005, ge=0.0, le=1.0)
    convergence_rounds: int = Field(default=5, ge=1, le=20)
    agent_model: str = ""
    judge_model: str = ""


class RunSimulationRequest(BaseModel):
    matter_id: str
    config: SimulationConfig = Field(default_factory=SimulationConfig)


class Simulation(BaseModel):
    simulation_id: str
    matter_id: str
    status: str
    config: dict[str, Any] = Field(default_factory=dict)
    total_rounds: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime


class Argument(BaseModel):
    argument_id: str
    round_id: str
    simulation_id: str
    party_id: str
    content: str
    strategy_type: str | None = None
    evidence_ids: list[str] = Field(default_factory=list)
    novelty_score: float | None = None
    response_to_argument_id: str | None = None
    response_type: str | None = None  # opener, counter_argument, supporting_argument, additional_info, concession, pass
    created_at: datetime


class JudgeEvaluation(BaseModel):
    eval_id: str
    argument_id: str
    evidence_support: float
    logical_coherence: float
    novelty: float
    strategic_impact: float
    overall_score: float
    reasoning: str | None = None
    created_at: datetime


class ScoringSnapshot(BaseModel):
    snapshot_id: str
    simulation_id: str
    round_number: int
    party_scores: dict[str, float] = Field(default_factory=dict)
    win_probabilities: dict[str, float] = Field(default_factory=dict)
    leverage_scores: dict[str, float] = Field(default_factory=dict)
    convergence_metrics: dict[str, Any] = Field(default_factory=dict)
    leverage_explanations: dict[str, str] = Field(default_factory=dict)
    financial_positions: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


# --- Chat ---


class ChatMessage(BaseModel):
    message_id: str
    matter_id: str
    role: str
    content: str
    created_at: datetime


class ChatRequest(BaseModel):
    message: str


# --- Output ---


class GenerateOutputRequest(BaseModel):
    matter_id: str
    simulation_id: str
    output_type: str = "memo"
    instructions: str = ""


# --- Health ---


class HealthResponse(BaseModel):
    status: str
    service: str
