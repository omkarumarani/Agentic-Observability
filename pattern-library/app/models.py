"""Pattern Library — Pydantic v2 request/response models."""
from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────
# Source reference (embedded in pattern.source_references JSONB)
# ─────────────────────────────────────────────────────────────

class SourceReference(BaseModel):
    title: str
    url: str
    source: str  # github | stackoverflow | reddit | hackernews | blog | docs


# ─────────────────────────────────────────────────────────────
# Pattern CRUD
# ─────────────────────────────────────────────────────────────

class PatternCreate(BaseModel):
    name: str
    description: str
    environment: str = "any"
    impacted_layers: list[str] = []
    recurrence_score: float = Field(0.5, ge=0.0, le=1.0)
    severity: str = "medium"        # critical | high | medium | low
    automation_readiness: str = "manual"  # safe | risky | manual
    oss_contribution_angle: Optional[str] = None
    source_references: list[SourceReference] = []
    confidence: float = Field(0.5, ge=0.0, le=1.0)


class PatternUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    environment: Optional[str] = None
    severity: Optional[str] = None
    automation_readiness: Optional[str] = None
    oss_contribution_angle: Optional[str] = None
    recurrence_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)


class PatternListItem(BaseModel):
    id: str
    name: str
    description: str
    severity: str
    environment: str
    impacted_layers: list[str]
    recurrence_score: float
    confidence: float
    automation_readiness: str
    evidence_count: int
    version: int
    deprecated: bool
    created_at: str
    updated_at: str


class PatternDetail(PatternListItem):
    oss_contribution_angle: Optional[str]
    source_references: Any
    signals: list[dict]
    fixes: list[dict]
    validations: list[dict]


# ─────────────────────────────────────────────────────────────
# Signals, Fixes, Validations
# ─────────────────────────────────────────────────────────────

class SignalCreate(BaseModel):
    signal_type: str            # metric | log | trace | alert
    name: str
    description: Optional[str] = None
    query_template: Optional[str] = None
    threshold_operator: Optional[str] = None  # > | < | >= | <= | =
    threshold_value: Optional[float] = None
    severity: str = "medium"
    weight: float = Field(1.0, ge=0.0, le=2.0)


class FixCreate(BaseModel):
    title: str
    description: Optional[str] = None
    fix_type: str               # config_change | restart | scale | alert_rule | playbook | runbook
    automation_level: str = "manual"    # autonomous | approval_required | manual
    content: Optional[str] = None
    risk_level: str = "medium"
    estimated_mttr_seconds: Optional[int] = None
    requires_restart: bool = False


class ValidationCreate(BaseModel):
    reproducer_scenario: Optional[str] = None
    reproduction_steps: Optional[str] = None
    detected: bool
    detection_latency_seconds: Optional[float] = None
    false_positive_rate: Optional[float] = None
    notes: Optional[str] = None
    validator: str = "manual"


# ─────────────────────────────────────────────────────────────
# Search & Matching
# ─────────────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    query: str
    severity: Optional[str] = None
    limit: int = Field(10, ge=1, le=50)


class SearchResult(BaseModel):
    id: str
    name: str
    description: str
    severity: str
    environment: str
    recurrence_score: float
    confidence: float
    similarity: float


class IncidentMatchRequest(BaseModel):
    """
    Hybrid pattern matching request.
    - signals: dict of live telemetry values { metric_name: float_value }
      e.g. {"cpu_usage": 0.92, "latency_p99": 1.5, "error_rate": 0.02}
    - incident_text: free text describing the incident (optional, used for embedding)
    - agent: "compute" or "storage"
    - top_k: number of patterns to return
    """
    signals: dict[str, float] = {}
    incident_text: Optional[str] = None
    agent: str = "compute"
    alert_name: Optional[str] = None
    top_k: int = Field(5, ge=1, le=20)


class IncidentMatchResult(BaseModel):
    pattern_id: str
    pattern_name: str
    severity: str
    environment: str
    rule_score: float           # 0.0–1.0; fraction of thresholds satisfied
    vector_similarity: float    # 0.0–1.0 cosine similarity
    combined_score: float       # 0.6*rule + 0.4*vector (or 1.0*rule if no embedding)
    confidence: float           # pattern's historical confidence
    recurrence_score: float     # how often this pattern recurs in the wild


# ─────────────────────────────────────────────────────────────
# Agent Assessments
# ─────────────────────────────────────────────────────────────

class AssessmentCreate(BaseModel):
    session_id: str
    agent: str                  # compute | storage
    alert_name: Optional[str] = None
    matched_patterns: list[dict] = []   # [{pattern_id, score, rank}]
    risk_score: Optional[float] = None
    recommendation: Optional[str] = None
    incident_summary: Optional[str] = None  # used for embedding generation


class AssessmentOutcome(BaseModel):
    outcome: str    # resolved | escalated | false_positive | unknown
