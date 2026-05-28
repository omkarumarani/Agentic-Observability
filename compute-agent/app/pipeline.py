"""
aiops-bridge/app/pipeline.py
────────────────────────────────────────────────────────────────
Agent-to-Agent Pipeline — HTTP endpoints for the xyOps Workflow canvas.

Architecture
────────────
Each endpoint is one "Agent node" in the xyOps Scheduler → Workflows
visual canvas.  When you click Run on the "AIOps Agent Pipeline" workflow,
xyOps fires each httpplug node in sequence.  You watch:
  • Each node turn green (success) or red (failure) on the canvas
  • Step-by-step [>>]/[OK]/[!!] comments appear live on the incident ticket

Pipeline session design
───────────────────────
All agent nodes share a session keyed by `session_id` (defaults to
`service_name`).  Node 1 creates the session + skeleton ticket.
Nodes 2–6 retrieve the session and add their results.  This way every
httpplug node can send a simple fixed JSON body without needing to
chain output from a previous node.

Endpoints (each = one xyOps workflow node)
──────────────────────────────────────────
  POST /pipeline/start          Agent 1 — creates session + skeleton ticket
  POST /pipeline/agent/logs     Agent 2 — Loki log fetcher
  POST /pipeline/agent/metrics  Agent 3 — Prometheus analyst
  POST /pipeline/agent/analyze  Agent 4 — Claude AI analyst
  POST /pipeline/agent/ticket   Agent 5 — Incident scribe (enriches body)
  POST /pipeline/agent/approval Agent 6 — Approval gateway

GET  /pipeline/session/{id}    — inspect current session state (debug)
────────────────────────────────────────────────────────────────
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .ai_analyst import (
    AI_ENABLED,
    build_enriched_ticket_body,
    fetch_loki_logs,
    fetch_prometheus_context,
    get_notify_list,
)
from .approval_workflow import request_approval, execute_autonomous
from .xyops_client import TOTAL_STEPS, post_step_comment
from obs_intelligence.feature_extractor import extract_features as _extract_features
from obs_intelligence.scenario_correlator import (
    load_catalog as _load_catalog,
    match_scenarios as _match_scenarios,
    match_best as _match_best,
)
from obs_intelligence.risk_scorer import score_risk as _score_risk
from obs_intelligence.recommender import recommend as _recommend
from obs_intelligence.evidence_builder import (
    build_evidence as _build_evidence,
    evidence_lines as _evidence_lines,
)
from obs_intelligence.llm_enricher import enrich as _llm_enrich
from obs_intelligence.confidence_model import build_uncertainty_profile as _build_uncertainty
from obs_intelligence.multi_pattern_correlator import correlate_patterns as _correlate_patterns
from obs_intelligence.timeseries_reasoner import build_timeseries_context as _build_ts_context
from . import autonomy_rules as _autonomy_rules
from . import pattern_client as _pattern_client
from .autonomy_engine import check_autonomy_for_new_service

logger = logging.getLogger("aiops-bridge.pipeline")

XYOPS_URL: str = os.getenv("XYOPS_URL", "http://xyops:5522")
REQUIRE_APPROVAL: bool = os.getenv("REQUIRE_APPROVAL", "true").lower() != "false"
APPROVAL_SEVERITY_THRESHOLD: set[str] = {"warning", "critical"}
SESSION_TTL_SECONDS: int = 3600  # sessions expire after 1 hour
_OBS_INTELLIGENCE_URL: str = os.getenv("OBS_INTELLIGENCE_URL", "http://obs-intelligence:9100")
# Seconds each workflow node visibly "runs" before completing — lets you watch
# the xyOps canvas step by step.  Set to 0 to disable.
WORKFLOW_STEP_DELAY: int = int(os.getenv("WORKFLOW_STEP_DELAY_SECONDS", "5"))


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline session store
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class PipelineSession:
    session_id: str
    service_name: str
    alert_name: str
    severity: str
    summary: str
    description: str
    dashboard_url: str
    starts_at: str
    bridge_trace_id: str = ""
    created_at: float = field(default_factory=time.time)

    # Skeleton ticket (created in /start so comments can land immediately)
    ticket_id: str = ""
    ticket_num: int = 0

    # Accumulated per-agent results
    logs: str = ""
    metrics: dict = field(default_factory=dict)
    analysis: dict = field(default_factory=dict)

    # Risk + evidence (populated by agent_analyze)
    risk_score: float = 0.0
    risk_level: str = "unknown"
    evidence_lines_data: list = field(default_factory=list)

    # Approval gate
    approval_id: str = ""
    approval_ticket_id: str = ""
    approval_ticket_num: int = 0
    autonomy_decision: str = "APPROVAL_GATED"
    outcome: str = "pending"
    completed_at: float | None = None
    mttr_seconds: float = 0.0

    # Pattern Library matches (populated by agent_analyze, Section F)
    matched_patterns: list = field(default_factory=list)

    # Current pipeline stage (for debugging/inspection)
    stage: str = "created"
    # Wall-clock seconds from session creation to each stage completion
    stage_durations: dict = field(default_factory=dict)


# {session_id: PipelineSession}
_sessions: dict[str, PipelineSession] = {}


# ═══════════════════════════════════════════════════════════════════════════════
# SQLite persistence — survives container restarts
# ═══════════════════════════════════════════════════════════════════════════════

_DB_PATH = os.getenv("PIPELINE_DB_PATH", "/data/pipeline.db")


def _db_conn() -> sqlite3.Connection:
    import pathlib
    pathlib.Path(_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_schema() -> None:
    with _db_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_sessions (
                session_id        TEXT PRIMARY KEY,
                service_name      TEXT,
                alert_name        TEXT,
                severity          TEXT,
                stage             TEXT,
                risk_score        REAL,
                risk_level        TEXT,
                autonomy_decision TEXT,
                analysis_json     TEXT,
                created_at        REAL,
                completed_at      REAL,
                mttr_seconds      REAL DEFAULT 0,
                outcome           TEXT DEFAULT 'pending'
            )
        """)
        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(pipeline_sessions)").fetchall()
        }
        if "mttr_seconds" not in cols:
            conn.execute(
                "ALTER TABLE pipeline_sessions ADD COLUMN mttr_seconds REAL DEFAULT 0"
            )


def _persist_session(session: PipelineSession) -> None:
    """UPSERT the current session state into pipeline_sessions."""
    try:
        analysis_json = json.dumps(session.analysis)
        with _db_conn() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_sessions
                    (session_id, service_name, alert_name, severity, stage,
                     risk_score, risk_level, autonomy_decision, analysis_json,
                     created_at, completed_at, mttr_seconds, outcome)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    stage             = excluded.stage,
                    risk_score        = excluded.risk_score,
                    risk_level        = excluded.risk_level,
                    autonomy_decision = excluded.autonomy_decision,
                    analysis_json     = excluded.analysis_json,
                    completed_at      = excluded.completed_at,
                    mttr_seconds      = excluded.mttr_seconds,
                    outcome           = excluded.outcome
                """,
                (
                    session.session_id,
                    session.service_name,
                    session.alert_name,
                    session.severity,
                    session.stage,
                    session.risk_score,
                    session.risk_level,
                    session.autonomy_decision,
                    analysis_json,
                    session.created_at,
                    session.completed_at,
                    session.mttr_seconds,
                    session.outcome,
                ),
            )
    except Exception as exc:
        logger.warning("_persist_session failed: %s", exc)


def _load_sessions_from_db() -> None:
    """Rebuild _sessions from DB rows created in the last 24 hours."""
    cutoff = time.time() - 86400
    try:
        with _db_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_sessions WHERE created_at > ?", (cutoff,)
            ).fetchall()
        for row in rows:
            if row["session_id"] in _sessions:
                continue   # already in memory (shouldn't happen at import time)
            try:
                analysis = json.loads(row["analysis_json"] or "{}")
            except Exception:
                analysis = {}
            session = PipelineSession(
                session_id   = row["session_id"],
                service_name = row["service_name"] or "",
                alert_name   = row["alert_name"] or "",
                severity     = row["severity"] or "warning",
                summary      = analysis.get("summary", ""),
                description  = "",
                dashboard_url= "http://grafana:3000",
                starts_at    = "",
                created_at   = row["created_at"] or time.time(),
                stage        = row["stage"] or "created",
                risk_score   = row["risk_score"] or 0.0,
                risk_level   = row["risk_level"] or "unknown",
                autonomy_decision=row["autonomy_decision"] or "APPROVAL_GATED",
                outcome      = row["outcome"] or "pending",
                completed_at = row["completed_at"],
                mttr_seconds = row["mttr_seconds"] or 0.0,
                analysis     = analysis,
            )
            _sessions[session.session_id] = session
        if rows:
            logger.info("Restored %d pipeline sessions from DB", len(rows))
    except Exception as exc:
        logger.warning("_load_sessions_from_db failed: %s", exc)


_ensure_schema()
_load_sessions_from_db()


# ── Module-level client refs — set by init_pipeline() on bridge startup ────────
_http: httpx.AsyncClient | None = None
_xyops_post_fn = None


def init_pipeline(http: httpx.AsyncClient, xyops_post_fn) -> None:
    """
    Called once from aiops-bridge main.py startup after _http is ready.
    Gives pipeline agents access to the shared httpx client and the
    xyOps POST helper (which already carries auth headers + base URL).
    """
    global _http, _xyops_post_fn
    _http = http
    _xyops_post_fn = xyops_post_fn
    logger.info("Pipeline agents initialized")


async def _post(path: str, body: dict) -> dict:
    """Delegate to main.py's _xyops_post (shared httpx client + auth headers)."""
    if _xyops_post_fn:
        return await _xyops_post_fn(path, body)
    return {"error": "pipeline not initialized — init_pipeline() not yet called"}


def _require_session(session_id: str) -> PipelineSession:
    """Load session or raise 404 if not found.

    Special alias ``default`` returns the most recently created session
    so the Command Center can poll without knowing the exact session_id.
    """
    if session_id == "default":
        if _sessions:
            return max(_sessions.values(), key=lambda s: s.created_at)
        # In-memory evicted (GC) or fresh restart — fall back to DB most-recent row
        try:
            with _db_conn() as conn:
                row = conn.execute(
                    "SELECT * FROM pipeline_sessions ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            if row:
                analysis = json.loads(row["analysis_json"] or "{}")
                return PipelineSession(
                    session_id        = row["session_id"],
                    service_name      = row["service_name"] or "",
                    alert_name        = row["alert_name"] or "",
                    severity          = row["severity"] or "warning",
                    summary           = analysis.get("summary", ""),
                    description       = "",
                    dashboard_url     = "http://grafana:3000",
                    starts_at         = "",
                    created_at        = row["created_at"] or time.time(),
                    stage             = row["stage"] or "complete",
                    risk_score        = row["risk_score"] or 0.0,
                    risk_level        = row["risk_level"] or "unknown",
                    autonomy_decision = row["autonomy_decision"] or "APPROVAL_GATED",
                    outcome           = row["outcome"] or "pending",
                    completed_at      = row["completed_at"],
                    mttr_seconds      = row["mttr_seconds"] or 0.0,
                    analysis          = analysis,
                )
        except Exception as exc:
            logger.warning("_require_session DB fallback failed: %s", exc)
        raise HTTPException(
            status_code=404,
            detail="No pipeline sessions exist yet. Trigger an alert first.",
        )
    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No pipeline session '{session_id}'. "
                "Run Agent 1 (POST /pipeline/start) first."
            ),
        )
    return session


def _gc_sessions() -> None:
    """Evict sessions older than SESSION_TTL_SECONDS."""
    cutoff = time.time() - SESSION_TTL_SECONDS
    expired = [k for k, s in _sessions.items() if s.created_at < cutoff]
    for k in expired:
        del _sessions[k]
        logger.info("Evicted expired pipeline session: %s", k)


def _compute_trust_metrics(service_name: str, action_type: str = "") -> dict:
    from .approval_history import history_store
    from .tier_registry import get_service_tier, get_tier_policy

    tier = get_service_tier(service_name)
    policy = get_tier_policy(tier)
    records = [r for r in history_store._records if r.service_name == service_name]

    approvals_recorded = sum(1 for r in records if r.decision in ("approved", "autonomous"))
    executed = [r for r in records if r.execution_outcome in ("success", "failure")]
    successes = sum(1 for r in executed if r.execution_outcome == "success")
    success_rate = (successes / len(executed)) if executed else 0.0
    approvals_needed = max(policy.min_approvals_for_autonomy - approvals_recorded, 0)
    success_rate_needed = policy.min_success_rate
    if approvals_needed == 0 and success_rate >= success_rate_needed:
        path_to_next_tier = f"Trust threshold met for {tier.value.upper()}→AUTO"
    else:
        path_to_next_tier = (
            f"{approvals_needed} more approvals + {success_rate_needed:.0%} success rate "
            f"to reach {tier.value.upper()}→AUTO"
        )

    return {
        "approvals_recorded": approvals_recorded,
        "success_rate": round(success_rate, 3),
        "successful_runs": successes,
        "executed_runs": len(executed),
        "next_tier": {
            "name": f"{tier.value}_auto",
            "approvals_needed": approvals_needed,
            "success_rate_needed": round(success_rate_needed, 2),
        },
        "path_to_next_tier": path_to_next_tier,
    }


def update_pipeline_session_state(
    session_id: str,
    *,
    stage: str | None = None,
    autonomy_decision: str | None = None,
    outcome: str | None = None,
    completed: bool = False,
) -> None:
    session = _sessions.get(session_id)
    if session:
        if stage:
            session.stage = stage
            session.stage_durations[stage] = round(time.time() - session.created_at, 2)
        if autonomy_decision:
            session.autonomy_decision = autonomy_decision
        if outcome:
            session.outcome = outcome
        if completed:
            session.completed_at = time.time()
            session.mttr_seconds = round(session.completed_at - session.created_at, 1)
        _persist_session(session)
        return

    try:
        with _db_conn() as conn:
            row = conn.execute(
                "SELECT created_at, stage FROM pipeline_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if not row:
                return
            created_at = row["created_at"] or time.time()
            next_stage = stage or row["stage"] or "created"
            next_completed_at = time.time() if completed else None
            next_mttr = round(next_completed_at - created_at, 1) if next_completed_at else 0.0
            updates: list[str] = []
            params: list[Any] = []
            if stage:
                updates.append("stage = ?")
                params.append(next_stage)
            if autonomy_decision:
                updates.append("autonomy_decision = ?")
                params.append(autonomy_decision)
            if outcome:
                updates.append("outcome = ?")
                params.append(outcome)
            if completed:
                updates.append("completed_at = ?")
                updates.append("mttr_seconds = ?")
                params.extend([next_completed_at, next_mttr])
            if updates:
                params.append(session_id)
                conn.execute(
                    f"UPDATE pipeline_sessions SET {', '.join(updates)} WHERE session_id = ?",
                    tuple(params),
                )
    except Exception as exc:
        logger.warning("update_pipeline_session_state failed: %s", exc)


async def _notify_coordinator(session: PipelineSession) -> None:
    """Fire-and-forget: tell obs-intelligence about this incident.

    v2: includes a ``signals`` snapshot so obs-intelligence can run the
    CrossDomainCorrelator without an extra Prometheus round-trip.  When a
    simultaneous storage incident is detected a ``unified_assessment`` is
    returned and we post it as a ticket comment so SREs see the cross-domain
    causal chain immediately.
    """
    try:
        m = session.metrics or {}
        payload = {
            "domain":       "compute",
            "service_name": session.service_name,
            "alert_name":   session.alert_name,
            "risk_score":   session.risk_score,
            "scenario_id":  session.analysis.get("scenario_id", ""),
            "run_id":       session.bridge_trace_id,
            "signals": {
                "error_rate":      _safe_float(m.get("error_rate_pct"), divisor=100.0),
                "latency_p99_ms":  _safe_float(m.get("p99_latency_ms")),
                "cpu_usage_pct":   _safe_float(m.get("cpu_pct"), divisor=100.0),
                "risk_level":      session.risk_level,
            },
        }
        if _http is None:
            return
        resp = await _http.post(
            f"{_OBS_INTELLIGENCE_URL}/intelligence/record-incident",
            json=payload,
            timeout=3.0,
        )
        data = resp.json()
        unified = data.get("unified_assessment")
        if unified and session.ticket_id:
            await _post_cross_domain_comment(session, unified)
    except Exception:
        pass  # coordinator is best-effort


def _safe_float(value, *, divisor: float = 1.0) -> float:
    """Return float(value) / divisor, or 0.0 on any error."""
    try:
        return float(value) / divisor
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


async def _post_cross_domain_comment(session: PipelineSession, unified: dict) -> None:
    """Append the unified cross-domain correlation summary to the xyOps ticket."""
    try:
        ctype   = unified.get("correlation_type", "UNKNOWN")
        primary = unified.get("primary_domain", "unknown")
        risk    = unified.get("combined_risk_level", "unknown").upper()
        score   = unified.get("combined_risk_score", 0.0)
        narrative = unified.get("narrative", "")
        chain   = unified.get("causal_chain") or []
        actions = unified.get("unified_recommended_actions") or []

        chain_md   = "\n".join(f"  {i+1}. {s}" for i, s in enumerate(chain))
        actions_md = "\n".join(f"  - {a}" for a in actions[:5])

        body = (
            f"### 🔗 Cross-Domain Correlation Detected\n"
            f"**Type:** `{ctype}`  |  **Primary domain:** `{primary}`  "
            f"|  **Combined risk:** `{risk}` ({score:.2f})\n\n"
            f"{narrative}\n\n"
            f"**Causal chain:**\n{chain_md}\n\n"
            f"**Unified recommended actions:**\n{actions_md}\n"
        )
        await _post(
            "/api/app/add_ticket_change/v1",
            {"id": session.ticket_id, "change": {"type": "comment", "body": body}},
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Request / Response models
# ═══════════════════════════════════════════════════════════════════════════════

class StartRequest(BaseModel):
    service_name: str
    alert_name: str
    severity: str = "warning"
    summary: str = ""
    description: str = ""
    dashboard_url: str = "http://grafana:3000/d/obs-overview"
    starts_at: str = ""
    session_id: str = ""     # defaults to service_name if not provided


class AgentRequest(BaseModel):
    session_id: str


# ═══════════════════════════════════════════════════════════════════════════════
# Router
# ═══════════════════════════════════════════════════════════════════════════════

pipeline_router = APIRouter(prefix="/pipeline", tags=["pipeline-agents"])


# ── Agent 1: Pipeline Start ────────────────────────────────────────────────────

@pipeline_router.post("/start")
async def pipeline_start(req: StartRequest) -> dict:
    """
    Agent 1 — Create the pipeline session and a skeleton incident ticket.

    Called by the first httpplug node in the xyOps "AIOps Agent Pipeline"
    workflow.  Returns the ticket number so you can open it immediately
    to watch the live step comments as subsequent agents run.
    """
    _gc_sessions()

    session_id = req.session_id or req.service_name
    bridge_trace_id = uuid.uuid4().hex

    session = PipelineSession(
        session_id=session_id,
        service_name=req.service_name,
        alert_name=req.alert_name,
        severity=req.severity,
        summary=req.summary or req.alert_name,
        description=req.description,
        dashboard_url=req.dashboard_url,
        starts_at=req.starts_at or datetime.now(timezone.utc).isoformat(),
        bridge_trace_id=bridge_trace_id,
    )
    _sessions[session_id] = session
    _persist_session(session)

    # Create skeleton ticket immediately so subsequent agents can post comments
    skeleton_body = (
        f"## Automated Incident — AIOps Agent Pipeline\n\n"
        f"| Field | Value |\n|---|---|\n"
        f"| **Service** | `{req.service_name}` |\n"
        f"| **Alert** | `{req.alert_name}` |\n"
        f"| **Severity** | `{req.severity.upper()}` |\n"
        f"| **Detected at** | {session.starts_at} |\n"
        f"| **Dashboard** | [{req.dashboard_url}]({req.dashboard_url}) |\n"
        f"| **Pipeline ID** | `{session_id}` |\n"
        f"| **OTel Trace** | `{bridge_trace_id}` — paste in Grafana → Tempo |\n\n"
        f"*AI pipeline agents running — watch the activity feed for live step updates.*\n\n"
        f"*Simultaneously open **Scheduler → Workflows → AIOps Agent Pipeline** "
        f"to watch each agent node turn green on the workflow canvas.*"
    )
    notify = get_notify_list(req.severity)
    create_payload: dict[str, Any] = {
        "subject": (
            f"[AIOPS] {req.alert_name} on {req.service_name} "
            f"[{req.severity.upper()}]: {session.summary}"
        ),
        "body": skeleton_body,
        "type": "issue",
        "status": "open",
    }
    if notify:
        create_payload["notify"] = notify

    create_result = await _post("/api/app/create_ticket/v1", create_payload)
    if create_result.get("error"):
        logger.warning(
            "Failed to create skeleton ticket  session=%s  error=%s",
            session_id, create_result.get("error"),
        )
        return {
            "status": "error",
            "session_id": session_id,
            "agent": "pipeline-start",
            "message": f"Ticket creation failed: {create_result.get('error')}",
        }

    session.ticket_id = create_result.get("ticket", {}).get("id", "")
    session.ticket_num = create_result.get("ticket", {}).get("num", 0)
    session.stage = "started"
    session.stage_durations["started"] = round(time.time() - session.created_at, 2)
    _persist_session(session)

    logger.info(
        "Pipeline session started  session=%s  ticket=#%s (%s)  service=%s  alert=%s",
        session_id, session.ticket_num, session.ticket_id,
        req.service_name, req.alert_name,
    )
    await asyncio.sleep(WORKFLOW_STEP_DELAY)
    return {
        "status": "started",
        "session_id": session_id,
        "agent": "pipeline-start",
        "ticket_id": session.ticket_id,
        "ticket_num": session.ticket_num,
        "message": (
            f"Pipeline started: {req.service_name} / {req.alert_name} "
            f"[{req.severity.upper()}] — Ticket #{session.ticket_num} created. "
            f"Open it in xyOps to watch live [>>]/[OK] comments as agents run."
        ),
    }


# ── Agent 2: Loki Log Fetcher ──────────────────────────────────────────────────

@pipeline_router.post("/agent/logs")
async def agent_logs(req: AgentRequest) -> dict:
    """
    Agent 2 — Fetch the last 50 Loki log lines for the service.

    Posts [>>] started and [OK] done comments to the incident ticket.
    Stores log text in the session for Agent 4 (Claude) to consume.
    """
    session = _require_session(req.session_id)

    await post_step_comment(
        session.ticket_id, 1, "started",
        f"Fetching log context for **{session.service_name}** (last 50 lines)...",
        _post,
    )

    if _http:
        session.logs = await fetch_loki_logs(session.service_name, _http)

    log_lines = session.logs.count("\n") + (1 if session.logs.strip() else 0)
    warn_count = session.logs.upper().count("WARN")
    session.stage = "logs"
    session.stage_durations["logs"] = round(time.time() - session.created_at, 2)
    _persist_session(session)

    await post_step_comment(
        session.ticket_id, 1, "done",
        f"Retrieved **{log_lines}** log lines ({warn_count} WARN events)",
        _post,
    )

    logger.info("Agent logs complete  session=%s  lines=%d", req.session_id, log_lines)
    await asyncio.sleep(WORKFLOW_STEP_DELAY)
    return {
        "status": "ok",
        "session_id": req.session_id,
        "agent": "loki-log-fetcher",
        "ticket_num": session.ticket_num,
        "log_lines": log_lines,
        "warn_count": warn_count,
        "message": (
            f"Fetched {log_lines} log lines ({warn_count} WARN) "
            f"for {session.service_name}"
        ),
    }


# ── Agent 3: Prometheus Analyst ────────────────────────────────────────────────

@pipeline_router.post("/agent/metrics")
async def agent_metrics(req: AgentRequest) -> dict:
    """
    Agent 3 — Fetch Prometheus golden signals for the service.

    Fetches: error_rate, p99_latency, p50_latency, RPS.
    Posts step comments and stores metrics in session for Agent 4.
    """
    session = _require_session(req.session_id)

    await post_step_comment(
        session.ticket_id, 2, "started",
        f"Fetching Prometheus golden signals for **{session.service_name}**...",
        _post,
    )

    if _http:
        session.metrics = await fetch_prometheus_context(session.service_name, _http)

    m_parts: list[str] = []
    if session.metrics.get("error_rate_pct") not in (None, "no data"):
        m_parts.append(f"error_rate={session.metrics['error_rate_pct']}%")
    if session.metrics.get("p99_latency_ms") not in (None, "no data"):
        m_parts.append(f"p99={session.metrics['p99_latency_ms']}ms")
    if session.metrics.get("rps") not in (None, "no data"):
        m_parts.append(f"rps={session.metrics['rps']}")
    metrics_str = "  ".join(m_parts) if m_parts else "no metrics available"
    session.stage = "metrics"
    session.stage_durations["metrics"] = round(time.time() - session.created_at, 2)
    _persist_session(session)

    await post_step_comment(
        session.ticket_id, 2, "done",
        f"Metrics snapshot: `{metrics_str}`",
        _post,
    )

    logger.info("Agent metrics complete  session=%s  %s", req.session_id, metrics_str)
    await asyncio.sleep(WORKFLOW_STEP_DELAY)
    return {
        "status": "ok",
        "session_id": req.session_id,
        "agent": "prometheus-analyst",
        "ticket_num": session.ticket_num,
        "metrics": session.metrics,
        "message": f"Golden signals for {session.service_name}: {metrics_str}",
    }


# ── Agent 4: Intelligence Pipeline + LLM Analyst ──────────────────────────────

# Module-level compute scenario catalog cache (loaded once at first call).
_compute_catalog: list | None = None


def _get_compute_catalog() -> list:
    global _compute_catalog
    if _compute_catalog is None:
        _compute_catalog = _load_catalog(domain="compute")
    return _compute_catalog


def _analysis_from_recommendation(rec, risk) -> dict:
    """Build a backward-compatible analysis dict from a Recommendation + RiskAssessment."""
    from .ai_analyst import _build_compute_stub_playbook
    return {
        "rca_summary":         rec.description,
        "recommended_action":  rec.action_type,
        "autonomy_level":      "autonomous" if rec.autonomous else "approval_gated",
        "ansible_playbook":    (
            _build_compute_stub_playbook(rec.action_type, rec.rollback_plan or "")
            if rec.action_type != "escalate" else ""
        ),
        "ansible_description": f"Deterministic remediation: {rec.display_name}",
        "test_plan": [
            f"Verify service health after {rec.action_type}",
            "Monitor error_rate and latency_p99 for 5 minutes post-action",
            "Check Grafana Agentic AI Operations dashboard for confirmation",
        ],
        "confidence":          f"{rec.confidence:.2f}",
        "provider":            "scenario-catalog",
        "risk_score":          round(risk.risk_score, 3),
        "risk_level":          risk.risk_level,
        "evidence_lines":      [],  # populated by pipeline after build_evidence
    }


def _build_combined_context(
    pattern_ctx: str,
    ts_context,
) -> str | None:
    """Merge pattern context and time-series context into one LLM injection block."""
    parts = []
    if pattern_ctx:
        parts.append(pattern_ctx)
    if ts_context:
        ts_block = ts_context.to_prompt_block()
        if ts_block:
            parts.append(ts_block)
    return "\n\n".join(parts) if parts else None


@pipeline_router.post("/agent/analyze")
async def agent_analyze(req: AgentRequest) -> dict:
    """
    Agent 4 — Intelligence pipeline: feature extraction → scenario correlation →
    risk scoring → recommendation → (optional) LLM enrichment.

    Always runs the deterministic intelligence pipeline regardless of AI
    availability.  If an AI key is configured, the LLM adds a rich incident
    narrative on top of the deterministic output.
    """
    session = _require_session(req.session_id)
    from .telemetry import compute_agent_ai_analysis_total

    # ── Step 1: extract typed features from session data ─────────────────────
    features = _extract_features(
        alert_name=session.alert_name,
        service_name=session.service_name,
        severity=session.severity,
        domain="compute",
        metrics=session.metrics,
        logs=session.logs,
    )

    # ── Step 1.5: Pattern Library matching (MCP search_patterns tool) ────────
    pattern_matches: list[dict] = []
    pattern_ctx: str = ""
    ts_context = None
    if _http:
        # Run pattern matching and time-series context in parallel
        pattern_task = asyncio.create_task(_pattern_client.search_patterns(
            http=_http,
            metrics=session.metrics,
            alert_name=session.alert_name,
            service_name=session.service_name,
            severity=session.severity,
        ))
        ts_task = asyncio.create_task(_build_ts_context(
            alert_name=session.alert_name,
            service_name=session.service_name,
            metrics_raw=session.metrics,
            http=_http,
        ))
        pattern_matches, ts_context = await asyncio.gather(
            pattern_task, ts_task, return_exceptions=False
        )
        pattern_ctx = _pattern_client.format_pattern_context(pattern_matches)
        session.matched_patterns = pattern_matches
        if pattern_matches:
            logger.info(
                "Pattern Library: %d matches for session=%s  top=%s (%.0f%%)",
                len(pattern_matches),
                req.session_id,
                pattern_matches[0].get("pattern_name", "?"),
                pattern_matches[0].get("combined_score", 0) * 100,
            )

    # ── Step 1.6: Multi-pattern correlation (Section P) ───────────────────────
    correlation = _correlate_patterns(
        pattern_matches,
        alert_name=session.alert_name,
        service_name=session.service_name,
    )

    # ── Step 2: scenario correlation ─────────────────────────────────────────
    catalog = _get_compute_catalog()
    matches = _match_scenarios(features, catalog)
    best_match, best_def = _match_best(features, catalog)

    # ── Step 3: risk scoring ─────────────────────────────────────────────────
    risk = _score_risk(features, best_match, "compute")

    # ── Step 4: build recommendation ─────────────────────────────────────────
    rec = _recommend(best_match, best_def, risk, "compute", _autonomy_rules)

    # ── Step 5: assemble evidence report ─────────────────────────────────────
    evidence = _build_evidence(
        trace_id=session.bridge_trace_id,
        incident_id=session.ticket_id,
        features=features,
        matches=matches,
        risk=risk,
        recommendations=[rec],
    )
    ev_lines = _evidence_lines(evidence)

    # ── Step 5.5: Confidence & Uncertainty profile (Section O) ───────────────
    top_pattern_score = (
        pattern_matches[0].get("combined_score", 0.0) if pattern_matches else 0.0
    )
    uncertainty = _build_uncertainty(
        features=features,
        scenario_matches=matches,
        risk=risk,
        domain="compute",
        pattern_library_top_score=top_pattern_score,
    )

    # Persist risk + evidence in session for downstream agents (ticket, approval)
    session.risk_score = risk.risk_score
    session.risk_level = risk.risk_level
    session.evidence_lines_data = ev_lines

    # ── Step 6: LLM enrichment (or deterministic fallback) ───────────────────
    if AI_ENABLED and _http:
        await post_step_comment(
            session.ticket_id, 3, "started",
            f"Running intelligence pipeline + LLM enrichment "
            f"(risk={risk.risk_level}, scenarios={len(matches)})...",
            _post,
        )
        enrichment = await _llm_enrich(evidence, rec, risk, _http,
                                        pattern_context=_build_combined_context(
                                            pattern_ctx, ts_context
                                        ))
        if enrichment:
            session.analysis = enrichment.to_analysis_dict()
            session.analysis["risk_score"] = round(risk.risk_score, 3)
            session.analysis["risk_level"] = risk.risk_level
            session.analysis["evidence_lines"] = ev_lines
            session.analysis["matched_patterns"] = pattern_matches
            session.analysis["uncertainty"] = uncertainty.to_dict()
            session.analysis["pattern_correlation"] = correlation.to_dict()
            session.analysis["timeseries_context"] = ts_context.to_dict() if ts_context else {}
            compute_agent_ai_analysis_total.labels(status="success").inc()
            provider = enrichment.provider
        else:
            session.analysis = _analysis_from_recommendation(rec, risk)
            session.analysis["evidence_lines"] = ev_lines
            session.analysis["matched_patterns"] = pattern_matches
            session.analysis["uncertainty"] = uncertainty.to_dict()
            session.analysis["pattern_correlation"] = correlation.to_dict()
            session.analysis["timeseries_context"] = ts_context.to_dict() if ts_context else {}
            compute_agent_ai_analysis_total.labels(status="deterministic").inc()
            provider = "scenario-catalog"
        session.stage = "analyzed"
        session.stage_durations["analyzed"] = round(time.time() - session.created_at, 2)
        _persist_session(session)
    else:
        session.analysis = _analysis_from_recommendation(rec, risk)
        session.analysis["evidence_lines"] = ev_lines
        session.analysis["matched_patterns"] = pattern_matches
        session.analysis["uncertainty"] = uncertainty.to_dict()
        session.analysis["pattern_correlation"] = correlation.to_dict()
        session.analysis["timeseries_context"] = ts_context.to_dict() if ts_context else {}
        compute_agent_ai_analysis_total.labels(status="deterministic").inc()
        provider = "scenario-catalog"
        session.stage = "analyzed"
        session.stage_durations["analyzed"] = round(time.time() - session.created_at, 2)
        _persist_session(session)

    # ── Step 6.5: fire-and-forget pattern assessment recording ────────────────
    if _http and pattern_matches:
        asyncio.create_task(
            _pattern_client.post_assessment(
                http=_http,
                session_id=req.session_id,
                alert_name=session.alert_name,
                matched_patterns=pattern_matches,
                risk_score=risk.risk_score,
                recommended_action=rec.action_type,
            )
        )

    # ── Optional: merge pre-computed signals from obs-intelligence ──────────────
    _obs_url = os.getenv("OBS_INTELLIGENCE_URL", "http://obs-intelligence:9100")
    try:
        if _http:
            resp = await _http.get(f"{_obs_url}/intelligence/current", timeout=3.0)
            if resp.status_code == 200:
                intel = resp.json()
                if intel.get("anomalies"):
                    session.analysis["obs_anomalies"] = intel["anomalies"]
                if intel.get("forecasts"):
                    session.analysis["obs_forecasts"] = intel["forecasts"]
                logger.debug(
                    "Merged obs-intelligence signals: anomalies=%d forecasts=%d",
                    len(intel.get("anomalies", [])),
                    len(intel.get("forecasts", [])),
                )
    except Exception:
        pass  # obs-intelligence unavailable — continue without pre-computed signals

    # Notify cross-domain coordinator (fire-and-forget — must not block pipeline)
    if _http:
        asyncio.create_task(_notify_coordinator(session))

    await post_step_comment(
        session.ticket_id, 3, "done",
        f"Analysis complete — risk: **{risk.risk_level.upper()}** "
        f"({risk.risk_score:.2f})  |  action: `{rec.action_type}`  |  "
        f"confidence: {rec.confidence:.0%}  |  provider: {provider}",
        _post,
    )
    logger.info(
        "Agent analyze complete  session=%s  risk=%s(%.3f)  action=%s  provider=%s",
        req.session_id, risk.risk_level, risk.risk_score, rec.action_type, provider,
    )
    await asyncio.sleep(WORKFLOW_STEP_DELAY)
    return {
        "status": "ok",
        "session_id": req.session_id,
        "agent": "intelligence-pipeline",
        "ticket_num": session.ticket_num,
        "ai_enabled": AI_ENABLED,
        "risk_score": round(risk.risk_score, 3),
        "risk_level": risk.risk_level,
        "recommended_action": rec.action_type,
        "confidence": f"{rec.confidence:.2f}",
        "scenario_matches": len(matches),
        "pattern_matches": len(pattern_matches),
        "decision_tier": uncertainty.decision_tier,
        "decision_confidence": round(uncertainty.decision_confidence, 3),
        "provider": provider,
        "message": (
            f"Intelligence pipeline: risk={risk.risk_level} ({risk.risk_score:.2f}), "
            f"action={rec.action_type}, confidence={rec.confidence:.0%}"
        ),
    }


# ── Agent 5: Incident Scribe ───────────────────────────────────────────────────

@pipeline_router.post("/agent/ticket")
async def agent_ticket(req: AgentRequest) -> dict:
    """
    Agent 5 — Replace the skeleton ticket body with the full enriched content.

    Uses all context accumulated by agents 2-4 to build the complete
    AI-enriched incident body (metrics table, RCA, Ansible playbook,
    test plan, rollback steps, GitHub PR suggestion).
    """
    session = _require_session(req.session_id)

    await post_step_comment(
        session.ticket_id, 4, "started",
        "Building full incident body with diagnostic context...",
        _post,
    )

    ticket_body = build_enriched_ticket_body(
        service_name=session.service_name,
        alert_name=session.alert_name,
        severity=session.severity,
        description=session.description,
        starts_at=session.starts_at,
        dashboard_url=session.dashboard_url,
        bridge_trace_id=session.bridge_trace_id,
        metrics=session.metrics,
        analysis=session.analysis,
        risk_score=session.risk_score,
        risk_level=session.risk_level,
        evidence_lines=session.evidence_lines_data,
    )

    await _post(
        "/api/app/update_ticket/v1",
        {"id": session.ticket_id, "body": ticket_body},
    )
    word_count = len(ticket_body.split())
    session.stage = "ticket_enriched"
    session.stage_durations["ticket_enriched"] = round(time.time() - session.created_at, 2)
    _persist_session(session)

    await post_step_comment(
        session.ticket_id, 4, "done",
        f"Incident body enriched — {word_count} words of AI diagnostic context",
        _post,
    )

    logger.info(
        "Agent ticket complete  session=%s  ticket=#%s  words=%d",
        req.session_id, session.ticket_num, word_count,
    )
    await asyncio.sleep(WORKFLOW_STEP_DELAY)
    return {
        "status": "ok",
        "session_id": req.session_id,
        "agent": "incident-scribe",
        "ticket_id": session.ticket_id,
        "ticket_num": session.ticket_num,
        "word_count": word_count,
        "message": (
            f"Ticket #{session.ticket_num} enriched with "
            f"{word_count} words of AI diagnostic context"
        ),
    }


# ── Agent 6: Approval Gateway ──────────────────────────────────────────────────

@pipeline_router.post("/agent/approval")
async def agent_approval(req: AgentRequest) -> dict:
    """
    Agent 6 — Approval gateway with graduated autonomy.

    Decision flow:
      1. Evaluate AutonomyEngine (tier + approval history + risk score)
      2a. AUTONOMOUS  → commit + auto-merge PR in Gitea, run Ansible directly,
                        record decision in history — no approval gate ticket.
      2b. GATED       → create xyOps change ticket with Approve / Decline buttons
                        as before (human-in-the-loop).
      3. Skipped      → no playbook available (AI disabled / low confidence).

    Over time, as humans approve actions and they execute successfully, the
    autonomy engine will unlock autonomous execution for that
    (service, action_type) pair based on the service's tier policy.
    """
    session = _require_session(req.session_id)

    needs_gate = (
        REQUIRE_APPROVAL
        and session.severity in APPROVAL_SEVERITY_THRESHOLD
        and session.analysis.get("ansible_playbook")
        and session.ticket_id
        and _http
    )

    if needs_gate:
        action_type = session.analysis.get("recommended_action", "")
        risk_score  = session.risk_score

        # ── Autonomy check ────────────────────────────────────────────────────
        autonomy = check_autonomy_for_new_service(
            service_name=session.service_name,
            action_type=action_type,
            risk_score=risk_score,
        )

        session.approval_id = f"apr-{uuid.uuid4().hex[:12]}"

        # Track telemetry
        from .telemetry import (
            compute_agent_approval_required_total,
            compute_agent_autonomous_actions_total,
            compute_agent_actions_total,
        )

        if autonomy.autonomous:
            # ── AUTONOMOUS path ───────────────────────────────────────────────
            compute_agent_autonomous_actions_total.inc()
            compute_agent_actions_total.labels(action_type="autonomous_execution").inc()

            await post_step_comment(
                session.ticket_id, 5, "started",
                f"🤖 **Autonomous execution** — trust threshold met for "
                f"`{action_type}` on `{session.service_name}` "
                f"(tier: `{autonomy.tier.value}`)  \n"
                f"> {autonomy.gate_reason}",
                _post,
            )

            auto_result = await execute_autonomous(
                session_id=req.session_id,
                approval_id=session.approval_id,
                incident_ticket_id=session.ticket_id,
                alert_name=session.alert_name,
                service_name=session.service_name,
                severity=session.severity,
                analysis=session.analysis,
                bridge_trace_id=session.bridge_trace_id,
                action_type=action_type,
                env_tier=autonomy.tier.value,
                risk_score=risk_score,
                auto_merge_pr=autonomy.auto_merge_pr,
                http=_http,
                xyops_post=_post,
            )

            session.approval_ticket_id = ""
            session.approval_ticket_num = 0
            session.autonomy_decision = "AUTONOMOUS"
            session.stage = "autonomous_executing"
            session.stage_durations["autonomous_executing"] = round(time.time() - session.created_at, 2)
            _persist_session(session)

            pr_detail = ""
            if auto_result.get("gitea_pr_num"):
                pr_detail = (
                    f"  \nGitea PR [#{auto_result['gitea_pr_num']}]"
                    f"({auto_result['gitea_pr_url']}) auto-merged ✓"
                )

            await post_step_comment(
                session.ticket_id, 5, "done",
                f"🤖 Autonomous playbook execution started{pr_detail}  \n"
                f"Watch [>>] comments for live results",
                _post,
            )

            logger.info(
                "Agent approval AUTONOMOUS  session=%s  action=%s  tier=%s  approval_id=%s",
                req.session_id, action_type, autonomy.tier.value, session.approval_id,
            )
            await asyncio.sleep(WORKFLOW_STEP_DELAY)
            return {
                "status": "ok",
                "session_id": req.session_id,
                "agent": "approval-gateway",
                "mode": "autonomous",
                "ticket_num": session.ticket_num,
                "approval_id": session.approval_id,
                "tier": autonomy.tier.value,
                "gate_reason": autonomy.gate_reason,
                "gitea_pr_num": auto_result.get("gitea_pr_num"),
                "message": (
                    f"Autonomous execution: {session.service_name}/{action_type} "
                    f"(tier={autonomy.tier.value}) — playbook running without human approval"
                ),
            }

        else:
            # ── GATED path (human approval required) ─────────────────────────
            compute_agent_approval_required_total.inc()
            compute_agent_actions_total.labels(action_type="approval_requested").inc()

            await post_step_comment(
                session.ticket_id, 5, "started",
                f"Creating **approval gate** ticket for human review  \n"
                f"Tier: `{autonomy.tier.value}` | "
                f"Gate reason: {autonomy.gate_reason}",
                _post,
            )
            approval_req = await request_approval(
                session_id=req.session_id,
                approval_id=session.approval_id,
                incident_ticket_id=session.ticket_id,
                alert_name=session.alert_name,
                service_name=session.service_name,
                severity=session.severity,
                analysis=session.analysis,
                bridge_trace_id=session.bridge_trace_id,
                xyops_post=_post,
                xyops_url=XYOPS_URL,
                http=_http,
                action_type=action_type,
                env_tier=autonomy.tier.value,
                risk_score=risk_score,
            )
            session.approval_ticket_id = approval_req.approval_ticket_id
            session.approval_ticket_num = approval_req.approval_ticket_num
            session.autonomy_decision = "APPROVAL_GATED"
            session.stage = "awaiting_approval"
            session.stage_durations["awaiting_approval"] = round(time.time() - session.created_at, 2)
            _persist_session(session)

            # Build trust progress message for the comment
            trust_msg = ""
            if autonomy.trust_score:
                ts = autonomy.trust_score
                policy = autonomy.tier_policy
                trust_msg = (
                    f"  \n**Autonomy progress:** "
                    f"{ts.approved_count + ts.autonomous_count}/"
                    f"{policy.min_approvals_for_autonomy} approvals needed "
                    f"({ts.success_rate:.0%} success rate, need ≥{policy.min_success_rate:.0%})"
                )

            await post_step_comment(
                session.ticket_id, 5, "waiting",
                f"Awaiting human approval — see **Ticket #{session.approval_ticket_num}**  \n"
                f"Approve: `POST /approval/{session.approval_id}/decision` "
                f"`{{\"approved\":true,\"decided_by\":\"your-name\"}}`"
                f"{trust_msg}",
                _post,
            )

            logger.info(
                "Agent approval GATED  session=%s  approval_id=%s  approval_ticket=#%s  tier=%s",
                req.session_id, session.approval_id, session.approval_ticket_num,
                autonomy.tier.value,
            )
            await asyncio.sleep(WORKFLOW_STEP_DELAY)
            return {
                "status": "ok",
                "session_id": req.session_id,
                "agent": "approval-gateway",
                "mode": "approval_required",
                "ticket_num": session.ticket_num,
                "approval_id": session.approval_id,
                "approval_ticket_id": session.approval_ticket_id,
                "approval_ticket_num": session.approval_ticket_num,
                "tier": autonomy.tier.value,
                "gate_reason": autonomy.gate_reason,
                "trust": autonomy.trust_score.reason if autonomy.trust_score else "no history",
                "message": (
                    f"Approval gate: Ticket #{session.approval_ticket_num} created — "
                    f"POST /approval/{session.approval_id}/decision to decide"
                ),
            }
    else:
        await post_step_comment(
            session.ticket_id, 5, "skipped",
            "Approval gate — N/A (no Ansible playbook or approval not required)",
            _post,
        )
        session.autonomy_decision = session.analysis.get("autonomy_level", "HUMAN_ONLY").upper()
        session.completed_at = time.time()
        session.mttr_seconds = round(session.completed_at - session.created_at, 1)
        session.stage = "complete"
        session.stage_durations["complete"] = round(time.time() - session.created_at, 2)
        _persist_session(session)
        logger.info(
            "Agent approval skipped  session=%s  reason=no_playbook_or_not_required",
            req.session_id,
        )
        await asyncio.sleep(WORKFLOW_STEP_DELAY)
        return {
            "status": "ok",
            "session_id": req.session_id,
            "agent": "approval-gateway",
            "mode": "skipped",
            "ticket_num": session.ticket_num,
            "message": "No approval gate needed — pipeline complete",
        }


# ── Debug: inspect session state ───────────────────────────────────────────────

_AGENT_STAGES = [
    ("ticket-creator",   "started"),
    ("log-fetcher",      "logs"),
    ("metrics-fetcher",  "metrics"),
    ("ai-analyst",       "analyzed"),
    ("ticket-writer",    "ticket_enriched"),
    ("approval-gateway", "awaiting_approval"),
]
_TERMINAL_STAGES = {"complete", "autonomous_executing"}


def _build_agent_states(session: "PipelineSession") -> list[dict]:
    """Convert pipeline stage into a list of AgentState dicts for the UI."""
    current = session.stage
    result = []
    reached_current = False
    for name, stage in _AGENT_STAGES:
        if current in _TERMINAL_STAGES:
            result.append({"name": name, "status": "completed"})
        elif current == stage:
            result.append({"name": name, "status": "running"})
            reached_current = True
        elif reached_current:
            result.append({"name": name, "status": "idle"})
        else:
            result.append({"name": name, "status": "completed"})
    return result


@pipeline_router.get("/active")
async def get_active_sessions() -> dict:
    """Return currently active (in-memory) pipeline sessions for the Command Center."""
    _gc_sessions()
    sessions = [
        {
            "session_id":  s.session_id,
            "service_name": s.service_name,
            "alert_name":  s.alert_name,
            "severity":    s.severity,
            "stage":       s.stage,
            "risk_score":  round(s.risk_score, 2),
            "risk_level":  s.risk_level,
            "outcome":     s.outcome,
            "created_at":  s.created_at,
            "age_seconds": round(time.time() - s.created_at),
            "domain":      "compute",
        }
        for s in sorted(_sessions.values(), key=lambda x: x.created_at, reverse=True)
    ]
    return {"sessions": sessions, "count": len(sessions)}


@pipeline_router.get("/history")
async def get_pipeline_history(
    limit: int = 50,
    outcome: str | None = None,
    service_name: str | None = None,
) -> dict:
    """Return completed pipeline sessions from SQLite for the Command Center history view."""
    try:
        query = "SELECT * FROM pipeline_sessions WHERE 1=1"
        params: list = []
        if outcome:
            query += " AND outcome = ?"
            params.append(outcome)
        if service_name:
            query += " AND service_name = ?"
            params.append(service_name)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        with _db_conn() as conn:
            rows = conn.execute(query, params).fetchall()

        history = [
            {
                "session_id":        row["session_id"],
                "service_name":      row["service_name"],
                "alert_name":        row["alert_name"],
                "severity":          row["severity"],
                "stage":             row["stage"],
                "risk_score":        round(row["risk_score"] or 0, 2),
                "risk_level":        row["risk_level"] or "unknown",
                "autonomy_decision": row["autonomy_decision"] or "APPROVAL_GATED",
                "outcome":           row["outcome"] or "pending",
                "created_at":        row["created_at"],
                "completed_at":      row["completed_at"],
                "mttr_seconds":      row["mttr_seconds"] or 0,
                "domain":            "compute",
            }
            for row in rows
        ]
        return {"history": history, "count": len(history)}
    except Exception as exc:
        logger.warning("get_pipeline_history failed: %s", exc)
        return {"history": [], "count": 0}

@pipeline_router.get("/session/{session_id}")
async def get_session(session_id: str) -> dict:
    """Return current session state for UI visualization."""
    session = _require_session(session_id)
    trust_metrics = _compute_trust_metrics(
        session.service_name,
        session.analysis.get("recommended_action", ""),
    )
    trust_progress = (
        f"{trust_metrics['approvals_recorded']} / "
        f"{trust_metrics['approvals_recorded'] + trust_metrics['next_tier']['approvals_needed']} approvals"
    )

    return {
        "session_id": session.session_id,
        "service_name": session.service_name,
        "alert_name": session.alert_name,
        "severity": session.severity,
        "summary": session.summary,
        "stage": session.stage,
        # ── PipelineState compatibility fields ──────────────────────────────
        # `status` maps the internal stage to running/completed/failed
        "status": (
            "completed" if session.stage == "complete"
            else "failed"   if session.outcome == "failure"
            else "running"
        ),
        # `agents` gives each pipeline step a name + status for the UI
        "agents": _build_agent_states(session),
        # `incident` block for the dashboard summary card
        "incident": {
            "service_name": session.service_name,
            "alert_name":   session.alert_name,
            "severity":     session.severity,
            "risk_score":   round(session.risk_score, 2),
            "scenario_matched": session.analysis.get("scenario_id"),
            "grafana_url":  session.dashboard_url,
        },
        "approval_required": bool(session.approval_id),
        # ────────────────────────────────────────────────────────────────────
        "ticket_id": session.ticket_id,
        "ticket_num": session.ticket_num,
        "approval_id": session.approval_id,
        "approval_ticket_num": session.approval_ticket_num,
        "risk_score": round(session.risk_score, 2),
        "risk_level": session.risk_level,
        "created_at": session.created_at,
        "age_seconds": round(time.time() - session.created_at),
        "completed_at": session.completed_at,
        "mttr_seconds": session.mttr_seconds,
        "outcome": session.outcome,
        # Raw agent data — frontend builds AgentStep[] via sessionToAgents()
        "logs": session.logs,
        "metrics": session.metrics,
        "stage_durations": session.stage_durations,
        # Analysis results for incident dashboard
        "analysis": {
            "root_cause": session.analysis.get("root_cause", "Analyzing..."),
            "recommended_action": session.analysis.get("recommended_action", "Pending analysis"),
            "confidence": session.analysis.get("confidence", 0),
            "scenario_id": session.analysis.get("scenario_id", "Unknown"),
            "scenario_confidence": session.analysis.get("scenario_confidence", 0),
            "provider": session.analysis.get("provider", "scenario-catalog"),
            "model": session.analysis.get("model"),
            "knowledge_entry_id": session.analysis.get("knowledge_entry_id"),
            "local_validation_status": session.analysis.get("local_validation_status"),
            "local_validation_confidence": session.analysis.get("local_validation_confidence"),
            "local_validation_reason": session.analysis.get("local_validation_reason"),
            "local_validation_completed": session.analysis.get("local_validation_completed", False),
            "knowledge_top_similarity": session.analysis.get("knowledge_top_similarity"),
            "local_model": session.analysis.get("local_model"),
            "uncertainty": session.analysis.get("uncertainty"),
        },
        # Autonomy decision for UI
        "autonomy_decision": session.autonomy_decision,
        "trust_progress": trust_progress,
        "trust_metrics": trust_metrics,
        # Pattern Library matches (Section F)
        "matched_patterns": session.matched_patterns,
    }


@pipeline_router.get("/events")
async def pipeline_events(request: Request):
    """SSE stream — emits a JSON event whenever the latest session's stage changes."""

    async def _stream():
        last_stage = None
        while True:
            if await request.is_disconnected():
                break
            # Find newest session
            session = max(_sessions.values(), key=lambda s: s.created_at) if _sessions else None
            if session:
                current = session.stage
                if current != last_stage:
                    payload = json.dumps({
                        "session_id": session.session_id,
                        "stage": current,
                        "service_name": session.service_name,
                        "severity": session.severity,
                        "outcome": session.outcome,
                        "timestamp": time.time(),
                    })
                    yield f"data: {payload}\n\n"
                    last_stage = current
                else:
                    yield ": heartbeat\n\n"
            else:
                yield ": heartbeat\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(_stream(), media_type="text/event-stream")
