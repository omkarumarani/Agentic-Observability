"""
storage-agent/app/pipeline.py
──────────────────────────────────────────────────────────────────────────────
Storage AIOps Agent Pipeline — HTTP endpoints for the xyOps Workflow canvas.

Each endpoint maps to one pipeline step (one xyOps workflow node).
Sessions are keyed by session_id (defaults to service_name).

Endpoints:
  POST /pipeline/start                  Agent 1 — create session + xyOps ticket
  POST /pipeline/agent/storage-metrics  Agent 2 — fetch Prometheus storage metrics
  POST /pipeline/agent/logs             Agent 3 — fetch Loki logs
  POST /pipeline/agent/analyze          Agent 4 — AI storage analysis
  POST /pipeline/agent/ticket           Agent 5 — enrich ticket body
  POST /pipeline/agent/approval         Agent 6 — route to approval / execute

GET  /pipeline/session/{id}            — inspect session state (debug)
──────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .storage_analyst import (
    AI_ENABLED,
    build_enriched_ticket_body,
    fetch_loki_logs,
    fetch_storage_metrics,
    get_notify_list,
)
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
from . import autonomy_rules as _autonomy_rules
from .telemetry import (
    storage_agent_ai_analysis_total,
    storage_agent_approval_required_total,
    storage_agent_autonomous_remediations_total,
    storage_agent_escalations_total,
    storage_agent_noisy_pvc_throttles_total,
    storage_agent_osd_reweights_total,
    storage_agent_actions_total,
)

logger = logging.getLogger("storage-agent.pipeline")

XYOPS_URL: str = os.getenv("XYOPS_URL", "http://xyops:5522")
XYOPS_API_KEY: str = os.getenv("XYOPS_API_KEY", "")
ANSIBLE_RUNNER_URL: str = os.getenv("ANSIBLE_RUNNER_URL", "http://ansible-runner:8080")
REQUIRE_APPROVAL: bool = os.getenv("STORAGE_REQUIRE_APPROVAL", "true").lower() != "false"
WORKFLOW_STEP_DELAY: int = int(os.getenv("WORKFLOW_STEP_DELAY_SECONDS", "5"))
SESSION_TTL_SECONDS: int = 3600
_OBS_INTELLIGENCE_URL: str = os.getenv("OBS_INTELLIGENCE_URL", "http://obs-intelligence:9100")
GRAFANA_EXTERNAL_URL: str = os.getenv("GRAFANA_EXTERNAL_URL", "http://localhost:3001")

TOTAL_STEPS = 5

# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline session store
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class StoragePipelineSession:
    session_id: str
    service_name: str
    alert_name: str
    severity: str
    summary: str
    description: str
    dashboard_url: str
    created_at: float = field(default_factory=time.time)
    ticket_id: str = ""
    ticket_num: int = 0
    logs_context: str = ""
    metrics_context: str = ""
    metrics_raw: dict = field(default_factory=dict)
    ai_result: dict = field(default_factory=dict)
    approval_id: str = ""
    bridge_trace_id: str = ""
    status: str = "init"

    # Risk + evidence (populated by pipeline_analyze)
    risk_score: float = 0.0
    risk_level: str = "unknown"
    evidence_lines_data: list = field(default_factory=list)

    # Pre-commit Ansible validation (populated by pipeline_approval)
    validation_passed: bool = False
    validation_result: dict = field(default_factory=dict)


_sessions: dict[str, StoragePipelineSession] = {}


def _prune_sessions() -> None:
    now = time.time()
    expired = [k for k, v in _sessions.items() if now - v.created_at > SESSION_TTL_SECONDS]
    for k in expired:
        del _sessions[k]


async def _notify_coordinator(session) -> None:
    """Fire-and-forget: tell obs-intelligence about this storage incident.

    v2: includes a ``signals`` snapshot so obs-intelligence can run the
    CrossDomainCorrelator without an extra Prometheus round-trip.  When a
    simultaneous compute incident is detected a ``unified_assessment`` is
    returned and we post it as a ticket comment.
    """
    try:
        import httpx as _httpx
        raw = session.metrics_raw or {}
        payload = {
            "domain":       "storage",
            "service_name": session.service_name,
            "alert_name":   session.alert_name,
            "risk_score":   session.risk_score,
            "scenario_id":  session.ai_result.get("scenario_id", ""),
            "run_id":       session.bridge_trace_id,
            "signals": {
                "io_latency_s":   float(raw.get("io_latency", 0) or 0),
                "pool_usage_pct": float(raw.get("pool_usage_pct", 0) or 0) / 100.0,
                "osd_up":         int(raw.get("osd_up", 0) or 0),
                "osd_total":      int(raw.get("osd_total", 0) or 0),
            },
        }
        async with _httpx.AsyncClient() as _c:
            resp = await _c.post(
                f"{_OBS_INTELLIGENCE_URL}/intelligence/record-incident",
                json=payload,
                timeout=3.0,
            )
            data = resp.json()
            unified = data.get("unified_assessment")
            if unified and session.ticket_id:
                await _post_cross_domain_comment_storage(session, unified, _c)
    except Exception:
        pass  # coordinator is best-effort


async def _post_cross_domain_comment_storage(session, unified: dict, http) -> None:
    """Append the unified cross-domain correlation summary to the storage xyOps ticket."""
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
        await _post_comment(session.ticket_id, step=0, status="alert", msg=body, http=http)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Request/response models
# ═══════════════════════════════════════════════════════════════════════════════

class StartRequest(BaseModel):
    service_name: str = "storage-simulator"
    alert_name: str = "CephOSDDown"
    severity: str = "warning"
    summary: str = "Storage incident"
    description: str = ""
    dashboard_url: str = GRAFANA_EXTERNAL_URL + "/d/agentic-ai-overview"


class AgentRequest(BaseModel):
    session_id: str = "storage-simulator"


# ═══════════════════════════════════════════════════════════════════════════════
# xyOps helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _xyops_headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if XYOPS_API_KEY:
        h["X-API-Key"] = XYOPS_API_KEY
    return h


async def _xyops_post(path: str, body: dict, http: httpx.AsyncClient) -> dict:
    try:
        resp = await http.post(
            f"{XYOPS_URL}{path}",
            json=body,
            headers=_xyops_headers(),
            timeout=10.0,
        )
        return resp.json()
    except Exception as exc:
        logger.warning("xyOps POST %s failed: %s", path, exc)
        return {"error": str(exc)}


async def _xyops_get(path: str, http: httpx.AsyncClient) -> dict:
    try:
        resp = await http.get(
            f"{XYOPS_URL}{path}",
            headers=_xyops_headers(),
            timeout=10.0,
        )
        return resp.json()
    except Exception as exc:
        logger.warning("xyOps GET %s failed: %s", path, exc)
        return {"error": str(exc)}


async def _post_comment(ticket_id: str, step: int, status: str, msg: str, http: httpx.AsyncClient) -> None:
    if not ticket_id:
        return
    icons = {"started": "[>>]", "done": "[OK]", "error": "[!!]", "waiting": "[..]", "skipped": "[--]"}
    icon = icons.get(status, "[??]")
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    body = f"{icon} **Step {step}/{TOTAL_STEPS}:** {msg}  \n`{now}`"
    await _xyops_post(
        "/api/app/add_ticket_change/v1",
        {"id": ticket_id, "change": {"type": "comment", "body": body}},
        http,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline router
# ═══════════════════════════════════════════════════════════════════════════════

pipeline_router = APIRouter(prefix="/pipeline", tags=["storage-pipeline"])


@pipeline_router.post("/start")
async def pipeline_start(req: StartRequest) -> dict:
    """Agent 1 — Create session and skeleton xyOps ticket."""
    _prune_sessions()

    session = StoragePipelineSession(
        session_id=req.service_name,
        service_name=req.service_name,
        alert_name=req.alert_name,
        severity=req.severity,
        summary=req.summary,
        description=req.description,
        dashboard_url=req.dashboard_url,
    )

    async with httpx.AsyncClient() as http:
        # Deduplication: skip if an open ticket already exists for this alert+service
        import urllib.parse as _urlparse
        q = _urlparse.quote(f"[Storage] {req.alert_name} {req.service_name} status:open")
        existing = await _xyops_get(f"/api/app/search_tickets/v1?query={q}&limit=5", http)
        open_tickets = [t for t in existing.get("rows", []) if t.get("status") == "open"]
        if open_tickets:
            existing_id = open_tickets[0].get("id", "")
            existing_num = open_tickets[0].get("num", 0)
            logger.info(
                "Skipping duplicate storage ticket — open ticket exists  "
                "alert=%s  service=%s  ticket_id=%s",
                req.alert_name, req.service_name, existing_id,
            )
            session.ticket_id = existing_id
            session.ticket_num = existing_num
            _sessions[session.session_id] = session
            return {"status": "ok", "session_id": session.session_id,
                    "ticket_id": existing_id, "ticket_num": existing_num, "duplicate": True}

        ticket_body = {
            "title": f"[Storage] {req.alert_name} on {req.service_name}",
            "severity": req.severity,
            "status": "open",
            "body": (
                f"**Storage Alert:** {req.alert_name}  \n"
                f"**Service:** {req.service_name}  \n"
                f"**Summary:** {req.summary}  \n\n"
                f"*Storage AIOps pipeline is running — watch this feed for live updates.*"
            ),
            "tags": ["storage", "aiops", req.alert_name.lower(), req.severity],
        }
        result = await _xyops_post("/api/app/create_ticket/v1", ticket_body, http)
        session.ticket_id = result.get("id", "")
        session.ticket_num = result.get("num", 0)

        if WORKFLOW_STEP_DELAY > 0:
            await asyncio.sleep(WORKFLOW_STEP_DELAY)

        await _post_comment(
            session.ticket_id, 1, "done",
            f"Pipeline started for **{req.alert_name}** on `{req.service_name}` "
            f"(severity: {req.severity})",
            http,
        )

    _sessions[session.session_id] = session
    logger.info("Storage pipeline started session=%s ticket=%s", session.session_id, session.ticket_id)
    return {
        "status": "ok",
        "session_id": session.session_id,
        "ticket_id": session.ticket_id,
        "ticket_num": session.ticket_num,
    }


@pipeline_router.post("/agent/storage-metrics")
async def pipeline_storage_metrics(req: AgentRequest) -> dict:
    """Agent 2 — Fetch storage metrics from Prometheus."""
    session = _sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"No session: {req.session_id}")

    async with httpx.AsyncClient() as http:
        await _post_comment(session.ticket_id, 2, "started",
                             f"Fetching storage metrics for `{session.alert_name}`", http)
        if WORKFLOW_STEP_DELAY > 0:
            await asyncio.sleep(WORKFLOW_STEP_DELAY)

        metrics_data = await fetch_storage_metrics(session.alert_name, http)
        session.metrics_context = metrics_data["summary"]
        session.metrics_raw = metrics_data["raw"]

        await _post_comment(session.ticket_id, 2, "done",
                             f"Storage metrics retrieved: {session.metrics_context[:120]}...", http)

    return {"status": "ok", "session_id": req.session_id, "metrics_lines": session.metrics_context.count("\n")}


@pipeline_router.post("/agent/logs")
async def pipeline_logs(req: AgentRequest) -> dict:
    """Agent 3 — Fetch Loki log context."""
    session = _sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"No session: {req.session_id}")

    async with httpx.AsyncClient() as http:
        await _post_comment(session.ticket_id, 3, "started",
                             f"Querying Loki for storage logs", http)
        if WORKFLOW_STEP_DELAY > 0:
            await asyncio.sleep(WORKFLOW_STEP_DELAY)

        logs = await fetch_loki_logs(
            f'{{service_name="{session.service_name}"}}',
            http,
        )
        session.logs_context = logs

        line_count = logs.count("\n")
        await _post_comment(session.ticket_id, 3, "done",
                             f"Log context retrieved ({line_count} lines)", http)

    return {"status": "ok", "session_id": req.session_id, "log_lines": line_count}


@pipeline_router.post("/agent/analyze")
async def pipeline_analyze(req: AgentRequest) -> dict:
    """Agent 4 — Intelligence pipeline: feature extraction → scenario correlation →
    risk scoring → recommendation → (optional) LLM enrichment."""
    session = _sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"No session: {req.session_id}")

    async with httpx.AsyncClient() as http:
        await _post_comment(
            session.ticket_id, 4, "started",
            f"Running intelligence pipeline for `{session.alert_name}`", http,
        )
        if WORKFLOW_STEP_DELAY > 0:
            await asyncio.sleep(WORKFLOW_STEP_DELAY)

        try:
            # ── Step 1: extract features ──────────────────────────────────────
            features = _extract_features(
                alert_name=session.alert_name,
                service_name=session.service_name,
                severity=session.severity,
                domain="storage",
                metrics={"raw": session.metrics_raw, "summary": session.metrics_context},
                logs=session.logs_context,
            )

            # ── Step 2: scenario correlation ──────────────────────────────────
            catalog = _get_storage_catalog()
            matches = _match_scenarios(features, catalog)
            best_match, best_def = _match_best(features, catalog)

            # ── Step 3: risk scoring ──────────────────────────────────────────
            risk = _score_risk(features, best_match, "storage")

            # ── Step 4: recommendation ────────────────────────────────────────
            rec = _recommend(best_match, best_def, risk, "storage", _autonomy_rules)

            # ── Step 5: evidence report ───────────────────────────────────────
            evidence = _build_evidence(
                trace_id=session.bridge_trace_id,
                incident_id=session.ticket_id,
                features=features,
                matches=matches,
                risk=risk,
                recommendations=[rec],
            )
            ev_lines = _evidence_lines(evidence)

            session.risk_score = risk.risk_score
            session.risk_level = risk.risk_level
            session.evidence_lines_data = ev_lines

            # ── Step 6: LLM enrichment or deterministic ───────────────────────
            if AI_ENABLED:
                enrichment = await _llm_enrich(evidence, rec, risk, http)
                if enrichment:
                    ai_result = enrichment.to_analysis_dict()
                    storage_agent_ai_analysis_total.labels(status="success").inc()
                    provider = enrichment.provider
                else:
                    ai_result = _storage_analysis_from_recommendation(rec, risk)
                    storage_agent_ai_analysis_total.labels(status="deterministic").inc()
                    provider = "scenario-catalog"
            else:
                ai_result = _storage_analysis_from_recommendation(rec, risk)
                storage_agent_ai_analysis_total.labels(status="deterministic").inc()
                provider = "scenario-catalog"

            ai_result["risk_score"] = round(risk.risk_score, 3)
            ai_result["risk_level"] = risk.risk_level
            ai_result["evidence_lines"] = ev_lines
            session.ai_result = ai_result

            # Notify cross-domain coordinator (fire-and-forget — must not block pipeline)
            asyncio.create_task(_notify_coordinator(session))

            # ── Optional: merge pre-computed signals from obs-intelligence ────────
            _obs_url = os.getenv("OBS_INTELLIGENCE_URL", "http://obs-intelligence:9100")
            try:
                resp = await http.get(f"{_obs_url}/intelligence/current", timeout=3.0)
                if resp.status_code == 200:
                    intel = resp.json()
                    if intel.get("anomalies"):
                        session.ai_result["obs_anomalies"] = intel["anomalies"]
                    if intel.get("forecasts"):
                        session.ai_result["obs_forecasts"] = intel["forecasts"]
                    logger.debug(
                        "Merged obs-intelligence signals: anomalies=%d forecasts=%d",
                        len(intel.get("anomalies", [])),
                        len(intel.get("forecasts", [])),
                    )
            except Exception:
                pass  # obs-intelligence unavailable — continue without pre-computed signals

            action = ai_result.get("recommended_action", "unknown")
            autonomy = ai_result.get("autonomy_level", "unknown")

            await _post_comment(
                session.ticket_id, 4, "done",
                f"Analysis complete: action=`{action}` autonomy=`{autonomy}` "
                f"risk=`{risk.risk_level}({risk.risk_score:.2f})` provider=`{provider}`",
                http,
            )

        except Exception as exc:
            storage_agent_ai_analysis_total.labels(status="failed").inc()
            logger.error("Intelligence pipeline error: %s", exc)
            await _post_comment(session.ticket_id, 4, "error",
                                 f"Analysis failed: {exc}", http)
            raise HTTPException(status_code=500, detail=str(exc))

    return {
        "status": "ok",
        "session_id": req.session_id,
        "action": session.ai_result.get("recommended_action"),
        "autonomy": session.ai_result.get("autonomy_level"),
        "confidence": session.ai_result.get("confidence"),
        "risk_score": round(session.risk_score, 3),
        "risk_level": session.risk_level,
    }


@pipeline_router.post("/agent/ticket")
async def pipeline_ticket(req: AgentRequest) -> dict:
    """Agent 5 — Enrich xyOps ticket with full analysis."""
    session = _sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"No session: {req.session_id}")

    async with httpx.AsyncClient() as http:
        await _post_comment(session.ticket_id, 5, "started",
                             "Building enriched ticket body", http)
        if WORKFLOW_STEP_DELAY > 0:
            await asyncio.sleep(WORKFLOW_STEP_DELAY)

        enriched_body = build_enriched_ticket_body(
            alert_name=session.alert_name,
            service_name=session.service_name,
            severity=session.severity,
            summary=session.summary,
            description=session.description,
            metrics_context=session.metrics_context,
            ai_result=session.ai_result,
            bridge_trace_id=session.bridge_trace_id,
            grafana_url=session.dashboard_url,
            risk_score=session.risk_score,
            risk_level=session.risk_level,
            evidence_lines=session.evidence_lines_data,
        )

        if session.ticket_id:
            await _xyops_post(
                "/api/app/update_ticket/v1",
                {"id": session.ticket_id, "body": enriched_body},
                http,
            )

        await _post_comment(session.ticket_id, 5, "done",
                             "Ticket enriched with RCA, playbook, and test plan", http)

    return {"status": "ok", "session_id": req.session_id}


@pipeline_router.post("/agent/approval")
async def pipeline_approval(req: AgentRequest) -> dict:
    """Agent 6 — Route to approval gate or execute autonomously."""
    session = _sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"No session: {req.session_id}")

    autonomy = session.ai_result.get("autonomy_level", "approval_gated")
    action = session.ai_result.get("recommended_action", "escalate")

    async with httpx.AsyncClient() as http:
        if WORKFLOW_STEP_DELAY > 0:
            await asyncio.sleep(WORKFLOW_STEP_DELAY)

        if autonomy == "human_only":
            # Critical — never automate
            storage_agent_escalations_total.inc()
            storage_agent_actions_total.labels(action_type="escalate").inc()
            await _post_comment(
                session.ticket_id, 5, "waiting",
                f"🔴 ESCALATED to storage SRE team — action=`{action}` "
                f"requires human-only decision. Do NOT automate.",
                http,
            )
            session.status = "escalated"
            return {"status": "escalated", "session_id": req.session_id, "action": action}

        if not REQUIRE_APPROVAL or autonomy == "autonomous":
            # Execute without approval gate
            storage_agent_autonomous_remediations_total.inc()
            _increment_action_counter(action)
            await _post_comment(
                session.ticket_id, 5, "done",
                f"🟢 Autonomous execution: `{action}` — running Ansible playbook",
                http,
            )
            await _run_playbook(session, http)
            session.status = "executed"
            return {"status": "executed", "session_id": req.session_id, "action": action}

        # Approval required
        storage_agent_approval_required_total.inc()
        storage_agent_actions_total.labels(action_type="approval_requested").inc()

        # ── Validate playbook BEFORE creating approval gate ──────────────
        playbook = session.ai_result.get("ansible_playbook", "")
        test_cases = session.ai_result.get("test_cases", [])
        if playbook:
            val_payload = {
                "playbook_yaml": playbook,
                "service_name": session.service_name,
                "alert_name": session.alert_name,
                "trace_id": session.bridge_trace_id,
                "test_cases": test_cases,
            }
            try:
                val_resp = await http.post(
                    f"{ANSIBLE_RUNNER_URL}/validate",
                    json=val_payload,
                    timeout=90.0,
                )
                if val_resp.status_code == 200:
                    val = val_resp.json()
                    all_passed = val.get("all_passed", True)
                    test_results = val.get("test_results", [])
                    passed_count = sum(1 for t in test_results if t.get("status") == "PASSED")
                    total_count = len(test_results)
                    session.validation_passed = all_passed
                    session.validation_result = {
                        "test_results": test_results,
                        "stdout": val.get("stdout", ""),
                        "all_passed": all_passed,
                    }
                    logger.info(
                        "Storage pre-commit validation: %s/%s passed  alert=%s",
                        passed_count, total_count, session.alert_name,
                    )
                    if not all_passed:
                        fail_lines = [
                            "## ❌ Ansible Pre-commit Validation FAILED",
                            "",
                            f"**Result:** {passed_count}/{total_count} test cases passed",
                            "",
                            "| Test Case | Status | Detail |",
                            "|---|---|---|",
                        ]
                        for tc in test_results:
                            icon = "✅" if tc.get("status") == "PASSED" else "❌"
                            fail_lines.append(
                                f"| `{tc.get('id', '?')}` {tc.get('name', '')} "
                                f"| {icon} {tc.get('status', '?')} "
                                f"| {tc.get('output', '')[:100]} |"
                            )
                        fail_lines += [
                            "",
                            "```",
                            val.get("stdout", "")[:1500],
                            "```",
                            "",
                            "> **Manual review required.** "
                            "Fix the playbook or investigate the alert manually.",
                        ]
                        await _post_comment(
                            session.ticket_id, 5, "error",
                            "\n".join(fail_lines), http,
                        )
                        session.status = "validation_failed"
                        return {
                            "status": "validation_failed",
                            "session_id": req.session_id,
                            "action": action,
                            "validation_result": session.validation_result,
                        }
                else:
                    session.validation_passed = True
                    logger.warning(
                        "Validator returned HTTP %d — treating as passed", val_resp.status_code
                    )
            except Exception as exc:
                session.validation_passed = True
                logger.warning("Validation call failed (fail-open): %s", exc)
        else:
            session.validation_passed = True

        # Create approval ticket in xyOps
        approval_body = (
            f"# Storage Remediation Approval Required\n\n"
            f"**Incident:** [{session.ticket_num or session.ticket_id}]\n"
            f"**Alert:** {session.alert_name}\n"
            f"**Action:** `{action}`\n"
            f"**Autonomy:** {autonomy}\n\n"
            f"## Playbook to Execute\n```yaml\n{session.ai_result.get('ansible_playbook', '')}\n```\n\n"
            f"**To approve:** POST to `http://storage-agent:9001/approval/{session.session_id}/decision` "
            f'with body `{{"approved": true, "decided_by": "your-name"}}`\n\n'
            f"**To decline:** same endpoint with `approved: false`"
        )
        approval_result = await _xyops_post(
            "/api/app/create_ticket/v1",
            {
                "title": f"[APPROVAL] Storage remediation: {action} for {session.alert_name}",
                "severity": session.severity,
                "status": "open",
                "body": approval_body,
                "tags": ["storage", "approval", "aiops", action],
            },
            http,
        )
        session.approval_id = approval_result.get("id", session.session_id)

        await _post_comment(
            session.ticket_id, 5, "waiting",
            f"🟡 Approval ticket created (ID: {session.approval_id}). "
            f"Human sign-off required for `{action}`.",
            http,
        )
        session.status = "awaiting_approval"
        return {
            "status": "awaiting_approval",
            "session_id": req.session_id,
            "action": action,
            "approval_ticket_id": session.approval_id,
        }


# ── Storage intelligence pipeline helpers ─────────────────────────────────────

_storage_catalog_cache: list | None = None


def _get_storage_catalog() -> list:
    """Return (and lazily initialise) the storage scenario catalog."""
    global _storage_catalog_cache
    if _storage_catalog_cache is None:
        _storage_catalog_cache = _load_catalog(domain="storage")
    return _storage_catalog_cache


def _storage_analysis_from_recommendation(rec, risk) -> dict:
    """Build a backward-compatible ai_result dict from a Recommendation + RiskAssessment."""
    from .storage_analyst import _build_stub_playbook
    return {
        "rca_summary":         rec.description,
        "recommended_action":  rec.action_type,
        "autonomy_level":      "autonomous" if rec.autonomous else "approval_gated",
        "ansible_playbook":    (
            _build_stub_playbook(rec.action_type, rec.rollback_plan or "")
            if rec.action_type != "escalate" else ""
        ),
        "test_plan": [
            "Verify current storage cluster health: ceph status",
            "Confirm alert condition with: ceph osd tree / ceph df",
            "Dry-run remediation in non-production first",
            f"Monitor cluster health score after {rec.action_type}",
        ],
        "confidence":  f"{rec.confidence:.2f}",
        "provider":    "scenario-catalog",
    }


def _increment_action_counter(action: str) -> None:
    """Increment the appropriate prometheus-client counter for an action."""
    storage_agent_actions_total.labels(action_type=action).inc()
    if action == "osd_reweight":
        storage_agent_osd_reweights_total.inc()
    elif action == "pvc_throttle":
        storage_agent_noisy_pvc_throttles_total.inc()
    elif action == "escalate":
        storage_agent_escalations_total.inc()


async def _run_playbook(session: StoragePipelineSession, http: httpx.AsyncClient) -> None:
    """Submit the Ansible playbook to ansible-runner."""
    playbook = session.ai_result.get("ansible_playbook", "")
    if not playbook:
        return
    try:
        resp = await http.post(
            f"{ANSIBLE_RUNNER_URL}/run",
            json={
                "playbook_yaml": playbook,
                "service_name": session.service_name,
                "alert_name": session.alert_name,
                "trace_id": session.bridge_trace_id,
                "test_cases": session.ai_result.get("test_cases", []),
            },
            timeout=120.0,
        )
        result = resp.json()
        logger.info("Ansible execution result: %s", result.get("status"))
    except Exception as exc:
        logger.warning("Ansible runner call failed: %s", exc)


@pipeline_router.get("/session/{session_id}")
async def get_session(session_id: str) -> dict:
    """Debug: inspect current pipeline session state."""
    if session_id == "default":
        if _sessions:
            session = max(_sessions.values(), key=lambda s: s.created_at)
        else:
            raise HTTPException(status_code=404, detail="No sessions found")
    else:
        session = _sessions.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"No session: {session_id}")
    return {
        "session_id": session.session_id,
        "service_name": session.service_name,
        "alert_name": session.alert_name,
        "severity": session.severity,
        "summary": session.summary,
        "ticket_id": session.ticket_id,
        "ticket_num": session.ticket_num,
        "approval_id": session.approval_id,
        "status": session.status,
        "risk_score": round(session.risk_score, 2),
        "risk_level": session.risk_level,
        "created_at": session.created_at,
        "age_seconds": round(time.time() - session.created_at),
        # Full analysis block — Block F fields included when AI is enabled
        "analysis": {
            "root_cause": session.ai_result.get("root_cause", "Analyzing..."),
            "recommended_action": session.ai_result.get("recommended_action", "Pending analysis"),
            "confidence": session.ai_result.get("confidence", 0),
            "scenario_id": session.ai_result.get("scenario_id", "Unknown"),
            "scenario_confidence": session.ai_result.get("scenario_confidence", 0),
            "provider": session.ai_result.get("provider", "scenario-catalog"),
            "model": session.ai_result.get("model"),
            "knowledge_entry_id": session.ai_result.get("knowledge_entry_id"),
            # Block F — local LLM validation fields
            "local_validation_status": session.ai_result.get("local_validation_status"),
            "local_validation_confidence": session.ai_result.get("local_validation_confidence"),
            "local_validation_reason": session.ai_result.get("local_validation_reason"),
            "local_validation_completed": session.ai_result.get("local_validation_completed", False),
            "knowledge_top_similarity": session.ai_result.get("knowledge_top_similarity"),
            "local_model": session.ai_result.get("local_model"),
            "source": session.ai_result.get("source", "external_llm"),
            "validation_mode": session.ai_result.get("validation_mode", "external_only"),
            "validated_by": session.ai_result.get("validated_by", []),
            "local_similar_count": session.ai_result.get("local_similar_count", 0),
        },
        "bridge_trace_id": session.bridge_trace_id,
    }


@pipeline_router.get("/history")
async def get_pipeline_history(limit: int = 50) -> dict:
    """Return recent pipeline sessions for the Command Center history view."""
    sessions = sorted(_sessions.values(), key=lambda s: s.created_at, reverse=True)[:limit]
    history = [
        {
            "session_id":   s.session_id,
            "service_name": s.service_name,
            "alert_name":   s.alert_name,
            "severity":     s.severity,
            "status":       s.status,
            "risk_score":   round(s.risk_score, 2),
            "risk_level":   s.risk_level,
            "created_at":   s.created_at,
            "ticket_num":   s.ticket_num,
            "domain":       "storage",
        }
        for s in sessions
    ]
    return {"history": history, "count": len(history)}


@pipeline_router.get("/events")
async def pipeline_events(request: Request):
    """SSE stream — emits a JSON event whenever the latest session stage changes."""
    import json as _json

    async def _stream():
        last_status = None
        while True:
            if await request.is_disconnected():
                break
            session = max(_sessions.values(), key=lambda s: s.created_at) if _sessions else None
            if session:
                current = session.status
                if current != last_status:
                    payload = _json.dumps({
                        "session_id": session.session_id,
                        "status": current,
                        "service_name": session.service_name,
                        "severity": session.severity,
                        "timestamp": time.time(),
                    })
                    yield f"data: {payload}\n\n"
                    last_status = current
                else:
                    yield ": heartbeat\n\n"
            else:
                yield ": heartbeat\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(_stream(), media_type="text/event-stream")


def init_pipeline(app) -> None:
    """Register the pipeline router with the FastAPI app."""
    app.include_router(pipeline_router)
