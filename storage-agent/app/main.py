"""
storage-agent/app/main.py
──────────────────────────────────────────────────────────────────────────────
Storage AIOps Agent — FastAPI service.

Receives Alertmanager webhooks for storage alerts (domain=storage),
runs a 6-step agent pipeline, and exposes Prometheus metrics.

Endpoints
─────────
  GET  /health                          — liveness probe
  GET  /metrics                         — Prometheus scrape endpoint
  POST /webhook                         — Alertmanager webhook receiver
  POST /approval/{session_id}/decision  — human approval callback
  GET  /approvals/pending               — list pending approval requests

Pipeline (called by xyOps workflow nodes):
  POST /pipeline/start
  POST /pipeline/agent/storage-metrics
  POST /pipeline/agent/logs
  POST /pipeline/agent/analyze
  POST /pipeline/agent/ticket
  POST /pipeline/agent/approval

Ports
─────
  9001  HTTP (obs-net internal + host)
──────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, status
from opentelemetry import trace
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, REGISTRY
from pydantic import BaseModel

from .pipeline import init_pipeline, _sessions
from .storage_analyst import AI_ENABLED
from .telemetry import (
    alert_processing_histogram,
    get_tracer,
    setup_telemetry,
    storage_agent_webhook_received_total,
    storage_agent_alert_processing_seconds,
)
from .xyops_provisioner import ensure_storage_workflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("storage-agent")

# ── FastAPI app ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(application: FastAPI):
    """Startup: provision xyOps storage workflow."""
    xyops_url = os.getenv("XYOPS_URL", "http://xyops:5522")
    xyops_api_key = os.getenv("XYOPS_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if xyops_api_key:
        headers["X-API-Key"] = xyops_api_key

    try:
        async with httpx.AsyncClient() as http:
            async def _post(path: str, body: dict) -> dict:
                try:
                    r = await http.post(f"{xyops_url}{path}", json=body, headers=headers, timeout=10.0)
                    return r.json()
                except Exception as e:
                    return {"error": str(e)}

            async def _get(path: str) -> dict:
                try:
                    r = await http.get(f"{xyops_url}{path}", headers=headers, timeout=10.0)
                    return r.json()
                except Exception as e:
                    return {"error": str(e)}

            await ensure_storage_workflow(_post, _get)
    except Exception as exc:
        logger.warning("xyOps provisioning skipped (xyOps may not be ready): %s", exc)

    logger.info("Storage agent ready  port=9001  ai_enabled=%s", AI_ENABLED)
    yield


app = FastAPI(
    title="Storage AIOps Agent",
    description="Receives Alertmanager storage alerts, runs AI analysis, and creates xyOps tickets.",
    version="1.0.0",
    lifespan=lifespan,
)

# ── Bootstrap OTel (before routes) ────────────────────────────────────────────
setup_telemetry(fastapi_app=app, service_name="storage-agent")
tracer = get_tracer()

# ── Register pipeline endpoints ────────────────────────────────────────────────
init_pipeline(app)

# ── Config ─────────────────────────────────────────────────────────────────────
XYOPS_URL: str = os.getenv("XYOPS_URL", "http://xyops:5522")
XYOPS_API_KEY: str = os.getenv("XYOPS_API_KEY", "")
BRIDGE_INTERNAL_URL: str = os.getenv("BRIDGE_INTERNAL_URL", "http://storage-agent:9001")
OBS_INTELLIGENCE_URL: str = os.getenv("OBS_INTELLIGENCE_URL", "http://obs-intelligence:9100")
GRAFANA_EXTERNAL_URL: str = os.getenv("GRAFANA_EXTERNAL_URL", "http://localhost:3001")

# ── Shared HTTP client ─────────────────────────────────────────────────────────
_http: httpx.AsyncClient | None = None


def _xyops_headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if XYOPS_API_KEY:
        h["X-API-Key"] = XYOPS_API_KEY
    return h


async def _xyops_post(path: str, body: dict) -> dict:
    if not _http:
        return {"error": "http client not ready"}
    try:
        resp = await _http.post(
            f"{XYOPS_URL}{path}", json=body, headers=_xyops_headers(), timeout=10.0
        )
        return resp.json()
    except Exception as exc:
        logger.warning("xyOps POST %s failed: %s", path, exc)
        return {"error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# HTTP client lifecycle (separate from lifespan — reused across requests)
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def _startup():
    global _http
    _http = httpx.AsyncClient()


@app.on_event("shutdown")
async def _shutdown():
    if _http:
        await _http.aclose()


# ═══════════════════════════════════════════════════════════════════════════════
# Health + Metrics
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "service": "storage-agent",
        "xyops_url": XYOPS_URL,
        "ai_enabled": AI_ENABLED,
        "active_sessions": len(_sessions),
    }


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus scrape endpoint — exposes storage agent action counters."""
    return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


# ═══════════════════════════════════════════════════════════════════════════════
# Approval callback
# ═══════════════════════════════════════════════════════════════════════════════

class ApprovalDecision(BaseModel):
    approved: bool
    decided_by: str = "unknown"
    notes: str = ""


@app.post("/approval/{session_id}/decision")
async def approval_decision(session_id: str, decision: ApprovalDecision) -> dict:
    """
    Called by a human (or xyOps event) to approve or decline a storage remediation.
    session_id matches the pipeline session / service_name used in the pipeline.
    """
    from .pipeline import _sessions, _run_playbook, _increment_action_counter
    from .telemetry import storage_agent_autonomous_remediations_total

    session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"No session: {session_id}")

    if session.status not in ("awaiting_approval",):
        return {"status": session.status, "message": "Not awaiting approval"}

    action = session.ai_result.get("recommended_action", "unknown")

    async with httpx.AsyncClient() as http:
        if decision.approved:
            # Gate: reject approval if playbook validation failed
            if not session.validation_passed:
                return {
                    "status": "validation_failed",
                    "session_id": session_id,
                    "message": "Cannot approve — playbook validation failed. Fix the playbook first.",
                    "validation_result": session.validation_result,
                }

            logger.info("Approval GRANTED by %s for session=%s action=%s", decision.decided_by, session_id, action)
            session.status = "executing"
            _increment_action_counter(action)
            storage_agent_autonomous_remediations_total.inc()

            await http.post(
                f"{XYOPS_URL}/api/app/add_ticket_change/v1",
                json={"id": session.ticket_id, "change": {
                    "type": "comment",
                    "body": f"[OK] **Approved by {decision.decided_by}** — executing `{action}` playbook",
                }},
                headers=_xyops_headers(),
                timeout=10.0,
            )
            await _run_playbook(session, http)
            session.status = "executed"
        else:
            logger.info("Approval DECLINED by %s for session=%s", decision.decided_by, session_id)
            session.status = "declined"
            await http.post(
                f"{XYOPS_URL}/api/app/add_ticket_change/v1",
                json={"id": session.ticket_id, "change": {
                    "type": "comment",
                    "body": f"[!!] **Declined by {decision.decided_by}** — remediation will NOT execute. Notes: {decision.notes}",
                }},
                headers=_xyops_headers(),
                timeout=10.0,
            )

    return {"status": session.status, "session_id": session_id, "decided_by": decision.decided_by}


@app.get("/approvals/pending")
async def list_pending_approvals() -> dict:
    pending = [
        {
            "session_id": s.session_id,
            "alert_name": s.alert_name,
            "service_name": s.service_name,
            "severity": s.severity,
            "action": s.ai_result.get("recommended_action"),
            "rca_summary": s.ai_result.get("rca_summary", ""),
            "ansible_playbook": s.ai_result.get("ansible_playbook", ""),
            "validation_passed": s.validation_passed,
            "validation_result": s.validation_result,
            "risk_score": s.risk_score,
            "created_at": s.created_at,
        }
        for s in _sessions.values()
        if s.status == "awaiting_approval"
    ]
    return {"count": len(pending), "items": pending}


# ═══════════════════════════════════════════════════════════════════════════════
# Predictive alert endpoint  (called by obs-intelligence background loop)
# ═══════════════════════════════════════════════════════════════════════════════

class PredictiveAlertPayload(BaseModel):
    service_name: str
    domain: str = "storage"
    scenario_id: str
    risk_score: float
    confidence: float
    description: str = ""
    forecast_breach_minutes: int = 0
    anomaly_metric: str = ""
    anomaly_z_score: float = 0.0


@app.post("/predictive-alert", status_code=status.HTTP_202_ACCEPTED)
async def predictive_alert(payload: PredictiveAlertPayload) -> dict:
    """
    Receive a pre-alert signal from obs-intelligence and create a [PREDICTIVE]
    approval-gated xyOps ticket before a real Prometheus alert fires.

    Always approval-gated regardless of STORAGE_REQUIRE_APPROVAL setting.
    """
    from .telemetry import storage_agent_predictive_incidents_total

    logger.info(
        "Predictive alert received  service=%s  scenario=%s  risk=%.2f  confidence=%.2f",
        payload.service_name, payload.scenario_id, payload.risk_score, payload.confidence,
    )

    forecast_note = (
        f" Forecast breach in **{payload.forecast_breach_minutes} min**."
        if payload.forecast_breach_minutes > 0 else ""
    )
    anomaly_note = (
        f" Anomaly Z-score on `{payload.anomaly_metric}`: **{payload.anomaly_z_score:.2f}**."
        if payload.anomaly_metric else ""
    )

    body = (
        f"## [PREDICTIVE] Pre-Alert Intelligence — Storage Agent\n\n"
        f"> This ticket was raised by the Obs-Intelligence Engine **before** a "
        f"Prometheus alert fired. A human must approve any action.\n\n"
        f"| Field | Value |\n|---|---|\n"
        f"| **Service** | `{payload.service_name}` |\n"
        f"| **Scenario** | `{payload.scenario_id}` |\n"
        f"| **Risk Score** | `{payload.risk_score:.2f}` |\n"
        f"| **Confidence** | `{payload.confidence:.2f}` |\n"
        f"| **Dashboard** | [Agentic AI Overview](" + GRAFANA_EXTERNAL_URL + "/d/agentic-ai-overview) |\n\n"
        f"### Intelligence Signals\n\n"
        f"{payload.description}{forecast_note}{anomaly_note}\n\n"
        f"### Recommended Action\n\n"
        f"Review scenario `{payload.scenario_id}` playbook. "
        f"**Approval required before any remediation executes.**"
    )

    create_payload = {
        "subject": (
            f"[PREDICTIVE] {payload.scenario_id} risk on {payload.service_name} "
            f"[risk={payload.risk_score:.2f} confidence={payload.confidence:.2f}]"
        ),
        "body": body,
        "type": "issue",
        "status": "open",
    }

    if not _http:
        return {"status": "error", "detail": "http client not ready"}

    result = await _xyops_post("/api/app/create_ticket/v1", create_payload)
    ticket_id = result.get("ticket", {}).get("id", "")
    ticket_num = result.get("ticket", {}).get("num", 0)

    if not result.get("error"):
        storage_agent_predictive_incidents_total.inc()
        logger.info(
            "Predictive ticket created #%s (%s)  service=%s  scenario=%s",
            ticket_num, ticket_id, payload.service_name, payload.scenario_id,
        )
    else:
        logger.warning("Failed to create predictive ticket: %s", result.get("error"))

    return {
        "status": "accepted",
        "ticket_id": ticket_id,
        "ticket_num": ticket_num,
        "service_name": payload.service_name,
        "scenario_id": payload.scenario_id,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Alertmanager webhook receiver
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/webhook", status_code=status.HTTP_200_OK)
async def alertmanager_webhook(request: Request) -> dict:
    """
    Receive Alertmanager webhook for storage domain alerts.
    For each firing alert, triggers the storage pipeline via /pipeline/start
    in the background (non-blocking, so Alertmanager always gets 200).
    """
    payload: dict[str, Any] = await request.json()
    group_status: str = payload.get("status", "unknown")
    alerts: list[dict] = payload.get("alerts", [])

    storage_agent_webhook_received_total.labels(group_status=group_status).inc()

    logger.info("Storage webhook received  status=%s  alerts=%d", group_status, len(alerts))

    for alert in alerts:
        alert_name: str = alert["labels"].get("alertname", "unknown")
        service_name: str = alert["labels"].get(
            "service_name", alert["labels"].get("job", "storage-simulator")
        )
        severity: str = alert["labels"].get("severity", "warning")
        alert_status: str = alert.get("status", group_status)
        summary: str = alert.get("annotations", {}).get("summary", alert_name)
        description: str = alert.get("annotations", {}).get("description", "")
        dashboard_url: str = alert.get("annotations", {}).get(
            "dashboard_url", GRAFANA_EXTERNAL_URL + "/d/agentic-ai-overview"
        )

        if alert_status == "firing":
            # Fire-and-forget: kick off the pipeline without blocking Alertmanager
            asyncio.create_task(
                _run_storage_pipeline(alert_name, service_name, severity, summary, description, dashboard_url)
            )
        else:
            logger.info("Storage alert resolved: %s / %s", alert_name, service_name)
            # Record the outcome in obs-intelligence for the SRE Incident Timeline.
            asyncio.create_task(
                _record_obs_intelligence_outcome(alert_name, service_name, "resolved")
            )

    return {"status": "ok", "received": len(alerts)}


async def _record_obs_intelligence_outcome(
    scenario_id: str,
    service_name: str,
    outcome: str,
) -> None:
    """Fire-and-forget: notify obs-intelligence of an alert resolution outcome."""
    try:
        async with httpx.AsyncClient() as http:
            await http.post(
                f"{OBS_INTELLIGENCE_URL}/intelligence/record-outcome",
                json={
                    "scenario_id": scenario_id,
                    "outcome": outcome,
                    "service_name": service_name,
                    "domain": "storage",
                },
                timeout=5.0,
            )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not record outcome in obs-intelligence: %s", exc)


async def _run_storage_pipeline(
    alert_name: str,
    service_name: str,
    severity: str,
    summary: str,
    description: str,
    dashboard_url: str,
) -> None:
    """Drive the 6-step storage pipeline end-to-end (background task)."""
    t_start = time.perf_counter()
    base = BRIDGE_INTERNAL_URL

    async with httpx.AsyncClient() as http:
        try:
            # Step 1: start
            r1 = await http.post(f"{base}/pipeline/start", json={
                "service_name": service_name,
                "alert_name": alert_name,
                "severity": severity,
                "summary": summary,
                "description": description,
                "dashboard_url": dashboard_url,
            }, timeout=30.0)
            r1.raise_for_status()

            body = {"session_id": service_name}
            for path in [
                "/pipeline/agent/storage-metrics",
                "/pipeline/agent/logs",
                "/pipeline/agent/analyze",
                "/pipeline/agent/ticket",
                "/pipeline/agent/approval",
            ]:
                r = await http.post(f"{base}{path}", json=body, timeout=120.0)
                r.raise_for_status()

            elapsed = time.perf_counter() - t_start
            storage_agent_alert_processing_seconds.observe(elapsed)
            logger.info("Storage pipeline complete  alert=%s  elapsed=%.2fs", alert_name, elapsed)

        except Exception as exc:
            elapsed = time.perf_counter() - t_start
            storage_agent_alert_processing_seconds.observe(elapsed)
            logger.error("Storage pipeline failed for %s: %s", alert_name, exc)
