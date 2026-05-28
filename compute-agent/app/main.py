"""
compute-agent/app/main.py
────────────────────────────────────────────────────────────────
Compute Agent — Prometheus Alertmanager → xyOps Incident Bridge
────────────────────────────────────────────────────────────────

What this service does
──────────────────────
1. Exposes  POST /webhook  — receives Alertmanager webhook payloads
2. For each FIRING alert:
     - Creates an incident ticket in xyOps REST API
     - Tags the ticket with service name, alert name, severity
     - Embeds the Grafana dashboard URL and the OTel trace_id of the
       bridge span so you can trace the exact moment the incident was created
3. For each RESOLVED alert:
     - Searches xyOps for open tickets matching that alert+service
     - Closes them with a resolution timestamp and trace_id

All HTTP calls to xyOps are made with httpx and are auto-instrumented
by HTTPXClientInstrumentor, so they appear as child spans in Tempo
under the same trace as the incoming Alertmanager webhook request.

Alertmanager webhook payload format (v4):
  {
    "version":   "4",
    "groupKey":  "<group key>",
    "status":    "firing" | "resolved",
    "receiver":  "xyops-incidents",
    "groupLabels":    { "alertname": "...", "service_name": "..." },
    "commonLabels":   { "severity": "warning", ... },
    "commonAnnotations": { "summary": "...", "description": "...", ... },
    "externalURL":    "http://alertmanager:9093",
    "alerts": [
      {
        "status":      "firing",
        "labels":      { "alertname": "HighErrorRate", "service_name": "frontend-api", ... },
        "annotations": { "summary": "...", "description": "...", "dashboard_url": "..." },
        "startsAt":    "2026-03-16T10:00:00Z",
        "endsAt":      "0001-01-01T00:00:00Z",
        "generatorURL":"http://prometheus:9090/..."
      }
    ]
  }

Environment variables
─────────────────────
  OTEL_SERVICE_NAME            compute-agent
  OTEL_EXPORTER_OTLP_ENDPOINT  http://otel-collector:4317
  OTEL_EXPORTER_OTLP_PROTOCOL  grpc
  XYOPS_URL                    http://xyops:5522
  XYOPS_API_KEY                (optional) API key for xyOps auth
  CLAUDE_API_KEY               Anthropic API key for AI enrichment
  CLAUDE_MODEL                 claude-3-5-haiku-20241022 (default)
  LOKI_URL                     http://loki:3100
  PROMETHEUS_URL               http://prometheus:9090
  GITHUB_REPO                  owner/repo for PR suggestions (optional)
  NOTIFY_EMAIL                 comma-separated email addresses for ticket notify
  ANSIBLE_RUNNER_URL           http://ansible-runner:8080 (optional)
"""

import asyncio
import logging
import os
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry import trace
from opentelemetry.trace.status import Status, StatusCode
from pydantic import BaseModel

from .ai_analyst import (
    AI_ENABLED,
    build_enriched_ticket_body,
    fetch_loki_logs,
    fetch_prometheus_context,
    generate_ai_analysis,
    generate_local_llm_analysis,
    deterministic_analysis,
    get_notify_list,
)
from .approval_workflow import (
    ApprovalRequest,
    execute_autonomous,
    get_pending,
    list_pending,
    process_decision,
    request_approval,
)
from .approval_history import history_store
from .tier_registry import (
    get_service_tier,
    get_tier_policy,
    list_all_tiers,
    reload_overrides,
)
from .autonomy_engine import check_autonomy
from .telemetry import (
    alert_processing_histogram,
    get_tracer,
    incident_counter,
    setup_telemetry,
    webhook_counter,
)
from .xyops_client import (
    TOTAL_STEPS,
    ensure_aiops_workflow,
    post_step_comment,
)
from .pipeline import init_pipeline, pipeline_router

# ── Logging (basic setup before OTel; enrichment happens inside setup_telemetry) ──
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("compute-agent")

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="AIOps Bridge",
    description=(
        "Receives Prometheus Alertmanager webhooks and creates/resolves "
        "incident tickets in xyOps. Fully OTel-instrumented."
    ),
    version="1.0.0",
)

# ── CORS middleware ───────────────────────────────────────────────────────────
# Allow browser access from ui-backend and command-center
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3005", "http://localhost:3500", "http://localhost:9005", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Bootstrap OTel (pass app so FastAPIInstrumentor can wrap it) ───────────────
setup_telemetry(fastapi_app=app, service_name="compute-agent")
tracer = get_tracer()

# ── Register agent-to-agent pipeline endpoints ────────────────────────────────
app.include_router(pipeline_router)

# ── Config from environment ────────────────────────────────────────────────────
XYOPS_URL: str = os.getenv("XYOPS_URL", "http://xyops:5522")
XYOPS_API_KEY: str = os.getenv("XYOPS_API_KEY", "")
REQUIRE_APPROVAL: bool = os.getenv("REQUIRE_APPROVAL", "true").lower() != "false"
# Only send to approval gate if severity >= this value
APPROVAL_SEVERITY_THRESHOLD: set[str] = {"warning", "critical"}
OBS_INTELLIGENCE_URL: str = os.getenv("OBS_INTELLIGENCE_URL", "http://obs-intelligence:9100")
LOCAL_LLM_MODEL: str = os.getenv("LOCAL_LLM_MODEL", "qwen3.5")


# ═══════════════════════════════════════════════════════════════════════════════
# Health endpoint
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health() -> dict:
    """Liveness probe — excluded from traces by OTel instrumentation."""
    return {
        "status": "ok",
        "service": "compute-agent",
        "xyops_url": XYOPS_URL,
        "ai_enabled": AI_ENABLED,
        "approval_required": REQUIRE_APPROVAL,
    }


@app.get("/metrics")
async def metrics() -> Response:
    """
    Prometheus scrape endpoint — exposes compute agent action counters.

    Scraped by Prometheus job 'compute-agent' (prometheus.yml).
    Feeds the 'Agentic AI Operations' Grafana dashboard.

    Metrics exposed:
      compute_agent_actions_total{action_type}
      compute_agent_restarts_total
      compute_agent_noisy_neighbour_reductions_total
      compute_agent_escalations_total
      compute_agent_autonomous_actions_total
      compute_agent_approval_required_total
      compute_agent_ai_analysis_total{status}
      compute_agent_webhook_received_total{group_status}
      compute_agent_alert_processing_seconds (histogram)
    """
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, REGISTRY
    return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


# ═══════════════════════════════════════════════════════════════════════════════
# Predictive alert endpoint  (called by obs-intelligence background loop)
# ═══════════════════════════════════════════════════════════════════════════════

class PredictiveAlertPayload(BaseModel):
    service_name: str
    domain: str = "compute"
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

    Always approval-gated regardless of REQUIRE_APPROVAL setting.
    """
    from .telemetry import compute_agent_predictive_incidents_total

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
        f"## [PREDICTIVE] Pre-Alert Intelligence — Compute Agent\n\n"
        f"> This ticket was raised by the Obs-Intelligence Engine **before** a "
        f"Prometheus alert fired. A human must approve any action.\n\n"
        f"| Field | Value |\n|---|---|\n"
        f"| **Service** | `{payload.service_name}` |\n"
        f"| **Scenario** | `{payload.scenario_id}` |\n"
        f"| **Risk Score** | `{payload.risk_score:.2f}` |\n"
        f"| **Confidence** | `{payload.confidence:.2f}` |\n"
        f"| **Dashboard** | [Agentic AI Overview](http://grafana:3000/d/agentic-ai-overview) |\n\n"
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
    notify = get_notify_list("warning")
    if notify:
        create_payload["notify"] = notify

    result = await _xyops_post("/api/app/create_ticket/v1", create_payload)
    ticket_id = result.get("ticket", {}).get("id", "")
    ticket_num = result.get("ticket", {}).get("num", 0)

    if not result.get("error"):
        compute_agent_predictive_incidents_total.inc()
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
# Human approval endpoints
# ═══════════════════════════════════════════════════════════════════════════════

class ApprovalDecision(BaseModel):
    approved: bool
    decided_by: str = "unknown"
    notes: str = ""


@app.post("/approval/{approval_id}/decision")
async def approval_decision(approval_id: str, decision: ApprovalDecision) -> dict:
    """
    Called by a human (or xyOps job event) to approve or decline a remediation.
    See approval_workflow.py for full flow documentation.
    """
    if not _http:
        raise HTTPException(status_code=503, detail="Service not ready")
    result = await process_decision(
        approval_id=approval_id,
        approved=decision.approved,
        decided_by=decision.decided_by,
        notes=decision.notes,
        http=_http,
        xyops_post=_xyops_post,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.get("/approval/{approval_id}")
async def get_approval(approval_id: str) -> dict:
    """Return current state of a pending approval request."""
    req = get_pending(approval_id)
    if not req:
        raise HTTPException(status_code=404, detail=f"No approval found: {approval_id}")
    return {
        "approval_id": req.approval_id,
        "status": req.status,
        "alert_name": req.alert_name,
        "service_name": req.service_name,
        "severity": req.severity,
        "created_at": req.created_at,
        "decided_by": req.decided_by,
        "decided_at": req.decided_at,
        "incident_ticket_id": req.incident_ticket_id,
        "approval_ticket_id": req.approval_ticket_id,
    }


@app.get("/approvals/pending")
async def list_pending_approvals() -> dict:
    """List all approvals awaiting human decision."""
    pending = list_pending()
    return {
        "count": len(pending),
        "items": [
            {
                "approval_id": r.approval_id,
                "session_id": r.session_id,
                "alert_name": r.alert_name,
                "service_name": r.service_name,
                "severity": r.severity,
                "created_at": r.created_at,
                "approval_ticket_id": r.approval_ticket_id,
                "rca_summary": r.rca_summary,
                "ansible_playbook": r.ansible_playbook,
                "ansible_description": r.ansible_description,
                "validation_passed": r.validation_passed,
                "validation_result": r.validation_result,
                "gitea_pr_url": r.gitea_pr_url,
                "gitea_pr_num": r.gitea_pr_num,
                "risk_score": r.risk_score,
                "env_tier": r.env_tier,
                "action_type": r.action_type,
            }
            for r in pending
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Autonomy status + history endpoints
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/autonomy/status/{service_name}")
async def autonomy_status(service_name: str, action_type: str = "", risk_score: float = 0.0) -> dict:
    """
    Return the current autonomy status for a service.

    If action_type is provided, also runs a live autonomy check to show
    whether the next execution of that action would be autonomous or gated.

    GET /autonomy/status/frontend-api?action_type=restart_service&risk_score=0.3

    Returns tier, policy, trust history, and a live autonomy decision.
    """
    tier = get_service_tier(service_name)
    policy = get_tier_policy(tier)

    # Summarise all known (action_type) combinations for this service
    all_records = history_store.get_history(service_name, action_type or "", 365)
    action_types_seen: set[str] = {r.action_type for r in history_store._records if r.service_name == service_name}  # noqa: SLF001

    trust_by_action: dict[str, dict] = {}
    for at in action_types_seen:
        ts = history_store.compute_trust_score(
            service_name=service_name,
            action_type=at,
            env_tier=tier.value,
            min_approvals=policy.min_approvals_for_autonomy,
            min_success_rate=policy.min_success_rate,
            window_days=policy.history_window_days,
        )
        trust_by_action[at] = {
            "total_decisions": ts.total_decisions,
            "approved": ts.approved_count,
            "declined": ts.declined_count,
            "autonomous": ts.autonomous_count,
            "success_rate": round(ts.success_rate, 3),
            "autonomy_eligible": ts.autonomy_eligible,
            "reason": ts.reason,
        }

    # Live decision for a specific action+risk (optional)
    live_decision: dict | None = None
    if action_type:
        decision = check_autonomy(
            service_name=service_name,
            action_type=action_type,
            risk_score=risk_score,
        )
        live_decision = decision.as_dict()

    service_records = [r for r in history_store._records if r.service_name == service_name]  # noqa: SLF001
    approvals_recorded = sum(1 for r in service_records if r.decision in ("approved", "autonomous"))
    executed_records = [r for r in service_records if r.execution_outcome in ("success", "failure")]
    success_count = sum(1 for r in executed_records if r.execution_outcome == "success")
    success_rate = (success_count / len(executed_records)) if executed_records else 0.0
    next_tier = {
        "name": f"{tier.value}_auto",
        "approvals_needed": max(policy.min_approvals_for_autonomy - approvals_recorded, 0),
        "success_rate_needed": policy.min_success_rate,
    }

    return {
        "service_name": service_name,
        "tier": tier.value,
        "approvals_recorded": approvals_recorded,
        "success_rate": round(success_rate, 3),
        "next_tier": next_tier,
        "policy": {
            "min_approvals_for_autonomy": policy.min_approvals_for_autonomy,
            "min_success_rate": policy.min_success_rate,
            "risk_ceiling": policy.risk_ceiling,
            "auto_merge_pr": policy.auto_merge_pr,
            "history_window_days": policy.history_window_days,
            "description": policy.description,
        },
        "trust_by_action": trust_by_action,
        "live_decision": live_decision,
    }


@app.get("/autonomy/history")
async def autonomy_history(window_days: int = 90) -> dict:
    """
    Return a summary of the approval history store.

    GET /autonomy/history?window_days=30

    Useful for dashboards and audits.
    """
    return history_store.get_summary(window_days=window_days)


@app.get("/autonomy/tiers")
async def autonomy_tiers() -> dict:
    """
    List all known services with their tier and policy.

    Includes overrides from SERVICE_TIER_MAP_JSON env var and
    /data/service_tiers.json config file.
    """
    return {
        "tiers": list_all_tiers(),
        "note": (
            "Override via SERVICE_TIER_MAP_JSON env var or /data/service_tiers.json. "
            "Unknown services default to production (safest tier)."
        ),
    }


@app.post("/autonomy/tiers/reload")
async def autonomy_tiers_reload() -> dict:
    """
    Force a reload of service tier overrides from disk / env.
    Useful after updating /data/service_tiers.json without restarting the container.
    """
    overrides = reload_overrides()
    return {"status": "reloaded", "override_count": len(overrides)}


# ═══════════════════════════════════════════════════════════════════════════════
# Alertmanager webhook receiver
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/webhook", status_code=status.HTTP_200_OK)
async def alertmanager_webhook(request: Request) -> dict:
    """
    Receive Alertmanager webhook and fan out to xyOps.

    Returns a summary of actions taken so Alertmanager can log it.
    Alertmanager retries on non-2xx, so we always return 200 even if
    xyOps is temporarily unavailable (we log the error instead of failing).
    """
    t_start = time.perf_counter()
    payload: dict[str, Any] = await request.json()

    group_status: str = payload.get("status", "unknown")
    alerts: list[dict] = payload.get("alerts", [])

    # Record the raw webhook receipt
    if webhook_counter is not None:
        webhook_counter.add(1, {"group_status": group_status})

    # Also record in prometheus-client counter (for direct scrape endpoint)
    from .telemetry import compute_agent_webhook_received_total
    compute_agent_webhook_received_total.labels(group_status=group_status).inc()

    current_span = trace.get_current_span()
    current_span.set_attribute("alertmanager.group_status", group_status)
    current_span.set_attribute("alertmanager.alert_count", len(alerts))
    current_span.set_attribute(
        "alertmanager.receiver",
        payload.get("receiver", ""),
    )

    logger.info(
        "Alertmanager webhook received  status=%s  alerts=%d",
        group_status,
        len(alerts),
    )

    results: list[dict] = []

    for alert in alerts:
        alert_name: str = alert["labels"].get("alertname", "unknown")
        service_name: str = alert["labels"].get(
            "service_name",
            alert["labels"].get("job", "unknown"),
        )
        severity: str = alert["labels"].get("severity", "warning")
        alert_status: str = alert.get("status", group_status)

        summary: str = alert.get("annotations", {}).get("summary", alert_name)
        description: str = alert.get("annotations", {}).get("description", "")
        dashboard_url: str = alert.get("annotations", {}).get(
            "dashboard_url", "http://grafana:3000"
        )
        starts_at: str = alert.get("startsAt", "")

        # Each alert gets its own child span so you can see individual
        # alert processing time in the Tempo waterfall.
        with tracer.start_as_current_span(
            f"process_alert.{alert_name}",
            kind=trace.SpanKind.INTERNAL,
        ) as span:
            span.set_attribute("alert.name", alert_name)
            span.set_attribute("alert.status", alert_status)
            span.set_attribute("alert.severity", severity)
            span.set_attribute("alert.service", service_name)
            span.set_attribute("alert.starts_at", starts_at)

            # Capture the bridge's own trace_id so we can embed it
            # in the xyOps ticket → click trace_id → see this span in Tempo
            ctx = trace.get_current_span().get_span_context()
            bridge_trace_id: str = format(ctx.trace_id, "032x") if ctx.is_valid else ""
            span.set_attribute("bridge.trace_id", bridge_trace_id)

            t_alert = time.perf_counter()
            try:
                if alert_status == "firing":
                    result = await _create_xyops_ticket(
                        alert_name=alert_name,
                        service_name=service_name,
                        severity=severity,
                        summary=summary,
                        description=description,
                        dashboard_url=dashboard_url,
                        starts_at=starts_at,
                        bridge_trace_id=bridge_trace_id,
                    )
                    span.set_attribute("xyops.ticket_id", result.get("ticket_id", ""))
                    span.set_status(Status(StatusCode.OK))
                    results.append(
                        {"alert": alert_name, "action": "ticket_created", **result}
                    )
                    if incident_counter is not None:
                        incident_counter.add(
                            1,
                            {
                                "action": "created",
                                "service": service_name,
                                "severity": severity,
                            },
                        )

                elif alert_status == "resolved":
                    result = await _resolve_xyops_tickets(
                        alert_name=alert_name,
                        service_name=service_name,
                        bridge_trace_id=bridge_trace_id,
                    )
                    span.set_attribute(
                        "xyops.resolved_count",
                        len(result.get("resolved_ids", [])),
                    )
                    span.set_status(Status(StatusCode.OK))
                    results.append(
                        {"alert": alert_name, "action": "tickets_resolved", **result}
                    )
                    if incident_counter is not None:
                        incident_counter.add(
                            1,
                            {
                                "action": "resolved",
                                "service": service_name,
                                "severity": severity,
                            },
                        )

                else:
                    logger.warning(
                        "Unknown alert status '%s' for %s — skipped",
                        alert_status,
                        alert_name,
                    )
                    results.append({"alert": alert_name, "action": "skipped"})

            except Exception as exc:  # noqa: BLE001
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                logger.error(
                    "Failed to process alert %s: %s", alert_name, exc
                )
                results.append({"alert": alert_name, "action": "error", "detail": str(exc)})
            finally:
                elapsed = time.perf_counter() - t_alert
                if alert_processing_histogram is not None:
                    alert_processing_histogram.record(
                        elapsed,
                        {"alert": alert_name, "status": alert_status},
                    )

    total_elapsed = time.perf_counter() - t_start
    logger.info(
        "Alertmanager webhook processed  %d alerts in %.3fs", len(alerts), total_elapsed
    )
    return {"status": "processed", "total_alerts": len(alerts), "results": results}


# ═══════════════════════════════════════════════════════════════════════════════
# xyOps API helpers
# ═══════════════════════════════════════════════════════════════════════════════

# Shared httpx client — connection-pooled, reused across requests.
# HTTPXClientInstrumentor (set up in telemetry.py) instruments this
# automatically, emitting a CLIENT span for every request.
_http: httpx.AsyncClient | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _http
    _http = httpx.AsyncClient(
        base_url=XYOPS_URL,
        timeout=httpx.Timeout(10.0),
        headers=_xyops_headers(),
    )
    logger.info("AIOps Bridge started  xyops_url=%s", XYOPS_URL)
    # Wire pipeline agents to the shared http client
    init_pipeline(_http, _xyops_post)
    # Register the visual agent-to-agent workflow in xyOps — run as background
    # task with retries so a slow DNS startup doesn't cause permanent failure.
    async def _register_workflow() -> None:
        for attempt in range(12):  # retry up to ~60 s
            try:
                await ensure_aiops_workflow(_xyops_post, _xyops_get)
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Could not register AIOps workflow (attempt %d/12): %s",
                    attempt + 1, exc,
                )
                await asyncio.sleep(5)
        logger.error("Gave up registering AIOps workflow after 12 attempts")

    asyncio.create_task(_register_workflow())
    # Initialise Gitea (local git server for playbook PR approvals) — non-blocking background task
    try:
        from .git_client import ensure_gitea_setup
        asyncio.create_task(ensure_gitea_setup(_http))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Gitea setup failed (non-fatal): %s", exc)


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _http:
        await _http.aclose()
    logger.info("AIOps Bridge shutdown")


def _xyops_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if XYOPS_API_KEY:
        headers["X-API-Key"] = XYOPS_API_KEY
    return headers


async def _xyops_post(path: str, body: dict) -> dict:
    """
    POST to xyOps REST API.

    The httpx call is auto-instrumented by HTTPXClientInstrumentor,
    so it appears as a CLIENT span in Tempo nested under the current span.
    Returns the parsed JSON or an error dict on failure.
    """
    if _http is None:
        return {"error": "http client not ready"}
    try:
        resp = await _http.post(path, json=body)
        if resp.status_code >= 400:
            logger.warning(
                "xyOps API %s returned HTTP %d: %s",
                path,
                resp.status_code,
                resp.text[:300],
            )
            return {"error": f"HTTP {resp.status_code}", "detail": resp.text[:300]}
        return resp.json()
    except httpx.RequestError as exc:
        logger.error("xyOps API request failed for %s: %s", path, exc)
        return {"error": str(exc)}


async def _xyops_get(path: str) -> dict:
    """GET from xyOps REST API with the same instrumentation as _xyops_post."""
    if _http is None:
        return {"rows": [], "error": "http client not ready"}
    try:
        resp = await _http.get(path, headers=_xyops_headers())
        if resp.status_code >= 400:
            logger.warning(
                "xyOps API %s returned HTTP %d: %s",
                path,
                resp.status_code,
                resp.text[:300],
            )
            return {"rows": [], "error": f"HTTP {resp.status_code}"}
        return resp.json()
    except httpx.RequestError as exc:
        logger.error("xyOps API GET failed for %s: %s", path, exc)
        return {"rows": [], "error": str(exc)}


# ── xyOps ticket creation ──────────────────────────────────────────────────────

_SEVERITY_TO_PRIORITY = {"critical": "urgent", "warning": "normal", "info": "low"}


async def _create_xyops_ticket(
    alert_name: str,
    service_name: str,
    severity: str,
    summary: str,
    description: str,
    dashboard_url: str,
    starts_at: str,
    bridge_trace_id: str,
) -> dict:
    """
    Create an AI-enriched incident ticket in xyOps with live per-step
    progress comments posted to the ticket activity feed as each stage runs.

    Pipeline stages (visible live in the xyOps ticket activity feed):
      0. Create skeleton ticket immediately (so it exists to receive updates)
      1. Fetch Loki log context for the service
      2. Fetch Prometheus metrics for the service
      3. Claude AI root-cause analysis (or SKIPPED if key not set)
      4. Update ticket body with enriched content
      5. Create approval-gate ticket (or N/A if not required)

    Open the ticket in xyOps and watch the [>>]/[OK]/[--] comments
    appear in real time as each stage completes.
    """
    # ── 0. Create skeleton ticket immediately ───────────────────────────────
    # We need a ticket ID before the AI pipeline runs so we have somewhere
    # to post live step comments.  The body is a minimal stub that gets
    # replaced with the full enriched content in Step 4.
    skeleton_body = (
        f"## Automated Incident — AIOps Bridge\n\n"
        f"| Field | Value |\n|---|---|\n"
        f"| **Service** | `{service_name}` |\n"
        f"| **Alert** | `{alert_name}` |\n"
        f"| **Severity** | `{severity.upper()}` |\n"
        f"| **Detected at** | {starts_at} |\n"
        f"| **Dashboard** | [{dashboard_url}]({dashboard_url}) |\n"
        f"| **OTel Trace** | `{bridge_trace_id}` — paste in Grafana → Tempo |\n\n"
        f"*AI pipeline starting — watch the activity feed below for live progress updates.*"
    )
    notify = get_notify_list(severity)
    create_payload = {
        "subject": f"[AIOPS] {alert_name} on {service_name} [{severity.upper()}]: {summary}",
        "body": skeleton_body,
        "type": "issue",
        "status": "open",
    }
    if notify:
        create_payload["notify"] = notify

    create_result = await _xyops_post("/api/app/create_ticket/v1", create_payload)
    if create_result.get("error"):
        logger.warning(
            "Failed to create xyOps ticket  alert=%s  service=%s  error=%s",
            alert_name, service_name,
            create_result.get("detail", create_result.get("error")),
        )
        return {"ticket_id": "", "xyops_response": create_result}

    ticket_id: str = create_result.get("ticket", {}).get("id", "")
    ticket_num: int = create_result.get("ticket", {}).get("num", 0)
    logger.info(
        "Created skeleton xyOps ticket #%s (%s)  alert=%s  service=%s",
        ticket_num, ticket_id, alert_name, service_name,
    )

    # ── 1 + 2. Fetch Loki logs AND Prometheus metrics in parallel ───────────
    # Match the diagram: both context sources fetched simultaneously.
    # Post both "started" comments together, then gather, then post "done".
    await asyncio.gather(
        post_step_comment(
            ticket_id, 1, "started",
            f"Fetching log context for **{service_name}** (last 50 lines)...",
            _xyops_post,
        ),
        post_step_comment(
            ticket_id, 2, "started",
            f"Fetching Prometheus golden signals for **{service_name}**...",
            _xyops_post,
        ),
    )

    if _http:
        logs, metrics = await asyncio.gather(
            fetch_loki_logs(service_name, _http),
            fetch_prometheus_context(service_name, _http),
        )
    else:
        logs: str = ""
        metrics: dict[str, Any] = {}

    log_lines = logs.count("\n") + (1 if logs.strip() else 0)
    warn_count = logs.upper().count("WARN")
    m_parts: list[str] = []
    if metrics.get("error_rate_pct") not in (None, "no data"):
        m_parts.append(f"error_rate={metrics['error_rate_pct']}%")
    if metrics.get("p99_latency_ms") not in (None, "no data"):
        m_parts.append(f"p99={metrics['p99_latency_ms']}ms")
    if metrics.get("rps") not in (None, "no data"):
        m_parts.append(f"rps={metrics['rps']}")
    metrics_str = "  ".join(m_parts) if m_parts else "no metrics data"

    await asyncio.gather(
        post_step_comment(
            ticket_id, 1, "done",
            f"Retrieved **{log_lines}** log lines ({warn_count} WARN events)",
            _xyops_post,
        ),
        post_step_comment(
            ticket_id, 2, "done",
            f"Metrics snapshot: `{metrics_str}`",
            _xyops_post,
        ),
    )

    # ── 3. AI root-cause analysis ────────────────────────────────────────────
    analysis: dict[str, Any] = {}
    if AI_ENABLED and _http:
        await post_step_comment(
            ticket_id, 3, "started",
            "Calling **Claude AI** for root cause analysis...",
            _xyops_post,
        )
        analysis = await generate_ai_analysis(
            alert_name=alert_name,
            service_name=service_name,
            severity=severity,
            description=description,
            logs=logs,
            metrics=metrics,
            http=_http,
        )
        confidence = analysis.get("confidence", "?")   # Claude returns "high"|"medium"|"low"
        rca_words = len(analysis.get("rca_summary", "").split())
        await post_step_comment(
            ticket_id, 3, "done",
            f"RCA complete — confidence: **{confidence}** | {rca_words}-word analysis",
            _xyops_post,
        )
        # Fallback tier 2: local LLM (Ollama) if cloud AI returned nothing
        if not analysis.get("ansible_playbook"):
            logger.info(
                "Cloud AI returned no playbook — trying local LLM fallback  "
                "alert=%s  service=%s", alert_name, service_name
            )
            await post_step_comment(
                ticket_id, 3, "started",
                "Cloud AI unavailable — trying **local LLM** (Ollama)...",
                _xyops_post,
            )
            analysis = await generate_local_llm_analysis(
                alert_name=alert_name,
                service_name=service_name,
                severity=severity,
                description=description,
                logs=logs,
                metrics=metrics,
                http=_http,
            )
            if analysis.get("ansible_playbook"):
                confidence = analysis.get("confidence", "low")
                await post_step_comment(
                    ticket_id, 3, "done",
                    f"Local LLM analysis complete — confidence: **{confidence}** (Ollama/{LOCAL_LLM_MODEL})",
                    _xyops_post,
                )

        # Fallback tier 3: deterministic scenario catalog
        if not analysis.get("ansible_playbook"):
            logger.info(
                "Local LLM returned no playbook — falling back to deterministic analysis  "
                "alert=%s  service=%s", alert_name, service_name
            )
            analysis = deterministic_analysis(
                alert_name=alert_name,
                service_name=service_name,
                severity=severity,
            )
            if analysis.get("ansible_playbook"):
                await post_step_comment(
                    ticket_id, 3, "done",
                    "AI unavailable — using **deterministic scenario analysis** (playbook generated)",
                    _xyops_post,
                )
    else:
        await post_step_comment(
            ticket_id, 3, "skipped",
            "AI analysis — SKIPPED (no API key set) — trying local LLM then deterministic",
            _xyops_post,
        )
        analysis = await generate_local_llm_analysis(
            alert_name=alert_name,
            service_name=service_name,
            severity=severity,
            description=description,
            logs=logs,
            metrics=metrics,
            http=_http,
        )
        if not analysis.get("ansible_playbook"):
            analysis = deterministic_analysis(
                alert_name=alert_name,
                service_name=service_name,
                severity=severity,
            )

    # ── 4. Update ticket body with enriched content ──────────────────────────
    await post_step_comment(
        ticket_id, 4, "started",
        "Updating incident body with full diagnostic context...",
        _xyops_post,
    )
    ticket_body = build_enriched_ticket_body(
        service_name=service_name,
        alert_name=alert_name,
        severity=severity,
        description=description,
        starts_at=starts_at,
        dashboard_url=dashboard_url,
        bridge_trace_id=bridge_trace_id,
        metrics=metrics,
        analysis=analysis,
    )
    await _xyops_post("/api/app/update_ticket/v1", {"id": ticket_id, "body": ticket_body})
    word_count = len(ticket_body.split())
    await post_step_comment(
        ticket_id, 4, "done",
        f"Incident body updated ({word_count} words of diagnostic context)",
        _xyops_post,
    )

    # ── 5. Approval gate ─────────────────────────────────────────────────────
    approval_ticket_id = ""
    approval_ticket_num = 0
    needs_approval = (
        REQUIRE_APPROVAL
        and severity in APPROVAL_SEVERITY_THRESHOLD
        and analysis.get("ansible_playbook")
        and ticket_id
        and _http
    )
    if needs_approval:
        await post_step_comment(
            ticket_id, 5, "started",
            "Creating **approval gate** ticket for human review...",
            _xyops_post,
        )
        approval_id = f"apr-{uuid.uuid4().hex[:12]}"
        approval_req = await request_approval(
            session_id=service_name,
            approval_id=approval_id,
            incident_ticket_id=ticket_id,
            alert_name=alert_name,
            service_name=service_name,
            severity=severity,
            analysis=analysis,
            bridge_trace_id=bridge_trace_id,
            xyops_post=_xyops_post,
            xyops_url=XYOPS_URL,
            http=_http,
            risk_score=0.0,
        )
        approval_ticket_id = approval_req.approval_ticket_id
        approval_ticket_num = approval_req.approval_ticket_num
        await post_step_comment(
            ticket_id, 5, "waiting",
            f"Awaiting human approval — see **Ticket #{approval_ticket_num}**  "
            f"(`{approval_id}`)",
            _xyops_post,
        )
        logger.info(
            "Approval gate created  approval_id=%s  approval_ticket=%s",
            approval_id, approval_ticket_id,
        )
    else:
        await post_step_comment(
            ticket_id, 5, "skipped",
            "Approval gate — N/A (no Ansible playbook or auto-approval mode)",
            _xyops_post,
        )

    logger.info(
        "Ticket pipeline complete  ticket=#%s (%s)  alert=%s  service=%s  "
        "severity=%s  ai=%s  approval=%s  trace=%s",
        ticket_num, ticket_id, alert_name, service_name,
        severity, bool(analysis), bool(approval_ticket_id), bridge_trace_id,
    )
    return {
        "ticket_id": ticket_id,
        "ticket_num": ticket_num,
        "approval_ticket_id": approval_ticket_id,
        "ai_enabled": bool(analysis),
        "xyops_response": create_result,
    }


# ── xyOps ticket resolution ────────────────────────────────────────────────────

async def _resolve_xyops_tickets(
    alert_name: str,
    service_name: str,
    bridge_trace_id: str,
) -> dict:
    """
    Find all open xyOps tickets for this alert+service and close them.

    Uses xyOps tag search to find matching open tickets then updates
    each one to status=closed with a resolution note.
    """
    # Search for open AIOPS tickets whose subject contains both alert and service names.
    # The subject is set to "[AIOPS] {alert_name} on {service_name} ...", so both words
    # appear and can be found by Unbase full-text search.
    q = urllib.parse.quote(f"[AIOPS] {alert_name} {service_name} status:open")
    search_path = f"/api/app/search_tickets/v1?query={q}&limit=50"
    search_result = await _xyops_get(search_path)
    tickets = search_result.get("rows", [])

    resolved_ids: list[str] = []
    now_utc = datetime.now(timezone.utc).isoformat()

    for ticket in tickets:
        tid = ticket.get("id", "")
        if not tid:
            continue

        existing_body = ticket.get("body", "")
        resolution_note = (
            f"\n\n---\n"
            f"## Auto-Resolved\n\n"
            f"| Field | Value |\n"
            f"|---|---|\n"
            f"| **Resolved at** | {now_utc} |\n"
            f"| **Resolution trace** | `{bridge_trace_id}` → open in Tempo |\n\n"
            f"*Alert cleared — resolved automatically by the AIOps Bridge.*"
        )

        update_payload = {
            "id": tid,
            "status": "closed",
            "body": existing_body + resolution_note,
        }

        await _xyops_post("/api/app/update_ticket/v1", update_payload)
        resolved_ids.append(tid)
        logger.info(
            "Resolved xyOps ticket %s  alert=%s  service=%s  trace=%s",
            tid,
            alert_name,
            service_name,
            bridge_trace_id,
        )

    if not resolved_ids:
        logger.info(
            "No open tickets found for alert=%s service=%s — nothing to resolve",
            alert_name,
            service_name,
        )

    # Record scenario outcome in obs-intelligence so the metric is visible
    # on the SRE Incident Timeline dashboard.
    try:
        if _http:
            await _http.post(
                f"{OBS_INTELLIGENCE_URL}/intelligence/record-outcome",
                json={
                    "scenario_id": alert_name,
                    "outcome": "resolved",
                    "service_name": service_name,
                    "domain": "compute",
                },
                timeout=5.0,
            )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not record outcome in obs-intelligence: %s", exc)

    return {"resolved_ids": resolved_ids, "searched_alert": alert_name}
