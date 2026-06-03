"""
aiops-bridge/app/xyops_client.py
────────────────────────────────────────────────────────────────
xyOps API helper utilities for the AI pipeline.

Two responsibilities
────────────────────
1. post_step_comment()
   Appends a timestamped progress comment to an incident ticket.
   Called at each stage of the AI analysis pipeline so the user
   can watch the ticket activity feed in xyOps and see comments
   accumulate in real time as the pipeline executes:

     [>>] Step 1/5: Fetching log context for **frontend-api**
     `10:23:01 UTC`
     [OK] Step 1/5: Retrieved 47 log lines (12 WARN events)
     `10:23:02 UTC`
     ...

2. ensure_aiops_workflow()
   Creates (once at bridge startup) a xyOps Workflow event named
   "AIOps AI Pipeline" under Scheduler → Workflows.  The workflow
   is a 6-node visual pipeline:

     Manual Trigger
       → 1. Fire Alert (httpplug → POST /webhook)
       → 2. Fetch Log Context (httpplug → Loki)
       → 3. Fetch Metrics (httpplug → Prometheus)
       → 4. AI Analysis (httpplug → GET /health shows ai_enabled)
       → 5. Check Approvals (httpplug → GET /approvals/pending)

   Run it from Scheduler → Workflows → AIOps AI Pipeline → Run.
   Watch each HTTP node light up green/red in the workflow canvas.
   Open the incident ticket simultaneously to watch the live step
   comments appear in the activity feed.

   If the workflow already exists (idempotent), the call is a no-op.
────────────────────────────────────────────────────────────────
"""

import json as _json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

logger = logging.getLogger("aiops-bridge.xyops_client")

# ── Target server for all workflow job nodes ──────────────────────────────────
# Must match the xyOps container hostname (docker-compose: hostname: xyops).
# Override via XYOPS_SERVER_HOSTNAME env var if your setup differs.
_WORKFLOW_TARGET: str = os.getenv("XYOPS_SERVER_HOSTNAME", "xyops")
_GRAFANA_EXTERNAL_URL: str = os.getenv("GRAFANA_EXTERNAL_URL", "http://localhost:3001")

# ── Step status icons ─────────────────────────────────────────────────────────
_STATUS_ICON: dict[str, str] = {
    "started":   "[>>]",
    "done":      "[OK]",
    "error":     "[!!]",
    "waiting":   "[..]",
    "skipped":   "[--]",
    "approved":  "[OK]",
    "declined":  "[!!]",
    "executing": "[>>]",
}

TOTAL_STEPS = 5  # logs · metrics · AI-or-skip · update-body · approval-or-skip

# ── Type alias ────────────────────────────────────────────────────────────────
_PostFn = Callable[[str, dict], Awaitable[dict]]


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Live step comments
# ═══════════════════════════════════════════════════════════════════════════════

async def post_step_comment(
    ticket_id: str,
    step_num: int,
    status: str,
    message: str,
    xyops_post: _PostFn,
    *,
    total_steps: int = TOTAL_STEPS,
) -> None:
    """
    Append one formatted progress comment to a xyOps ticket activity feed.

    Args:
        ticket_id:   xyOps internal ticket ID (e.g. "t1a2b3c4")
        step_num:    Current step number (1-based)
        status:      "started" | "done" | "error" | "waiting" | "skipped"
        message:     Human-readable description of what this step did
        xyops_post:  Bound coroutine that POSTs to the xyOps REST API
        total_steps: Denominator for "Step N/M" display (default 5)

    Renders in the xyOps ticket as, e.g.:
        [OK] Step 2/5: Metrics: `error_rate=14.2%  p99=2847ms  rps=4.1`
        `10:23:02 UTC`
    """
    if not ticket_id:
        return

    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    icon = _STATUS_ICON.get(status, "[??]")
    body = f"{icon} **Step {step_num}/{total_steps}:** {message}  \n`{now}`"

    result = await xyops_post(
        "/api/app/add_ticket_change/v1",
        {"id": ticket_id, "change": {"type": "comment", "body": body}},
    )
    if result.get("error"):
        logger.warning(
            "Step comment failed (ticket=%s step=%d/%d): %s",
            ticket_id,
            step_num,
            total_steps,
            result.get("error"),
        )


async def post_outcome_comment(
    ticket_id: str,
    status: str,
    message: str,
    xyops_post: _PostFn,
) -> None:
    """
    Append a free-form outcome comment (not tied to a step number).
    Used by the approval workflow to report a human decision or
    playbook execution result back to the incident ticket.

    Renders as, e.g.:
        [OK] **Approved by alice** — Remediation playbook executing
        `10:25:14 UTC`
    """
    if not ticket_id:
        return

    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    icon = _STATUS_ICON.get(status, "[??]")
    body = f"{icon} **{message}**  \n`{now}`"

    result = await xyops_post(
        "/api/app/add_ticket_change/v1",
        {"id": ticket_id, "change": {"type": "comment", "body": body}},
    )
    if result.get("error"):
        logger.warning(
            "Outcome comment failed (ticket=%s): %s",
            ticket_id,
            result.get("error"),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 2. AIOps Pipeline Workflow in xyOps Scheduler → Workflows
# ═══════════════════════════════════════════════════════════════════════════════

_WORKFLOW_EVENT_ID = "aiops_pipeline_wf"
_WORKFLOW_TITLE = "AIOps Agent Pipeline"

# ── Shared httpplug constants ──────────────────────────────────────────────────
_BRIDGE = "http://aiops-bridge:9000"
_CT_JSON = "Content-Type: application/json"

# Body for Agent 1 — full demo alert context (hardcoded for a repeatable test run)
_DEMO_START_BODY = (
    '{"service_name":"frontend-api",'
    '"alert_name":"HighErrorRate",'
    '"severity":"warning",'
    '"summary":"AIOps Agent Pipeline test - HighErrorRate on frontend-api",'
    '"description":"Test run from the xyOps AIOps Agent Pipeline workflow. '
    'Watch this ticket activity feed for live per-agent progress updates.",'
    f'"dashboard_url":"{_GRAFANA_EXTERNAL_URL}/d/obs-overview"' + "}"
)

# Body reused by Agents 2–6 — session is keyed by service_name
_AGENT_BODY = '{"session_id":"frontend-api"}'

# ── Workflow canvas nodes ─────────────────────────────────────────────────────
# Left-to-right chain; slight vertical stagger improves readability on canvas.
_NODES: list[dict[str, Any]] = [
    # ── Trigger ───────────────────────────────────────────────────────────────
    {
        "id": "wf_trigger",
        "type": "trigger",
        "x": 80,
        "y": 340,
    },
    # ── Agent 1: Create session + skeleton ticket ─────────────────────────────
    {
        "id": "wf_n1",
        "type": "job",
        "x": 300,
        "y": 260,
        "data": {
            "label": "Agent 1 — Pipeline Start",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "alarm",
            "params": {
                "method": "POST",
                "url": f"{_BRIDGE}/pipeline/start",
                "headers": _CT_JSON,
                "data": _DEMO_START_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    # ── Agent 2: Fetch Loki logs ───────────────────────────────────────────────
    {
        "id": "wf_n2",
        "type": "job",
        "x": 540,
        "y": 320,
        "data": {
            "label": "Agent 2 — Loki Log Fetcher",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "search",
            "params": {
                "method": "POST",
                "url": f"{_BRIDGE}/pipeline/agent/logs",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    # ── Agent 3: Fetch Prometheus metrics ─────────────────────────────────────
    {
        "id": "wf_n3",
        "type": "job",
        "x": 780,
        "y": 260,
        "data": {
            "label": "Agent 3 — Prometheus Analyst",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "chart",
            "params": {
                "method": "POST",
                "url": f"{_BRIDGE}/pipeline/agent/metrics",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    # ── Agent 4: Claude AI root-cause analysis ────────────────────────────────
    {
        "id": "wf_n4",
        "type": "job",
        "x": 1020,
        "y": 320,
        "data": {
            "label": "Agent 4 — Claude AI Analyst",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "cpu",
            "params": {
                "method": "POST",
                "url": f"{_BRIDGE}/pipeline/agent/analyze",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "120",
            },
        },
    },
    # ── Agent 5: Enrich incident ticket body ──────────────────────────────────
    {
        "id": "wf_n5",
        "type": "job",
        "x": 1260,
        "y": 260,
        "data": {
            "label": "Agent 5 — Incident Scribe",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "edit",
            "params": {
                "method": "POST",
                "url": f"{_BRIDGE}/pipeline/agent/ticket",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    # ── Agent 6: Human approval gate ──────────────────────────────────────────
    {
        "id": "wf_n6",
        "type": "job",
        "x": 1500,
        "y": 320,
        "data": {
            "label": "Agent 6 — Approval Gateway",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "check",
            "params": {
                "method": "POST",
                "url": f"{_BRIDGE}/pipeline/agent/approval",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
]

_CONNECTIONS: list[dict[str, Any]] = [
    {"id": "wf_c0", "source": "wf_trigger", "dest": "wf_n1"},
    {"id": "wf_c1", "source": "wf_n1",      "dest": "wf_n2"},
    {"id": "wf_c2", "source": "wf_n2",      "dest": "wf_n3"},
    {"id": "wf_c3", "source": "wf_n3",      "dest": "wf_n4"},
    {"id": "wf_c4", "source": "wf_n4",      "dest": "wf_n5"},
    {"id": "wf_c5", "source": "wf_n5",      "dest": "wf_n6"},
]


async def ensure_aiops_workflow(xyops_post: _PostFn, xyops_get: Callable) -> None:
    """
    Create (or recreate) the "AIOps Agent Pipeline" workflow in xyOps.
    Idempotent — if the workflow already exists it is deleted and recreated
    so that node changes in Python are always reflected in the UI.

    The workflow appears under Scheduler → Workflows in the xyOps UI.

    How to use:
      1. Click Run on the AIOps Agent Pipeline workflow.
      2. Watch each agent node turn green on the canvas (left to right).
      3. Open the incident ticket that Agent 1 creates — each agent
         posts a live [>>]/[OK] step comment as it finishes.
    """
    # If the workflow already exists, update it; otherwise create it.
    # (delete_event requires a separate "Delete Events" privilege — avoid it)
    existing = await xyops_get(
        f"/api/app/get_event/v1?id={_WORKFLOW_EVENT_ID}"
    )
    event_exists = bool(existing.get("event"))

    payload: dict[str, Any] = {
        "id": _WORKFLOW_EVENT_ID,
        "title": _WORKFLOW_TITLE,
        "type": "workflow",
        "category": "general",
        "enabled": True,
        "notes": (
            "Agent-to-agent AIOps pipeline — each node is a dedicated AI agent.\n\n"
            "**How to run:**\n"
            "1. Click **Run** on this workflow.\n"
            "2. Watch Agents 1-6 turn green on the canvas (left → right).\n"
            "3. Agent 1 creates an incident ticket — open it and watch the\n"
            "   activity feed for live [>>]/[OK] step comments as each\n"
            "   agent completes its job.\n\n"
            "Agents: Pipeline Start → Loki Logs → Prometheus Metrics → "
            "Claude AI → Incident Scribe → Approval Gateway"
        ),
        "triggers": [
            {"id": "wf_trigger", "type": "manual", "enabled": True}
        ],
        "workflow": {
            "start": "wf_trigger",
            "nodes": _NODES,
            "connections": _CONNECTIONS,
        },
    }

    api_path = "/api/app/update_event/v1" if event_exists else "/api/app/create_event/v1"
    result = await xyops_post(api_path, payload)
    action = "Updated" if event_exists else "Created"
    if result.get("error") or result.get("code", 0) != 0:
        logger.warning(
            "Failed to %s AIOps Agent Pipeline workflow: %s",
            action.lower(),
            result.get("description") or result.get("error"),
        )
    else:
        logger.info(
            "%s AIOps Agent Pipeline workflow in xyOps: "
            "Scheduler → Workflows → '%s'",
            action, _WORKFLOW_TITLE,
        )

    await _ensure_predictive_alert_workflow_compute(xyops_post, xyops_get)


# ═══════════════════════════════════════════════════════════════════════════════
# 2b. Compute Predictive Alert Workflow
# ═══════════════════════════════════════════════════════════════════════════════

_PRED_COMPUTE_EVENT_ID = "compute_predictive_alert_wf"
_PRED_COMPUTE_TITLE = "Compute Predictive Alert Workflow"

_DEMO_PREDICTIVE_COMPUTE_BODY = (
    '{"service_name":"frontend-api",'
    '"domain":"compute",'
    '"scenario_id":"anomaly_error_rate_pct",'
    '"risk_score":0.80,'
    '"confidence":0.75,'
    '"description":"Error rate is trending anomalously upward before threshold breach.",'
    '"forecast_breach_minutes":30,'
    '"anomaly_metric":"error_rate_pct",'
    '"anomaly_z_score":3.1}'
)

_PRED_COMPUTE_NODES: list[dict[str, Any]] = [
    {
        "id": "pred_c_trigger",
        "type": "trigger",
        "x": 80,
        "y": 340,
    },
    {
        "id": "pred_c_n1",
        "type": "job",
        "x": 340,
        "y": 260,
        "data": {
            "label": "Send Predictive Alert",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "bell",
            "params": {
                "method": "POST",
                "url": f"{_BRIDGE}/predictive-alert",
                "headers": _CT_JSON,
                "data": _DEMO_PREDICTIVE_COMPUTE_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
]

_PRED_COMPUTE_CONNECTIONS: list[dict[str, Any]] = [
    {"id": "pred_c_c0", "source": "pred_c_trigger", "dest": "pred_c_n1"},
]


async def _ensure_predictive_alert_workflow_compute(
    xyops_post: _PostFn, xyops_get: Callable
) -> None:
    """Provision the Compute Predictive Alert Workflow in xyOps (idempotent)."""
    existing = await xyops_get(f"/api/app/get_event/v1?id={_PRED_COMPUTE_EVENT_ID}")
    event_exists = bool(existing.get("event"))
    payload: dict[str, Any] = {
        "id": _PRED_COMPUTE_EVENT_ID,
        "title": _PRED_COMPUTE_TITLE,
        "type": "workflow",
        "category": "general",
        "enabled": True,
        "notes": (
            "Demonstrates the Compute Agent receiving a predictive alert from "
            "obs-intelligence before a Prometheus alert fires. Creates a "
            "[PREDICTIVE] approval-gated xyOps ticket."
        ),
        "triggers": [{"id": "pred_c_trigger", "type": "manual", "enabled": True}],
        "workflow": {
            "start": "pred_c_trigger",
            "nodes": _PRED_COMPUTE_NODES,
            "connections": _PRED_COMPUTE_CONNECTIONS,
        },
    }
    api_path = "/api/app/update_event/v1" if event_exists else "/api/app/create_event/v1"
    result = await xyops_post(api_path, payload)
    action = "Updated" if event_exists else "Created"
    if result.get("error") or result.get("code", 0) != 0:
        logger.warning(
            "Failed to %s Compute Predictive Alert workflow: %s",
            action.lower(),
            result.get("description") or result.get("error"),
        )
    else:
        logger.info(
            "%s Compute Predictive Alert workflow: "
            "Scheduler → Workflows → '%s'",
            action, _PRED_COMPUTE_TITLE,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Approval ticket action events (Approve / Decline buttons)
# ═══════════════════════════════════════════════════════════════════════════════

async def create_approval_events(
    approval_id: str,
    bridge_host: str,
    xyops_post: _PostFn,
) -> tuple[str, str]:
    """
    Create two one-shot httpplug events in xyOps for this approval.
    These appear as clickable ▶ Run buttons on the approval ticket.

    Returns (approve_event_id, decline_event_id).
    """
    short = approval_id.replace("-", "")[:10]
    approve_id = f"aiops_ap_{short}"
    decline_id = f"aiops_dc_{short}"

    for event_id, label, icon, approved_val in [
        (approve_id, "✅ Approve Remediation", "check", True),
        (decline_id, "❌ Decline Remediation", "x",     False),
    ]:
        data_body = _json.dumps({
            "approved": approved_val,
            "decided_by": "xyops_user",
            "notes": (
                "Approved via xyOps ticket button"
                if approved_val else
                "Declined via xyOps ticket button"
            ),
        })
        event_payload: dict[str, Any] = {
            "id": event_id,
            "title": f"{label} — {approval_id[:8]}",
            "plugin": "httpplug",
            "category": "general",
            "icon": icon,
            "enabled": True,
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "params": {
                "method": "POST",
                "url": f"{bridge_host}/approval/{approval_id}/decision",
                "headers": "Content-Type: application/json",
                "data": data_body,
                "success_match": "status",
                "timeout": "30",
            },
        }
        result = await xyops_post("/api/app/create_event/v1", event_payload)
        if result.get("error") or result.get("code", 0) != 0:
            logger.warning(
                "Failed to create approval event %s: %s",
                event_id,
                result.get("description") or result.get("error"),
            )
        else:
            logger.info("Created approval event %s", event_id)

    return approve_id, decline_id
