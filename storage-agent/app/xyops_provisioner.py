"""
storage-agent/app/xyops_provisioner.py
──────────────────────────────────────────────────────────────────────────────
Provisions the "Storage AIOps Agent Pipeline" workflow into xyOps.

Creates (or updates) a 6-node visual workflow under Scheduler → Workflows
that mirrors the compute agent pipeline but calls storage-agent endpoints.

Workflow nodes:
  Trigger → Agent 1 Start → Agent 2 Storage Metrics → Agent 3 Logs
          → Agent 4 AI Analysis → Agent 5 Ticket Scribe → Agent 6 Approval

Run it from xyOps UI: Scheduler → Workflows → "Storage AIOps Agent Pipeline" → Run
──────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
from typing import Any, Awaitable, Callable

logger = logging.getLogger("storage-agent.xyops_provisioner")

_WORKFLOW_TARGET: str = os.getenv("XYOPS_SERVER_HOSTNAME", "xyops")
_GRAFANA_EXTERNAL_URL: str = os.getenv("GRAFANA_EXTERNAL_URL", "http://localhost:3001")
_WORKFLOW_EVENT_ID = "storage_aiops_pipeline_wf"
_WORKFLOW_TITLE = "Storage AIOps Agent Pipeline"

# ── Predictive Alert Workflow ─────────────────────────────────────────────────
_PRED_WORKFLOW_EVENT_ID = "storage_predictive_alert_wf"
_PRED_WORKFLOW_TITLE = "Storage Predictive Alert Workflow"

_STORAGE_AGENT = "http://storage-agent:9001"
_CT_JSON = "Content-Type: application/json"

# Demo body for Agent 1 — triggers a CephOSDDown scenario
_DEMO_START_BODY = (
    '{"service_name":"storage-simulator",'
    '"alert_name":"CephOSDDown",'
    '"severity":"warning",'
    '"summary":"Storage AIOps Pipeline test - CephOSDDown on storage-simulator",'
    '"description":"Test run from the xyOps Storage AIOps Agent Pipeline workflow. '
    'Watch this ticket activity feed for live per-agent progress.",'
    f'"dashboard_url":"{_GRAFANA_EXTERNAL_URL}/d/agentic-ai-overview"' + "}"
)

_AGENT_BODY = '{"session_id":"storage-simulator"}'

_PostFn = Callable[[str, dict], Awaitable[dict]]

_NODES: list[dict[str, Any]] = [
    {
        "id": "sw_trigger",
        "type": "trigger",
        "x": 80,
        "y": 340,
    },
    {
        "id": "sw_n1",
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
                "url": f"{_STORAGE_AGENT}/pipeline/start",
                "headers": _CT_JSON,
                "data": _DEMO_START_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    {
        "id": "sw_n2",
        "type": "job",
        "x": 540,
        "y": 320,
        "data": {
            "label": "Agent 2 — Storage Metrics",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "chart",
            "params": {
                "method": "POST",
                "url": f"{_STORAGE_AGENT}/pipeline/agent/storage-metrics",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    {
        "id": "sw_n3",
        "type": "job",
        "x": 780,
        "y": 260,
        "data": {
            "label": "Agent 3 — Loki Log Fetcher",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "search",
            "params": {
                "method": "POST",
                "url": f"{_STORAGE_AGENT}/pipeline/agent/logs",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    {
        "id": "sw_n4",
        "type": "job",
        "x": 1020,
        "y": 320,
        "data": {
            "label": "Agent 4 — AI Storage Analyst",
            "plugin": "httpplug",
            "targets": [_WORKFLOW_TARGET],
            "algo": "first",
            "category": "general",
            "icon": "cpu",
            "params": {
                "method": "POST",
                "url": f"{_STORAGE_AGENT}/pipeline/agent/analyze",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "120",
            },
        },
    },
    {
        "id": "sw_n5",
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
                "url": f"{_STORAGE_AGENT}/pipeline/agent/ticket",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
    {
        "id": "sw_n6",
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
                "url": f"{_STORAGE_AGENT}/pipeline/agent/approval",
                "headers": _CT_JSON,
                "data": _AGENT_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
]

_CONNECTIONS: list[dict[str, Any]] = [
    {"id": "sw_c0", "source": "sw_trigger", "dest": "sw_n1"},
    {"id": "sw_c1", "source": "sw_n1",       "dest": "sw_n2"},
    {"id": "sw_c2", "source": "sw_n2",       "dest": "sw_n3"},
    {"id": "sw_c3", "source": "sw_n3",       "dest": "sw_n4"},
    {"id": "sw_c4", "source": "sw_n4",       "dest": "sw_n5"},
    {"id": "sw_c5", "source": "sw_n5",       "dest": "sw_n6"},
]


async def ensure_storage_workflow(xyops_post: _PostFn, xyops_get: Callable) -> None:
    """
    Create (or update) the Storage AIOps Agent Pipeline workflow in xyOps.
    Idempotent — safe to call on every startup.
    """
    existing = await xyops_get(f"/api/app/get_event/v1?id={_WORKFLOW_EVENT_ID}")
    event_exists = bool(existing.get("event"))

    payload: dict[str, Any] = {
        "id": _WORKFLOW_EVENT_ID,
        "title": _WORKFLOW_TITLE,
        "type": "workflow",
        "category": "general",
        "enabled": True,
        "notes": (
            "Storage-specialised AIOps pipeline — each node is a dedicated agent.\n\n"
            "**How to run:**\n"
            "1. Click **Run** on this workflow.\n"
            "2. Watch Agents 1-6 turn green on the canvas.\n"
            "3. Open the ticket that Agent 1 creates and watch live progress comments.\n\n"
            "Agents: Pipeline Start → Storage Metrics → Loki Logs → "
            "AI Storage Analyst → Incident Scribe → Approval Gateway\n\n"
            "**Storage scenarios:** CephOSDDown, CephPoolFull, NoisyPVCDetected, "
            "PVCHighLatency, CephClusterDegraded"
        ),
        "triggers": [
            {"id": "sw_trigger", "type": "manual", "enabled": True}
        ],
        "workflow": {
            "start": "sw_trigger",
            "nodes": _NODES,
            "connections": _CONNECTIONS,
        },
    }

    api_path = "/api/app/update_event/v1" if event_exists else "/api/app/create_event/v1"
    result = await xyops_post(api_path, payload)
    action = "Updated" if event_exists else "Created"

    if result.get("error") or result.get("code", 0) != 0:
        logger.warning(
            "Failed to %s Storage AIOps Pipeline workflow: %s",
            action.lower(),
            result.get("description") or result.get("error"),
        )
    else:
        logger.info(
            "%s Storage AIOps Pipeline workflow in xyOps: "
            "Scheduler → Workflows → '%s'",
            action,
            _WORKFLOW_TITLE,
        )

    # Also provision the Predictive Alert Workflow
    await _ensure_predictive_alert_workflow(xyops_post, xyops_get)


# ── Predictive Alert Workflow nodes ──────────────────────────────────────────
_DEMO_PREDICTIVE_BODY = (
    '{"service_name":"storage-simulator",'
    '"domain":"storage",'
    '"scenario_id":"pool_fill_forecast_breach",'
    '"risk_score":0.82,'
    '"confidence":0.78,'
    '"description":"Pool utilisation trending toward 95% critical threshold.",'
    '"forecast_breach_minutes":45,'
    '"anomaly_metric":"storage_pool_used_bytes",'
    '"anomaly_z_score":2.9}'
)

_PRED_NODES: list[dict[str, Any]] = [
    {
        "id": "pred_trigger",
        "type": "trigger",
        "x": 80,
        "y": 340,
    },
    {
        "id": "pred_n1",
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
                "url": f"{_STORAGE_AGENT}/predictive-alert",
                "headers": _CT_JSON,
                "data": _DEMO_PREDICTIVE_BODY,
                "success_match": '"status"',
                "timeout": "30",
            },
        },
    },
]

_PRED_CONNECTIONS: list[dict[str, Any]] = [
    {"id": "pred_c0", "source": "pred_trigger", "dest": "pred_n1"},
]


async def _ensure_predictive_alert_workflow(xyops_post: _PostFn, xyops_get: Callable) -> None:
    """Provision the Storage Predictive Alert Workflow in xyOps (idempotent)."""
    existing = await xyops_get(f"/api/app/get_event/v1?id={_PRED_WORKFLOW_EVENT_ID}")
    event_exists = bool(existing.get("event"))

    payload: dict[str, Any] = {
        "id": _PRED_WORKFLOW_EVENT_ID,
        "title": _PRED_WORKFLOW_TITLE,
        "type": "workflow",
        "category": "general",
        "enabled": True,
        "notes": (
            "Demonstrates the Storage Agent receiving a predictive alert from "
            "the Obs-Intelligence Engine — *before* a Prometheus alert fires.\n\n"
            "The workflow POSTs a high-risk scenario payload to "
            "storage-agent:9001/predictive-alert which creates a [PREDICTIVE] "
            "approval-gated xyOps ticket.\n\n"
            "In production this is triggered automatically by obs-intelligence "
            "when risk_score > 0.75 AND confidence > 0.7 AND no alert is firing."
        ),
        "triggers": [
            {"id": "pred_trigger", "type": "manual", "enabled": True}
        ],
        "workflow": {
            "start": "pred_trigger",
            "nodes": _PRED_NODES,
            "connections": _PRED_CONNECTIONS,
        },
    }

    api_path = "/api/app/update_event/v1" if event_exists else "/api/app/create_event/v1"
    result = await xyops_post(api_path, payload)
    action = "Updated" if event_exists else "Created"

    if result.get("error") or result.get("code", 0) != 0:
        logger.warning(
            "Failed to %s Predictive Alert workflow: %s",
            action.lower(),
            result.get("description") or result.get("error"),
        )
    else:
        logger.info(
            "%s Predictive Alert workflow in xyOps: "
            "Scheduler → Workflows → '%s'",
            action,
            _PRED_WORKFLOW_TITLE,
        )

    api_path = "/api/app/update_event/v1" if event_exists else "/api/app/create_event/v1"
    result = await xyops_post(api_path, payload)
    action = "Updated" if event_exists else "Created"

    if result.get("error") or result.get("code", 0) != 0:
        logger.warning(
            "Failed to %s Storage AIOps Pipeline workflow: %s",
            action.lower(),
            result.get("description") or result.get("error"),
        )
    else:
        logger.info(
            "%s Storage AIOps Pipeline workflow in xyOps: "
            "Scheduler → Workflows → '%s'",
            action,
            _WORKFLOW_TITLE,
        )
