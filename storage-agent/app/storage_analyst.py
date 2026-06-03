"""
storage-agent/app/storage_analyst.py
───────────────────────────────────────────────────────────────
Storage-aware AI analyst.

Fetches storage metrics from Prometheus and logs from Loki,
then sends the context to OpenAI (gpt-4o) or Anthropic Claude
for storage-domain root-cause analysis and Ceph remediation
playbook generation.

Provider auto-detection (same as aiops-bridge):
  OPENAI_API_KEY set  → OpenAI (priority)
  CLAUDE_API_KEY set  → Anthropic Claude
  Neither set         → AI disabled, deterministic analysis used

Storage scenarios handled:
  - CephOSDDown         → reweight / remove OSD
  - CephMultipleOSDDown → escalate (never auto-remediate)
  - CephPoolNearFull    → extend quota / archive data
  - CephPoolFull        → critical capacity action
  - PVCHighLatency      → investigate IO saturation
  - NoisyPVCDetected    → apply IO throttle via StorageClass QoS
  - CephClusterDegraded → composite investigation
"""

import json
import logging
import os
from typing import Any

import httpx
from obs_intelligence.models import ObsFeatures
from obs_intelligence.scenario_correlator import load_catalog as _sc_load_catalog, match_best as _sc_match_best
from obs_intelligence.telemetry_client import (
    fetch_instant_metric as _fetch_instant_metric,
    fetch_loki_context as _fetch_loki_context,
)

logger = logging.getLogger("storage-agent.analyst")

# ── Config ──────────────────────────────────────────────────────────────────────
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
CLAUDE_API_KEY: str = os.getenv("CLAUDE_API_KEY", "")

_USE_OPENAI: bool = bool(OPENAI_API_KEY)
_USE_CLAUDE: bool = bool(CLAUDE_API_KEY) and not _USE_OPENAI
_DEFAULT_MODEL = "gpt-4o-mini" if _USE_OPENAI else "claude-3-5-haiku-20241022"

AI_MODEL: str = os.getenv("AI_MODEL") or os.getenv("CLAUDE_MODEL") or _DEFAULT_MODEL
LOKI_URL: str = os.getenv("LOKI_URL", "http://loki:3100")
PROMETHEUS_URL: str = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
STORAGE_SIMULATOR_URL: str = os.getenv("STORAGE_SIMULATOR_URL", "http://storage-simulator:9200")
NOTIFY_EMAIL: str = os.getenv("NOTIFY_EMAIL", "")
GRAFANA_EXTERNAL_URL: str = os.getenv("GRAFANA_EXTERNAL_URL", "http://localhost:3001")

AI_ENABLED: bool = _USE_OPENAI or _USE_CLAUDE

_OPENAI_URL = "https://api.openai.com/v1/chat/completions"
_CLAUDE_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_HEADERS = {
    "Content-Type": "application/json",
    "anthropic-version": "2023-06-01",
}

logger.info(
    "AI provider: %s  model: %s",
    "openai" if _USE_OPENAI else ("claude" if _USE_CLAUDE else "disabled"),
    AI_MODEL if AI_ENABLED else "n/a",
)


def get_notify_list() -> list[str]:
    return [e.strip() for e in NOTIFY_EMAIL.split(",") if e.strip()]


# ═══════════════════════════════════════════════════════════════════════════════
# Prometheus storage metric fetchers
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_storage_metrics(
    alert_name: str,
    http: httpx.AsyncClient,
) -> dict[str, Any]:
    """
    Query Prometheus for storage metrics relevant to the given alert.
    Returns a dict of metric snapshots and a human-readable summary string.
    """
    queries = {
        "osd_status":      'storage_osd_up',
        "pool_fill_pct":   'storage_pool_used_bytes / storage_pool_capacity_bytes',
        "pool_used_gb":    'storage_pool_used_bytes / 1073741824',
        "io_latency_ms":   'storage_io_latency_seconds * 1000',
        "pvc_iops_read":   'storage_pvc_iops{operation="read"}',
        "pvc_iops_write":  'storage_pvc_iops{operation="write"}',
        "cluster_health":  'storage_cluster_health_score',
        "degraded_pgs":    'storage_degraded_placement_groups',
    }

    results: dict[str, Any] = {}
    lines: list[str] = []

    for name, expr in queries.items():
        metric_results = await _fetch_instant_metric(expr, http, PROMETHEUS_URL)
        results[name] = metric_results
        for r in metric_results:
            labels = r.get("metric", {})
            value = r.get("value", [None, "n/a"])[1]
            label_str = " ".join(f'{k}="{v}"' for k, v in labels.items() if k not in ("__name__",))
            try:
                lines.append(f"  {name}{{{label_str}}} = {float(value):.4f}" if label_str else f"  {name} = {float(value):.4f}")
            except (ValueError, TypeError):
                lines.append(f"  {name} = {value}")

    summary = "Storage Metrics Snapshot:\n" + ("\n".join(lines) if lines else "  (no data available)")
    return {"raw": results, "summary": summary}


async def fetch_loki_logs(
    label_query: str,
    http: httpx.AsyncClient,
    limit: int = 40,
) -> str:
    """
    Query Loki for recent storage-related log lines.
    label_query example: '{service_name="storage-agent"}'
    Delegates to the shared obs_intelligence telemetry client.
    """
    raw = await _fetch_loki_context(label_query, http, limit, LOKI_URL)
    if raw:
        line_count = raw.count("\n") + 1
        return f"Recent logs ({line_count} lines):\n{raw}"
    return "(no log lines found)"


# ═══════════════════════════════════════════════════════════════════════════════
# Deterministic action selector (scenario-catalog driven, LLM-free fallback)
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level catalog cache — populated on first call to deterministic_analysis().
_storage_catalog: list | None = None


def _get_storage_catalog() -> list:
    """Return (and lazily initialise) the storage scenario catalog."""
    global _storage_catalog
    if _storage_catalog is None:
        _storage_catalog = _sc_load_catalog(domain="storage")
    return _storage_catalog


def deterministic_analysis(alert_name: str, metrics_summary: str) -> dict[str, str]:
    """Return a deterministic storage action recommendation without LLM.

    Uses the scenario YAML catalog (obs-intelligence/scenarios/storage/) to
    match the alert name and any available metrics against defined scenarios.
    Falls back to a generic escalate response when no scenario matches.
    """
    import datetime as _dt

    features = ObsFeatures(
        alert_name=alert_name,
        service_name="storage",
        severity="warning",
        domain="storage",
        timestamp=_dt.datetime.now(_dt.timezone.utc),
    )

    catalog = _get_storage_catalog()
    best_match, best_def = _sc_match_best(features, catalog)

    if best_match and best_def:
        return {
            "rca_summary": best_def.rca,
            "recommended_action": best_def.action,
            "autonomy_level": best_def.autonomy,
            "ansible_playbook": _build_stub_playbook(alert_name, best_def.playbook_hint),
            "test_plan": [
                "Verify current storage cluster health: ceph status",
                "Confirm alert condition with: ceph osd tree / ceph df",
                "Dry-run remediation steps in non-production first",
                f"Monitor cluster health score for 5 minutes after {best_def.action}",
            ],
            "confidence": f"{best_match.confidence:.2f}",
            "provider": "scenario-catalog",
        }

    # No scenario matched — generic escalation
    return {
        "rca_summary": (
            f"Unrecognised storage alert '{alert_name}'. "
            "No scenario catalog match found. Escalate to storage SRE team."
        ),
        "recommended_action": "escalate",
        "autonomy_level": "human_only",
        "ansible_playbook": _build_stub_playbook(
            alert_name, "ESCALATE — no matching scenario found"
        ),
        "test_plan": ["Manual investigation required — no scenario matched"],
        "confidence": "0.00",
        "provider": "scenario-catalog",
    }


def _build_stub_playbook(alert_name: str, hint: str) -> str:
    return f"""---
# Storage Remediation Playbook — {alert_name}
# Generated by: storage-agent rule engine (deterministic)
# Review carefully before execution.
- name: Storage incident remediation — {alert_name}
  hosts: storage_nodes
  gather_facts: false
  tasks:
    - name: Pre-check — verify cluster health
      command: ceph status
      register: ceph_status
      changed_when: false

    - name: Remediation step
      # {hint}
      debug:
        msg: "Review and implement: {hint}"

    - name: Post-check — confirm recovery
      command: ceph health
      register: health_check
      changed_when: false
"""


# ═══════════════════════════════════════════════════════════════════════════════
# AI analysis (OpenAI / Claude)
# ═══════════════════════════════════════════════════════════════════════════════

_STORAGE_SYSTEM_PROMPT = """You are an expert SRE specializing in Ceph distributed storage,
Kubernetes persistent volumes (PVCs), and storage reliability engineering.

You are part of an automated AIOps system. When given storage alert context,
metrics, and logs, you must provide:
1. Root cause analysis (2-3 sentences, specific to Ceph/PVC patterns)
2. Recommended action (one of: osd_reweight, pvc_throttle, pool_expand_advisory,
   pool_critical_action, investigate_io, cluster_assessment, escalate)
3. Autonomy level (one of: autonomous, approval_gated, human_only)
   - autonomous: safe, reversible, low blast radius (e.g. PVC throttle)
   - approval_gated: significant change, needs human sign-off (e.g. OSD reweight)
   - human_only: critical risk, never automate (e.g. multiple OSD failure)
4. An Ansible playbook (YAML, ready to execute, with pre/post health checks)
5. Test plan (3-5 bullet points to verify the fix worked)

Respond ONLY with valid JSON matching this schema:
{
  "rca_summary": "string",
  "recommended_action": "string",
  "autonomy_level": "autonomous | approval_gated | human_only",
  "ansible_playbook": "string (YAML)",
  "test_plan": ["step1", "step2", ...],
  "confidence": "high | medium | low",
  "provider": "openai | claude"
}

CRITICAL: If multiple OSDs are down (CephMultipleOSDDown), ALWAYS set autonomy_level to human_only.
CRITICAL: The system must work if this response fails — fallback to osd_reweight/escalation is acceptable."""


async def generate_ai_analysis(
    alert_name: str,
    service_name: str,
    severity: str,
    summary: str,
    description: str,
    metrics_context: str,
    logs_context: str,
    http: httpx.AsyncClient,
) -> dict[str, str]:
    """
    Call OpenAI or Claude for storage-specific RCA and playbook generation.
    Falls back to deterministic_analysis() if AI is disabled or fails.
    """
    if not AI_ENABLED:
        return deterministic_analysis(alert_name, metrics_context)

    user_prompt = f"""Storage Alert Received:
  Alert: {alert_name}
  Service: {service_name}
  Severity: {severity}
  Summary: {summary}
  Description: {description}

{metrics_context}

{logs_context}

Provide root cause analysis and remediation playbook for this storage incident."""

    try:
        if _USE_OPENAI:
            result = await _call_openai(user_prompt, http)
        else:
            result = await _call_claude(user_prompt, http)

        if result:
            result["provider"] = "openai" if _USE_OPENAI else "claude"
            return result
    except Exception as exc:
        logger.warning("AI analysis failed, using deterministic fallback: %s", exc)

    return deterministic_analysis(alert_name, metrics_context)


async def _call_openai(user_prompt: str, http: httpx.AsyncClient) -> dict | None:
    resp = await http.post(
        _OPENAI_URL,
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": AI_MODEL,
            "messages": [
                {"role": "system", "content": _STORAGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
        },
        timeout=90.0,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    # Strip markdown code fences if present
    clean = content.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(clean)


async def _call_claude(user_prompt: str, http: httpx.AsyncClient) -> dict | None:
    resp = await http.post(
        _CLAUDE_URL,
        headers={**_CLAUDE_HEADERS, "x-api-key": CLAUDE_API_KEY},
        json={
            "model": AI_MODEL,
            "max_tokens": 2048,
            "system": _STORAGE_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_prompt}],
        },
        timeout=90.0,
    )
    resp.raise_for_status()
    content = resp.json()["content"][0]["text"]
    clean = content.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(clean)


def build_enriched_ticket_body(
    alert_name: str,
    service_name: str,
    severity: str,
    summary: str,
    description: str,
    metrics_context: str,
    ai_result: dict[str, str],
    bridge_trace_id: str,
    grafana_url: str = GRAFANA_EXTERNAL_URL + "/d/agentic-ai-overview",
    risk_score: float = 0.0,
    risk_level: str = "",
    evidence_lines: list | None = None,
) -> str:
    """Build the enriched xyOps ticket body for a storage incident."""
    autonomy = ai_result.get("autonomy_level", "unknown")
    action = ai_result.get("recommended_action", "unknown")
    confidence = ai_result.get("confidence", "unknown")
    provider = ai_result.get("provider", "unknown")

    autonomy_badge = {
        "autonomous":     "🟢 AUTONOMOUS — will execute without approval",
        "approval_gated": "🟡 APPROVAL REQUIRED — awaiting human sign-off",
        "human_only":     "🔴 HUMAN ONLY — do not automate",
    }.get(autonomy, f"⚪ {autonomy}")

    # ── Risk badge ────────────────────────────────────────────────────────────
    _eff_risk_score = risk_score or ai_result.get("risk_score", 0.0)
    _eff_risk_level = risk_level or ai_result.get("risk_level", "")
    risk_badge = ""
    if _eff_risk_level:
        risk_badge = {
            "critical": "🔴 CRITICAL",
            "high":     "🟠 HIGH",
            "medium":   "🟡 MEDIUM",
            "low":      "🟢 LOW",
        }.get(_eff_risk_level.lower(), f"⚪ {_eff_risk_level.upper()}")

    lines = [
        f"# Storage Incident: {alert_name}",
        f"**Service:** {service_name}  |  **Severity:** {severity.upper()}",
        f"**Autonomy:** {autonomy_badge}",
        f"**Action:** `{action}`  |  **Confidence:** {confidence}  |  **AI:** {provider}",
    ]

    if risk_badge:
        lines.append(
            f"**Risk:** {risk_badge}  |  **Score:** `{_eff_risk_score:.3f}`"
        )

    lines += ["", "---"]

    # ── Evidence observations ────────────────────────────────────────────────
    _eff_evidence = evidence_lines or ai_result.get("evidence_lines") or []
    if _eff_evidence:
        lines += ["## 🔍 Evidence Observations", ""]
        lines.extend(_eff_evidence)
        lines.append("")

    lines += [
        "## Root Cause Analysis",
        ai_result.get("rca_summary", "N/A"),
        "",
        "## Storage Metrics",
        "```",
        metrics_context,
        "```",
        "",
        "## Recommended Ansible Playbook",
        "```yaml",
        ai_result.get("ansible_playbook", "# (no playbook generated)"),
        "```",
        "",
        "## Test Plan",
    ]
    for step in ai_result.get("test_plan", []):
        lines.append(f"- {step}")

    if bridge_trace_id:
        lines += ["", "---", f"**Bridge Trace:** `{bridge_trace_id}`"]
    if grafana_url:
        lines += [f"**Dashboard:** {grafana_url}"]

    notify = get_notify_list()
    if notify:
        lines += ["", f"**Notify:** {', '.join(notify)}"]

    return "\n".join(lines)
