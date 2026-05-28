"""
compute-agent/app/pattern_client.py
────────────────────────────────────────────────────────────────
MCP Tool: search_patterns
─────────────────────────
Async client for the Pattern Library service (Section F of NewProject.md).

Converts raw session metrics to the normalised signal format expected by
POST /patterns/match-incident, calls the service, and returns both the
raw match list and a compact context string ready for LLM prompt injection.

Signal unit conventions (pattern-library schema):
  cpu_usage     — float 0.0–1.0  (fraction, not percent)
  latency_p99   — float seconds  (not milliseconds)
  error_rate    — float 0.0–1.0  (fraction, not percent)
  memory_usage  — float 0.0–1.0  (fraction, not percent)
  rps           — float requests/second (unchanged)

Session metric keys → normalised names:
  cpu_usage_pct     / 100  → cpu_usage
  p99_latency_ms    / 1000 → latency_p99
  error_rate_pct    / 100  → error_rate
  memory_usage_pct  / 100  → memory_usage
  rps                      → rps  (pass-through)
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger("aiops-bridge.pattern_client")

_PATTERN_LIBRARY_URL: str = os.getenv(
    "PATTERN_LIBRARY_URL", "http://pattern-library:9300"
)
_TOP_K: int = 3          # patterns injected into LLM context
_TIMEOUT: float = 5.0    # seconds — must not slow the critical pipeline path


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def build_signals(metrics: dict[str, Any]) -> dict[str, float]:
    """
    Convert session.metrics to the normalised signal dict for pattern matching.
    Only includes keys that have a valid numeric value.
    """
    raw_cpu   = metrics.get("cpu_usage_pct")
    raw_lat   = metrics.get("p99_latency_ms")
    raw_err   = metrics.get("error_rate_pct")
    raw_mem   = metrics.get("memory_usage_pct")
    raw_rps   = metrics.get("rps")

    signals: dict[str, float] = {}
    if raw_cpu is not None:
        signals["cpu_usage"] = float(raw_cpu) / 100.0
    if raw_lat is not None:
        signals["latency_p99"] = float(raw_lat) / 1_000.0
    if raw_err is not None:
        signals["error_rate"] = float(raw_err) / 100.0
    if raw_mem is not None:
        signals["memory_usage"] = float(raw_mem) / 100.0
    if raw_rps is not None:
        signals["rps"] = float(raw_rps)
    return signals


async def search_patterns(
    http: httpx.AsyncClient,
    metrics: dict[str, Any],
    alert_name: str,
    service_name: str,
    severity: str,
    top_k: int = _TOP_K,
) -> list[dict]:
    """
    MCP search_patterns tool implementation.

    Normalises session metrics, calls POST /patterns/match-incident, and
    returns raw IncidentMatchResult dicts sorted by combined_score.
    Returns [] on any error so the pipeline is never blocked.
    """
    signals = build_signals(metrics)
    incident_text = (
        f"{severity.upper()} alert {alert_name} on service {service_name}"
    )
    payload = {
        "signals": signals,
        "incident_text": incident_text,
        "agent": "compute",
        "alert_name": alert_name,
        "top_k": top_k,
    }
    try:
        resp = await http.post(
            f"{_PATTERN_LIBRARY_URL}/patterns/match-incident",
            json=payload,
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning(
                "pattern-library returned HTTP %d — skipping pattern context",
                resp.status_code,
            )
            return []
        return resp.json()
    except httpx.TimeoutException:
        logger.warning("pattern-library timed out after %.1fs — skipping", _TIMEOUT)
        return []
    except Exception as exc:  # noqa: BLE001
        logger.warning("pattern-library unavailable: %s — skipping", exc)
        return []


def format_pattern_context(matches: list[dict]) -> str:
    """
    Build a compact ≤800-token context block from top pattern matches.
    Injected into the LLM prompt under KNOWN PATTERNS FROM PATTERN LIBRARY.
    Returns "" when matches list is empty (no injection).
    """
    if not matches:
        return ""

    lines: list[str] = []
    for rank, m in enumerate(matches[:_TOP_K], start=1):
        name     = m.get("pattern_name", "unknown")
        score    = m.get("combined_score", 0.0)
        severity = m.get("severity", "?")
        rec_sc   = m.get("recurrence_score", 0.0)
        lines.append(
            f"  {rank}. [{severity.upper()}] {name}  "
            f"(match={score:.0%}  recurrence={rec_sc:.0%})"
        )

    return (
        "KNOWN PATTERNS FROM PATTERN LIBRARY\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(lines)
        + "\n"
        "These are previously-documented incident patterns from live production "
        "data. Where a pattern matches, prefer its established fix strategy over "
        "a newly derived one — unless the current evidence contradicts it."
    )


async def post_assessment(
    http: httpx.AsyncClient,
    session_id: str,
    alert_name: str,
    matched_patterns: list[dict],
    risk_score: float,
    recommended_action: str,
) -> None:
    """
    Fire-and-forget: record this agent's assessment in the pattern library
    so the pattern confidence scores improve over time.
    """
    payload = {
        "session_id": session_id,
        "agent": "compute",
        "alert_name": alert_name,
        "matched_patterns": [
            {
                "pattern_id": m.get("pattern_id"),
                "score": m.get("combined_score", 0.0),
                "rank": rank + 1,
            }
            for rank, m in enumerate(matched_patterns)
        ],
        "risk_score": risk_score,
        "recommendation": recommended_action,
        "incident_summary": f"{alert_name} — risk {risk_score:.3f}",
    }
    try:
        await http.post(
            f"{_PATTERN_LIBRARY_URL}/assessments",
            json=payload,
            timeout=_TIMEOUT,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not post pattern assessment: %s", exc)
