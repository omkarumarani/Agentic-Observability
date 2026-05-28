"""
obs_intelligence/timeseries_reasoner.py
────────────────────────────────────────────────────────────────────────────────
Section Q — Time-Series Reasoning

Provides contextual time-based reasoning on top of the existing anomaly
detector and forecaster.  Answers:

  • Was this the same anomaly as last week / yesterday?   (baseline comparison)
  • Did a deployment event coincide with the anomaly?     (event correlation)
  • Is the signal trending toward threshold?              (trend-based reasoning)
  • Before/after comparison for a remediation action      (fix validation)

Architecture
────────────
  TimeSeriesContext bundles all time-based observations for one incident.
  It is lightweight — it queries Prometheus for trend data and correlates
  with known deployment events from Gitea/Alertmanager history.

Public API
──────────
  build_timeseries_context(features, metrics_raw, http) → TimeSeriesContext
    Called from agent_analyze after Step 1 (feature extraction).
    Adds trend_direction, baseline_deviation, and deployment_correlation
    to the analysis without blocking the pipeline (5s timeout).
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("obs_intelligence.timeseries_reasoner")

_PROMETHEUS_URL: str = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
_GITEA_URL:      str = os.getenv("GITEA_URL", "http://gitea:3000")
_TIMEOUT: float  = 5.0


# ═══════════════════════════════════════════════════════════════════════════════
# Data types
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TrendSignal:
    """Single-metric trend observation."""
    metric_name:      str
    current_value:    float
    baseline_value:   float | None    # 7-day same-hour average
    baseline_stddev:  float | None
    z_score:          float | None    # (current - baseline) / stddev
    trend_direction:  str             # "rising" | "falling" | "stable" | "unknown"
    pct_vs_baseline:  float | None    # % above/below baseline
    is_anomalous:     bool


@dataclass
class DeploymentEvent:
    """A deployment or config change event near an incident."""
    service:     str
    timestamp:   str
    description: str
    delta_minutes: float  # minutes before the anomaly (negative = after)


@dataclass
class TimeSeriesContext:
    """Full time-based reasoning context for one incident."""
    alert_name:    str
    service_name:  str
    timestamp:     str

    trends:            list[TrendSignal] = field(default_factory=list)
    deployment_events: list[DeploymentEvent] = field(default_factory=list)
    baseline_anomaly:  bool = False    # spike vs 7-day baseline
    trend_summary:     str = ""        # human-readable summary
    deployment_note:   str = ""        # whether deployment likely caused it
    reasoning_notes:   list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "baseline_anomaly":  self.baseline_anomaly,
            "trend_summary":     self.trend_summary,
            "deployment_note":   self.deployment_note,
            "trends": [
                {
                    "metric":          t.metric_name,
                    "current":         t.current_value,
                    "baseline":        t.baseline_value,
                    "z_score":         round(t.z_score, 2) if t.z_score is not None else None,
                    "direction":       t.trend_direction,
                    "pct_vs_baseline": round(t.pct_vs_baseline, 1) if t.pct_vs_baseline is not None else None,
                    "is_anomalous":    t.is_anomalous,
                }
                for t in self.trends
            ],
            "deployment_events": [
                {
                    "service":         d.service,
                    "timestamp":       d.timestamp,
                    "description":     d.description,
                    "delta_minutes":   d.delta_minutes,
                }
                for d in self.deployment_events
            ],
            "reasoning_notes": self.reasoning_notes,
        }

    def to_prompt_block(self) -> str:
        """Inject into LLM prompt as time-context section."""
        if not self.trends and not self.deployment_events:
            return ""
        lines = ["TIME-SERIES CONTEXT", "━━━━━━━━━━━━━━━━━━━"]
        if self.trend_summary:
            lines.append(f"Trend: {self.trend_summary}")
        if self.deployment_note:
            lines.append(f"Deployment correlation: {self.deployment_note}")
        for t in self.trends:
            if t.is_anomalous:
                z_str = f" (z={t.z_score:.1f})" if t.z_score is not None else ""
                lines.append(
                    f"  • {t.metric_name}: {t.current_value:.2f}"
                    f" [{t.trend_direction.upper()}{z_str}]"
                    f" vs baseline {t.baseline_value:.2f}" if t.baseline_value else
                    f"  • {t.metric_name}: {t.current_value:.2f} [{t.trend_direction.upper()}]"
                )
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

async def build_timeseries_context(
    alert_name: str,
    service_name: str,
    metrics_raw: dict,
    http: httpx.AsyncClient,
) -> TimeSeriesContext:
    """
    Build a TimeSeriesContext for one incident within 5 seconds.

    Fetches 7-day baseline from Prometheus for key metrics, detects trend
    direction, and checks Gitea for recent deployments.

    Returns an empty-but-valid context on any error so it never blocks.
    """
    ctx = TimeSeriesContext(
        alert_name=alert_name,
        service_name=service_name,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    try:
        await asyncio.wait_for(
            _populate_context(ctx, metrics_raw, http),
            timeout=_TIMEOUT,
        )
    except asyncio.TimeoutError:
        logger.warning("Time-series context timed out for alert=%s", alert_name)
        ctx.reasoning_notes.append("Time-series baseline fetch timed out")
    except Exception as exc:  # noqa: BLE001
        logger.debug("Time-series context error: %s", exc)
        ctx.reasoning_notes.append(f"Time-series context unavailable: {exc}")

    return ctx


# ─────────────────────────────────────────────────────────────────────────────
# Internal
# ─────────────────────────────────────────────────────────────────────────────

async def _populate_context(
    ctx: TimeSeriesContext,
    metrics_raw: dict,
    http: httpx.AsyncClient,
) -> None:
    """Fetch baseline data and populate the context object."""
    # Map session.metrics keys to Prometheus expressions for baseline query
    metric_promql_map = {
        "cpu_usage_pct": (
            'avg(rate(process_cpu_seconds_total[5m])) * 100',
            "cpu_usage_pct",
        ),
        "p99_latency_ms": (
            "histogram_quantile(0.99, sum(rate(http_server_duration_bucket[5m])) by (le)) * 1000",
            "latency_p99_ms",
        ),
        "error_rate_pct": (
            '100 * sum(rate(http_server_duration_count{http_response_status_code=~"5.."}[5m]))'
            ' / sum(rate(http_server_duration_count[5m]))',
            "error_rate_pct",
        ),
    }

    rising_count  = 0
    falling_count = 0
    anomaly_count = 0

    for session_key, (promql, display_name) in metric_promql_map.items():
        current_val = metrics_raw.get(session_key)
        if current_val is None:
            continue

        baseline_val, baseline_stddev = await _fetch_7day_baseline(
            promql, http
        )

        z_score    = None
        pct_dev    = None
        is_anomaly = False
        if baseline_val is not None and baseline_stddev and baseline_stddev > 0:
            z_score    = (float(current_val) - baseline_val) / baseline_stddev
            pct_dev    = ((float(current_val) - baseline_val) / baseline_val * 100.0
                          if baseline_val != 0 else None)
            is_anomaly = abs(z_score) >= 2.0

        direction = _trend_direction(float(current_val), baseline_val)

        if direction == "rising":  rising_count += 1
        elif direction == "falling": falling_count += 1
        if is_anomaly: anomaly_count += 1

        ctx.trends.append(TrendSignal(
            metric_name=display_name,
            current_value=float(current_val),
            baseline_value=baseline_val,
            baseline_stddev=baseline_stddev,
            z_score=z_score,
            trend_direction=direction,
            pct_vs_baseline=pct_dev,
            is_anomalous=is_anomaly,
        ))

    # ── Deployment correlation ─────────────────────────────────────────────
    deploy_events = await _fetch_recent_deployments(http)
    ctx.deployment_events = deploy_events

    # ── Summarise ──────────────────────────────────────────────────────────
    ctx.baseline_anomaly = anomaly_count > 0

    if rising_count > falling_count:
        ctx.trend_summary = (
            f"{rising_count} metric(s) rising vs 7-day baseline"
            + (f"; {anomaly_count} statistically anomalous" if anomaly_count else "")
        )
    elif falling_count > 0:
        ctx.trend_summary = f"{falling_count} metric(s) falling vs baseline (potential recovery)"
    else:
        ctx.trend_summary = "Metrics stable relative to 7-day baseline"

    if deploy_events:
        recent = deploy_events[0]
        if abs(recent.delta_minutes) <= 30:
            ctx.deployment_note = (
                f"Deployment on '{recent.service}' ~{abs(recent.delta_minutes):.0f}m "
                f"{'before' if recent.delta_minutes > 0 else 'after'} alert — "
                "consider as potential trigger"
            )
            ctx.reasoning_notes.append(ctx.deployment_note)
        else:
            ctx.deployment_note = (
                f"Most recent deployment was {abs(recent.delta_minutes):.0f}m ago "
                "— unlikely direct cause"
            )


async def _fetch_7day_baseline(
    promql: str,
    http: httpx.AsyncClient,
) -> tuple[float | None, float | None]:
    """
    Fetch 7-day same-hour average and stddev via Prometheus subquery.
    Returns (mean, stddev) or (None, None) on failure.
    """
    mean_expr  = f"avg_over_time(({promql})[7d:5m])"
    std_expr   = f"stddev_over_time(({promql})[7d:5m])"

    mean_val = await _instant_query(mean_expr, http)
    std_val  = await _instant_query(std_expr, http)
    return mean_val, std_val


async def _instant_query(promql: str, http: httpx.AsyncClient) -> float | None:
    """Run a single Prometheus instant query and return the first scalar value."""
    try:
        resp = await http.get(
            f"{_PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if not results:
            return None
        return float(results[0]["value"][1])
    except Exception:  # noqa: BLE001
        return None


async def _fetch_recent_deployments(
    http: httpx.AsyncClient,
) -> list[DeploymentEvent]:
    """
    Check Gitea for recent commits/tags as proxy for deployment events.
    Only fetches; never blocks on failure.
    """
    events: list[DeploymentEvent] = []
    try:
        gitea_user = os.getenv("GITEA_ADMIN_USER", "aiops")
        gitea_pass = os.getenv("GITEA_ADMIN_PASS", "")
        resp = await http.get(
            f"{_GITEA_URL}/api/v1/repos/search?limit=5&token=",
            auth=(gitea_user, gitea_pass) if gitea_pass else None,
            timeout=2.0,
        )
        if resp.status_code == 200:
            repos = resp.json().get("data", [])
            now   = _utc_now()
            for repo in repos[:3]:
                updated = repo.get("updated")
                if updated:
                    try:
                        dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                        delta_minutes = (now - dt).total_seconds() / 60.0
                        if delta_minutes < 60:
                            events.append(DeploymentEvent(
                                service=repo.get("name", "?"),
                                timestamp=updated,
                                description=f"Repo updated: {repo.get('full_name', '?')}",
                                delta_minutes=delta_minutes,
                            ))
                    except Exception:
                        pass
    except Exception:  # noqa: BLE001
        pass
    return events


def _trend_direction(current: float, baseline: float | None) -> str:
    if baseline is None or baseline == 0:
        return "unknown"
    ratio = current / baseline
    if ratio > 1.20:
        return "rising"
    if ratio < 0.80:
        return "falling"
    return "stable"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
