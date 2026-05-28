"""
obs-intelligence test fixtures.

Environment variables are set BEFORE importing application modules to prevent
module-level side-effects (OTel, DB connections, ChromaDB, etc.).
"""
from __future__ import annotations

import os
import sys

# ── Environment stubs (before any app import) ─────────────────────────────────
os.environ.setdefault("PROMETHEUS_URL", "http://localhost:9999")
os.environ.setdefault("LOKI_URL", "http://localhost:9998")
os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
os.environ.setdefault("OTEL_SERVICE_NAME", "obs-intelligence-test")
os.environ.setdefault("SCENARIOS_DIR", os.path.join(os.path.dirname(__file__), "..", "scenarios"))

# Ensure the obs_intelligence package is importable
_obs_pkg = os.path.join(os.path.dirname(__file__), "..", "app")
if _obs_pkg not in sys.path:
    sys.path.insert(0, _obs_pkg)

import pytest
from datetime import datetime, timezone

from obs_intelligence.models import (
    ObsFeatures,
    ScenarioMatch,
    RiskAssessment,
    Recommendation,
    EvidenceReport,
    AnomalySignal,
    ForecastResult,
)
from obs_intelligence.scenario_loader import ScenarioDef, ConditionDef


# ═══════════════════════════════════════════════════════════════════════════════
# Shared fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def compute_features() -> ObsFeatures:
    """Compute-domain features with elevated error rate and latency."""
    return ObsFeatures(
        alert_name="HighErrorRate",
        service_name="frontend-api",
        severity="critical",
        domain="compute",
        timestamp=datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        error_rate=0.15,
        latency_p95=0.250,
        latency_p99=0.620,
        cpu_usage=0.71,
        memory_usage=0.55,
        request_rate=120.0,
        active_connections=85,
        recent_error_count=12,
        recent_warning_count=5,
        log_anomaly_detected=True,
    )


@pytest.fixture
def storage_features() -> ObsFeatures:
    """Storage-domain features with OSD down and pool near-full."""
    return ObsFeatures(
        alert_name="CephOSDDown",
        service_name="storage-cluster",
        severity="critical",
        domain="storage",
        timestamp=datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        osd_up_count=9,
        osd_total_count=10,
        pool_usage_pct=0.82,
        cluster_health_score=1,
        degraded_pgs=25,
        io_latency=0.045,
        pvc_iops=500.0,
        recent_error_count=3,
        recent_warning_count=8,
        log_anomaly_detected=True,
    )


@pytest.fixture
def low_risk_features() -> ObsFeatures:
    """Compute features with low severity and no anomalies."""
    return ObsFeatures(
        alert_name="MinorLatency",
        service_name="backend-api",
        severity="warning",
        domain="compute",
        timestamp=datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        error_rate=0.005,
        latency_p99=0.120,
        request_rate=50.0,
    )


@pytest.fixture
def sample_scenario_def() -> ScenarioDef:
    """Scenario definition matching high error rate."""
    return ScenarioDef(
        scenario_id="HIGH_ERROR_RATE",
        display_name="High Error Rate",
        domain="compute",
        conditions=[
            ConditionDef(field="error_rate", operator="gt", threshold=0.05, weight=0.4),
            ConditionDef(field="latency_p99", operator="gt", threshold=0.5, weight=0.3),
            ConditionDef(field="log_anomaly_detected", operator="true", weight=0.2),
        ],
        action="restart_service",
        autonomy="approval_gated",
        rca="High error rate detected — likely cascading failure from upstream deploy.",
        playbook_hint="Restart the failing pods and check recent deployments.",
        alert_name_patterns=["*ErrorRate*", "*Error*"],
        alert_match_weight=0.3,
        confidence_threshold=0.3,
    )


@pytest.fixture
def storage_scenario_def() -> ScenarioDef:
    """Scenario definition matching OSD down."""
    return ScenarioDef(
        scenario_id="OSD_DOWN",
        display_name="Ceph OSD Down",
        domain="storage",
        conditions=[
            ConditionDef(field="osd_up_count", operator="lt", threshold=10, weight=0.4),
            ConditionDef(field="cluster_health_score", operator="lt", threshold=2, weight=0.3),
            ConditionDef(field="degraded_pgs", operator="gt", threshold=0, weight=0.2),
        ],
        action="osd_reweight",
        autonomy="approval_gated",
        rca="Ceph OSD is down, causing degraded placement groups.",
        playbook_hint="Check OSD status, reweight or restart the OSD daemon.",
        alert_name_patterns=["CephOSD*"],
        alert_match_weight=0.3,
        confidence_threshold=0.3,
    )


@pytest.fixture
def sample_scenario_match() -> ScenarioMatch:
    return ScenarioMatch(
        scenario_id="HIGH_ERROR_RATE",
        display_name="High Error Rate",
        confidence=0.85,
        domain="compute",
        matched_features=["error_rate", "latency_p99", "log_anomaly_detected"],
    )


@pytest.fixture
def sample_risk() -> RiskAssessment:
    return RiskAssessment(
        risk_score=0.72,
        risk_level="high",
        contributing_factors=["severity=critical", "scenario_confidence=0.85"],
        blast_radius="single service + dependents",
        time_to_impact="immediate",
        requires_approval=True,
    )


@pytest.fixture
def sample_recommendation() -> Recommendation:
    return Recommendation(
        action_type="restart_service",
        display_name="High Error Rate",
        description="Restart the failing pods.",
        confidence=0.85,
        autonomous=False,
        ansible_playbook="restart_service.yml",
        xyops_workflow="Compute AIOps Agent Pipeline",
        estimated_duration="30s–2 min",
        rollback_plan="Re-deploy the previous container image version.",
    )


@pytest.fixture
def sample_evidence(
    compute_features,
    sample_scenario_match,
    sample_risk,
    sample_recommendation,
) -> EvidenceReport:
    return EvidenceReport(
        trace_id="abc123",
        incident_id="INC-001",
        features=compute_features,
        scenario_matches=[sample_scenario_match],
        risk=sample_risk,
        recommendations=[sample_recommendation],
        generated_at=datetime(2025, 1, 15, 12, 0, 5, tzinfo=timezone.utc),
    )
