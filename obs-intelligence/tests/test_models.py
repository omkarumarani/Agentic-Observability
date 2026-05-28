"""Tests for obs_intelligence.models — dataclass integrity checks."""
from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import fields

from obs_intelligence.models import (
    ObsFeatures,
    ScenarioMatch,
    RiskAssessment,
    Recommendation,
    EvidenceReport,
    AnomalySignal,
    ForecastResult,
)


class TestObsFeatures:
    def test_defaults(self):
        f = ObsFeatures(
            alert_name="X", service_name="svc",
            severity="warning", domain="compute",
            timestamp=datetime.now(timezone.utc),
        )
        assert f.error_rate == 0.0
        assert f.osd_up_count == 0
        assert f.log_anomaly_detected is False
        assert f.labels == {}
        assert f.annotations == {}
        assert f.recurrence_count == 0

    def test_all_fields_present(self):
        field_names = {fld.name for fld in fields(ObsFeatures)}
        expected = {
            "alert_name", "service_name", "severity", "domain", "timestamp",
            "error_rate", "latency_p95", "latency_p99", "cpu_usage",
            "memory_usage", "request_rate", "active_connections",
            "osd_up_count", "osd_total_count", "pool_usage_pct",
            "cluster_health_score", "degraded_pgs", "io_latency", "pvc_iops",
            "recent_error_count", "recent_warning_count", "log_anomaly_detected",
            "recurrence_count", "labels", "annotations",
        }
        assert expected.issubset(field_names)


class TestScenarioMatch:
    def test_defaults(self):
        m = ScenarioMatch(
            scenario_id="X", display_name="X",
            confidence=0.5, domain="compute",
        )
        assert m.matched_features == []
        assert m.scenario_file is None


class TestRiskAssessment:
    def test_defaults(self):
        r = RiskAssessment(risk_score=0.5, risk_level="medium")
        assert r.contributing_factors == []
        assert r.blast_radius == "unknown"
        assert r.time_to_impact is None
        assert r.requires_approval is True


class TestRecommendation:
    def test_defaults(self):
        rec = Recommendation(
            action_type="escalate", display_name="X",
            description="desc", confidence=0.5,
        )
        assert rec.autonomous is False
        assert rec.ansible_playbook is None
        assert rec.xyops_workflow is None
        assert rec.estimated_duration is None
        assert rec.rollback_plan is None


class TestEvidenceReport:
    def test_defaults(self):
        f = ObsFeatures(
            alert_name="X", service_name="svc",
            severity="warning", domain="compute",
            timestamp=datetime.now(timezone.utc),
        )
        r = EvidenceReport(trace_id="t", incident_id="i", features=f)
        assert r.scenario_matches == []
        assert r.risk is None
        assert r.recommendations == []
        assert r.ai_summary is None
        assert r.engine_version == "1.0.0"


class TestAnomalySignal:
    def test_defaults(self):
        s = AnomalySignal(
            metric_name="x", current_value=1.0,
            baseline_mean=0.5, baseline_stddev=0.1, z_score=5.0,
        )
        assert s.anomaly_type == "spike"
        assert s.detected_at is None
        assert s.confidence == 0.0


class TestForecastResult:
    def test_defaults(self):
        f = ForecastResult(metric_name="x")
        assert f.forecast_values == []
        assert f.forecast_timestamps == []
        assert f.predicted_breach is None
        assert f.threshold is None
        assert f.model_used == "linear"
        assert f.horizon_minutes == 60
