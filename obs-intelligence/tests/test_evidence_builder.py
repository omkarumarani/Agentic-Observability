"""Tests for obs_intelligence.evidence_builder."""
from __future__ import annotations

from datetime import datetime, timezone

from obs_intelligence.models import (
    ObsFeatures,
    ScenarioMatch,
    RiskAssessment,
    Recommendation,
    EvidenceReport,
)
from obs_intelligence.evidence_builder import build_evidence, evidence_lines


# ═══════════════════════════════════════════════════════════════════════════════
# build_evidence
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuildEvidence:
    def test_assembles_complete_report(
        self, compute_features, sample_scenario_match, sample_risk, sample_recommendation,
    ):
        report = build_evidence(
            trace_id="trace-1",
            incident_id="INC-042",
            features=compute_features,
            matches=[sample_scenario_match],
            risk=sample_risk,
            recommendations=[sample_recommendation],
        )
        assert isinstance(report, EvidenceReport)
        assert report.trace_id == "trace-1"
        assert report.incident_id == "INC-042"
        assert report.features is compute_features
        assert report.scenario_matches == [sample_scenario_match]
        assert report.risk is sample_risk
        assert report.recommendations == [sample_recommendation]
        assert report.generated_at is not None

    def test_empty_matches_and_recommendations(self, compute_features, sample_risk):
        report = build_evidence(
            trace_id="t", incident_id="", features=compute_features,
            matches=[], risk=sample_risk, recommendations=[],
        )
        assert report.scenario_matches == []
        assert report.recommendations == []


# ═══════════════════════════════════════════════════════════════════════════════
# evidence_lines — compute domain
# ═══════════════════════════════════════════════════════════════════════════════


class TestEvidenceLinesCompute:
    def test_contains_alert_identity(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        joined = "\n".join(lines)
        assert "HighErrorRate" in joined
        assert "frontend-api" in joined
        assert "CRITICAL" in joined

    def test_contains_error_rate(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("Error rate" in l for l in lines)

    def test_contains_latency(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("Latency p99" in l for l in lines)

    def test_contains_request_rate(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("Request rate" in l for l in lines)

    def test_contains_cpu_usage(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("CPU usage" in l for l in lines)

    def test_contains_log_anomaly(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("Log anomaly" in l for l in lines)

    def test_contains_scenario_match(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("High Error Rate" in l for l in lines)
        assert any("confidence" in l.lower() for l in lines)

    def test_contains_risk_level(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("Risk level" in l for l in lines)

    def test_contains_recommended_action(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        assert any("Recommended action" in l for l in lines)

    def test_warning_badge_on_high_error_rate(self, sample_evidence):
        lines = evidence_lines(sample_evidence)
        error_line = [l for l in lines if "Error rate" in l]
        if error_line:
            assert "⚠" in error_line[0]


# ═══════════════════════════════════════════════════════════════════════════════
# evidence_lines — storage domain
# ═══════════════════════════════════════════════════════════════════════════════


class TestEvidenceLinesStorage:
    def _make_storage_report(self, storage_features, sample_risk):
        match = ScenarioMatch(
            scenario_id="OSD_DOWN", display_name="OSD Down",
            confidence=0.8, domain="storage",
            matched_features=["osd_up_count", "cluster_health_score"],
        )
        rec = Recommendation(
            action_type="osd_reweight", display_name="OSD Down",
            description="Reweight OSD", confidence=0.8,
        )
        return build_evidence(
            trace_id="t", incident_id="INC-S1",
            features=storage_features, matches=[match],
            risk=sample_risk, recommendations=[rec],
        )

    def test_contains_osd_status(self, storage_features, sample_risk):
        report = self._make_storage_report(storage_features, sample_risk)
        lines = evidence_lines(report)
        assert any("OSD status" in l for l in lines)

    def test_contains_pool_usage(self, storage_features, sample_risk):
        report = self._make_storage_report(storage_features, sample_risk)
        lines = evidence_lines(report)
        assert any("Pool usage" in l for l in lines)

    def test_contains_cluster_health_warning(self, storage_features, sample_risk):
        report = self._make_storage_report(storage_features, sample_risk)
        lines = evidence_lines(report)
        assert any("Cluster health" in l for l in lines)

    def test_contains_degraded_pgs(self, storage_features, sample_risk):
        report = self._make_storage_report(storage_features, sample_risk)
        lines = evidence_lines(report)
        assert any("Degraded" in l for l in lines)

    def test_no_scenario_no_match_line(self, storage_features, sample_risk):
        report = build_evidence(
            trace_id="t", incident_id="",
            features=storage_features, matches=[],
            risk=sample_risk, recommendations=[],
        )
        lines = evidence_lines(report)
        assert not any("Best scenario match" in l for l in lines)


# ═══════════════════════════════════════════════════════════════════════════════
# evidence_lines — edge cases
# ═══════════════════════════════════════════════════════════════════════════════


class TestEvidenceLinesEdgeCases:
    def test_no_risk(self, compute_features):
        report = EvidenceReport(
            trace_id="t", incident_id="",
            features=compute_features,
            risk=None,
        )
        lines = evidence_lines(report)
        assert not any("Risk level" in l for l in lines)

    def test_multiple_scenario_matches(self, compute_features, sample_risk):
        m1 = ScenarioMatch(
            scenario_id="A", display_name="Match A", confidence=0.9, domain="compute",
        )
        m2 = ScenarioMatch(
            scenario_id="B", display_name="Match B", confidence=0.6, domain="compute",
        )
        report = build_evidence(
            trace_id="t", incident_id="",
            features=compute_features, matches=[m1, m2],
            risk=sample_risk, recommendations=[],
        )
        lines = evidence_lines(report)
        assert any("Match A" in l for l in lines)
        assert any("Other candidates" in l for l in lines)
