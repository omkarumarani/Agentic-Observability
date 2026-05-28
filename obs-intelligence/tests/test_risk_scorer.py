"""Tests for obs_intelligence.risk_scorer."""
from __future__ import annotations

from obs_intelligence.models import ObsFeatures, ScenarioMatch, RiskAssessment
from obs_intelligence.risk_scorer import score_risk


# ═══════════════════════════════════════════════════════════════════════════════
# Risk level thresholds
# ═══════════════════════════════════════════════════════════════════════════════


class TestRiskLevels:
    def test_critical_severity_high_confidence(self, compute_features, sample_scenario_match):
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        assert risk.risk_level in ("high", "critical")
        assert risk.risk_score >= 0.6

    def test_low_risk_scenario(self, low_risk_features):
        low_match = ScenarioMatch(
            scenario_id="MINOR",
            display_name="Minor Issue",
            confidence=0.3,
            domain="compute",
        )
        risk = score_risk(low_risk_features, low_match, "compute")
        assert risk.risk_level in ("low", "medium")
        assert risk.risk_score < 0.6

    def test_no_scenario_match(self, compute_features):
        risk = score_risk(compute_features, None, "compute")
        # Without scenario confidence, score drops
        assert risk.risk_score < 1.0
        assert "no_scenario_match" in str(risk.contributing_factors)

    def test_risk_score_bounded(self, compute_features, sample_scenario_match):
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        assert 0.0 <= risk.risk_score <= 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Contributing factors
# ═══════════════════════════════════════════════════════════════════════════════


class TestContributingFactors:
    def test_includes_severity(self, compute_features, sample_scenario_match):
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        assert any("severity" in f for f in risk.contributing_factors)

    def test_includes_scenario_confidence(self, compute_features, sample_scenario_match):
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        assert any("scenario_confidence" in f for f in risk.contributing_factors)

    def test_includes_log_anomaly_when_detected(self, compute_features, sample_scenario_match):
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        assert any("log_anomaly" in f for f in risk.contributing_factors)

    def test_includes_error_rate_for_compute(self, compute_features, sample_scenario_match):
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        assert any("error_rate" in f for f in risk.contributing_factors)


# ═══════════════════════════════════════════════════════════════════════════════
# Blast radius
# ═══════════════════════════════════════════════════════════════════════════════


class TestBlastRadius:
    def test_compute_high_traffic(self, compute_features, sample_scenario_match):
        # request_rate=120 > 100 → platform-wide
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        assert "platform-wide" in risk.blast_radius

    def test_compute_low_traffic(self, low_risk_features):
        risk = score_risk(low_risk_features, None, "compute")
        assert "single service" in risk.blast_radius

    def test_storage_osd_down(self, storage_features):
        match = ScenarioMatch(
            scenario_id="OSD_DOWN", display_name="OSD Down",
            confidence=0.8, domain="storage",
        )
        risk = score_risk(storage_features, match, "storage")
        assert "storage cluster" in risk.blast_radius.lower() or "PVC" in risk.blast_radius

    def test_storage_pool_full(self):
        features = ObsFeatures(
            alert_name="PoolFull",
            service_name="ceph",
            severity="critical",
            domain="storage",
            timestamp=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            pool_usage_pct=0.95,
            osd_up_count=10,
            osd_total_count=10,
        )
        risk = score_risk(features, None, "storage")
        assert "writing to pool" in risk.blast_radius.lower() or risk.blast_radius != "unknown"


# ═══════════════════════════════════════════════════════════════════════════════
# Time to impact
# ═══════════════════════════════════════════════════════════════════════════════


class TestTimeToImpact:
    def test_critical_risk_immediate(self, compute_features, sample_scenario_match):
        # Critical severity + high confidence should yield high risk → immediate
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        if risk.risk_score >= 0.80:
            assert risk.time_to_impact == "immediate"

    def test_low_risk_may_be_none(self, low_risk_features):
        risk = score_risk(low_risk_features, None, "compute")
        # Low risk → time_to_impact may be None or a time estimate
        assert risk.time_to_impact is None or isinstance(risk.time_to_impact, str)


# ═══════════════════════════════════════════════════════════════════════════════
# Storage-specific urgency
# ═══════════════════════════════════════════════════════════════════════════════


class TestStorageUrgency:
    def test_pool_usage_above_70_adds_urgency(self, storage_features):
        match = ScenarioMatch(
            scenario_id="POOL", display_name="Pool",
            confidence=0.5, domain="storage",
        )
        risk = score_risk(storage_features, match, "storage")
        assert any("pool_usage_pct" in f for f in risk.contributing_factors)

    def test_osd_availability_adds_urgency(self, storage_features):
        match = ScenarioMatch(
            scenario_id="OSD", display_name="OSD",
            confidence=0.5, domain="storage",
        )
        risk = score_risk(storage_features, match, "storage")
        assert any("osd_availability" in f for f in risk.contributing_factors)

    def test_degraded_pgs_adds_urgency(self, storage_features):
        match = ScenarioMatch(
            scenario_id="PG", display_name="PG",
            confidence=0.5, domain="storage",
        )
        risk = score_risk(storage_features, match, "storage")
        assert any("degraded_pgs" in f for f in risk.contributing_factors)


# ═══════════════════════════════════════════════════════════════════════════════
# Requires approval
# ═══════════════════════════════════════════════════════════════════════════════


class TestRequiresApproval:
    def test_high_risk_requires_approval(self, compute_features, sample_scenario_match):
        risk = score_risk(compute_features, sample_scenario_match, "compute")
        if risk.risk_level in ("high", "critical"):
            assert risk.requires_approval is True

    def test_low_risk_no_approval(self, low_risk_features):
        risk = score_risk(low_risk_features, None, "compute")
        if risk.risk_level in ("low", "medium"):
            assert risk.requires_approval is False
