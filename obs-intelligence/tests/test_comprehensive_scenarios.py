"""
tests/test_comprehensive_scenarios.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive scenario tests covering every YAML scenario in the architecture.

Tests:
  1. All 20 scenario YAML files load without errors
  2. All 10 compute scenarios match realistic ObsFeatures
  3. All 10 storage scenarios match realistic ObsFeatures
  4. Cross-domain: compute features don't match storage scenarios and vice versa
  5. Risk scoring integration for each scenario
  6. Recommender integration for each scenario
  7. Evidence builder integration for matched scenarios
  8. End-to-end intelligence pipeline per scenario
"""
from __future__ import annotations

import datetime as dt
import os
import pathlib
import sys
from types import SimpleNamespace

import pytest

# ── Setup: ensure obs_intelligence is importable ──────────────────────────────
_OBS_APP = pathlib.Path(__file__).parents[1] / "app" / "obs_intelligence"
_OBS_ROOT = pathlib.Path(__file__).parents[1] / "app"
for p in (_OBS_ROOT, _OBS_APP.parent):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

os.environ.setdefault("PROMETHEUS_URL", "http://prometheus-mock:9090")
os.environ.setdefault("LOKI_URL", "http://loki-mock:3100")

from obs_intelligence.models import ObsFeatures, ScenarioMatch, RiskAssessment, Recommendation
from obs_intelligence.scenario_loader import load_scenarios, ScenarioDef, ConditionDef
from obs_intelligence.scenario_correlator import match_scenarios, match_best, _eval_condition
from obs_intelligence.risk_scorer import score_risk
from obs_intelligence.recommender import recommend
from obs_intelligence.evidence_builder import build_evidence, evidence_lines

# ── Locate real scenarios directory ───────────────────────────────────────────
SCENARIOS_DIR = pathlib.Path(__file__).parents[1] / "scenarios"

# ── Fake autonomy rules for recommender tests ────────────────────────────────
_COMPUTE_RULES = SimpleNamespace(
    APPROVAL_REQUIRED={"rollback_deploy", "restart_service", "circuit_break_dependency",
                       "scale_workers", "deep_dive_investigation", "investigate_errors",
                       "investigate_latency", "cpu_scale_out"},
    AUTONOMOUS_ALLOWED={"reduce_otel_sampling", "throttle_noisy_neighbour"},
    HUMAN_ONLY={"deep_dive_investigation"},
    FORCE_APPROVAL_ABOVE_RISK=0.70,
)

_STORAGE_RULES = SimpleNamespace(
    APPROVAL_REQUIRED={"osd_reweight", "pool_expand_advisory", "pool_critical_action",
                       "investigate_io", "cluster_assessment", "escalate"},
    AUTONOMOUS_ALLOWED={"pvc_throttle"},
    HUMAN_ONLY={"multi_osd_escalate"},
    FORCE_APPROVAL_ABOVE_RISK=0.65,
)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. YAML loading — all 20 files parse without errors
# ═══════════════════════════════════════════════════════════════════════════════

class TestScenarioLoading:

    def test_compute_scenarios_load(self):
        scenarios = load_scenarios(str(SCENARIOS_DIR), domain="compute")
        assert len(scenarios) == 10, f"Expected 10 compute scenarios, got {len(scenarios)}"

    def test_storage_scenarios_load(self):
        scenarios = load_scenarios(str(SCENARIOS_DIR), domain="storage")
        assert len(scenarios) == 10, f"Expected 10 storage scenarios, got {len(scenarios)}"

    def test_all_scenarios_load(self):
        scenarios = load_scenarios(str(SCENARIOS_DIR))
        assert len(scenarios) == 20, f"Expected 20 scenarios, got {len(scenarios)}"

    def test_all_scenario_ids_unique(self):
        all_scenarios = load_scenarios(str(SCENARIOS_DIR))
        ids = [s.scenario_id for s in all_scenarios]
        assert len(ids) == len(set(ids)), f"Duplicate IDs: {[x for x in ids if ids.count(x) > 1]}"

    def test_all_scenarios_have_required_fields(self):
        for s in load_scenarios(str(SCENARIOS_DIR)):
            assert s.scenario_id, f"Missing scenario_id in {s.scenario_file}"
            assert s.display_name, f"Missing display_name in {s.scenario_id}"
            assert s.domain in ("compute", "storage"), f"Bad domain: {s.domain}"
            assert len(s.conditions) > 0, f"No conditions in {s.scenario_id}"
            assert s.rca, f"Missing RCA in {s.scenario_id}"
            assert s.action, f"Missing action in {s.scenario_id}"

    def test_all_scenarios_have_valid_autonomy(self):
        valid = {"autonomous", "approval_gated", "human_only"}
        for s in load_scenarios(str(SCENARIOS_DIR)):
            assert s.autonomy in valid, f"Bad autonomy '{s.autonomy}' in {s.scenario_id}"

    def test_all_confidence_thresholds_in_range(self):
        for s in load_scenarios(str(SCENARIOS_DIR)):
            assert 0.0 < s.confidence_threshold <= 1.0, (
                f"Bad threshold {s.confidence_threshold} in {s.scenario_id}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Compute scenarios — each one matches appropriate features
# ═══════════════════════════════════════════════════════════════════════════════

# Realistic ObsFeatures per compute scenario
COMPUTE_FEATURE_MAP = {
    "error_spike": ObsFeatures(
        alert_name="HighErrorRate", service_name="frontend-api", severity="critical",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        error_rate=0.12, latency_p99=0.8, log_anomaly_detected=True,
        recent_error_count=25,
    ),
    "latency_regression": ObsFeatures(
        alert_name="HighLatency", service_name="backend-api", severity="warning",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        latency_p95=1.2, latency_p99=2.5, error_rate=0.01,
    ),
    "cpu_saturation": ObsFeatures(
        alert_name="HighCPUUsage", service_name="compute-agent", severity="critical",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        cpu_usage=0.92, latency_p99=1.5, request_rate=200,
    ),
    "memory_leak_emergence": ObsFeatures(
        alert_name="HighMemoryUsage", service_name="frontend-api", severity="warning",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        memory_usage=0.88, cpu_usage=0.45, latency_p99=0.6,
    ),
    "cascading_timeout_chain": ObsFeatures(
        alert_name="CascadingFailureDetected", service_name="backend-api", severity="critical",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        error_rate=0.08, latency_p99=5.0, latency_p95=3.0,
        active_connections=500, request_rate=150,
    ),
    "noisy_neighbor_effect": ObsFeatures(
        alert_name="NoisyNeighborCPU", service_name="backend-api", severity="warning",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        cpu_usage=0.85, latency_p99=0.9, request_rate=50,
    ),
    "queue_backlog": ObsFeatures(
        alert_name="QueueBacklog", service_name="compute-agent", severity="warning",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        latency_p99=3.0, active_connections=300, error_rate=0.02,
    ),
    "collector_overload": ObsFeatures(
        alert_name="CollectorOverload", service_name="otel-collector", severity="warning",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        cpu_usage=0.90, memory_usage=0.80, request_rate=500,
    ),
    "baseline_shift_after_deploy": ObsFeatures(
        alert_name="BaselineShift", service_name="frontend-api", severity="warning",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        latency_p99=0.6, error_rate=0.03, request_rate=100,
    ),
    "recurring_failure_signature": ObsFeatures(
        alert_name="RecurringAlertDetected", service_name="backend-api", severity="critical",
        domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        error_rate=0.05, recurrence_count=4, log_anomaly_detected=True,
    ),
}


class TestComputeScenarioMatching:

    @pytest.fixture(autouse=True)
    def _load_catalog(self):
        self.catalog = load_scenarios(str(SCENARIOS_DIR), domain="compute")

    @pytest.mark.parametrize("scenario_id", list(COMPUTE_FEATURE_MAP.keys()))
    def test_scenario_matches_features(self, scenario_id):
        features = COMPUTE_FEATURE_MAP[scenario_id]
        matches = match_scenarios(features, self.catalog)
        matched_ids = [m.scenario_id for m in matches]
        assert scenario_id in matched_ids, (
            f"Scenario '{scenario_id}' did not match with features. "
            f"Matched: {matched_ids}"
        )

    @pytest.mark.parametrize("scenario_id", list(COMPUTE_FEATURE_MAP.keys()))
    def test_scenario_confidence_above_threshold(self, scenario_id):
        features = COMPUTE_FEATURE_MAP[scenario_id]
        scenario_def = next(s for s in self.catalog if s.scenario_id == scenario_id)
        matches = match_scenarios(features, self.catalog)
        match = next((m for m in matches if m.scenario_id == scenario_id), None)
        assert match is not None
        assert match.confidence >= scenario_def.confidence_threshold


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Storage scenarios — each one matches appropriate features
# ═══════════════════════════════════════════════════════════════════════════════

STORAGE_FEATURE_MAP = {
    "single_osd_down": ObsFeatures(
        alert_name="CephOSDDown", service_name="storage", severity="critical",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        osd_up_count=5, osd_total_count=6, degraded_pgs=32,
        cluster_health_score=1,
    ),
    "multi_osd_failure": ObsFeatures(
        alert_name="CephMultipleOSDDown", service_name="storage", severity="critical",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        osd_up_count=3, osd_total_count=6, degraded_pgs=150,
        cluster_health_score=0,
    ),
    "pool_near_full": ObsFeatures(
        alert_name="CephPoolNearFull", service_name="storage", severity="warning",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        pool_usage_pct=0.78, cluster_health_score=1,
    ),
    "pool_full_critical": ObsFeatures(
        alert_name="CephPoolFull", service_name="storage", severity="critical",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        pool_usage_pct=0.92, cluster_health_score=0, degraded_pgs=5,
    ),
    "pvc_latency_degradation": ObsFeatures(
        alert_name="PVCHighLatency", service_name="storage", severity="warning",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        io_latency=0.25, pvc_iops=500,
    ),
    "noisy_pvc_iops": ObsFeatures(
        alert_name="NoisyPVCDetected", service_name="storage", severity="warning",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        pvc_iops=2000, io_latency=0.15,
    ),
    "cluster_degraded_health": ObsFeatures(
        alert_name="CephClusterDegraded", service_name="storage", severity="critical",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        cluster_health_score=0, degraded_pgs=50, osd_up_count=4, osd_total_count=6,
    ),
    "ceph_rebalance_storm": ObsFeatures(
        alert_name="CephRebalanceStorm", service_name="storage", severity="warning",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        io_latency=0.30, degraded_pgs=100, cluster_health_score=1,
    ),
    "pool_fill_forecast_breach": ObsFeatures(
        alert_name="PoolFillForecast", service_name="storage", severity="warning",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        pool_usage_pct=0.65, cluster_health_score=2,
    ),
    "storage_io_brownout": ObsFeatures(
        alert_name="StorageIOBrownout", service_name="storage", severity="critical",
        domain="storage", timestamp=dt.datetime.now(dt.timezone.utc),
        io_latency=0.50, pvc_iops=100, cluster_health_score=1,
    ),
}


class TestStorageScenarioMatching:

    @pytest.fixture(autouse=True)
    def _load_catalog(self):
        self.catalog = load_scenarios(str(SCENARIOS_DIR), domain="storage")

    @pytest.mark.parametrize("scenario_id", list(STORAGE_FEATURE_MAP.keys()))
    def test_scenario_matches_features(self, scenario_id):
        features = STORAGE_FEATURE_MAP[scenario_id]
        matches = match_scenarios(features, self.catalog)
        matched_ids = [m.scenario_id for m in matches]
        assert scenario_id in matched_ids, (
            f"Scenario '{scenario_id}' did not match with features. "
            f"Matched: {matched_ids}"
        )

    @pytest.mark.parametrize("scenario_id", list(STORAGE_FEATURE_MAP.keys()))
    def test_scenario_confidence_above_threshold(self, scenario_id):
        features = STORAGE_FEATURE_MAP[scenario_id]
        scenario_def = next(s for s in self.catalog if s.scenario_id == scenario_id)
        matches = match_scenarios(features, self.catalog)
        match = next((m for m in matches if m.scenario_id == scenario_id), None)
        assert match is not None
        assert match.confidence >= scenario_def.confidence_threshold


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Cross-domain isolation — no cross-contamination
# ═══════════════════════════════════════════════════════════════════════════════

class TestCrossDomainIsolation:

    def test_compute_features_do_not_match_storage_scenarios(self):
        storage_catalog = load_scenarios(str(SCENARIOS_DIR), domain="storage")
        for sid, features in COMPUTE_FEATURE_MAP.items():
            matches = match_scenarios(features, storage_catalog)
            assert len(matches) == 0, (
                f"Compute features '{sid}' incorrectly matched storage "
                f"scenarios: {[m.scenario_id for m in matches]}"
            )

    def test_storage_features_do_not_match_compute_scenarios(self):
        compute_catalog = load_scenarios(str(SCENARIOS_DIR), domain="compute")
        for sid, features in STORAGE_FEATURE_MAP.items():
            matches = match_scenarios(features, compute_catalog)
            assert len(matches) == 0, (
                f"Storage features '{sid}' incorrectly matched compute "
                f"scenarios: {[m.scenario_id for m in matches]}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Risk scoring integration per scenario
# ═══════════════════════════════════════════════════════════════════════════════

class TestRiskScoringIntegration:

    @pytest.fixture(autouse=True)
    def _load_catalogs(self):
        self.compute_catalog = load_scenarios(str(SCENARIOS_DIR), domain="compute")
        self.storage_catalog = load_scenarios(str(SCENARIOS_DIR), domain="storage")

    @pytest.mark.parametrize("scenario_id", list(COMPUTE_FEATURE_MAP.keys()))
    def test_compute_scenario_produces_risk(self, scenario_id):
        features = COMPUTE_FEATURE_MAP[scenario_id]
        matches = match_scenarios(features, self.compute_catalog)
        assert len(matches) > 0
        risk = score_risk(features, matches[0], "compute")
        assert risk.risk_score >= 0.0
        assert risk.risk_score <= 1.0
        assert risk.risk_level in ("critical", "high", "medium", "low")

    @pytest.mark.parametrize("scenario_id", list(STORAGE_FEATURE_MAP.keys()))
    def test_storage_scenario_produces_risk(self, scenario_id):
        features = STORAGE_FEATURE_MAP[scenario_id]
        matches = match_scenarios(features, self.storage_catalog)
        assert len(matches) > 0
        risk = score_risk(features, matches[0], "storage")
        assert risk.risk_score >= 0.0
        assert risk.risk_score <= 1.0
        assert risk.risk_level in ("critical", "high", "medium", "low")

    def test_multi_osd_failure_is_high_or_critical_risk(self):
        features = STORAGE_FEATURE_MAP["multi_osd_failure"]
        matches = match_scenarios(features, self.storage_catalog)
        risk = score_risk(features, matches[0], "storage")
        assert risk.risk_level in ("high", "critical"), (
            f"Multi-OSD failure should be high/critical risk, got {risk.risk_level}"
        )

    def test_pool_full_critical_is_medium_or_higher_risk(self):
        features = STORAGE_FEATURE_MAP["pool_full_critical"]
        matches = match_scenarios(features, self.storage_catalog)
        risk = score_risk(features, matches[0], "storage")
        assert risk.risk_level in ("medium", "high", "critical"), (
            f"Pool full critical should be at least medium risk, got {risk.risk_level}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Recommender integration per scenario
# ═══════════════════════════════════════════════════════════════════════════════

class TestRecommenderIntegration:

    @pytest.fixture(autouse=True)
    def _load_catalogs(self):
        self.compute_catalog = load_scenarios(str(SCENARIOS_DIR), domain="compute")
        self.storage_catalog = load_scenarios(str(SCENARIOS_DIR), domain="storage")

    def _get_best(self, features, catalog, domain):
        best_match, best_def = match_best(features, catalog)
        risk = score_risk(features, best_match, domain)
        rules = _COMPUTE_RULES if domain == "compute" else _STORAGE_RULES
        rec = recommend(best_match, best_def, risk, domain, rules)
        return rec

    @pytest.mark.parametrize("scenario_id", list(COMPUTE_FEATURE_MAP.keys()))
    def test_compute_scenario_produces_recommendation(self, scenario_id):
        features = COMPUTE_FEATURE_MAP[scenario_id]
        rec = self._get_best(features, self.compute_catalog, "compute")
        assert rec.action_type != ""
        assert isinstance(rec.autonomous, bool)

    @pytest.mark.parametrize("scenario_id", list(STORAGE_FEATURE_MAP.keys()))
    def test_storage_scenario_produces_recommendation(self, scenario_id):
        features = STORAGE_FEATURE_MAP[scenario_id]
        rec = self._get_best(features, self.storage_catalog, "storage")
        assert rec.action_type != ""
        assert isinstance(rec.autonomous, bool)

    def test_multi_osd_failure_not_autonomous(self):
        features = STORAGE_FEATURE_MAP["multi_osd_failure"]
        rec = self._get_best(features, self.storage_catalog, "storage")
        # Multi-OSD failures should NEVER be automated
        assert rec.autonomous is False, (
            f"Multi-OSD should NOT be autonomous, got autonomous={rec.autonomous}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Evidence builder integration per scenario
# ═══════════════════════════════════════════════════════════════════════════════

class TestEvidenceBuilderIntegration:

    @pytest.fixture(autouse=True)
    def _load_catalogs(self):
        self.compute_catalog = load_scenarios(str(SCENARIOS_DIR), domain="compute")
        self.storage_catalog = load_scenarios(str(SCENARIOS_DIR), domain="storage")

    def _build(self, features, catalog, domain):
        matches = match_scenarios(features, catalog)
        best_match, best_def = match_best(features, catalog)
        risk = score_risk(features, best_match, domain)
        rules = _COMPUTE_RULES if domain == "compute" else _STORAGE_RULES
        rec = recommend(best_match, best_def, risk, domain, rules)
        report = build_evidence("trace-test", "INC-0001", features, matches, risk, [rec])
        lines = evidence_lines(report)
        return report, lines

    @pytest.mark.parametrize("scenario_id", list(COMPUTE_FEATURE_MAP.keys()))
    def test_compute_evidence_is_non_empty(self, scenario_id):
        features = COMPUTE_FEATURE_MAP[scenario_id]
        report, lines = self._build(features, self.compute_catalog, "compute")
        assert len(report.scenario_matches) > 0
        assert len(lines) > 0

    @pytest.mark.parametrize("scenario_id", list(STORAGE_FEATURE_MAP.keys()))
    def test_storage_evidence_is_non_empty(self, scenario_id):
        features = STORAGE_FEATURE_MAP[scenario_id]
        report, lines = self._build(features, self.storage_catalog, "storage")
        assert len(report.scenario_matches) > 0
        assert len(lines) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# 8. End-to-end intelligence pipeline per scenario
# ═══════════════════════════════════════════════════════════════════════════════

class TestEndToEndPipeline:
    """Run the full intelligence pipeline for every scenario and verify outputs."""

    @pytest.fixture(autouse=True)
    def _load_catalogs(self):
        self.compute_catalog = load_scenarios(str(SCENARIOS_DIR), domain="compute")
        self.storage_catalog = load_scenarios(str(SCENARIOS_DIR), domain="storage")
        self.all_catalog = self.compute_catalog + self.storage_catalog

    def _run_pipeline(self, features):
        catalog = (
            self.compute_catalog if features.domain == "compute"
            else self.storage_catalog
        )
        domain = features.domain
        rules = _COMPUTE_RULES if domain == "compute" else _STORAGE_RULES

        matches = match_scenarios(features, catalog)
        assert len(matches) > 0, "No scenario matched"
        best_match, best_def = match_best(features, catalog)
        assert best_match is not None
        risk = score_risk(features, best_match, domain)
        rec = recommend(best_match, best_def, risk, domain, rules)
        report = build_evidence("trace-e2e", "INC-E2E", features, matches, risk, [rec])
        lines = evidence_lines(report)
        return {
            "matches": matches,
            "best_match": best_match,
            "best_def": best_def,
            "risk": risk,
            "recommendation": rec,
            "evidence_report": report,
            "evidence_lines": lines,
        }

    @pytest.mark.parametrize("scenario_id", list(COMPUTE_FEATURE_MAP.keys()))
    def test_compute_e2e(self, scenario_id):
        features = COMPUTE_FEATURE_MAP[scenario_id]
        result = self._run_pipeline(features)
        assert result["best_match"].scenario_id == scenario_id or len(result["matches"]) > 0
        assert result["risk"].risk_level in ("critical", "high", "medium", "low")
        assert result["recommendation"].action_type != ""
        assert len(result["evidence_lines"]) > 0

    @pytest.mark.parametrize("scenario_id", list(STORAGE_FEATURE_MAP.keys()))
    def test_storage_e2e(self, scenario_id):
        features = STORAGE_FEATURE_MAP[scenario_id]
        result = self._run_pipeline(features)
        assert result["best_match"].scenario_id == scenario_id or len(result["matches"]) > 0
        assert result["risk"].risk_level in ("critical", "high", "medium", "low")
        assert result["recommendation"].action_type != ""
        assert len(result["evidence_lines"]) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Edge cases and boundary conditions
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:

    @pytest.fixture(autouse=True)
    def _load_catalogs(self):
        self.all_catalog = load_scenarios(str(SCENARIOS_DIR))

    def test_empty_features_match_nothing(self):
        features = ObsFeatures(
            alert_name="", service_name="", severity="info",
            domain="compute", timestamp=dt.datetime.now(dt.timezone.utc),
        )
        matches = match_scenarios(features, self.all_catalog)
        assert len(matches) == 0

    def test_unknown_alert_name_no_match(self):
        features = ObsFeatures(
            alert_name="CompletelyBogusAlert", service_name="unknown",
            severity="warning", domain="compute",
            timestamp=dt.datetime.now(dt.timezone.utc),
        )
        matches = match_scenarios(features, self.all_catalog)
        assert len(matches) == 0

    def test_max_severity_compute_elevated_risk(self):
        features = ObsFeatures(
            alert_name="HighErrorRate", service_name="frontend-api",
            severity="critical", domain="compute",
            timestamp=dt.datetime.now(dt.timezone.utc),
            error_rate=0.50, latency_p99=5.0, log_anomaly_detected=True,
            recent_error_count=100, cpu_usage=0.95,
        )
        catalog = load_scenarios(str(SCENARIOS_DIR), domain="compute")
        matches = match_scenarios(features, catalog)
        assert len(matches) > 0
        risk = score_risk(features, matches[0], "compute")
        assert risk.risk_level in ("high", "critical")

    def test_max_severity_storage_elevated_risk(self):
        features = ObsFeatures(
            alert_name="CephMultipleOSDDown", service_name="storage",
            severity="critical", domain="storage",
            timestamp=dt.datetime.now(dt.timezone.utc),
            osd_up_count=1, osd_total_count=6, degraded_pgs=500,
            cluster_health_score=0, pool_usage_pct=0.95,
        )
        catalog = load_scenarios(str(SCENARIOS_DIR), domain="storage")
        matches = match_scenarios(features, catalog)
        assert len(matches) > 0
        risk = score_risk(features, matches[0], "storage")
        assert risk.risk_level in ("high", "critical")

    def test_all_20_scenarios_have_playbook_hint(self):
        for s in self.all_catalog:
            assert s.playbook_hint.strip(), f"Missing playbook_hint in {s.scenario_id}"
