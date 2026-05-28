"""Tests for obs_intelligence.recommender."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from obs_intelligence.models import RiskAssessment, Recommendation, ScenarioMatch
from obs_intelligence.scenario_loader import ScenarioDef, ConditionDef
from obs_intelligence.recommender import recommend, recommend_all, _clamp_autonomy


# ── Fake autonomy rules module ───────────────────────────────────────────────

def _make_rules(**kw):
    defaults = {
        "APPROVAL_REQUIRED": {"rollback_deploy", "osd_reweight"},
        "AUTONOMOUS_ALLOWED": {"pvc_throttle", "reduce_otel_sampling"},
        "HUMAN_ONLY": {"deep_dive_investigation"},
        "FORCE_APPROVAL_ABOVE_RISK": 0.70,
    }
    defaults.update(kw)
    return SimpleNamespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════════
# recommend()
# ═══════════════════════════════════════════════════════════════════════════════


class TestRecommend:
    def test_returns_recommendation_for_matched_scenario(
        self, sample_scenario_match, sample_scenario_def, sample_risk
    ):
        rules = _make_rules()
        rec = recommend(sample_scenario_match, sample_scenario_def, sample_risk, "compute", rules)
        assert isinstance(rec, Recommendation)
        assert rec.action_type == "restart_service"
        assert rec.display_name == "High Error Rate"
        assert rec.confidence == sample_scenario_match.confidence

    def test_no_match_returns_escalation(self, sample_risk):
        rules = _make_rules()
        rec = recommend(None, None, sample_risk, "compute", rules)
        assert rec.action_type == "escalate"
        assert rec.display_name == "Manual Escalation"
        assert rec.confidence == 0.0
        assert rec.autonomous is False

    def test_playbook_set(self, sample_scenario_match, sample_scenario_def, sample_risk):
        rules = _make_rules()
        rec = recommend(sample_scenario_match, sample_scenario_def, sample_risk, "compute", rules)
        assert rec.ansible_playbook == "restart_service.yml"

    def test_xyops_workflow_includes_domain(
        self, sample_scenario_match, sample_scenario_def, sample_risk
    ):
        rules = _make_rules()
        rec = recommend(sample_scenario_match, sample_scenario_def, sample_risk, "storage", rules)
        assert "Storage" in rec.xyops_workflow

    def test_estimated_duration(self, sample_scenario_match, sample_scenario_def, sample_risk):
        rules = _make_rules()
        rec = recommend(sample_scenario_match, sample_scenario_def, sample_risk, "compute", rules)
        assert rec.estimated_duration is not None

    def test_rollback_plan(self, sample_scenario_match, sample_scenario_def, sample_risk):
        rules = _make_rules()
        rec = recommend(sample_scenario_match, sample_scenario_def, sample_risk, "compute", rules)
        assert rec.rollback_plan is not None


# ═══════════════════════════════════════════════════════════════════════════════
# _clamp_autonomy
# ═══════════════════════════════════════════════════════════════════════════════


class TestClampAutonomy:
    def test_human_only_always_wins(self):
        rules = _make_rules()
        low_risk = RiskAssessment(risk_score=0.1, risk_level="low")
        result = _clamp_autonomy("deep_dive_investigation", "autonomous", low_risk, rules)
        assert result == "human_only"

    def test_approval_required_clamps_autonomous(self):
        rules = _make_rules()
        low_risk = RiskAssessment(risk_score=0.1, risk_level="low")
        result = _clamp_autonomy("rollback_deploy", "autonomous", low_risk, rules)
        assert result == "approval_gated"

    def test_approval_required_keeps_human_only(self):
        rules = _make_rules()
        low_risk = RiskAssessment(risk_score=0.1, risk_level="low")
        result = _clamp_autonomy("rollback_deploy", "human_only", low_risk, rules)
        assert result == "human_only"

    def test_high_risk_overrides_autonomous(self):
        rules = _make_rules()
        high_risk = RiskAssessment(risk_score=0.85, risk_level="critical")
        result = _clamp_autonomy("pvc_throttle", "autonomous", high_risk, rules)
        assert result == "approval_gated"

    def test_low_risk_permits_autonomous(self):
        rules = _make_rules()
        low_risk = RiskAssessment(risk_score=0.2, risk_level="low")
        result = _clamp_autonomy("pvc_throttle", "autonomous", low_risk, rules)
        assert result == "autonomous"

    def test_scenario_value_passthrough(self):
        rules = _make_rules()
        medium_risk = RiskAssessment(risk_score=0.5, risk_level="medium")
        result = _clamp_autonomy("restart_service", "approval_gated", medium_risk, rules)
        assert result == "approval_gated"


# ═══════════════════════════════════════════════════════════════════════════════
# recommend_all
# ═══════════════════════════════════════════════════════════════════════════════


class TestRecommendAll:
    def test_multiple_recommendations_sorted(self, sample_risk):
        rules = _make_rules()
        match1 = ScenarioMatch(
            scenario_id="A", display_name="A", confidence=0.9, domain="compute",
        )
        defn1 = ScenarioDef(
            scenario_id="A", display_name="A", domain="compute",
            action="restart_service", conditions=[],
        )
        match2 = ScenarioMatch(
            scenario_id="B", display_name="B", confidence=0.6, domain="compute",
        )
        defn2 = ScenarioDef(
            scenario_id="B", display_name="B", domain="compute",
            action="investigate_errors", conditions=[],
        )
        recs = recommend_all([(match1, defn1), (match2, defn2)], sample_risk, "compute", rules)
        assert len(recs) == 2
        assert recs[0].confidence >= recs[1].confidence

    def test_empty_input(self, sample_risk):
        rules = _make_rules()
        recs = recommend_all([], sample_risk, "compute", rules)
        assert recs == []
