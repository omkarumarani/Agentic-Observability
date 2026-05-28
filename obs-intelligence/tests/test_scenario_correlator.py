"""Tests for obs_intelligence.scenario_correlator."""
from __future__ import annotations

import os
import textwrap
from unittest.mock import patch

import pytest

from obs_intelligence.models import ObsFeatures, ScenarioMatch
from obs_intelligence.scenario_loader import ScenarioDef, ConditionDef
from obs_intelligence.scenario_correlator import (
    load_catalog,
    match_scenarios,
    match_best,
    _eval_condition,
)


# ═══════════════════════════════════════════════════════════════════════════════
# _eval_condition
# ═══════════════════════════════════════════════════════════════════════════════


class TestEvalCondition:
    def test_gt_true(self):
        c = ConditionDef(field="x", operator="gt", threshold=5.0, weight=0.1)
        assert _eval_condition(c, 10.0) is True

    def test_gt_false(self):
        c = ConditionDef(field="x", operator="gt", threshold=5.0, weight=0.1)
        assert _eval_condition(c, 3.0) is False

    def test_lt(self):
        c = ConditionDef(field="x", operator="lt", threshold=5.0, weight=0.1)
        assert _eval_condition(c, 3.0) is True

    def test_gte(self):
        c = ConditionDef(field="x", operator="gte", threshold=5.0, weight=0.1)
        assert _eval_condition(c, 5.0) is True

    def test_lte(self):
        c = ConditionDef(field="x", operator="lte", threshold=5.0, weight=0.1)
        assert _eval_condition(c, 5.0) is True

    def test_eq(self):
        c = ConditionDef(field="x", operator="eq", threshold=5.0, weight=0.1)
        assert _eval_condition(c, 5.0) is True

    def test_ne(self):
        c = ConditionDef(field="x", operator="ne", threshold=5.0, weight=0.1)
        assert _eval_condition(c, 6.0) is True

    def test_bool_true(self):
        c = ConditionDef(field="x", operator="true", weight=0.1)
        assert _eval_condition(c, True) is True
        assert _eval_condition(c, False) is False

    def test_bool_false(self):
        c = ConditionDef(field="x", operator="false", weight=0.1)
        assert _eval_condition(c, False) is True
        assert _eval_condition(c, True) is False

    def test_non_numeric_value_returns_false(self):
        c = ConditionDef(field="x", operator="gt", threshold=5.0, weight=0.1)
        assert _eval_condition(c, "not a number") is False


# ═══════════════════════════════════════════════════════════════════════════════
# match_scenarios
# ═══════════════════════════════════════════════════════════════════════════════


class TestMatchScenarios:
    def test_matching_compute_scenario(self, compute_features, sample_scenario_def):
        # Patch the outcome store to return 0 adjustment
        with patch("obs_intelligence.scenario_correlator._outcome_store") as mock_os:
            mock_os.get_weight_adjustment.return_value = 0.0
            matches = match_scenarios(compute_features, [sample_scenario_def])

        assert len(matches) >= 1
        best = matches[0]
        assert best.scenario_id == "HIGH_ERROR_RATE"
        assert best.confidence > 0.3
        assert "error_rate" in best.matched_features

    def test_no_match_below_threshold(self, low_risk_features):
        scenario = ScenarioDef(
            scenario_id="EXTREME_HIGH",
            display_name="Extreme High Error",
            domain="compute",
            conditions=[
                ConditionDef(field="error_rate", operator="gt", threshold=0.9, weight=1.0),
            ],
            confidence_threshold=0.5,
        )
        with patch("obs_intelligence.scenario_correlator._outcome_store") as mock_os:
            mock_os.get_weight_adjustment.return_value = 0.0
            matches = match_scenarios(low_risk_features, [scenario])
        assert len(matches) == 0

    def test_alert_name_pattern_filter(self, compute_features, sample_scenario_def):
        # Change alert name so it doesn't match the pattern
        compute_features.alert_name = "SlowResponse"
        with patch("obs_intelligence.scenario_correlator._outcome_store") as mock_os:
            mock_os.get_weight_adjustment.return_value = 0.0
            matches = match_scenarios(compute_features, [sample_scenario_def])
        assert len(matches) == 0

    def test_empty_catalog(self, compute_features):
        matches = match_scenarios(compute_features, [])
        assert matches == []

    def test_sorted_by_confidence_descending(self, compute_features):
        high_match = ScenarioDef(
            scenario_id="A",
            display_name="A",
            domain="compute",
            conditions=[
                ConditionDef(field="error_rate", operator="gt", threshold=0.01, weight=1.0),
            ],
            confidence_threshold=0.1,
        )
        low_match = ScenarioDef(
            scenario_id="B",
            display_name="B",
            domain="compute",
            conditions=[
                ConditionDef(field="error_rate", operator="gt", threshold=0.01, weight=0.1),
                ConditionDef(field="cpu_usage", operator="gt", threshold=0.99, weight=0.9),
            ],
            confidence_threshold=0.05,
        )
        with patch("obs_intelligence.scenario_correlator._outcome_store") as mock_os:
            mock_os.get_weight_adjustment.return_value = 0.0
            matches = match_scenarios(compute_features, [high_match, low_match])
        assert len(matches) >= 1
        if len(matches) >= 2:
            assert matches[0].confidence >= matches[1].confidence

    def test_storage_scenario_matching(self, storage_features, storage_scenario_def):
        with patch("obs_intelligence.scenario_correlator._outcome_store") as mock_os:
            mock_os.get_weight_adjustment.return_value = 0.0
            matches = match_scenarios(storage_features, [storage_scenario_def])
        assert len(matches) >= 1
        assert matches[0].scenario_id == "OSD_DOWN"


# ═══════════════════════════════════════════════════════════════════════════════
# match_best
# ═══════════════════════════════════════════════════════════════════════════════


class TestMatchBest:
    def test_returns_best_match_and_def(self, compute_features, sample_scenario_def):
        with patch("obs_intelligence.scenario_correlator._outcome_store") as mock_os:
            mock_os.get_weight_adjustment.return_value = 0.0
            match, defn = match_best(compute_features, [sample_scenario_def])
        assert match is not None
        assert defn is not None
        assert match.scenario_id == "HIGH_ERROR_RATE"
        assert defn.action == "restart_service"

    def test_returns_none_pair_with_no_catalog(self, compute_features):
        match, defn = match_best(compute_features, [])
        assert match is None
        assert defn is None

    def test_returns_none_pair_when_nothing_matches(self, low_risk_features):
        scenario = ScenarioDef(
            scenario_id="IMPOSSIBLE",
            display_name="Impossible",
            domain="compute",
            conditions=[
                ConditionDef(field="error_rate", operator="gt", threshold=0.999, weight=1.0),
            ],
            confidence_threshold=0.9,
        )
        with patch("obs_intelligence.scenario_correlator._outcome_store") as mock_os:
            mock_os.get_weight_adjustment.return_value = 0.0
            match, defn = match_best(low_risk_features, [scenario])
        assert match is None
        assert defn is None


# ═══════════════════════════════════════════════════════════════════════════════
# load_catalog
# ═══════════════════════════════════════════════════════════════════════════════


class TestLoadCatalog:
    def test_load_catalog_from_real_dir(self):
        scenarios_dir = os.path.join(
            os.path.dirname(__file__), "..", "scenarios"
        )
        if not os.path.isdir(scenarios_dir):
            pytest.skip("scenarios/ directory not found")
        catalog = load_catalog(domain="compute", scenarios_dir=scenarios_dir)
        assert isinstance(catalog, list)
        for s in catalog:
            assert s.domain == "compute"
