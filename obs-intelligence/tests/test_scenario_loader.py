"""Tests for obs_intelligence.scenario_loader."""
from __future__ import annotations

import os
import textwrap
import tempfile
from pathlib import Path

import pytest

from obs_intelligence.scenario_loader import (
    load_scenarios,
    ScenarioDef,
    ScenarioSchemaError,
    _VALID_OPERATORS,
    _VALID_DOMAINS,
    _VALID_AUTONOMY,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Valid YAML loading
# ═══════════════════════════════════════════════════════════════════════════════


class TestLoadScenarios:
    def test_load_compute_scenarios_from_real_dir(self):
        scenarios_dir = os.path.join(
            os.path.dirname(__file__), "..", "scenarios"
        )
        if not os.path.isdir(scenarios_dir):
            pytest.skip("scenarios/ directory not found")
        results = load_scenarios(scenarios_dir, domain="compute")
        assert isinstance(results, list)
        assert all(isinstance(s, ScenarioDef) for s in results)
        assert all(s.domain == "compute" for s in results)

    def test_load_storage_scenarios_from_real_dir(self):
        scenarios_dir = os.path.join(
            os.path.dirname(__file__), "..", "scenarios"
        )
        if not os.path.isdir(scenarios_dir):
            pytest.skip("scenarios/ directory not found")
        results = load_scenarios(scenarios_dir, domain="storage")
        assert isinstance(results, list)
        assert all(s.domain == "storage" for s in results)

    def test_load_all_domains(self):
        scenarios_dir = os.path.join(
            os.path.dirname(__file__), "..", "scenarios"
        )
        if not os.path.isdir(scenarios_dir):
            pytest.skip("scenarios/ directory not found")
        results = load_scenarios(scenarios_dir)
        domains = {s.domain for s in results}
        # Should have at least compute and storage
        assert "compute" in domains or "storage" in domains

    def test_missing_directory_returns_empty(self, tmp_path):
        results = load_scenarios(str(tmp_path / "nonexistent"))
        assert results == []

    def test_sorted_by_scenario_id(self):
        scenarios_dir = os.path.join(
            os.path.dirname(__file__), "..", "scenarios"
        )
        if not os.path.isdir(scenarios_dir):
            pytest.skip("scenarios/ directory not found")
        results = load_scenarios(scenarios_dir)
        ids = [s.scenario_id for s in results]
        assert ids == sorted(ids)

    def test_valid_yaml_loading(self, tmp_path):
        compute_dir = tmp_path / "compute"
        compute_dir.mkdir()
        yaml_content = textwrap.dedent("""\
            scenario_id: TEST_SCENARIO
            display_name: Test Scenario
            domain: compute
            action: restart_service
            autonomy: approval_gated
            conditions:
              - field: error_rate
                operator: gt
                threshold: 0.05
                weight: 0.4
        """)
        (compute_dir / "test.yaml").write_text(yaml_content)
        results = load_scenarios(str(tmp_path), domain="compute")
        assert len(results) == 1
        s = results[0]
        assert s.scenario_id == "TEST_SCENARIO"
        assert s.display_name == "Test Scenario"
        assert s.domain == "compute"
        assert s.action == "restart_service"
        assert s.autonomy == "approval_gated"
        assert len(s.conditions) == 1
        assert s.conditions[0].field == "error_rate"
        assert s.conditions[0].operator == "gt"
        assert s.conditions[0].threshold == 0.05
        assert s.conditions[0].weight == 0.4


# ═══════════════════════════════════════════════════════════════════════════════
# Schema validation errors
# ═══════════════════════════════════════════════════════════════════════════════


class TestSchemaValidation:
    def _write_and_load(self, tmp_path, yaml_content: str):
        d = tmp_path / "compute"
        d.mkdir(exist_ok=True)
        (d / "bad.yaml").write_text(yaml_content)
        return load_scenarios(str(tmp_path), domain="compute")

    def test_missing_scenario_id(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="scenario_id"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                display_name: X
                domain: compute
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
            """))

    def test_missing_display_name(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="display_name"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                domain: compute
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
            """))

    def test_missing_domain(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="domain"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
            """))

    def test_empty_conditions(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="conditions"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                domain: compute
                conditions: []
            """))

    def test_invalid_operator(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="operator"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                domain: compute
                conditions:
                  - field: x
                    operator: INVALID
                    threshold: 1
            """))

    def test_invalid_domain(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="domain"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                domain: networking
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
            """))

    def test_invalid_autonomy(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="autonomy"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                domain: compute
                autonomy: full_auto
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
            """))

    def test_negative_weight(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="weight"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                domain: compute
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
                    weight: -0.5
            """))

    def test_zero_confidence_threshold(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="confidence_threshold"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                domain: compute
                confidence_threshold: 0.0
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
            """))

    def test_alert_match_weight_out_of_range(self, tmp_path):
        with pytest.raises(ScenarioSchemaError, match="alert_match_weight"):
            self._write_and_load(tmp_path, textwrap.dedent("""\
                scenario_id: X
                display_name: X
                domain: compute
                alert_match_weight: 1.5
                conditions:
                  - field: x
                    operator: gt
                    threshold: 1
            """))


# ═══════════════════════════════════════════════════════════════════════════════
# Constants validation
# ═══════════════════════════════════════════════════════════════════════════════


class TestConstants:
    def test_valid_operators(self):
        expected = {"gt", "lt", "gte", "lte", "eq", "ne", "true", "false"}
        assert _VALID_OPERATORS == expected

    def test_valid_domains(self):
        assert _VALID_DOMAINS == {"compute", "storage"}

    def test_valid_autonomy(self):
        assert _VALID_AUTONOMY == {"autonomous", "approval_gated", "human_only"}
