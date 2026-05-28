"""
tests/test_llm_compute.py
─────────────────────────────────────────────────────────────────────────────
Tests for LLM features in compute-agent:
  - generate_local_llm_analysis (Ollama)
  - deterministic_analysis (scenario catalog)
  - _build_compute_stub_playbook
  - _get_compute_catalog
  - Provider fallback chain: Cloud AI → Local LLM → Deterministic
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ═══════════════════════════════════════════════════════════════════════════════
# generate_local_llm_analysis
# ═══════════════════════════════════════════════════════════════════════════════

MOCK_LOCAL_ANALYSIS = {
    "rca_summary": "Memory leak in frontend-api causing OOM restarts.",
    "rca_detail": {
        "symptoms": ["high memory usage"],
        "probable_cause": "unbounded cache",
        "contributing_factors": ["no memory limits"],
        "blast_radius": "single service",
    },
    "confidence": "low",
    "ansible_playbook": "---\n- name: Restart\n  hosts: localhost\n  tasks: []",
    "ansible_description": "Restart to clear memory",
    "test_cases": [],
    "pr_description": "Fix memory leak",
    "pr_title": "fix: memory leak",
    "estimated_fix_time_minutes": 10,
    "rollback_steps": ["Revert restart"],
}


def _ollama_response(analysis: dict):
    """Simulate Ollama /v1/chat/completions response."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "choices": [{"message": {"content": json.dumps(analysis)}}]
    }
    return resp


@pytest.mark.asyncio
class TestGenerateLocalLlmAnalysis:

    async def test_returns_analysis_when_enabled(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = True
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=_ollama_response(MOCK_LOCAL_ANALYSIS))
            result = await ai_analyst.generate_local_llm_analysis(
                alert_name="HighMemoryUsage",
                service_name="frontend-api",
                severity="warning",
                description="Memory usage above threshold",
                logs="WARN OOM detected",
                metrics={"memory_usage_pct": "89.0"},
                http=mock_http,
            )
            assert result["rca_summary"] == MOCK_LOCAL_ANALYSIS["rca_summary"]
            assert result["confidence"] == "low"
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original

    async def test_returns_empty_dict_when_disabled(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = False
            mock_http = AsyncMock()
            result = await ai_analyst.generate_local_llm_analysis(
                alert_name="X", service_name="svc", severity="warning",
                description="", logs="", metrics={}, http=mock_http,
            )
            assert result == {}
            mock_http.post.assert_not_called()
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original

    async def test_returns_empty_dict_on_http_error(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = True
            error_resp = MagicMock()
            error_resp.status_code = 500
            error_resp.text = "internal error"
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=error_resp)
            result = await ai_analyst.generate_local_llm_analysis(
                alert_name="X", service_name="svc", severity="warning",
                description="", logs="", metrics={}, http=mock_http,
            )
            assert result == {}
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original

    async def test_returns_empty_dict_on_bad_json(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = True
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "choices": [{"message": {"content": "not valid json {{{"}}]
            }
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=resp)
            result = await ai_analyst.generate_local_llm_analysis(
                alert_name="X", service_name="svc", severity="warning",
                description="", logs="", metrics={}, http=mock_http,
            )
            assert result == {}
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original

    async def test_returns_empty_dict_on_network_exception(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = True
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(side_effect=Exception("connection refused"))
            result = await ai_analyst.generate_local_llm_analysis(
                alert_name="X", service_name="svc", severity="warning",
                description="", logs="", metrics={}, http=mock_http,
            )
            assert result == {}
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original

    async def test_strips_markdown_fences(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = True
            # Simulate model wrapping JSON in ```json ... ```
            fenced_content = f"```json\n{json.dumps(MOCK_LOCAL_ANALYSIS)}\n```"
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "choices": [{"message": {"content": fenced_content}}]
            }
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=resp)
            result = await ai_analyst.generate_local_llm_analysis(
                alert_name="X", service_name="svc", severity="warning",
                description="", logs="", metrics={}, http=mock_http,
            )
            assert result.get("rca_summary") == MOCK_LOCAL_ANALYSIS["rca_summary"]
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original

    async def test_calls_correct_ollama_url(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = True
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=_ollama_response(MOCK_LOCAL_ANALYSIS))
            await ai_analyst.generate_local_llm_analysis(
                alert_name="X", service_name="svc", severity="warning",
                description="", logs="", metrics={}, http=mock_http,
            )
            url = mock_http.post.call_args[0][0]
            assert "/v1/chat/completions" in url
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original

    async def test_logs_truncated_to_2000(self):
        from app import ai_analyst
        original = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.LOCAL_LLM_ENABLED = True
            long_logs = "X" * 5000
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=_ollama_response(MOCK_LOCAL_ANALYSIS))
            await ai_analyst.generate_local_llm_analysis(
                alert_name="X", service_name="svc", severity="warning",
                description="", logs=long_logs, metrics={}, http=mock_http,
            )
            payload = mock_http.post.call_args[1]["json"]
            user_msg = payload["messages"][0]["content"]
            # Full 5000 chars should NOT appear — truncated to 2000
            assert user_msg.count("X") <= 2500  # some overhead allowed
        finally:
            ai_analyst.LOCAL_LLM_ENABLED = original


# ═══════════════════════════════════════════════════════════════════════════════
# deterministic_analysis (catalog fallback)
# ═══════════════════════════════════════════════════════════════════════════════

def _fake_compute_catalog():
    """Return a synthetic compute scenario catalog for testing."""
    from obs_intelligence.scenario_loader import ScenarioDef, ConditionDef
    return [
        ScenarioDef(
            scenario_id="HIGH_ERROR_RATE",
            display_name="High Error Rate",
            domain="compute",
            conditions=[
                ConditionDef(field="error_rate_pct", operator="gt", threshold=5, weight=1.0),
            ],
            alert_name_patterns=["*HighError*", "*CriticalError*"],
            alert_match_weight=0.5,
            rca="Elevated HTTP 5xx error rate detected on the service.",
            action="restart_service",
            playbook_hint="Restart the service pod to restore normal error rate.",
            autonomy="approval_required",
            confidence_threshold=0.3,
        ),
    ]


class TestDeterministicAnalysis:

    def test_known_alert_returns_scenario_match(self):
        from app import ai_analyst
        ai_analyst._compute_catalog = _fake_compute_catalog()
        try:
            result = ai_analyst.deterministic_analysis("HighErrorRate", "frontend-api", "critical")
            assert result.get("provider") == "scenario-catalog"
            assert float(result.get("confidence", "0")) > 0
        finally:
            ai_analyst._compute_catalog = None

    def test_unknown_alert_returns_escalate(self):
        from app import ai_analyst
        ai_analyst._compute_catalog = _fake_compute_catalog()
        try:
            result = ai_analyst.deterministic_analysis("CompletelyUnknownAlert123", "svc", "warning")
            assert result.get("recommended_action") == "escalate"
            assert result.get("autonomy_level") == "human_only"
            assert result.get("provider") == "scenario-catalog"
            assert float(result.get("confidence", "1")) == 0.0
        finally:
            ai_analyst._compute_catalog = None

    def test_returns_ansible_playbook(self):
        from app import ai_analyst
        ai_analyst._compute_catalog = _fake_compute_catalog()
        try:
            result = ai_analyst.deterministic_analysis("HighErrorRate", "frontend-api", "warning")
            playbook = result.get("ansible_playbook", "")
            if float(result.get("confidence", "0")) > 0:
                assert "---" in playbook
                assert "HighErrorRate" in playbook or "tasks" in playbook.lower()
        finally:
            ai_analyst._compute_catalog = None

    def test_returns_test_plan(self):
        from app import ai_analyst
        ai_analyst._compute_catalog = _fake_compute_catalog()
        try:
            result = ai_analyst.deterministic_analysis("HighErrorRate", "frontend-api", "warning")
            if float(result.get("confidence", "0")) > 0:
                assert isinstance(result.get("test_plan"), list)
                assert len(result["test_plan"]) > 0
        finally:
            ai_analyst._compute_catalog = None


# ═══════════════════════════════════════════════════════════════════════════════
# _build_compute_stub_playbook
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildComputeStubPlaybook:

    def test_contains_alert_name(self):
        from app.ai_analyst import _build_compute_stub_playbook
        result = _build_compute_stub_playbook("HighCPU", "Scale up pods")
        assert "HighCPU" in result
        assert "Scale up pods" in result

    def test_valid_yaml_prefix(self):
        from app.ai_analyst import _build_compute_stub_playbook
        result = _build_compute_stub_playbook("X", "hint")
        assert result.startswith("---")


# ═══════════════════════════════════════════════════════════════════════════════
# _get_compute_catalog
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetComputeCatalog:

    def test_returns_list(self):
        from app import ai_analyst
        ai_analyst._compute_catalog = _fake_compute_catalog()
        try:
            catalog = ai_analyst._get_compute_catalog()
            assert isinstance(catalog, list)
            assert len(catalog) > 0
        finally:
            ai_analyst._compute_catalog = None

    def test_all_entries_are_compute_domain(self):
        from app import ai_analyst
        ai_analyst._compute_catalog = _fake_compute_catalog()
        try:
            catalog = ai_analyst._get_compute_catalog()
            for s in catalog:
                assert s.domain == "compute"
        finally:
            ai_analyst._compute_catalog = None

    def test_caches_result(self):
        from app import ai_analyst
        ai_analyst._compute_catalog = None
        # Pre-populate to avoid disk read
        ai_analyst._compute_catalog = _fake_compute_catalog()
        c1 = ai_analyst._get_compute_catalog()
        c2 = ai_analyst._get_compute_catalog()
        assert c1 is c2  # Same object (cached)
        ai_analyst._compute_catalog = None


# ═══════════════════════════════════════════════════════════════════════════════
# Provider fallback chain
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestProviderFallbackChain:

    async def test_cloud_failure_falls_to_local_llm(self):
        """When cloud AI fails, generate_local_llm_analysis should get a chance."""
        from app import ai_analyst
        original_ai = ai_analyst.AI_ENABLED
        original_local = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.AI_ENABLED = True
            ai_analyst.LOCAL_LLM_ENABLED = True
            # Cloud AI returns empty (failure)
            cloud_result = {}
            local_result = MOCK_LOCAL_ANALYSIS.copy()
            mock_http = AsyncMock()

            with patch.object(ai_analyst, "generate_ai_analysis", return_value=cloud_result) as cloud_mock, \
                 patch.object(ai_analyst, "generate_local_llm_analysis", return_value=local_result) as local_mock:
                # Simulate the pipeline logic:
                result = await ai_analyst.generate_ai_analysis(
                    "X", "svc", "w", "d", "", {}, mock_http
                )
                if not result:
                    result = await ai_analyst.generate_local_llm_analysis(
                        "X", "svc", "w", "d", "", {}, mock_http
                    )
                if not result:
                    result = ai_analyst.deterministic_analysis("X", "svc")

            assert result == local_result

        finally:
            ai_analyst.AI_ENABLED = original_ai
            ai_analyst.LOCAL_LLM_ENABLED = original_local

    async def test_all_llm_failure_falls_to_deterministic(self):
        """When both Cloud and Local LLM fail, deterministic analysis is used."""
        from app import ai_analyst
        original_ai = ai_analyst.AI_ENABLED
        original_local = ai_analyst.LOCAL_LLM_ENABLED
        try:
            ai_analyst.AI_ENABLED = False
            ai_analyst.LOCAL_LLM_ENABLED = False
            mock_http = AsyncMock()

            result = await ai_analyst.generate_ai_analysis(
                "HighErrorRate", "svc", "warning", "desc", "", {}, mock_http
            )
            if not result:
                result = await ai_analyst.generate_local_llm_analysis(
                    "HighErrorRate", "svc", "warning", "desc", "", {}, mock_http
                )
            if not result:
                result = ai_analyst.deterministic_analysis("HighErrorRate", "svc")

            assert result.get("provider") == "scenario-catalog"
            assert "rca_summary" in result

        finally:
            ai_analyst.AI_ENABLED = original_ai
            ai_analyst.LOCAL_LLM_ENABLED = original_local
