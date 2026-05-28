"""
tests/test_llm_storage.py
─────────────────────────────────────────────────────────────────────────────
Tests for LLM features in storage-agent:
  - generate_ai_analysis (OpenAI / Claude / fallback)
  - _call_openai
  - _call_claude
  - deterministic_analysis with mocked catalog
  - _build_stub_playbook
  - build_enriched_ticket_body edge cases
  - Provider fallback chain
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ── Synthetic catalog helper ──────────────────────────────────────────────────

def _fake_storage_catalog():
    """Return a synthetic storage scenario catalog for testing."""
    from obs_intelligence.scenario_loader import ScenarioDef, ConditionDef
    return [
        ScenarioDef(
            scenario_id="CEPH_OSD_DOWN",
            display_name="Ceph OSD Down",
            domain="storage",
            conditions=[
                ConditionDef(field="osd_down_count", operator="gt", threshold=0, weight=0.6),
            ],
            alert_name_patterns=["*OSDDown*", "*CephOSD*"],
            alert_match_weight=0.5,
            rca="One or more Ceph OSDs are down, reducing cluster redundancy.",
            action="osd_reweight",
            playbook_hint="Reweight or remove the downed OSD from CRUSH map.",
            autonomy="approval_gated",
            confidence_threshold=0.3,
        ),
    ]


# ── OpenAI / Claude mock responses ───────────────────────────────────────────

MOCK_AI_RESULT = {
    "rca_summary": "OSD.5 is down due to disk failure.",
    "recommended_action": "osd_reweight",
    "autonomy_level": "approval_gated",
    "ansible_playbook": "---\n- name: Reweight OSD\n  hosts: storage\n  tasks: []",
    "test_plan": ["Check ceph osd tree", "Verify PG recovery"],
    "confidence": "high",
}


def _openai_response(analysis: dict):
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "choices": [{"message": {"content": json.dumps(analysis)}}]
    }
    return resp


def _claude_response(analysis: dict):
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "content": [{"text": json.dumps(analysis)}]
    }
    return resp


# ═══════════════════════════════════════════════════════════════════════════════
# _call_openai
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestCallOpenai:

    async def test_happy_path(self):
        from app import storage_analyst as sa
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=_openai_response(MOCK_AI_RESULT))
        result = await sa._call_openai("test prompt", mock_http)
        assert result["rca_summary"] == MOCK_AI_RESULT["rca_summary"]

    async def test_strips_markdown_fences(self):
        from app import storage_analyst as sa
        fenced = f"```json\n{json.dumps(MOCK_AI_RESULT)}\n```"
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"choices": [{"message": {"content": fenced}}]}
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=resp)
        result = await sa._call_openai("test prompt", mock_http)
        assert result["rca_summary"] == MOCK_AI_RESULT["rca_summary"]

    async def test_raises_on_http_error(self):
        from app import storage_analyst as sa
        import httpx
        mock_http = AsyncMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock(side_effect=httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock()
        ))
        mock_http.post = AsyncMock(return_value=resp)
        with pytest.raises(httpx.HTTPStatusError):
            await sa._call_openai("test", mock_http)

    async def test_raises_on_bad_json(self):
        from app import storage_analyst as sa
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"choices": [{"message": {"content": "not json {"}}]}
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=resp)
        with pytest.raises(json.JSONDecodeError):
            await sa._call_openai("test", mock_http)


# ═══════════════════════════════════════════════════════════════════════════════
# _call_claude
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestCallClaude:

    async def test_happy_path(self):
        from app import storage_analyst as sa
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=_claude_response(MOCK_AI_RESULT))
        result = await sa._call_claude("test prompt", mock_http)
        assert result["rca_summary"] == MOCK_AI_RESULT["rca_summary"]

    async def test_strips_markdown_fences(self):
        from app import storage_analyst as sa
        fenced = f"```json\n{json.dumps(MOCK_AI_RESULT)}\n```"
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"content": [{"text": fenced}]}
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=resp)
        result = await sa._call_claude("test prompt", mock_http)
        assert result["rca_summary"] == MOCK_AI_RESULT["rca_summary"]


# ═══════════════════════════════════════════════════════════════════════════════
# generate_ai_analysis
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestGenerateAiAnalysis:

    async def test_returns_deterministic_when_ai_disabled(self):
        from app import storage_analyst as sa
        original = sa.AI_ENABLED
        try:
            sa.AI_ENABLED = False
            sa._storage_catalog = _fake_storage_catalog()
            mock_http = AsyncMock()
            result = await sa.generate_ai_analysis(
                "CephOSDDown", "storage", "critical", "OSD down",
                "desc", "metrics", "logs", mock_http,
            )
            assert result.get("provider") == "scenario-catalog"
        finally:
            sa.AI_ENABLED = original
            sa._storage_catalog = None

    async def test_openai_success_sets_provider(self):
        from app import storage_analyst as sa
        orig_ai = sa.AI_ENABLED
        orig_openai = sa._USE_OPENAI
        orig_claude = sa._USE_CLAUDE
        try:
            sa.AI_ENABLED = True
            sa._USE_OPENAI = True
            sa._USE_CLAUDE = False
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=_openai_response(MOCK_AI_RESULT))
            result = await sa.generate_ai_analysis(
                "CephOSDDown", "storage", "critical", "OSD down",
                "desc", "metrics", "logs", mock_http,
            )
            assert result["provider"] == "openai"
            assert result["rca_summary"] == MOCK_AI_RESULT["rca_summary"]
        finally:
            sa.AI_ENABLED = orig_ai
            sa._USE_OPENAI = orig_openai
            sa._USE_CLAUDE = orig_claude

    async def test_claude_success_sets_provider(self):
        from app import storage_analyst as sa
        orig_ai = sa.AI_ENABLED
        orig_openai = sa._USE_OPENAI
        orig_claude = sa._USE_CLAUDE
        try:
            sa.AI_ENABLED = True
            sa._USE_OPENAI = False
            sa._USE_CLAUDE = True
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(return_value=_claude_response(MOCK_AI_RESULT))
            result = await sa.generate_ai_analysis(
                "CephOSDDown", "storage", "critical", "OSD down",
                "desc", "metrics", "logs", mock_http,
            )
            assert result["provider"] == "claude"
        finally:
            sa.AI_ENABLED = orig_ai
            sa._USE_OPENAI = orig_openai
            sa._USE_CLAUDE = orig_claude

    async def test_falls_back_to_deterministic_on_ai_exception(self):
        from app import storage_analyst as sa
        orig_ai = sa.AI_ENABLED
        orig_openai = sa._USE_OPENAI
        orig_claude = sa._USE_CLAUDE
        try:
            sa.AI_ENABLED = True
            sa._USE_OPENAI = True
            sa._USE_CLAUDE = False
            sa._storage_catalog = _fake_storage_catalog()
            mock_http = AsyncMock()
            mock_http.post = AsyncMock(side_effect=Exception("network error"))
            result = await sa.generate_ai_analysis(
                "CephOSDDown", "storage", "critical", "OSD down",
                "desc", "metrics", "logs", mock_http,
            )
            assert result.get("provider") == "scenario-catalog"
        finally:
            sa.AI_ENABLED = orig_ai
            sa._USE_OPENAI = orig_openai
            sa._USE_CLAUDE = orig_claude
            sa._storage_catalog = None

    async def test_falls_back_when_ai_returns_none(self):
        from app import storage_analyst as sa
        orig_ai = sa.AI_ENABLED
        orig_openai = sa._USE_OPENAI
        orig_claude = sa._USE_CLAUDE
        try:
            sa.AI_ENABLED = True
            sa._USE_OPENAI = True
            sa._USE_CLAUDE = False
            sa._storage_catalog = _fake_storage_catalog()
            # _call_openai returns None on bad JSON
            with patch.object(sa, "_call_openai", return_value=None):
                result = await sa.generate_ai_analysis(
                    "CephOSDDown", "storage", "critical", "OSD down",
                    "desc", "metrics", "logs", AsyncMock(),
                )
            assert result.get("provider") == "scenario-catalog"
        finally:
            sa.AI_ENABLED = orig_ai
            sa._USE_OPENAI = orig_openai
            sa._USE_CLAUDE = orig_claude
            sa._storage_catalog = None


# ═══════════════════════════════════════════════════════════════════════════════
# deterministic_analysis with mocked catalog
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeterministicAnalysisCatalog:

    def test_known_alert_matches(self):
        from app import storage_analyst as sa
        sa._storage_catalog = _fake_storage_catalog()
        try:
            result = sa.deterministic_analysis("CephOSDDown", "metrics text")
            assert result["provider"] == "scenario-catalog"
            assert float(result["confidence"]) > 0
            assert result["recommended_action"] == "osd_reweight"
        finally:
            sa._storage_catalog = None

    def test_unknown_alert_escalates(self):
        from app import storage_analyst as sa
        sa._storage_catalog = _fake_storage_catalog()
        try:
            result = sa.deterministic_analysis("CompletelyUnknown", "metrics")
            assert result["recommended_action"] == "escalate"
            assert result["autonomy_level"] == "human_only"
            assert float(result["confidence"]) == 0.0
        finally:
            sa._storage_catalog = None

    def test_returns_all_required_fields(self):
        from app import storage_analyst as sa
        sa._storage_catalog = _fake_storage_catalog()
        try:
            result = sa.deterministic_analysis("CephOSDDown", "metrics")
            for field in ("rca_summary", "recommended_action", "autonomy_level",
                          "ansible_playbook", "test_plan", "confidence", "provider"):
                assert field in result, f"Missing field: {field}"
        finally:
            sa._storage_catalog = None


# ═══════════════════════════════════════════════════════════════════════════════
# _build_stub_playbook
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildStubPlaybookExtra:

    def test_contains_ceph_health_check(self):
        from app.storage_analyst import _build_stub_playbook
        pb = _build_stub_playbook("CephOSDDown", "Reweight OSD")
        assert "ceph status" in pb
        assert "ceph health" in pb

    def test_contains_alert_and_hint(self):
        from app.storage_analyst import _build_stub_playbook
        pb = _build_stub_playbook("CephPoolFull", "Expand pool quota")
        assert "CephPoolFull" in pb
        assert "Expand pool quota" in pb

    def test_yaml_prefix(self):
        from app.storage_analyst import _build_stub_playbook
        pb = _build_stub_playbook("X", "Y")
        assert pb.strip().startswith("---")


# ═══════════════════════════════════════════════════════════════════════════════
# build_enriched_ticket_body edge cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildEnrichedTicketBodyEdgeCases:

    def _base_args(self, **overrides):
        defaults = dict(
            alert_name="CephOSDDown",
            service_name="storage",
            severity="critical",
            summary="OSD down",
            description="OSD.5 down",
            metrics_context="osd_status = 0",
            ai_result=MOCK_AI_RESULT.copy(),
            bridge_trace_id="abc123",
        )
        defaults.update(overrides)
        return defaults

    def test_no_risk_info(self):
        from app.storage_analyst import build_enriched_ticket_body
        body = build_enriched_ticket_body(**self._base_args())
        # Should still produce valid markdown
        assert "# Storage Incident" in body
        assert "CephOSDDown" in body

    def test_with_risk_badge(self):
        from app.storage_analyst import build_enriched_ticket_body
        body = build_enriched_ticket_body(**self._base_args(
            risk_score=0.85, risk_level="critical"
        ))
        assert "CRITICAL" in body
        assert "0.850" in body

    def test_evidence_lines_rendered(self):
        from app.storage_analyst import build_enriched_ticket_body
        evidence = ["- OSD.5 is down", "- Pool fill at 92%"]
        body = build_enriched_ticket_body(**self._base_args(evidence_lines=evidence))
        assert "OSD.5 is down" in body
        assert "Pool fill at 92%" in body
        assert "Evidence Observations" in body

    def test_no_bridge_trace_id(self):
        from app.storage_analyst import build_enriched_ticket_body
        body = build_enriched_ticket_body(**self._base_args(bridge_trace_id=""))
        assert "Bridge Trace" not in body

    def test_autonomy_badges(self):
        from app.storage_analyst import build_enriched_ticket_body
        for level, expected in [
            ("autonomous", "AUTONOMOUS"),
            ("approval_gated", "APPROVAL REQUIRED"),
            ("human_only", "HUMAN ONLY"),
        ]:
            ai_result = MOCK_AI_RESULT.copy()
            ai_result["autonomy_level"] = level
            body = build_enriched_ticket_body(**self._base_args(ai_result=ai_result))
            assert expected in body

    def test_notify_list_included(self):
        from app import storage_analyst as sa
        original = sa.NOTIFY_EMAIL
        try:
            sa.NOTIFY_EMAIL = "sre@example.com, ops@example.com"
            from app.storage_analyst import build_enriched_ticket_body
            body = build_enriched_ticket_body(**self._base_args())
            assert "sre@example.com" in body
            assert "ops@example.com" in body
        finally:
            sa.NOTIFY_EMAIL = original


# ═══════════════════════════════════════════════════════════════════════════════
# _get_storage_catalog + caching
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetStorageCatalog:

    def test_returns_list(self):
        from app import storage_analyst as sa
        sa._storage_catalog = _fake_storage_catalog()
        try:
            catalog = sa._get_storage_catalog()
            assert isinstance(catalog, list)
            assert len(catalog) > 0
        finally:
            sa._storage_catalog = None

    def test_caches_result(self):
        from app import storage_analyst as sa
        sa._storage_catalog = _fake_storage_catalog()
        c1 = sa._get_storage_catalog()
        c2 = sa._get_storage_catalog()
        assert c1 is c2
        sa._storage_catalog = None

    def test_all_entries_are_storage_domain(self):
        from app import storage_analyst as sa
        sa._storage_catalog = _fake_storage_catalog()
        try:
            for s in sa._get_storage_catalog():
                assert s.domain == "storage"
        finally:
            sa._storage_catalog = None


# ═══════════════════════════════════════════════════════════════════════════════
# get_notify_list
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetNotifyListExtra:

    def test_trailing_commas(self):
        from app import storage_analyst as sa
        original = sa.NOTIFY_EMAIL
        try:
            sa.NOTIFY_EMAIL = "a@b.com, , c@d.com,"
            result = sa.get_notify_list()
            assert result == ["a@b.com", "c@d.com"]
        finally:
            sa.NOTIFY_EMAIL = original
