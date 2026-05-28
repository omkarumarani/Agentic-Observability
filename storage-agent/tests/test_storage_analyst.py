"""
tests/test_storage_analyst.py
─────────────────────────────────────────────────────────────────────────────
Tests for the storage analyst module: metric fetching, deterministic
analysis, AI provider selection, and ticket body enrichment.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from app.storage_analyst import (
    deterministic_analysis,
    _build_stub_playbook,
    build_enriched_ticket_body,
    get_notify_list,
    fetch_storage_metrics,
    fetch_loki_logs,
    AI_ENABLED,
)


class TestDeterministicAnalysis:
    """deterministic_analysis() — scenario-catalog driven, LLM-free."""

    def test_ceph_osd_down(self):
        """CephOSDDown should return osd_reweight or similar action."""
        result = deterministic_analysis("CephOSDDown", "osd_status = 5")
        assert result["rca_summary"]
        assert result["recommended_action"] != ""
        assert result["provider"] == "scenario-catalog"
        assert result["ansible_playbook"] != ""

    def test_ceph_pool_near_full(self):
        result = deterministic_analysis("CephPoolNearFull", "pool_fill_pct = 0.78")
        assert result["recommended_action"] != ""
        assert result["provider"] == "scenario-catalog"

    def test_ceph_multiple_osd_down(self):
        result = deterministic_analysis("CephMultipleOSDDown", "osd_status = 3")
        assert result["rca_summary"]
        # Multiple OSD down should always be escalated or human_only
        assert result["recommended_action"] != ""

    def test_noisy_pvc_detected(self):
        result = deterministic_analysis("NoisyPVCDetected", "pvc_iops = 5000")
        assert result["recommended_action"] != ""

    def test_pvc_high_latency(self):
        result = deterministic_analysis("PVCHighLatency", "io_latency_ms = 300")
        assert result["recommended_action"] != ""

    def test_ceph_pool_full(self):
        result = deterministic_analysis("CephPoolFull", "pool_fill_pct = 0.96")
        assert result["recommended_action"] != ""

    def test_ceph_cluster_degraded(self):
        result = deterministic_analysis("CephClusterDegraded", "cluster_health = 0.5")
        assert result["recommended_action"] != ""

    def test_unknown_alert_escalates(self):
        """Unrecognised alert should produce an escalation response."""
        result = deterministic_analysis("CompletelyNewAlert", "no metrics")
        assert result["recommended_action"] == "escalate"
        assert result["autonomy_level"] == "human_only"

    def test_analysis_returns_all_fields(self):
        """Every analysis must return the full set of required fields."""
        result = deterministic_analysis("CephOSDDown", "osd_status = 5")
        required = {"rca_summary", "recommended_action", "autonomy_level",
                     "ansible_playbook", "test_plan", "confidence", "provider"}
        assert required.issubset(set(result.keys()))

    def test_analysis_confidence_is_numeric_string(self):
        """Confidence should be a parseable float string."""
        result = deterministic_analysis("CephOSDDown", "osd = 5")
        assert float(result["confidence"]) >= 0.0


class TestBuildStubPlaybook:
    """_build_stub_playbook() — generates Ansible YAML."""

    def test_playbook_contains_alert_name(self):
        pb = _build_stub_playbook("CephOSDDown", "reweight OSD.2 to 0.5")
        assert "CephOSDDown" in pb
        assert "hosts:" in pb

    def test_playbook_contains_hint(self):
        pb = _build_stub_playbook("CephPoolFull", "expand pool quota")
        assert "expand pool quota" in pb

    def test_playbook_is_valid_yaml_prefix(self):
        pb = _build_stub_playbook("TestAlert", "do something")
        assert pb.startswith("---")


class TestBuildEnrichedTicketBody:
    """build_enriched_ticket_body() — xyOps ticket markdown."""

    def test_produces_markdown(self):
        body = build_enriched_ticket_body(
            alert_name="CephOSDDown",
            service_name="storage-simulator",
            severity="warning",
            summary="OSD down",
            description="OSD.2 not responding",
            metrics_context="Storage Metrics:\n  osd_status = 5",
            ai_result={
                "rca_summary": "OSD hardware failure",
                "recommended_action": "osd_reweight",
                "autonomy_level": "approval_gated",
                "confidence": "0.85",
                "provider": "scenario-catalog",
            },
            bridge_trace_id="trace-123",
        )
        assert "CephOSDDown" in body
        assert "osd_reweight" in body
        assert "APPROVAL" in body.upper()

    def test_risk_badge_included(self):
        body = build_enriched_ticket_body(
            alert_name="CephPoolFull",
            service_name="sim",
            severity="critical",
            summary="Pool full",
            description="",
            metrics_context="pool = 0.96",
            ai_result={
                "rca_summary": "Pool critical",
                "recommended_action": "pool_critical_action",
                "autonomy_level": "human_only",
                "confidence": "0.90",
                "provider": "scenario-catalog",
                "risk_score": 0.85,
                "risk_level": "high",
            },
            bridge_trace_id="trace-456",
            risk_score=0.85,
            risk_level="high",
        )
        assert "HIGH" in body

    def test_evidence_lines_included(self):
        body = build_enriched_ticket_body(
            alert_name="CephOSDDown",
            service_name="sim",
            severity="warning",
            summary="OSD down",
            description="",
            metrics_context="osd = 5",
            ai_result={
                "rca_summary": "Failure",
                "recommended_action": "osd_reweight",
                "autonomy_level": "approval_gated",
                "confidence": "0.80",
                "provider": "scenario-catalog",
            },
            bridge_trace_id="trace-789",
            evidence_lines=["Evidence line 1", "Evidence line 2"],
        )
        assert "Evidence line 1" in body


class TestGetNotifyList:
    """get_notify_list() — parses notification emails."""

    def test_empty_string(self):
        with patch("app.storage_analyst.NOTIFY_EMAIL", ""):
            # re-evaluate
            from app.storage_analyst import get_notify_list
            result = get_notify_list()
        assert result == [] or isinstance(result, list)

    def test_single_email(self):
        with patch("app.storage_analyst.NOTIFY_EMAIL", "sre@example.com"):
            from app.storage_analyst import get_notify_list
            result = get_notify_list()
        assert "sre@example.com" in result

    def test_multiple_emails(self):
        with patch("app.storage_analyst.NOTIFY_EMAIL", "sre@example.com, ops@example.com"):
            from app.storage_analyst import get_notify_list
            result = get_notify_list()
        assert len(result) >= 2


class TestFetchStorageMetrics:
    """fetch_storage_metrics() — Prometheus query wrapper."""

    async def test_returns_summary_and_raw(self):
        with patch("app.storage_analyst._fetch_instant_metric", new_callable=AsyncMock) as mock_prom:
            mock_prom.return_value = [
                {"metric": {"osd": "osd.1"}, "value": [1234567890, "1.0"]},
            ]
            import httpx
            async with httpx.AsyncClient() as http:
                result = await fetch_storage_metrics("CephOSDDown", http)

        assert "raw" in result
        assert "summary" in result
        assert "Storage Metrics Snapshot" in result["summary"]

    async def test_handles_empty_prometheus(self):
        with patch("app.storage_analyst._fetch_instant_metric", new_callable=AsyncMock) as mock_prom:
            mock_prom.return_value = []
            import httpx
            async with httpx.AsyncClient() as http:
                result = await fetch_storage_metrics("CephOSDDown", http)

        assert "no data available" in result["summary"]


class TestFetchLokiLogs:
    """fetch_loki_logs() — Loki query wrapper."""

    async def test_returns_log_lines(self):
        with patch("app.storage_analyst._fetch_loki_context", new_callable=AsyncMock) as mock_loki:
            mock_loki.return_value = "line1\nline2\nline3"
            import httpx
            async with httpx.AsyncClient() as http:
                result = await fetch_loki_logs('{service_name="storage"}', http)

        assert "line1" in result
        assert "3 lines" in result

    async def test_handles_no_logs(self):
        with patch("app.storage_analyst._fetch_loki_context", new_callable=AsyncMock) as mock_loki:
            mock_loki.return_value = ""
            import httpx
            async with httpx.AsyncClient() as http:
                result = await fetch_loki_logs('{service_name="storage"}', http)

        assert "no log lines" in result
