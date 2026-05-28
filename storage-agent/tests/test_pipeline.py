"""
tests/test_pipeline.py
─────────────────────────────────────────────────────────────────────────────
Tests for the storage pipeline endpoints (pipeline_router).

Each agent step is tested independently via HTTP, exactly as xyOps
workflow nodes call them.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from app.pipeline import (
    StoragePipelineSession,
    _sessions,
    _prune_sessions,
    _increment_action_counter,
    _storage_analysis_from_recommendation,
)
import time


def _make_session(**overrides) -> StoragePipelineSession:
    """Create a StoragePipelineSession with sensible defaults."""
    defaults = dict(
        session_id="test-session",
        service_name="storage-simulator",
        alert_name="CephOSDDown",
        severity="warning",
        summary="OSD down",
        description="OSD.2 not responding",
        dashboard_url="http://grafana:3000",
        ticket_id="t_test_123",
        ticket_num=42,
        bridge_trace_id="trace-123",
    )
    defaults.update(overrides)
    return StoragePipelineSession(**defaults)


class TestPipelineStart:
    """POST /pipeline/start — Agent 1: create session + xyOps ticket."""

    async def test_start_creates_session(self, client):
        _sessions.clear()
        with patch("app.pipeline._xyops_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"id": "t_123", "num": 1}
            with patch("app.pipeline._post_comment", new_callable=AsyncMock):
                resp = await client.post("/pipeline/start", json={
                    "service_name": "test-svc",
                    "alert_name": "CephOSDDown",
                    "severity": "warning",
                    "summary": "OSD down",
                })

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["session_id"] == "test-svc"
        assert "test-svc" in _sessions
        _sessions.clear()

    async def test_start_default_values(self, client):
        _sessions.clear()
        with patch("app.pipeline._xyops_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"id": "t_456", "num": 2}
            with patch("app.pipeline._post_comment", new_callable=AsyncMock):
                resp = await client.post("/pipeline/start", json={})

        assert resp.status_code == 200
        data = resp.json()
        assert data["session_id"] == "storage-simulator"
        _sessions.clear()


class TestPipelineStorageMetrics:
    """POST /pipeline/agent/storage-metrics — Agent 2."""

    async def test_metrics_requires_session(self, client):
        _sessions.clear()
        resp = await client.post("/pipeline/agent/storage-metrics", json={"session_id": "missing"})
        assert resp.status_code == 404

    async def test_metrics_fetches_prometheus(self, client):
        _sessions.clear()
        _sessions["test-svc"] = _make_session(session_id="test-svc")

        with patch("app.pipeline.fetch_storage_metrics", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = {
                "raw": {"osd_status": [{"value": [None, "5"]}]},
                "summary": "Storage Metrics Snapshot:\n  osd_status = 5",
            }
            with patch("app.pipeline._post_comment", new_callable=AsyncMock):
                resp = await client.post("/pipeline/agent/storage-metrics", json={"session_id": "test-svc"})

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert _sessions["test-svc"].metrics_context != ""
        _sessions.clear()


class TestPipelineLogs:
    """POST /pipeline/agent/logs — Agent 3."""

    async def test_logs_requires_session(self, client):
        _sessions.clear()
        resp = await client.post("/pipeline/agent/logs", json={"session_id": "missing"})
        assert resp.status_code == 404

    async def test_logs_fetches_loki(self, client):
        _sessions.clear()
        _sessions["test-svc"] = _make_session(session_id="test-svc")

        with patch("app.pipeline.fetch_loki_logs", new_callable=AsyncMock) as mock_logs:
            mock_logs.return_value = "storage-agent: OSD.2 marked down\nstorage-agent: rebalancing started"
            with patch("app.pipeline._post_comment", new_callable=AsyncMock):
                resp = await client.post("/pipeline/agent/logs", json={"session_id": "test-svc"})

        assert resp.status_code == 200
        assert resp.json()["log_lines"] >= 1
        assert "OSD.2" in _sessions["test-svc"].logs_context
        _sessions.clear()


class TestPipelineAnalyze:
    """POST /pipeline/agent/analyze — Agent 4: intelligence pipeline."""

    async def test_analyze_requires_session(self, client):
        _sessions.clear()
        resp = await client.post("/pipeline/agent/analyze", json={"session_id": "missing"})
        assert resp.status_code == 404

    async def test_analyze_deterministic(self, client):
        """Without AI enabled, should fall back to scenario-catalog analysis."""
        _sessions.clear()
        session = _make_session(
            session_id="test-svc",
            metrics_context="Storage Metrics Snapshot:\n  osd_status = 5",
            metrics_raw={"osd_status": [{"value": [None, "5"]}]},
            logs_context="OSD.2 marked down",
        )
        _sessions["test-svc"] = session

        with patch("app.pipeline._post_comment", new_callable=AsyncMock), \
             patch("app.pipeline._notify_coordinator", new_callable=AsyncMock):
            # Mock obs-intelligence merge to avoid external call
            with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json.return_value = {"anomalies": [], "forecasts": []}
                mock_get.return_value = mock_resp

                resp = await client.post("/pipeline/agent/analyze", json={"session_id": "test-svc"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["action"] is not None
        assert data["risk_score"] >= 0
        assert _sessions["test-svc"].risk_score >= 0
        _sessions.clear()


class TestPipelineTicket:
    """POST /pipeline/agent/ticket — Agent 5: enrich ticket body."""

    async def test_ticket_requires_session(self, client):
        _sessions.clear()
        resp = await client.post("/pipeline/agent/ticket", json={"session_id": "missing"})
        assert resp.status_code == 404

    async def test_ticket_enrichment(self, client):
        _sessions.clear()
        session = _make_session(session_id="test-svc")
        session.ai_result = {
            "rca_summary": "OSD.2 hardware failure",
            "recommended_action": "osd_reweight",
            "autonomy_level": "approval_gated",
            "confidence": "0.85",
            "provider": "scenario-catalog",
            "ansible_playbook": "---\n- hosts: storage_nodes",
        }
        session.metrics_context = "Storage Metrics Snapshot:\n  osd_status = 5"
        _sessions["test-svc"] = session

        with patch("app.pipeline._xyops_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"code": 0}
            with patch("app.pipeline._post_comment", new_callable=AsyncMock):
                resp = await client.post("/pipeline/agent/ticket", json={"session_id": "test-svc"})

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        _sessions.clear()


class TestPipelineApproval:
    """POST /pipeline/agent/approval — Agent 6: approval routing."""

    async def test_approval_human_only_escalates(self, client):
        _sessions.clear()
        session = _make_session(session_id="test-svc")
        session.ai_result = {
            "recommended_action": "multi_osd_escalate",
            "autonomy_level": "human_only",
            "ansible_playbook": "---\n- hosts: all",
        }
        _sessions["test-svc"] = session

        with patch("app.pipeline._post_comment", new_callable=AsyncMock):
            resp = await client.post("/pipeline/agent/approval", json={"session_id": "test-svc"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "escalated"
        assert _sessions["test-svc"].status == "escalated"
        _sessions.clear()

    async def test_approval_gated_creates_approval(self, client):
        _sessions.clear()
        session = _make_session(session_id="test-svc")
        session.ai_result = {
            "recommended_action": "osd_reweight",
            "autonomy_level": "approval_gated",
            "ansible_playbook": "---\n- hosts: storage_nodes",
            "test_cases": [],
        }
        _sessions["test-svc"] = session

        with patch("app.pipeline._xyops_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"id": "approval-123"}
            with patch("app.pipeline._post_comment", new_callable=AsyncMock):
                # Mock httpx.AsyncClient used inside pipeline_approval for validation
                mock_http = AsyncMock()
                mock_http.__aenter__ = AsyncMock(return_value=mock_http)
                mock_http.__aexit__ = AsyncMock(return_value=False)
                mock_val_resp = MagicMock()
                mock_val_resp.status_code = 200
                mock_val_resp.json.return_value = {
                    "all_passed": True,
                    "test_results": [{"id": "tc1", "name": "syntax", "status": "PASSED", "output": "ok"}],
                    "stdout": "PLAY OK",
                }
                mock_http.post.return_value = mock_val_resp
                with patch("app.pipeline.httpx.AsyncClient", return_value=mock_http):

                    resp = await client.post("/pipeline/agent/approval", json={"session_id": "test-svc"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "awaiting_approval"
        assert _sessions["test-svc"].validation_passed is True
        _sessions.clear()

    async def test_approval_validation_failure_blocks(self, client):
        """When ansible validation fails, pipeline should stop, not await approval."""
        _sessions.clear()
        session = _make_session(session_id="test-svc")
        session.ai_result = {
            "recommended_action": "osd_reweight",
            "autonomy_level": "approval_gated",
            "ansible_playbook": "---\n- hosts: storage_nodes\n  broken_yaml: !!!",
            "test_cases": [],
        }
        _sessions["test-svc"] = session

        with patch("app.pipeline._xyops_post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = {"id": "approval-456"}
            with patch("app.pipeline._post_comment", new_callable=AsyncMock):
                mock_http = AsyncMock()
                mock_http.__aenter__ = AsyncMock(return_value=mock_http)
                mock_http.__aexit__ = AsyncMock(return_value=False)
                mock_val_resp = MagicMock()
                mock_val_resp.status_code = 200
                mock_val_resp.json.return_value = {
                    "all_passed": False,
                    "test_results": [
                        {"id": "tc1", "name": "syntax", "status": "FAILED", "output": "syntax error"},
                    ],
                    "stdout": "PLAY FAILED",
                }
                mock_http.post.return_value = mock_val_resp
                with patch("app.pipeline.httpx.AsyncClient", return_value=mock_http):

                    resp = await client.post("/pipeline/agent/approval", json={"session_id": "test-svc"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "validation_failed"
        assert _sessions["test-svc"].validation_passed is False
        _sessions.clear()


class TestSessionManagement:
    """Session store, pruning, and retrieval."""

    async def test_get_session(self, client):
        _sessions.clear()
        _sessions["test-svc"] = _make_session(session_id="test-svc")
        resp = await client.get("/pipeline/session/test-svc")
        assert resp.status_code == 200
        assert resp.json()["session_id"] == "test-svc"
        _sessions.clear()

    async def test_get_default_session(self, client):
        _sessions.clear()
        _sessions["svc-a"] = _make_session(session_id="svc-a", created_at=100.0)
        _sessions["svc-b"] = _make_session(session_id="svc-b", created_at=200.0)
        resp = await client.get("/pipeline/session/default")
        assert resp.status_code == 200
        assert resp.json()["session_id"] == "svc-b"
        _sessions.clear()

    async def test_get_session_not_found(self, client):
        _sessions.clear()
        resp = await client.get("/pipeline/session/nonexistent")
        assert resp.status_code == 404

    async def test_get_default_no_sessions(self, client):
        _sessions.clear()
        resp = await client.get("/pipeline/session/default")
        assert resp.status_code == 404

    def test_prune_expired_sessions(self):
        _sessions.clear()
        old = _make_session(session_id="old", created_at=time.time() - 7200)
        fresh = _make_session(session_id="fresh", created_at=time.time())
        _sessions["old"] = old
        _sessions["fresh"] = fresh
        _prune_sessions()
        assert "old" not in _sessions
        assert "fresh" in _sessions
        _sessions.clear()

    async def test_pipeline_history(self, client):
        _sessions.clear()
        _sessions["svc-a"] = _make_session(session_id="svc-a", created_at=100.0)
        _sessions["svc-b"] = _make_session(session_id="svc-b", created_at=200.0)
        resp = await client.get("/pipeline/history")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        assert data["history"][0]["session_id"] == "svc-b"  # newest first
        _sessions.clear()


class TestHelperFunctions:
    """Unit tests for pipeline helper functions."""

    def test_increment_action_counter(self):
        """_increment_action_counter should not raise for any action type."""
        for action in ["osd_reweight", "pvc_throttle", "escalate", "other_action"]:
            _increment_action_counter(action)

    def test_storage_analysis_from_recommendation(self):
        """Should produce a valid ai_result dict from Recommendation + RiskAssessment."""
        from unittest.mock import MagicMock
        rec = MagicMock()
        rec.description = "OSD.2 hardware failure"
        rec.action_type = "osd_reweight"
        rec.autonomous = False
        rec.rollback_plan = "revert weight to original"
        rec.confidence = 0.85

        risk = MagicMock()
        risk.risk_score = 0.65
        risk.risk_level = "high"

        result = _storage_analysis_from_recommendation(rec, risk)
        assert result["rca_summary"] == "OSD.2 hardware failure"
        assert result["recommended_action"] == "osd_reweight"
        assert result["autonomy_level"] == "approval_gated"
        assert "ansible_playbook" in result
        assert result["provider"] == "scenario-catalog"
