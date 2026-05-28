"""
tests/test_approval.py
─────────────────────────────────────────────────────────────────────────────
Tests for the storage-agent approval workflow.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from app.pipeline import StoragePipelineSession, _sessions
import time


def _make_session(**overrides) -> StoragePipelineSession:
    defaults = dict(
        session_id="test-approval",
        service_name="storage-simulator",
        alert_name="CephOSDDown",
        severity="warning",
        summary="OSD down",
        description="OSD.2 not responding",
        dashboard_url="http://grafana:3000",
        ticket_id="t_approval_123",
        ticket_num=42,
        bridge_trace_id="trace-approval",
        status="awaiting_approval",
        validation_passed=True,
        validation_result={"all_passed": True, "test_results": [], "stdout": "OK"},
    )
    defaults.update(overrides)
    return StoragePipelineSession(**defaults)


class TestApprovalDecision:
    """POST /approval/{session_id}/decision"""

    async def test_approve_valid_session(self, client):
        _sessions.clear()
        session = _make_session()
        session.ai_result = {
            "recommended_action": "osd_reweight",
            "ansible_playbook": "---\n- hosts: storage_nodes",
            "test_cases": [],
        }
        _sessions["test-approval"] = session

        mock_http = AsyncMock()
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)
        with patch("app.pipeline._run_playbook", new_callable=AsyncMock), \
             patch("app.main.httpx.AsyncClient", return_value=mock_http):

            resp = await client.post("/approval/test-approval/decision", json={
                "approved": True,
                "decided_by": "admin-user",
                "notes": "Reviewed and approved",
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "executed"
        assert data["decided_by"] == "admin-user"
        _sessions.clear()

    async def test_decline_valid_session(self, client):
        _sessions.clear()
        session = _make_session()
        session.ai_result = {"recommended_action": "osd_reweight"}
        _sessions["test-approval"] = session

        mock_http = AsyncMock()
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)
        with patch("app.main.httpx.AsyncClient", return_value=mock_http):

            resp = await client.post("/approval/test-approval/decision", json={
                "approved": False,
                "decided_by": "admin-user",
                "notes": "Needs investigation first",
            })

        assert resp.status_code == 200
        assert resp.json()["status"] == "declined"
        _sessions.clear()

    async def test_approve_blocked_by_validation(self, client):
        """Approval should be rejected when validation_passed is False."""
        _sessions.clear()
        session = _make_session(
            validation_passed=False,
            validation_result={
                "all_passed": False,
                "test_results": [{"id": "tc1", "status": "FAILED", "output": "error"}],
                "stdout": "FAIL",
            },
        )
        session.ai_result = {"recommended_action": "osd_reweight"}
        _sessions["test-approval"] = session

        resp = await client.post("/approval/test-approval/decision", json={
            "approved": True,
            "decided_by": "admin-user",
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "validation_failed"
        assert "validation_result" in data
        _sessions.clear()

    async def test_approve_missing_session(self, client):
        _sessions.clear()
        resp = await client.post("/approval/nonexistent/decision", json={
            "approved": True,
            "decided_by": "admin",
        })
        assert resp.status_code == 404

    async def test_approve_wrong_state(self, client):
        """Cannot approve a session that's not in awaiting_approval state."""
        _sessions.clear()
        session = _make_session(status="executed")
        session.ai_result = {"recommended_action": "osd_reweight"}
        _sessions["test-approval"] = session

        resp = await client.post("/approval/test-approval/decision", json={
            "approved": True,
            "decided_by": "admin",
        })

        assert resp.status_code == 200
        assert resp.json()["status"] == "executed"
        assert "Not awaiting approval" in resp.json()["message"]
        _sessions.clear()


class TestPendingApprovals:
    """GET /approvals/pending"""

    async def test_list_pending_empty(self, client):
        _sessions.clear()
        resp = await client.get("/approvals/pending")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["items"] == []

    async def test_list_pending_with_sessions(self, client):
        _sessions.clear()
        session = _make_session()
        session.ai_result = {
            "recommended_action": "osd_reweight",
            "rca_summary": "OSD failure",
            "ansible_playbook": "---",
        }
        _sessions["test-approval"] = session

        # Add a non-pending session that should be excluded
        executed = _make_session(session_id="executed-svc", status="executed")
        executed.ai_result = {"recommended_action": "pvc_throttle"}
        _sessions["executed-svc"] = executed

        resp = await client.get("/approvals/pending")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        item = data["items"][0]
        assert item["session_id"] == "test-approval"
        assert item["action"] == "osd_reweight"
        assert item["validation_passed"] is True
        assert item["rca_summary"] == "OSD failure"
        _sessions.clear()


class TestPredictiveAlert:
    """POST /predictive-alert"""

    async def test_predictive_alert_accepted(self, client):
        # Ensure _http is set in main module
        import app.main as main_mod
        original_http = main_mod._http

        with patch.object(main_mod, "_http", new=MagicMock()):
            with patch("app.main._xyops_post", new_callable=AsyncMock) as mock_post:
                mock_post.return_value = {"ticket": {"id": "pred-t1", "num": 99}}

                resp = await client.post("/predictive-alert", json={
                    "service_name": "storage-simulator",
                    "domain": "storage",
                    "scenario_id": "anomaly_io_latency",
                    "risk_score": 0.85,
                    "confidence": 0.9,
                    "description": "IO latency anomalous",
                    "forecast_breach_minutes": 30,
                    "anomaly_metric": "io_latency_s",
                    "anomaly_z_score": 3.2,
                })

        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["service_name"] == "storage-simulator"

        main_mod._http = original_http

    async def test_predictive_alert_no_http(self, client):
        """When _http is None, should return error gracefully."""
        import app.main as main_mod
        original_http = main_mod._http

        main_mod._http = None
        resp = await client.post("/predictive-alert", json={
            "service_name": "storage-simulator",
            "domain": "storage",
            "scenario_id": "anomaly_pool_fill",
            "risk_score": 0.7,
            "confidence": 0.8,
        })

        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "error"

        main_mod._http = original_http
