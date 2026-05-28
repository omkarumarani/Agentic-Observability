"""
tests/test_webhook.py
─────────────────────────────────────────────────────────────────────────────
Tests for the Alertmanager webhook endpoint and background pipeline trigger.
"""

import pytest
from unittest.mock import patch, AsyncMock


class TestWebhookEndpoint:
    """POST /webhook — Alertmanager webhook receiver."""

    async def test_webhook_firing_returns_200(self, client):
        """Firing alert should return 200 and acknowledge receipt."""
        from tests.conftest import STORAGE_FIRING_PAYLOAD

        with patch("app.main._run_storage_pipeline", new_callable=AsyncMock):
            resp = await client.post("/webhook", json=STORAGE_FIRING_PAYLOAD)

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["received"] == 1

    async def test_webhook_resolved_returns_200(self, client):
        """Resolved alert should return 200 and record outcome."""
        from tests.conftest import STORAGE_RESOLVED_PAYLOAD

        with patch("app.main._record_obs_intelligence_outcome", new_callable=AsyncMock):
            resp = await client.post("/webhook", json=STORAGE_RESOLVED_PAYLOAD)

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    async def test_webhook_multiple_alerts(self, client):
        """Multiple alerts in single payload should all be processed."""
        payload = {
            "version": "4",
            "status": "firing",
            "receiver": "storage-alerts",
            "groupLabels": {},
            "commonLabels": {"severity": "warning"},
            "commonAnnotations": {},
            "alerts": [
                {
                    "status": "firing",
                    "labels": {"alertname": "CephOSDDown", "service_name": "sim-1", "severity": "warning"},
                    "annotations": {"summary": "OSD down"},
                    "startsAt": "2026-03-22T10:00:00Z",
                    "endsAt": "0001-01-01T00:00:00Z",
                },
                {
                    "status": "firing",
                    "labels": {"alertname": "CephPoolNearFull", "service_name": "sim-2", "severity": "warning"},
                    "annotations": {"summary": "Pool near full"},
                    "startsAt": "2026-03-22T10:00:00Z",
                    "endsAt": "0001-01-01T00:00:00Z",
                },
            ],
        }

        with patch("app.main._run_storage_pipeline", new_callable=AsyncMock):
            resp = await client.post("/webhook", json=payload)

        assert resp.status_code == 200
        assert resp.json()["received"] == 2

    async def test_webhook_empty_alerts(self, client):
        """Empty alerts list should still return 200."""
        payload = {
            "version": "4",
            "status": "firing",
            "receiver": "storage-alerts",
            "groupLabels": {},
            "commonLabels": {},
            "commonAnnotations": {},
            "alerts": [],
        }
        resp = await client.post("/webhook", json=payload)
        assert resp.status_code == 200
        assert resp.json()["received"] == 0

    async def test_webhook_mixed_firing_and_resolved(self, client):
        """Payload with both firing and resolved alerts should handle both."""
        payload = {
            "version": "4",
            "status": "firing",
            "receiver": "storage-alerts",
            "groupLabels": {},
            "commonLabels": {},
            "commonAnnotations": {},
            "alerts": [
                {
                    "status": "firing",
                    "labels": {"alertname": "CephOSDDown", "service_name": "sim", "severity": "warning"},
                    "annotations": {"summary": "OSD down"},
                    "startsAt": "2026-03-22T10:00:00Z",
                    "endsAt": "0001-01-01T00:00:00Z",
                },
                {
                    "status": "resolved",
                    "labels": {"alertname": "CephPoolNearFull", "service_name": "sim", "severity": "warning"},
                    "annotations": {"summary": "Pool resolved"},
                    "startsAt": "2026-03-22T10:00:00Z",
                    "endsAt": "2026-03-22T10:05:00Z",
                },
            ],
        }

        with patch("app.main._run_storage_pipeline", new_callable=AsyncMock) as mock_pipeline, \
             patch("app.main._record_obs_intelligence_outcome", new_callable=AsyncMock) as mock_outcome:
            resp = await client.post("/webhook", json=payload)

        assert resp.status_code == 200
        assert resp.json()["received"] == 2
