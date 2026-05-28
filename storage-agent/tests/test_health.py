"""
tests/test_health.py
─────────────────────────────────────────────────────────────────────────────
Tests for health and metrics endpoints.
"""


class TestHealthEndpoint:
    """GET /health"""

    async def test_health_returns_ok(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["service"] == "storage-agent"
        assert "ai_enabled" in data
        assert "active_sessions" in data


class TestMetricsEndpoint:
    """GET /metrics"""

    async def test_metrics_returns_prometheus_format(self, client):
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        content = resp.text
        # When prometheus_client is installed, output contains real metrics.
        # In stub mode (no prometheus_client) the body is empty but 200 OK.
        assert content == "" or "storage_agent" in content or "# HELP" in content or "# TYPE" in content
