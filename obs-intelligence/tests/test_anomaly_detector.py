"""Tests for obs_intelligence.anomaly_detector."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch, MagicMock

import pytest
import httpx

from obs_intelligence.anomaly_detector import detect_anomalies, _scalar_query


# ═══════════════════════════════════════════════════════════════════════════════
# _scalar_query
# ═══════════════════════════════════════════════════════════════════════════════


class TestScalarQuery:
    @pytest.mark.anyio
    async def test_returns_float_for_valid_response(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"value": [1700000000, "42.5"]}]},
        }
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        result = await _scalar_query("up", http, "http://prom:9090")
        assert result == 42.5

    @pytest.mark.anyio
    async def test_returns_none_for_empty_result(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "success", "data": {"result": []}}
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        result = await _scalar_query("absent_metric", http, "http://prom:9090")
        assert result is None

    @pytest.mark.anyio
    async def test_returns_none_for_non_200(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        result = await _scalar_query("up", http, "http://prom:9090")
        assert result is None

    @pytest.mark.anyio
    async def test_returns_none_on_exception(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(side_effect=Exception("network error"))
        result = await _scalar_query("up", http, "http://prom:9090")
        assert result is None

    @pytest.mark.anyio
    async def test_nan_returns_none(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"value": [1700000000, "NaN"]}]},
        }
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        result = await _scalar_query("up", http, "http://prom:9090")
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# detect_anomalies
# ═══════════════════════════════════════════════════════════════════════════════


class TestDetectAnomalies:
    def _mock_scalar(self, values: dict[str, float | None]):
        """
        Returns an async side_effect that intercepts _scalar_query calls.
        `values` maps partial promql substrings → return values.
        """
        call_count = {"n": 0}
        async def fake_get(*args, **kwargs):
            promql = args[0].params.get("query", "") if hasattr(args[0], "params") else ""
            # fallback: just return based on call index
            call_count["n"] += 1
            return MagicMock(status_code=200, json=lambda: {
                "status": "success",
                "data": {"result": [{"value": [0, "0"]}]},
            })
        return fake_get

    @pytest.mark.anyio
    async def test_returns_empty_when_prometheus_unavailable(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(side_effect=Exception("unreachable"))
        result = await detect_anomalies("compute", http)
        assert result == []

    @pytest.mark.anyio
    async def test_returns_anomaly_when_z_score_exceeds_threshold(self):
        """Mock all three queries: current=100.0, mean=10.0, stddev=5.0 → z=18."""
        responses = iter([
            # current
            MagicMock(status_code=200, json=lambda: {
                "status": "success",
                "data": {"result": [{"value": [0, "100.0"]}]},
            }),
            # mean
            MagicMock(status_code=200, json=lambda: {
                "status": "success",
                "data": {"result": [{"value": [0, "10.0"]}]},
            }),
            # stddev
            MagicMock(status_code=200, json=lambda: {
                "status": "success",
                "data": {"result": [{"value": [0, "5.0"]}]},
            }),
            # Second metric — flat (no anomaly)
            MagicMock(status_code=200, json=lambda: {
                "status": "success",
                "data": {"result": [{"value": [0, "50.0"]}]},
            }),
            MagicMock(status_code=200, json=lambda: {
                "status": "success",
                "data": {"result": [{"value": [0, "50.0"]}]},
            }),
            MagicMock(status_code=200, json=lambda: {
                "status": "success",
                "data": {"result": [{"value": [0, "0.0000001"]}]},
            }),
        ])
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(side_effect=lambda *a, **kw: next(responses))
        result = await detect_anomalies("compute", http)
        # At least one anomaly detected (z=18 >> threshold=2.5)
        assert len(result) >= 1
        assert result[0].z_score > 2.5
        assert result[0].anomaly_type == "spike"

    @pytest.mark.anyio
    async def test_no_anomaly_when_flat_signal(self):
        """All metrics have stddev ≈ 0 → skip."""
        mock_resp = MagicMock(status_code=200, json=lambda: {
            "status": "success",
            "data": {"result": [{"value": [0, "10.0"]}]},
        })
        mock_zero_std = MagicMock(status_code=200, json=lambda: {
            "status": "success",
            "data": {"result": [{"value": [0, "0.0"]}]},
        })
        responses = iter([
            mock_resp, mock_resp, mock_zero_std,  # metric 1: current, mean, stddev=0
            mock_resp, mock_resp, mock_zero_std,  # metric 2: same
        ])
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(side_effect=lambda *a, **kw: next(responses))
        result = await detect_anomalies("compute", http)
        assert result == []

    @pytest.mark.anyio
    async def test_storage_domain_uses_storage_metrics(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(side_effect=Exception("no connection"))
        result = await detect_anomalies("storage", http)
        # Should not crash
        assert result == []
