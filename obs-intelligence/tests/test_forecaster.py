"""Tests for obs_intelligence.forecaster."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import httpx
import numpy as np

from obs_intelligence.forecaster import run_forecasts, _forecast_one, _range_query


# ═══════════════════════════════════════════════════════════════════════════════
# _range_query
# ═══════════════════════════════════════════════════════════════════════════════


class TestRangeQuery:
    @pytest.mark.anyio
    async def test_returns_values_for_valid_response(self):
        values = [[1700000000, "10"], [1700000060, "11"], [1700000120, "12"]]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"values": values}]},
        }
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        result = await _range_query("up", http, "http://prom:9090", "0", "100", "30s")
        assert result == values

    @pytest.mark.anyio
    async def test_returns_empty_for_non_200(self):
        mock_resp = MagicMock(status_code=503)
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        result = await _range_query("up", http, "http://prom:9090", "0", "100", "30s")
        assert result == []

    @pytest.mark.anyio
    async def test_returns_empty_on_exception(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(side_effect=Exception("net error"))
        result = await _range_query("up", http, "http://prom:9090", "0", "100", "30s")
        assert result == []

    @pytest.mark.anyio
    async def test_returns_empty_for_no_results(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "success", "data": {"result": []}}
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        result = await _range_query("up", http, "http://prom:9090", "0", "100", "30s")
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════════
# _forecast_one
# ═══════════════════════════════════════════════════════════════════════════════


class TestForecastOne:
    @pytest.mark.anyio
    async def test_returns_none_with_insufficient_data(self):
        """Fewer than 4 data points → returns None."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"values": [[0, "10"], [60, "11"]]}]},
        }
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        cfg = {
            "metric_name": "test", "promql": "up",
            "threshold": 100.0, "step": "1m", "lookback_minutes": 30,
        }
        result = await _forecast_one(cfg, http, "http://prom:9090")
        assert result is None

    @pytest.mark.anyio
    async def test_linear_forecast_with_threshold_breach(self):
        """Linearly increasing data that will breach threshold."""
        now = int(time.time())
        # 30 values increasing from 50 to 79 over 30 minutes
        values = [
            [now - (30 - i) * 60, str(50.0 + i)]
            for i in range(30)
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"values": values}]},
        }
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        cfg = {
            "metric_name": "pool_fill_pct",
            "promql": "test",
            "threshold": 85.0,
            "step": "1m",
            "lookback_minutes": 30,
        }
        result = await _forecast_one(cfg, http, "http://prom:9090")
        assert result is not None
        assert result.metric_name == "pool_fill_pct"
        assert result.predicted_breach is not None
        assert result.threshold == 85.0
        assert len(result.forecast_values) > 0
        assert len(result.confidence_interval_lower) == len(result.forecast_values)
        assert len(result.confidence_interval_upper) == len(result.forecast_values)

    @pytest.mark.anyio
    async def test_no_breach_when_flat_signal(self):
        now = int(time.time())
        values = [[now - (10 - i) * 60, "10.0"] for i in range(10)]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"values": values}]},
        }
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        cfg = {
            "metric_name": "test", "promql": "x",
            "threshold": 85.0, "step": "1m", "lookback_minutes": 10,
        }
        result = await _forecast_one(cfg, http, "http://prom:9090")
        assert result is not None
        assert result.predicted_breach is None


# ═══════════════════════════════════════════════════════════════════════════════
# run_forecasts
# ═══════════════════════════════════════════════════════════════════════════════


class TestRunForecasts:
    @pytest.mark.anyio
    async def test_returns_list_even_when_prometheus_unreachable(self):
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(side_effect=Exception("unreachable"))
        results = await run_forecasts(http)
        assert isinstance(results, list)

    @pytest.mark.anyio
    async def test_returns_results_for_valid_data(self):
        now = int(time.time())
        values = [[now - (20 - i) * 60, str(30.0 + i * 0.5)] for i in range(20)]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"values": values}]},
        }
        http = AsyncMock(spec=httpx.AsyncClient)
        http.get = AsyncMock(return_value=mock_resp)
        results = await run_forecasts(http)
        assert len(results) >= 1
        for r in results:
            assert r.metric_name is not None
            assert r.model_used in ("linear", "exponential_growth")
