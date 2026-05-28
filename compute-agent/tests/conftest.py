"""
tests/conftest.py
─────────────────────────────────────────────────────────────────────────────
Shared pytest fixtures for aiops-bridge tests.

All external I/O (xyOps API, Loki, Prometheus, OpenAI/Claude) is mocked
so the tests are fully offline — no running services required.
"""

import os
import pathlib
import sys
import types
from unittest.mock import MagicMock

# Make obs_intelligence importable during offline test runs (no pip install needed).
_OBS_INTELLIGENCE_APP = pathlib.Path(__file__).parents[2] / "obs-intelligence" / "app"
if str(_OBS_INTELLIGENCE_APP) not in sys.path:
    sys.path.insert(0, str(_OBS_INTELLIGENCE_APP))

# ── Stub prometheus_client if not installed (e.g. local test runs) ────────────
# The app.telemetry module imports prometheus_client at module level.  In the
# Docker image the package is present, but for lightweight local test runs we
# inject a minimal mock so the import succeeds without the real library.
if "prometheus_client" not in sys.modules:
    _pc = types.ModuleType("prometheus_client")

    class _FakeCounter:
        """Minimal Counter stub — .labels().inc() is a no-op."""
        def __init__(self, *a, **kw):
            pass
        def labels(self, *a, **kw):
            return self
        def inc(self, *a, **kw):
            pass

    class _FakeHistogram:
        """Minimal Histogram stub — .labels().observe() is a no-op."""
        def __init__(self, *a, **kw):
            pass
        def labels(self, *a, **kw):
            return self
        def observe(self, *a, **kw):
            pass

    _pc.Counter = _FakeCounter
    _pc.Histogram = _FakeHistogram
    _pc.CONTENT_TYPE_LATEST = "text/plain"
    _pc.generate_latest = lambda *a, **kw: b""
    _pc.REGISTRY = MagicMock()
    sys.modules["prometheus_client"] = _pc

import pytest

# ── Set env vars BEFORE any app module is imported ─────────────────────────
# This avoids module-level AI_ENABLED / OPENAI_API_KEY being set to ""
os.environ.setdefault("OPENAI_API_KEY", "sk-test-fake-key-for-unit-tests")
os.environ.setdefault("AI_MODEL", "gpt-4o-mini")
os.environ.setdefault("XYOPS_URL", "http://xyops-mock:5522")
os.environ.setdefault("XYOPS_API_KEY", "test-api-key")
os.environ.setdefault("LOKI_URL", "http://loki-mock:3100")
os.environ.setdefault("PROMETHEUS_URL", "http://prometheus-mock:9090")
os.environ.setdefault("REQUIRE_APPROVAL", "true")
os.environ.setdefault("ANSIBLE_RUNNER_URL", "http://ansible-runner-mock:8080")
os.environ.setdefault("DISABLE_OTEL_EXPORTERS", "true")

from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, MagicMock


# ── Minimal ASGI app fixture ─────────────────────────────────────────────────
# Import app lazily so env vars above are already set when modules load.

@pytest.fixture(scope="session")
def app():
    """Import and return the FastAPI app (session-scoped: imported once)."""
    # Patch OTel setup so it doesn't try to connect to a real collector
    import unittest.mock as mock
    with mock.patch("app.telemetry.setup_telemetry"):
        from app.main import app as fastapi_app
    return fastapi_app


@pytest.fixture
async def client(app):
    """Async HTTP test client wrapping the FastAPI app."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


# ── xyOps mock POST helper ───────────────────────────────────────────────────
@pytest.fixture
def mock_xyops_post():
    """
    Returns an AsyncMock that simulates a successful xyOps POST.
    Default response: {"code": 0, "ticket": {"id": "t_test_id", "num": 99}}
    """
    mock = AsyncMock(return_value={
        "code": 0,
        "ticket": {"id": "t_test_id", "num": 99},
    })
    return mock


@pytest.fixture
def mock_xyops_post_comment():
    """xyOps POST mock that returns a clean comment success response."""
    return AsyncMock(return_value={"code": 0})


# ── Shared alert payloads ─────────────────────────────────────────────────────
FIRING_PAYLOAD = {
    "version": "4",
    "status": "firing",
    "receiver": "xyops-incidents",
    "groupLabels": {"alertname": "HighErrorRate", "service_name": "frontend-api"},
    "commonLabels": {"severity": "warning"},
    "commonAnnotations": {
        "summary": "High error rate on frontend-api",
        "description": "Error rate exceeded threshold",
        "dashboard_url": "http://grafana:3000/d/obs-overview",
    },
    "alerts": [
        {
            "status": "firing",
            "labels": {
                "alertname": "HighErrorRate",
                "service_name": "frontend-api",
                "severity": "warning",
            },
            "annotations": {
                "summary": "High error rate on frontend-api",
                "description": "Error rate exceeded threshold",
                "dashboard_url": "http://grafana:3000/d/obs-overview",
            },
            "startsAt": "2026-03-17T10:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
        }
    ],
}

RESOLVED_PAYLOAD = {
    "version": "4",
    "status": "resolved",
    "receiver": "xyops-incidents",
    "groupLabels": {"alertname": "HighErrorRate", "service_name": "frontend-api"},
    "commonLabels": {"severity": "warning"},
    "commonAnnotations": {"summary": "High error rate on frontend-api"},
    "alerts": [
        {
            "status": "resolved",
            "labels": {
                "alertname": "HighErrorRate",
                "service_name": "frontend-api",
                "severity": "warning",
            },
            "annotations": {"summary": "Resolved"},
            "startsAt": "2026-03-17T10:00:00Z",
            "endsAt": "2026-03-17T10:05:00Z",
        }
    ],
}
