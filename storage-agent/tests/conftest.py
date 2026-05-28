"""
tests/conftest.py
─────────────────────────────────────────────────────────────────────────────
Shared pytest fixtures for storage-agent tests.

All external I/O (xyOps API, Loki, Prometheus, ansible-runner) is mocked
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
if "prometheus_client" not in sys.modules:
    _pc = types.ModuleType("prometheus_client")

    class _FakeCounter:
        def __init__(self, *a, **kw):
            pass
        def labels(self, *a, **kw):
            return self
        def inc(self, *a, **kw):
            pass

    class _FakeHistogram:
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
os.environ.setdefault("XYOPS_URL", "http://xyops-mock:5522")
os.environ.setdefault("XYOPS_API_KEY", "test-api-key")
os.environ.setdefault("LOKI_URL", "http://loki-mock:3100")
os.environ.setdefault("PROMETHEUS_URL", "http://prometheus-mock:9090")
os.environ.setdefault("STORAGE_REQUIRE_APPROVAL", "true")
os.environ.setdefault("ANSIBLE_RUNNER_URL", "http://ansible-runner-mock:8080")
os.environ.setdefault("DISABLE_OTEL_EXPORTERS", "true")
os.environ.setdefault("WORKFLOW_STEP_DELAY_SECONDS", "0")

from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture(scope="session")
def app():
    """Import and return the FastAPI app (session-scoped: imported once)."""
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


# ── Storage alert payloads ────────────────────────────────────────────────────

STORAGE_FIRING_PAYLOAD = {
    "version": "4",
    "status": "firing",
    "receiver": "storage-alerts",
    "groupLabels": {"alertname": "CephOSDDown", "service_name": "storage-simulator"},
    "commonLabels": {"severity": "warning"},
    "commonAnnotations": {
        "summary": "Ceph OSD down on storage-simulator",
        "description": "OSD.2 is not responding",
    },
    "alerts": [
        {
            "status": "firing",
            "labels": {
                "alertname": "CephOSDDown",
                "service_name": "storage-simulator",
                "severity": "warning",
            },
            "annotations": {
                "summary": "Ceph OSD down on storage-simulator",
                "description": "OSD.2 is not responding",
                "dashboard_url": "http://grafana:3000/d/agentic-ai-overview",
            },
            "startsAt": "2026-03-22T10:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
        }
    ],
}

STORAGE_RESOLVED_PAYLOAD = {
    "version": "4",
    "status": "resolved",
    "receiver": "storage-alerts",
    "groupLabels": {"alertname": "CephOSDDown", "service_name": "storage-simulator"},
    "commonLabels": {"severity": "warning"},
    "commonAnnotations": {"summary": "Ceph OSD down resolved"},
    "alerts": [
        {
            "status": "resolved",
            "labels": {
                "alertname": "CephOSDDown",
                "service_name": "storage-simulator",
                "severity": "warning",
            },
            "annotations": {"summary": "Ceph OSD down resolved"},
            "startsAt": "2026-03-22T10:00:00Z",
            "endsAt": "2026-03-22T10:05:00Z",
        }
    ],
}

# Alert payloads for all storage scenario types
POOL_NEAR_FULL_PAYLOAD = {
    "version": "4",
    "status": "firing",
    "receiver": "storage-alerts",
    "groupLabels": {"alertname": "CephPoolNearFull"},
    "commonLabels": {"severity": "warning"},
    "commonAnnotations": {"summary": "Pool fill above 75%"},
    "alerts": [
        {
            "status": "firing",
            "labels": {
                "alertname": "CephPoolNearFull",
                "service_name": "storage-simulator",
                "severity": "warning",
            },
            "annotations": {
                "summary": "Pool fill above 75%",
                "description": "Storage pool is approaching capacity",
            },
            "startsAt": "2026-03-22T10:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
        }
    ],
}

MULTIPLE_OSD_DOWN_PAYLOAD = {
    "version": "4",
    "status": "firing",
    "receiver": "storage-alerts",
    "groupLabels": {"alertname": "CephMultipleOSDDown"},
    "commonLabels": {"severity": "critical"},
    "commonAnnotations": {"summary": "Multiple OSDs down"},
    "alerts": [
        {
            "status": "firing",
            "labels": {
                "alertname": "CephMultipleOSDDown",
                "service_name": "storage-simulator",
                "severity": "critical",
            },
            "annotations": {
                "summary": "Multiple OSDs down — data at risk",
                "description": "3 out of 6 OSDs not responding",
            },
            "startsAt": "2026-03-22T10:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
        }
    ],
}

NOISY_PVC_PAYLOAD = {
    "version": "4",
    "status": "firing",
    "receiver": "storage-alerts",
    "groupLabels": {"alertname": "NoisyPVCDetected"},
    "commonLabels": {"severity": "warning"},
    "commonAnnotations": {"summary": "Noisy PVC detected"},
    "alerts": [
        {
            "status": "firing",
            "labels": {
                "alertname": "NoisyPVCDetected",
                "service_name": "storage-simulator",
                "severity": "warning",
            },
            "annotations": {
                "summary": "PVC pvc-noisy-001 generating excessive IOPS",
                "description": "IOPS exceeded threshold",
            },
            "startsAt": "2026-03-22T10:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
        }
    ],
}

PVC_HIGH_LATENCY_PAYLOAD = {
    "version": "4",
    "status": "firing",
    "receiver": "storage-alerts",
    "groupLabels": {"alertname": "PVCHighLatency"},
    "commonLabels": {"severity": "warning"},
    "commonAnnotations": {"summary": "PVC latency exceeded threshold"},
    "alerts": [
        {
            "status": "firing",
            "labels": {
                "alertname": "PVCHighLatency",
                "service_name": "storage-simulator",
                "severity": "warning",
            },
            "annotations": {
                "summary": "PVC IO latency above 200ms",
                "description": "High latency on PVC storage path",
            },
            "startsAt": "2026-03-22T10:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
        }
    ],
}

POOL_FULL_PAYLOAD = {
    "version": "4",
    "status": "firing",
    "receiver": "storage-alerts",
    "groupLabels": {"alertname": "CephPoolFull"},
    "commonLabels": {"severity": "critical"},
    "commonAnnotations": {"summary": "Pool is completely full"},
    "alerts": [
        {
            "status": "firing",
            "labels": {
                "alertname": "CephPoolFull",
                "service_name": "storage-simulator",
                "severity": "critical",
            },
            "annotations": {
                "summary": "Ceph pool at 95%+ capacity",
                "description": "Write operations are failing",
            },
            "startsAt": "2026-03-22T10:00:00Z",
            "endsAt": "0001-01-01T00:00:00Z",
        }
    ],
}
