"""Tests for obs_intelligence.feature_extractor."""
from __future__ import annotations

from obs_intelligence.feature_extractor import (
    extract_features,
    _safe_float,
    _prom_float,
)


# ═══════════════════════════════════════════════════════════════════════════════
# _safe_float
# ═══════════════════════════════════════════════════════════════════════════════


class TestSafeFloat:
    def test_numeric_string(self):
        assert _safe_float("12.5") == 12.5

    def test_with_scale(self):
        assert _safe_float("200", scale=0.001) == 0.2

    def test_none_returns_zero(self):
        assert _safe_float(None) == 0.0

    def test_no_data_returns_zero(self):
        assert _safe_float("no data") == 0.0

    def test_parse_error_returns_zero(self):
        assert _safe_float("parse error") == 0.0

    def test_integer_value(self):
        assert _safe_float(42) == 42.0

    def test_empty_string(self):
        assert _safe_float("") == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# _prom_float
# ═══════════════════════════════════════════════════════════════════════════════


class TestPromFloat:
    def test_valid_prometheus_result(self):
        result = {"metric": {"__name__": "x"}, "value": [1700000000, "42.5"]}
        assert _prom_float(result) == 42.5

    def test_missing_value_key(self):
        assert _prom_float({"metric": {}}) == 0.0

    def test_not_a_dict(self):
        assert _prom_float("string") == 0.0

    def test_none_input(self):
        assert _prom_float(None) == 0.0

    def test_value_not_numeric(self):
        result = {"value": [1700000000, "NaN-string"]}
        assert _prom_float(result) == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Compute feature extraction
# ═══════════════════════════════════════════════════════════════════════════════


class TestExtractComputeFeatures:
    def test_basic_compute_metrics(self):
        metrics = {
            "error_rate_pct": "5.0",
            "p99_latency_ms": "620.0",
            "p50_latency_ms": "250.0",
            "rps": "120.5",
        }
        f = extract_features("HighErrorRate", "frontend-api", "critical", "compute", metrics, "")
        assert f.alert_name == "HighErrorRate"
        assert f.service_name == "frontend-api"
        assert f.severity == "critical"
        assert f.domain == "compute"
        assert abs(f.error_rate - 0.05) < 1e-6   # 5% → 0.05
        assert abs(f.latency_p99 - 0.620) < 1e-6  # 620ms → 0.620s
        assert abs(f.latency_p95 - 0.250) < 1e-6  # 250ms → 0.250s
        assert abs(f.request_rate - 120.5) < 1e-2

    def test_missing_compute_metrics_default_zero(self):
        f = extract_features("X", "svc", "warning", "compute", {}, "")
        assert f.error_rate == 0.0
        assert f.latency_p99 == 0.0
        assert f.request_rate == 0.0

    def test_cpu_and_memory(self):
        metrics = {"cpu_usage_pct": "71.0", "memory_usage_pct": "55.0"}
        f = extract_features("X", "svc", "warning", "compute", metrics, "")
        assert abs(f.cpu_usage - 0.71) < 1e-6
        assert abs(f.memory_usage - 0.55) < 1e-6


# ═══════════════════════════════════════════════════════════════════════════════
# Storage feature extraction
# ═══════════════════════════════════════════════════════════════════════════════


class TestExtractStorageFeatures:
    def test_nested_raw_dict(self):
        metrics = {
            "raw": {
                "osd_status": [
                    {"metric": {}, "value": [0, "1"]},
                    {"metric": {}, "value": [0, "1"]},
                    {"metric": {}, "value": [0, "0"]},
                ],
                "pool_fill_pct": [{"metric": {}, "value": [0, "0.82"]}],
                "cluster_health": [{"metric": {}, "value": [0, "1"]}],
                "degraded_pgs": [{"metric": {}, "value": [0, "25"]}],
                "io_latency_ms": [{"metric": {}, "value": [0, "45"]}],
            },
            "summary": "Storage Metrics Snapshot",
        }
        f = extract_features("CephOSDDown", "cluster", "critical", "storage", metrics, "")
        assert f.osd_up_count == 2
        assert f.osd_total_count == 3
        assert abs(f.pool_usage_pct - 0.82) < 1e-6
        assert f.cluster_health_score == 1
        assert f.degraded_pgs == 25
        assert abs(f.io_latency - 0.045) < 1e-4  # 45ms → 0.045s

    def test_flat_dict_fallback(self):
        metrics = {
            "osd_status": [
                {"metric": {}, "value": [0, "1"]},
            ],
            "pool_fill_pct": [{"metric": {}, "value": [0, "0.5"]}],
        }
        f = extract_features("X", "svc", "warning", "storage", metrics, "")
        assert f.osd_up_count == 1
        assert f.osd_total_count == 1
        assert abs(f.pool_usage_pct - 0.5) < 1e-6

    def test_pvc_iops(self):
        metrics = {
            "raw": {
                "pvc_iops_read": [{"metric": {}, "value": [0, "200"]}],
                "pvc_iops_write": [{"metric": {}, "value": [0, "300"]}],
            }
        }
        f = extract_features("X", "svc", "warning", "storage", metrics, "")
        assert abs(f.pvc_iops - 500.0) < 1e-6


# ═══════════════════════════════════════════════════════════════════════════════
# Log signal extraction
# ═══════════════════════════════════════════════════════════════════════════════


class TestLogSignals:
    def test_error_and_warning_counts(self):
        logs = "ERROR something\nWARN something\nERROR again\nWARN x\nERROR y"
        f = extract_features("X", "svc", "warning", "compute", {}, logs)
        assert f.recent_error_count == 3
        assert f.recent_warning_count == 2

    def test_anomaly_detected_warning_severity(self):
        # For warning severity, anomaly threshold is 5
        logs = "ERROR\n" * 5
        f = extract_features("X", "svc", "warning", "compute", {}, logs)
        assert f.log_anomaly_detected is True

    def test_no_anomaly_below_threshold_warning(self):
        logs = "ERROR\n" * 4
        f = extract_features("X", "svc", "warning", "compute", {}, logs)
        assert f.log_anomaly_detected is False

    def test_anomaly_detected_critical_lower_threshold(self):
        # For critical severity, anomaly threshold is 2
        logs = "ERROR\nERROR\n"
        f = extract_features("X", "svc", "critical", "compute", {}, logs)
        assert f.log_anomaly_detected is True

    def test_empty_logs(self):
        f = extract_features("X", "svc", "warning", "compute", {}, "")
        assert f.recent_error_count == 0
        assert f.log_anomaly_detected is False

    def test_unknown_domain(self):
        f = extract_features("X", "svc", "warning", "unknown", {}, "")
        assert f.domain == "unknown"
        # Should not crash
        assert f.error_rate == 0.0
