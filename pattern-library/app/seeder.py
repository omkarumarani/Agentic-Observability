"""Pattern Library — startup seeder.

Inserts the first 5 foundational failure patterns if the library is empty.
Each pattern includes:
  - Full metadata (severity, impacted layers, recurrence, oss angle)
  - 3–5 detection signals (metric thresholds, log patterns, alerts)
  - 2–3 fixes (ordered from safest to most invasive)

Seeding is idempotent: patterns are matched by name and skipped if found.
Embeddings are generated lazily (Ollama may be offline at startup).
"""
import json
import logging
import uuid

from .db import get_pool
from .embedder import embed_text, vec_to_str

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Foundational pattern definitions
# ─────────────────────────────────────────────────────────────

SEED_PATTERNS = [
    # ──────────────────────────────────────────────────────────
    # Pattern 1: CPU Throttling Misidentified as Saturation
    # Root cause: resource limits too low — node is NOT overloaded
    # ──────────────────────────────────────────────────────────
    {
        "name": "cpu_throttling_resource_limits",
        "description": (
            "Container CPU is being throttled by Kubernetes resource limits, causing latency "
            "spikes that look identical to true CPU saturation — but the node is not overloaded. "
            "Triggered when limits are copied from other services without profiling. The CPU "
            "usage gauge stays below the limit but CFS throttling adds unpredictable micro-pauses, "
            "inflating p99 latency while p50 remains normal."
        ),
        "environment": "kubernetes",
        "impacted_layers": ["app", "infra"],
        "severity": "high",
        "recurrence_score": 0.85,
        "confidence": 0.80,
        "automation_readiness": "risky",
        "oss_contribution_angle": (
            "Contribute a Prometheus recording rule that computes throttle_ratio = "
            "container_cpu_cfs_throttled_seconds_total / container_cpu_cfs_periods_total "
            "and an alert that fires only when throttle_ratio > 0.25 while cpu_usage < 0.80, "
            "distinguishing true saturation from limit-induced throttling."
        ),
        "source_references": [
            {
                "title": "Kubernetes: Assign CPU Resources to Containers",
                "url": "https://kubernetes.io/docs/tasks/configure-pod-container/assign-cpu-resource/",
                "source": "docs",
            },
            {
                "title": "CFS bandwidth throttling explainer — Cindy Sridharan",
                "url": "https://medium.com/over-engineering/cpu-cfs-throttling-and-kubernetes-3a85a2429073",
                "source": "blog",
            },
            {
                "title": "OpenTelemetry Collector: container CPU metrics",
                "url": "https://opentelemetry.io/docs/specs/semconv/system/container-metrics/",
                "source": "docs",
            },
        ],
        "signals": [
            {
                "signal_type": "metric",
                "name": "container_cpu_cfs_throttled_ratio",
                "description": "Fraction of CPU scheduling periods where the container was throttled. > 0.25 indicates significant throttling.",
                "query_template": "rate(container_cpu_cfs_throttled_seconds_total{container!=''}[5m]) / rate(container_cpu_cfs_periods_total{container!=''}[5m])",
                "threshold_operator": ">",
                "threshold_value": 0.25,
                "severity": "high",
                "weight": 1.8,
            },
            {
                "signal_type": "metric",
                "name": "cpu_usage",
                "description": "Actual CPU usage fraction. Low usage (< 0.80) combined with high throttle confirms this pattern vs true saturation.",
                "query_template": "rate(container_cpu_usage_seconds_total[5m]) / on(pod,container) kube_pod_container_resource_limits{resource='cpu'}",
                "threshold_operator": "<",
                "threshold_value": 0.80,
                "severity": "medium",
                "weight": 1.2,
            },
            {
                "signal_type": "metric",
                "name": "latency_p99",
                "description": "p99 request latency in seconds. Throttling inflates p99 while p50 stays healthy.",
                "query_template": "histogram_quantile(0.99, rate(http_server_request_duration_seconds_bucket[5m]))",
                "threshold_operator": ">=",
                "threshold_value": 1.0,
                "severity": "high",
                "weight": 1.0,
            },
            {
                "signal_type": "log",
                "name": "context_deadline_exceeded",
                "description": "gRPC/HTTP 'context deadline exceeded' errors caused by CPU micro-pauses blocking goroutines.",
                "query_template": "{service_name=~\".+\"} |= \"context deadline exceeded\"",
                "threshold_operator": None,
                "threshold_value": None,
                "severity": "medium",
                "weight": 0.8,
            },
        ],
        "fixes": [
            {
                "title": "Increase CPU limit with 2× headroom",
                "description": "Double the current CPU limit to eliminate throttling. Measure actual p99 CPU first with kubectl top.",
                "fix_type": "config_change",
                "automation_level": "approval_required",
                "risk_level": "medium",
                "estimated_mttr_seconds": 180,
                "requires_restart": True,
                "content": "kubectl set resources deployment/<service> --limits=cpu=2000m --requests=cpu=1000m",
            },
            {
                "title": "Add throttle_ratio Prometheus alert",
                "description": "Replace the generic HighCPU alert with a throttle-aware alert that distinguishes throttling from saturation.",
                "fix_type": "alert_rule",
                "automation_level": "autonomous",
                "risk_level": "low",
                "estimated_mttr_seconds": 60,
                "requires_restart": False,
                "content": (
                    "- alert: CPUThrottlingHigh\n"
                    "  expr: |\n"
                    "    rate(container_cpu_cfs_throttled_seconds_total[5m])\n"
                    "    / rate(container_cpu_cfs_periods_total[5m]) > 0.25\n"
                    "  for: 5m\n"
                    "  labels: { severity: warning }\n"
                    "  annotations:\n"
                    "    summary: 'CPU throttling >25% on {{ $labels.container }}'\n"
                    "    runbook: 'Increase CPU limit — do NOT treat as saturation'"
                ),
            },
            {
                "title": "Profile with py-spy / async-profiler to find real ceiling",
                "description": "Before raising limits blindly, profile to confirm whether the limit is misconfigured or the app genuinely needs more CPU.",
                "fix_type": "runbook",
                "automation_level": "manual",
                "risk_level": "low",
                "estimated_mttr_seconds": 1800,
                "requires_restart": False,
                "content": (
                    "# Python services\n"
                    "kubectl exec -it <pod> -- py-spy top --pid 1\n\n"
                    "# JVM services\n"
                    "kubectl exec -it <pod> -- jcmd 1 VM.native_memory summary"
                ),
            },
        ],
    },

    # ──────────────────────────────────────────────────────────
    # Pattern 2: OTel Collector Pipeline Backpressure
    # Root cause: exporter queue saturated → spans/metrics dropped silently
    # ──────────────────────────────────────────────────────────
    {
        "name": "collector_pipeline_backpressure",
        "description": (
            "The OpenTelemetry Collector exporter queue is saturated — spans and metrics are "
            "being silently dropped at the send queue layer. The collector appears healthy "
            "(CPU and memory within limits) but its internal queue metrics show overflow. "
            "Manifests as missing trace spans in Tempo and unexplained metric gaps in Prometheus. "
            "Triggered by traffic spikes, slow downstream backends, or insufficient queue sizing."
        ),
        "environment": "any",
        "impacted_layers": ["collector", "infra"],
        "severity": "high",
        "recurrence_score": 0.78,
        "confidence": 0.82,
        "automation_readiness": "manual",
        "oss_contribution_angle": (
            "Contribute a Grafana dashboard panel group for collector health: queue utilisation "
            "gauge (queue_size / queue_capacity), send failure rate, and drop rate by exporter. "
            "Submit to grafana.com/grafana/dashboards as 'OTel Collector Health'."
        ),
        "source_references": [
            {
                "title": "OpenTelemetry Collector: configuring exporters and queues",
                "url": "https://opentelemetry.io/docs/collector/configuration/#exporters",
                "source": "docs",
            },
            {
                "title": "OTel Collector GitHub: exporter queue_size settings",
                "url": "https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md",
                "source": "github",
            },
        ],
        "signals": [
            {
                "signal_type": "metric",
                "name": "otelcol_exporter_queue_size",
                "description": "Current exporter queue depth. Approaching queue_capacity means imminent drops.",
                "query_template": "otelcol_exporter_queue_size / otelcol_exporter_queue_capacity",
                "threshold_operator": ">",
                "threshold_value": 0.80,
                "severity": "high",
                "weight": 2.0,
            },
            {
                "signal_type": "metric",
                "name": "otelcol_exporter_send_failed_spans_total",
                "description": "Rate of spans that failed to send. Non-zero means active data loss.",
                "query_template": "rate(otelcol_exporter_send_failed_spans_total[5m])",
                "threshold_operator": ">",
                "threshold_value": 0.0,
                "severity": "critical",
                "weight": 1.8,
            },
            {
                "signal_type": "metric",
                "name": "otelcol_processor_dropped_spans_total",
                "description": "Spans dropped by processors (batch overflow). Always 0 in healthy state.",
                "query_template": "rate(otelcol_processor_dropped_spans_total[5m])",
                "threshold_operator": ">",
                "threshold_value": 0.0,
                "severity": "high",
                "weight": 1.5,
            },
            {
                "signal_type": "log",
                "name": "sending_queue_is_full",
                "description": "Collector log line confirming queue overflow and active dropping.",
                "query_template": "{service_name=\"otel-collector\"} |= \"sending_queue is full\"",
                "threshold_operator": None,
                "threshold_value": None,
                "severity": "critical",
                "weight": 1.8,
            },
            {
                "signal_type": "trace",
                "name": "missing_spans_in_tempo",
                "description": "Traces visible in application but incomplete/absent in Tempo — confirms drop at collector.",
                "query_template": "{ span.status.code = ERROR } | count() by (service.name)",
                "threshold_operator": None,
                "threshold_value": None,
                "severity": "medium",
                "weight": 1.0,
            },
        ],
        "fixes": [
            {
                "title": "Increase exporter queue_size",
                "description": "Raise queue_size to buffer traffic spikes. Start at 5000; tune based on peak queue_size metric.",
                "fix_type": "config_change",
                "automation_level": "manual",
                "risk_level": "low",
                "estimated_mttr_seconds": 60,
                "requires_restart": True,
                "content": (
                    "# otel-collector/config.yaml — exporters section\n"
                    "exporters:\n"
                    "  otlp/tempo:\n"
                    "    endpoint: tempo:4317\n"
                    "    sending_queue:\n"
                    "      enabled: true\n"
                    "      num_consumers: 10\n"
                    "      queue_size: 5000\n"
                    "    retry_on_failure:\n"
                    "      enabled: true\n"
                    "      initial_interval: 5s\n"
                    "      max_interval: 30s"
                ),
            },
            {
                "title": "Tune batch processor to reduce send frequency",
                "description": "Larger batches reduce send overhead and queue pressure during spikes.",
                "fix_type": "config_change",
                "automation_level": "manual",
                "risk_level": "low",
                "estimated_mttr_seconds": 60,
                "requires_restart": True,
                "content": (
                    "# otel-collector/config.yaml — processors section\n"
                    "processors:\n"
                    "  batch/traces:\n"
                    "    send_batch_size: 512\n"
                    "    timeout: 5s\n"
                    "  batch/metrics:\n"
                    "    send_batch_size: 1000\n"
                    "    timeout: 10s"
                ),
            },
            {
                "title": "Scale collector horizontally or add load-balanced fanout",
                "description": "If queue tuning is insufficient, run 2+ collector replicas behind the app's OTLP endpoint.",
                "fix_type": "scale",
                "automation_level": "approval_required",
                "risk_level": "medium",
                "estimated_mttr_seconds": 300,
                "requires_restart": False,
                "content": (
                    "# docker-compose override for horizontal scaling\n"
                    "services:\n"
                    "  otel-collector:\n"
                    "    deploy:\n"
                    "      replicas: 2\n"
                    "      resources:\n"
                    "        limits: { cpus: '1.0', memory: '512M' }"
                ),
            },
        ],
    },

    # ──────────────────────────────────────────────────────────
    # Pattern 3: Prometheus Scrape Miss — Metric Disappears
    # Root cause: scrape timeout too short or target CPU-throttled at scrape time
    # ──────────────────────────────────────────────────────────
    {
        "name": "prometheus_scrape_miss",
        "description": (
            "Prometheus intermittently fails to scrape a target, causing metric series to "
            "disappear from dashboards and triggering spurious 'no data' alerts. The service "
            "is healthy but the scrape times out or returns an empty response. Common when "
            "the target container is CPU-throttled exactly during the scrape window, or when "
            "the /metrics endpoint generates high-cardinality output that takes > scrape_timeout."
        ),
        "environment": "any",
        "impacted_layers": ["infra", "collector"],
        "severity": "medium",
        "recurrence_score": 0.72,
        "confidence": 0.75,
        "automation_readiness": "safe",
        "oss_contribution_angle": (
            "Submit a Prometheus alert rule for 'scrape_miss_ratio' that computes "
            "1 - avg_over_time(up[5m]) per target and fires when > 0.1 (>10% miss rate) "
            "with distinct labels for the affected job, avoiding alert fatigue from single misses."
        ),
        "source_references": [
            {
                "title": "Prometheus scrape_config reference",
                "url": "https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config",
                "source": "docs",
            },
            {
                "title": "Prometheus: up metric and target health",
                "url": "https://prometheus.io/docs/concepts/jobs_instances/#automatically-generated-labels-and-time-series",
                "source": "docs",
            },
        ],
        "signals": [
            {
                "signal_type": "metric",
                "name": "up",
                "description": "Prometheus synthetic metric: 1 when last scrape succeeded, 0 when it failed.",
                "query_template": "up{job=\"app-metrics\"}",
                "threshold_operator": "<",
                "threshold_value": 1.0,
                "severity": "high",
                "weight": 2.0,
            },
            {
                "signal_type": "metric",
                "name": "scrape_duration_seconds",
                "description": "Time taken for last scrape. Approaching scrape_timeout indicates risk of miss.",
                "query_template": "scrape_duration_seconds{job=\"app-metrics\"}",
                "threshold_operator": ">",
                "threshold_value": 8.0,
                "severity": "medium",
                "weight": 1.5,
            },
            {
                "signal_type": "metric",
                "name": "scrape_samples_scraped",
                "description": "Number of samples scraped. A drop to 0 despite target being up confirms partial failure.",
                "query_template": "scrape_samples_scraped{job=\"app-metrics\"}",
                "threshold_operator": "<",
                "threshold_value": 1.0,
                "severity": "high",
                "weight": 1.5,
            },
            {
                "signal_type": "log",
                "name": "scrape_timeout_log",
                "description": "Prometheus log message confirming scrape timeout.",
                "query_template": "{service_name=\"prometheus\"} |= \"context deadline exceeded\" |= \"scrape\"",
                "threshold_operator": None,
                "threshold_value": None,
                "severity": "medium",
                "weight": 1.2,
            },
        ],
        "fixes": [
            {
                "title": "Increase scrape_timeout in prometheus.yml",
                "description": "Raise scrape_timeout from default 10s to 30s. Must not exceed scrape_interval.",
                "fix_type": "config_change",
                "automation_level": "autonomous",
                "risk_level": "low",
                "estimated_mttr_seconds": 30,
                "requires_restart": False,
                "content": (
                    "# prometheus/prometheus.yml — global or per-job\n"
                    "global:\n"
                    "  scrape_interval: 30s\n"
                    "  scrape_timeout: 25s  # must be < scrape_interval\n\n"
                    "scrape_configs:\n"
                    "  - job_name: app-metrics\n"
                    "    scrape_timeout: 25s\n"
                    "    static_configs:\n"
                    "      - targets: ['otel-collector:8889']"
                ),
            },
            {
                "title": "Reduce metric cardinality at OTel Collector",
                "description": "Trim high-cardinality labels (request_id, user_id) in the collector transform processor to reduce /metrics payload size.",
                "fix_type": "config_change",
                "automation_level": "manual",
                "risk_level": "low",
                "estimated_mttr_seconds": 300,
                "requires_restart": True,
                "content": (
                    "# otel-collector/config.yaml — processors section\n"
                    "processors:\n"
                    "  attributes/drop_high_cardinality:\n"
                    "    actions:\n"
                    "      - key: request_id\n"
                    "        action: delete\n"
                    "      - key: user_id\n"
                    "        action: delete"
                ),
            },
        ],
    },

    # ──────────────────────────────────────────────────────────
    # Pattern 4: OOMKilled Resource Misconfiguration
    # Root cause: memory limit too low → container killed silently
    # ──────────────────────────────────────────────────────────
    {
        "name": "oom_kill_resource_misconfiguration",
        "description": (
            "Container is repeatedly OOMKilled because the memory limit is set below the "
            "service's actual working set peak. The service restarts cleanly and memory "
            "usage looks normal on short windows (fresh containers start with low RSS). "
            "Restart counter climbs silently; in-flight requests fail at the moment of kill. "
            "Especially common for JVM services where heap is configured without headroom "
            "for JVM overhead, code cache, and thread stacks."
        ),
        "environment": "kubernetes",
        "impacted_layers": ["app", "infra"],
        "severity": "critical",
        "recurrence_score": 0.80,
        "confidence": 0.85,
        "automation_readiness": "risky",
        "oss_contribution_angle": (
            "Contribute a Kubernetes resource recommendation script that reads "
            "container_memory_working_set_bytes p99 over 7 days and outputs recommended "
            "limits with 30% headroom. Package as a kubectl plugin 'kubectl recommend-resources'."
        ),
        "source_references": [
            {
                "title": "Kubernetes: OOMKilled — Out of Memory Management",
                "url": "https://kubernetes.io/docs/tasks/configure-pod-container/assign-memory-resource/",
                "source": "docs",
            },
            {
                "title": "JVM memory in containers — Evan Shortiss",
                "url": "https://developers.redhat.com/articles/2022/04/19/best-practices-java-memory-management-containers",
                "source": "blog",
            },
        ],
        "signals": [
            {
                "signal_type": "metric",
                "name": "kube_pod_container_status_restarts_total",
                "description": "Restart counter delta over 1h. Persistent restarts combined with memory pressure confirm OOM.",
                "query_template": "increase(kube_pod_container_status_restarts_total[1h])",
                "threshold_operator": ">",
                "threshold_value": 2.0,
                "severity": "critical",
                "weight": 2.0,
            },
            {
                "signal_type": "metric",
                "name": "memory_working_set_ratio",
                "description": "Memory working set as fraction of limit. > 0.90 means imminent OOM.",
                "query_template": "container_memory_working_set_bytes / on(pod,container) kube_pod_container_resource_limits{resource='memory'}",
                "threshold_operator": ">",
                "threshold_value": 0.90,
                "severity": "critical",
                "weight": 2.0,
            },
            {
                "signal_type": "log",
                "name": "oomkilled_event",
                "description": "Kubernetes pod event containing OOMKilled reason.",
                "query_template": "{service_name=~\".+\"} |= \"OOMKilled\"",
                "threshold_operator": None,
                "threshold_value": None,
                "severity": "critical",
                "weight": 2.0,
            },
            {
                "signal_type": "log",
                "name": "java_oom_error",
                "description": "JVM OutOfMemoryError: heap limit reached before GC can reclaim.",
                "query_template": "{service_name=~\".+\"} |= \"java.lang.OutOfMemoryError\"",
                "threshold_operator": None,
                "threshold_value": None,
                "severity": "critical",
                "weight": 1.5,
            },
        ],
        "fixes": [
            {
                "title": "Increase memory limit with 30% headroom above p99 peak",
                "description": "Query p99 working set over 7 days, add 30% headroom, apply as new limit.",
                "fix_type": "config_change",
                "automation_level": "approval_required",
                "risk_level": "medium",
                "estimated_mttr_seconds": 300,
                "requires_restart": True,
                "content": (
                    "# Step 1: measure p99 peak (7-day window)\n"
                    "# PromQL: quantile_over_time(0.99, container_memory_working_set_bytes{container='<svc>'}[7d])\n\n"
                    "# Step 2: apply with 30% headroom\n"
                    "kubectl set resources deployment/<service> \\\n"
                    "  --limits=memory=<p99_bytes * 1.3>Mi \\\n"
                    "  --requests=memory=<p99_bytes * 0.8>Mi"
                ),
            },
            {
                "title": "Configure JVM heap at 75% of container limit",
                "description": "JVM requires headroom beyond -Xmx for metaspace, thread stacks, and code cache (~25%).",
                "fix_type": "config_change",
                "automation_level": "approval_required",
                "risk_level": "medium",
                "estimated_mttr_seconds": 180,
                "requires_restart": True,
                "content": (
                    "# Example: container limit 1Gi → Xmx 768m\n"
                    "env:\n"
                    "  - name: JAVA_OPTS\n"
                    "    value: \"-Xmx768m -Xms512m -XX:+UseContainerSupport\"\n\n"
                    "# Or via JVM ergonomics (Java 11+):\n"
                    "# -XX:MaxRAMPercentage=75.0 (auto-calculates from container limit)"
                ),
            },
            {
                "title": "Add memory saturation alert before OOM occurs",
                "description": "Alert at 85% working set / limit to give SREs time to act before the kill.",
                "fix_type": "alert_rule",
                "automation_level": "autonomous",
                "risk_level": "low",
                "estimated_mttr_seconds": 30,
                "requires_restart": False,
                "content": (
                    "- alert: ContainerMemoryNearLimit\n"
                    "  expr: |\n"
                    "    container_memory_working_set_bytes\n"
                    "    / on(pod,container) kube_pod_container_resource_limits{resource='memory'} > 0.85\n"
                    "  for: 5m\n"
                    "  labels: { severity: warning }\n"
                    "  annotations:\n"
                    "    summary: 'Memory at {{ $value | humanizePercentage }} of limit on {{ $labels.container }}'"
                ),
            },
        ],
    },

    # ──────────────────────────────────────────────────────────
    # Pattern 5: Misleading P99 from Histogram Bucket Saturation
    # Root cause: histogram buckets too coarse for actual latency range
    # ──────────────────────────────────────────────────────────
    {
        "name": "misleading_p99_histogram_bucket_saturation",
        "description": (
            "A Prometheus histogram_quantile(0.99, ...) alert fires but the service is healthy. "
            "The root cause is that all observed request durations fall into the highest-defined "
            "histogram bucket ('le=+Inf' or 'le=10.0'), and quantile interpolation returns the "
            "bucket boundary as the p99 estimate — not the actual latency. Common after service "
            "migrations that change the latency profile, or when histogram buckets are copy-pasted "
            "from a different service with a different order of magnitude."
        ),
        "environment": "any",
        "impacted_layers": ["app", "infra"],
        "severity": "low",
        "recurrence_score": 0.68,
        "confidence": 0.78,
        "automation_readiness": "safe",
        "oss_contribution_angle": (
            "Contribute a PromQL lint rule to the OpenTelemetry semantic-conventions repo "
            "that detects when >95% of histogram observations fall in the highest bucket "
            "and emits a warning metric 'histogram_bucket_saturation_ratio' — enabling a "
            "Grafana panel that flags misconfigured histograms before they create alert noise."
        ),
        "source_references": [
            {
                "title": "Prometheus: histograms and summaries",
                "url": "https://prometheus.io/docs/practices/histograms/",
                "source": "docs",
            },
            {
                "title": "OpenTelemetry SDK: explicit histogram bucket configuration",
                "url": "https://opentelemetry.io/docs/specs/otel/metrics/sdk/#explicit-bucket-histogram-aggregation",
                "source": "docs",
            },
            {
                "title": "Grafana blog: how to pick histogram buckets",
                "url": "https://grafana.com/blog/2021/01/01/how-to-pick-histogram-buckets/",
                "source": "blog",
            },
        ],
        "signals": [
            {
                "signal_type": "metric",
                "name": "histogram_top_bucket_saturation",
                "description": "Fraction of requests in the highest bucket. If = 1.0 then p99 is always the bucket boundary, not real latency.",
                "query_template": (
                    "rate(http_server_request_duration_seconds_bucket{le='10.0'}[5m])\n"
                    "/ rate(http_server_request_duration_seconds_count[5m])"
                ),
                "threshold_operator": ">",
                "threshold_value": 0.95,
                "severity": "medium",
                "weight": 2.0,
            },
            {
                "signal_type": "metric",
                "name": "latency_p99",
                "description": "P99 latency — should be cross-checked against p50. P99≫p50 with no error spike is the signature.",
                "query_template": "histogram_quantile(0.99, rate(http_server_request_duration_seconds_bucket[5m]))",
                "threshold_operator": ">=",
                "threshold_value": 1.0,
                "severity": "medium",
                "weight": 1.0,
            },
            {
                "signal_type": "metric",
                "name": "error_rate",
                "description": "HTTP 5xx error rate. Near-zero error rate during a 'high latency' alert suggests the alert is misleading.",
                "query_template": "rate(http_server_requests_total{status=~'5..'}[5m]) / rate(http_server_requests_total[5m])",
                "threshold_operator": "<",
                "threshold_value": 0.01,
                "severity": "low",
                "weight": 1.5,
            },
            {
                "signal_type": "alert",
                "name": "HighP99WithoutErrors",
                "description": "HighLatency alert fires simultaneously with no HighErrorRate — classic misleading histogram symptom.",
                "query_template": None,
                "threshold_operator": None,
                "threshold_value": None,
                "severity": "low",
                "weight": 1.2,
            },
        ],
        "fixes": [
            {
                "title": "Reconfigure OTel SDK histogram buckets to cover actual latency range",
                "description": "Set explicit_histogram_buckets to cover p1–p99.9 of measured latency. Measure current range first with min/max_over_time.",
                "fix_type": "config_change",
                "automation_level": "autonomous",
                "risk_level": "low",
                "estimated_mttr_seconds": 120,
                "requires_restart": True,
                "content": (
                    "# Python OTel SDK — configure explicit buckets\n"
                    "from opentelemetry.sdk.metrics import MeterProvider\n"
                    "from opentelemetry.sdk.metrics.view import View\n"
                    "from opentelemetry.sdk.metrics.aggregation import ExplicitBucketHistogramAggregation\n\n"
                    "view = View(\n"
                    "    instrument_name='http.server.request.duration',\n"
                    "    aggregation=ExplicitBucketHistogramAggregation(\n"
                    "        boundaries=[0.001, 0.005, 0.01, 0.025, 0.05,\n"
                    "                    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]\n"
                    "    ),\n"
                    ")"
                ),
            },
            {
                "title": "Add bucket saturation PromQL recording rule",
                "description": "Track top-bucket utilisation as a dedicated metric to detect bucket misconfiguration automatically.",
                "fix_type": "alert_rule",
                "automation_level": "autonomous",
                "risk_level": "low",
                "estimated_mttr_seconds": 30,
                "requires_restart": False,
                "content": (
                    "# prometheus/alert-rules.yml\n"
                    "groups:\n"
                    "  - name: histogram_health\n"
                    "    rules:\n"
                    "      - record: job:histogram_top_bucket_saturation:ratio5m\n"
                    "        expr: |\n"
                    "          rate(http_server_request_duration_seconds_bucket{le='+Inf'}[5m])\n"
                    "          / rate(http_server_request_duration_seconds_count[5m])\n"
                    "      - alert: HistogramBucketSaturated\n"
                    "        expr: job:histogram_top_bucket_saturation:ratio5m > 0.95\n"
                    "        for: 10m\n"
                    "        labels: { severity: warning }\n"
                    "        annotations:\n"
                    "          summary: 'Histogram buckets too coarse — p99 is unreliable for {{ $labels.job }}'"
                ),
            },
        ],
    },
]


# ─────────────────────────────────────────────────────────────
# Seeder logic
# ─────────────────────────────────────────────────────────────

async def seed_patterns(force: bool = False) -> int:
    """
    Insert foundational patterns if the library is empty (or force=True).
    Returns the number of patterns inserted.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM patterns")
        if count > 0 and not force:
            logger.info("Pattern library already has %d patterns — skipping seed", count)
            return 0

    inserted = 0
    for p in SEED_PATTERNS:
        try:
            inserted += await _insert_pattern(p)
        except Exception as exc:
            logger.error("Failed to seed pattern '%s': %s", p["name"], exc)

    logger.info("Seeder complete — %d patterns inserted", inserted)
    return inserted


async def _insert_pattern(p: dict) -> int:
    """Insert one pattern with its signals and fixes. Returns 1 if inserted, 0 if skipped."""
    pool = get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM patterns WHERE name = $1", p["name"]
        )
        if existing:
            logger.debug("Pattern '%s' already exists — skipping", p["name"])
            return 0

        # Generate embedding (may be None if Ollama is offline)
        embed_text_str = f"{p['name']} {p['description']}"
        embedding = await embed_text(embed_text_str)
        embed_val = vec_to_str(embedding) if embedding else None

        pattern_id = await conn.fetchval(
            """
            INSERT INTO patterns (
                name, description, environment, impacted_layers,
                recurrence_score, severity, automation_readiness,
                oss_contribution_angle, source_references,
                confidence, embedding
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::vector)
            RETURNING id
            """,
            p["name"],
            p["description"],
            p.get("environment", "any"),
            p.get("impacted_layers", []),
            p.get("recurrence_score", 0.5),
            p.get("severity", "medium"),
            p.get("automation_readiness", "manual"),
            p.get("oss_contribution_angle"),
            json.dumps(p.get("source_references", [])),
            p.get("confidence", 0.5),
            embed_val,
        )

        for sig in p.get("signals", []):
            await conn.execute(
                """
                INSERT INTO pattern_signals (
                    pattern_id, signal_type, name, description,
                    query_template, threshold_operator, threshold_value, severity, weight
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                pattern_id,
                sig["signal_type"],
                sig["name"],
                sig.get("description"),
                sig.get("query_template"),
                sig.get("threshold_operator"),
                sig.get("threshold_value"),
                sig.get("severity", "medium"),
                sig.get("weight", 1.0),
            )

        for fix in p.get("fixes", []):
            await conn.execute(
                """
                INSERT INTO pattern_fixes (
                    pattern_id, title, description, fix_type,
                    automation_level, content, risk_level,
                    estimated_mttr_seconds, requires_restart
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                pattern_id,
                fix["title"],
                fix.get("description"),
                fix["fix_type"],
                fix.get("automation_level", "manual"),
                fix.get("content"),
                fix.get("risk_level", "medium"),
                fix.get("estimated_mttr_seconds"),
                fix.get("requires_restart", False),
            )

        logger.info(
            "Seeded pattern '%s' (id=%s, signals=%d, fixes=%d)",
            p["name"],
            pattern_id,
            len(p.get("signals", [])),
            len(p.get("fixes", [])),
        )
        return 1
