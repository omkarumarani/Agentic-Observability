#!/usr/bin/env python3
"""
troublemaker/troublemaker.py
-----------------------------
Chaos / traffic generator for the Observability Learning lab.

Runs continuously, sleeping MIN_SLEEP_SECONDS–MAX_SLEEP_SECONDS between
rounds.  Each round picks one scenario by weighted random selection and fires
HTTP requests at the frontend-api.

The traffic creates rich, realistic telemetry across all three signal types:
  Prometheus metrics  — request counters, latency histograms, error rates
  Loki logs           — structured lines with trace_id correlation
  Tempo traces        — distributed multi-service span waterfalls

Scenarios
---------
  steady_normal     Stable healthy baseline traffic.
  burst_traffic     Sudden spike of rapid-fire requests.
  latency_spike     Flood the slow endpoints to push p95/p99 latency up.
  error_spike       Flood /error so the error rate metric spikes.
  backend_failure   Flood /backend-error to create 3-span ERROR waterfalls.
  slow_backend      Flood /backend-slow to show downstream latency attribution.
  mixed_chaos       Random mix of all endpoints — noisy realistic baseline.
  slow_burn         Alternating normal/slow rounds to show gradual drift.

Configuration (environment variables — all optional)
-----------------------------------------------------
  FRONTEND_BASE_URL      default: http://frontend-api:8080
  MIN_SLEEP_SECONDS      default: 5
  MAX_SLEEP_SECONDS      default: 25
  BURST_MIN              default: 20
  BURST_MAX              default: 50
  STEADY_REQUESTS        default: 10
  REQUEST_TIMEOUT        default: 15   (seconds per request)
  ENABLE_BURSTS          default: true
  ENABLE_ERRORS          default: true
  ENABLE_SLOW_CALLS      default: true
  SCHEDULE_CSV_PATH      default: /data/scenario_schedule.csv
  HTTP_PORT              default: 8088   (GET /scenarios schedule endpoint)
"""

import csv
import http.server
import json
import logging
import os
import random
import socketserver
import threading
import time
from dataclasses import dataclass
from typing import Callable

import requests

# ─────────────────────────────────────────────────────────────────────────────
# Logging — one human-readable line per event
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("troublemaker")


# ─────────────────────────────────────────────────────────────────────────────
# Configuration helpers
# ─────────────────────────────────────────────────────────────────────────────
def _bool_env(name: str, default: str = "true") -> bool:
    """Return True unless the env var is explicitly set to a falsy value."""
    return os.getenv(name, default).strip().lower() not in ("false", "0", "no", "off")


BASE_URL         = os.getenv("FRONTEND_BASE_URL",  "http://frontend-api:8080").rstrip("/")
STORAGE_SIM_URL  = os.getenv("STORAGE_SIMULATOR_URL", "http://storage-simulator:9200").rstrip("/")
MIN_SLEEP        = float(os.getenv("MIN_SLEEP_SECONDS",  "5"))
MAX_SLEEP        = float(os.getenv("MAX_SLEEP_SECONDS", "25"))
BURST_MIN        = int(os.getenv("BURST_MIN",          "20"))
BURST_MAX        = int(os.getenv("BURST_MAX",          "50"))
STEADY_REQUESTS  = int(os.getenv("STEADY_REQUESTS",    "10"))
REQUEST_TIMEOUT  = float(os.getenv("REQUEST_TIMEOUT",  "15"))
ENABLE_BURSTS    = _bool_env("ENABLE_BURSTS")
ENABLE_ERRORS    = _bool_env("ENABLE_ERRORS")
ENABLE_SLOW      = _bool_env("ENABLE_SLOW_CALLS")
CSV_PATH         = os.getenv("SCHEDULE_CSV_PATH", "/data/scenario_schedule.csv")
HTTP_PORT        = int(os.getenv("HTTP_PORT", "8088"))


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint catalogue
# These correspond to the routes defined in frontend-api/app/main.py.
# ─────────────────────────────────────────────────────────────────────────────
ENDPOINTS_NORMAL = ["/ok", "/backend-ok"]
ENDPOINTS_SLOW   = ["/slow", "/backend-slow"]
ENDPOINTS_ERROR  = ["/error", "/backend-error"]
ENDPOINTS_ALL    = ENDPOINTS_NORMAL + ENDPOINTS_SLOW + ENDPOINTS_ERROR
ENDPOINTS_SICK   = ["/sick", "/sick-partial", "/backend-sick", "/backend-sick-partial"]


# ─────────────────────────────────────────────────────────────────────────────
# Scenario Schedule — CSV persistence
#
# Every completed scenario run is appended as one row to CSV_PATH.
# The file is created with a header row on first startup.
# Columns: timestamp_start, timestamp_end, scenario, endpoints,
#          request_count, burst_size, ok, err, avg_ms, max_ms, notes
#
# How to use this for Grafana correlation
# ───────────────────────────────────────
# 1. Note the timestamp_start of a scenario (e.g. burst_traffic at 14:03:22).
# 2. Open Grafana and set the time range to [timestamp_start - 30s, timestamp_end + 60s].
# 3. The Prometheus Request Rate panel will show the spike aligned to that window.
# 4. Switch to the Loki Recent Logs panel — the dense cluster of log lines will
#    appear at exactly the same timestamp.
# 5. Open Tempo Trace Explorer — traces created during that window appear as a
#    dense band.  Click any trace to open the waterfall.
# The CSV is therefore your ground-truth event log that makes every telemetry
# anomaly explainable.
# ─────────────────────────────────────────────────────────────────────────────
_CSV_FIELDNAMES = [
    "timestamp_start",
    "timestamp_end",
    "scenario",
    "endpoints",
    "request_count",
    "burst_size",
    "ok",
    "err",
    "avg_ms",
    "max_ms",
    "notes",
]


def _init_csv() -> None:
    """Create the /data directory and write the CSV header if the file does not yet exist."""
    os.makedirs(os.path.dirname(os.path.abspath(CSV_PATH)), exist_ok=True)
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="") as fh:
            csv.DictWriter(fh, fieldnames=_CSV_FIELDNAMES).writeheader()
        log.info("Created scenario schedule at %s", CSV_PATH)
    else:
        log.info("Appending to existing schedule at %s", CSV_PATH)


def _append_row(row: dict) -> None:
    """Append one completed-scenario row to the CSV (called from the main thread only)."""
    with open(CSV_PATH, "a", newline="") as fh:
        csv.DictWriter(fh, fieldnames=_CSV_FIELDNAMES).writerow(row)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario Schedule — HTTP endpoint
#
# A tiny read-only HTTP server runs in a background daemon thread so you can
# inspect the schedule table without exec-ing into the container.
#
# Endpoints
# ─────────
#   GET /scenarios           → text/csv   (raw CSV file)
#   GET /scenarios?json      → JSON array (all rows as objects)
#   GET /health              → 200 OK     (liveness probe)
#
# Usage from the host
# ───────────────────
#   curl http://localhost:8088/scenarios
#   curl 'http://localhost:8088/scenarios?json' | python -m json.tool
# ─────────────────────────────────────────────────────────────────────────────
class _ScheduleHandler(http.server.BaseHTTPRequestHandler):
    """Read-only HTTP handler for the scenario schedule."""

    def do_GET(self) -> None:  # noqa: N802
        path, _, query = self.path.partition("?")
        if path == "/scenarios":
            if "json" in query:
                self._serve_json()
            else:
                self._serve_csv()
        elif path == "/health":
            self._respond(200, "text/plain; charset=utf-8", b"ok\n")
        else:
            body = (
                b"Troublemaker scenario schedule\n\n"
                b"  GET /scenarios          CSV  (all recorded scenarios)\n"
                b"  GET /scenarios?json     JSON array\n"
                b"  GET /health             liveness probe\n"
            )
            self._respond(404, "text/plain; charset=utf-8", body)

    def _serve_csv(self) -> None:
        try:
            with open(CSV_PATH, "rb") as fh:
                data = fh.read()
        except FileNotFoundError:
            data = b"(no scenarios recorded yet)\n"
        self._respond(200, "text/csv; charset=utf-8", data)

    def _serve_json(self) -> None:
        try:
            with open(CSV_PATH, newline="") as fh:
                rows = list(csv.DictReader(fh))
        except FileNotFoundError:
            rows = []
        data = json.dumps(rows, indent=2).encode()
        self._respond(200, "application/json", data)

    def _respond(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:  # noqa: ANN002
        # Demote HTTP access logs to DEBUG so they don't clutter scenario output.
        log.debug("http  " + fmt, *args)


def _start_http_server() -> None:
    """Bind the schedule HTTP server and start it in a background daemon thread."""

    class _ReuseServer(socketserver.TCPServer):
        allow_reuse_address = True

    server = _ReuseServer(("", HTTP_PORT), _ScheduleHandler)
    thread = threading.Thread(target=server.serve_forever, name="schedule-http", daemon=True)
    thread.start()
    log.info("Schedule HTTP server listening on :%d", HTTP_PORT)
    log.info("  CSV:  curl http://localhost:%d/scenarios", HTTP_PORT)
    log.info("  JSON: curl 'http://localhost:%d/scenarios?json'", HTTP_PORT)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────
def _get(path: str) -> tuple[int, float]:
    """
    Fire one GET request to BASE_URL + path.

    Returns (http_status_code, elapsed_seconds).
    Returns (0, elapsed) on timeout or connection error so callers never raise.
    """
    url = BASE_URL + path
    start = time.perf_counter()
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        elapsed = time.perf_counter() - start
        return resp.status_code, elapsed
    except requests.exceptions.Timeout:
        elapsed = time.perf_counter() - start
        log.warning("  timeout      url=%s  elapsed=%.2fs", url, elapsed)
        return 0, elapsed
    except requests.exceptions.ConnectionError as exc:
        elapsed = time.perf_counter() - start
        log.warning("  conn_err     url=%s  error=%s", url, exc)
        return 0, elapsed


def _fire_requests(
    endpoints: list[str],
    count: int,
    delay_between: float = 0.0,
) -> dict:
    """
    Send `count` requests, cycling through the given `endpoints` list in order.

    Returns a summary dict: {total, ok, err, avg_ms, max_ms}.
    """
    ok = err = 0
    durations: list[float] = []

    for i in range(count):
        path = endpoints[i % len(endpoints)]
        status, elapsed = _get(path)
        durations.append(elapsed)
        if 200 <= status < 400:
            ok += 1
        else:
            err += 1
        if delay_between > 0:
            time.sleep(delay_between)

    avg_ms = (sum(durations) / len(durations) * 1000) if durations else 0.0
    max_ms = (max(durations) * 1000) if durations else 0.0
    return {"total": count, "ok": ok, "err": err, "avg_ms": avg_ms, "max_ms": max_ms}


# ─────────────────────────────────────────────────────────────────────────────
# Scenario functions
#
# Each function is self-contained: it decides how many requests to send,
# fires them, logs before and after, and documents its observability impact.
# ─────────────────────────────────────────────────────────────────────────────

def scenario_steady_normal() -> dict:
    """
    What it generates
    -----------------
    Prometheus  Steady increment on http_requests_total. Latency histogram
                sits at the fast baseline (< 50 ms). Good "calm" period to
                compare against spikes.
    Loki        Stream of INFO log lines from both services, all status=200.
    Tempo       Clean 2-span waterfalls on /backend-ok (frontend SERVER span
                wrapping a backend SERVER span sharing the same trace_id).

    Learning value
    --------------
    Establishes a healthy baseline. On Grafana dashboards, this appears as
    a flat horizontal line — your reference "normal" state.
    """
    n = STEADY_REQUESTS
    log.info("[steady_normal]    Sending %d requests to %s — delay=0.3s", n, ENDPOINTS_NORMAL)
    r = _fire_requests(ENDPOINTS_NORMAL, n, delay_between=0.3)
    log.info(
        "[steady_normal]    Done  total=%d  ok=%d  err=%d  avg=%.0fms  max=%.0fms",
        r["total"], r["ok"], r["err"], r["avg_ms"], r["max_ms"],
    )
    return {
        "endpoints":     ", ".join(ENDPOINTS_NORMAL),
        "request_count": n,
        "burst_size":    0,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         "steady baseline",
    }


def scenario_burst_traffic() -> dict:
    """
    What it generates
    -----------------
    Prometheus  Sudden vertical spike on the request rate graph. The rate()
                function will show a clear narrow peak.
                http_requests_in_flight climbs then drops sharply.
    Loki        Dense cluster of log lines arriving within a few seconds,
                visible as a bright column in the log volume bar chart.
    Tempo       Many trace IDs appear at the same timestamp — the Trace
                Explorer will show a dense band of dots at that point.

    Learning value
    --------------
    Teaches you to distinguish a burst (narrow spike, returns to baseline)
    from a sustained load (wide plateau). Correlate the Prometheus rate spike
    with the Loki log density — they will align perfectly in time.
    """
    n = random.randint(BURST_MIN, BURST_MAX)
    log.info("[burst_traffic]    Sending burst of %d requests to %s — no delay", n, ENDPOINTS_NORMAL)
    r = _fire_requests(ENDPOINTS_NORMAL, n, delay_between=0.0)
    log.info(
        "[burst_traffic]    Done  total=%d  ok=%d  err=%d  avg=%.0fms  max=%.0fms",
        r["total"], r["ok"], r["err"], r["avg_ms"], r["max_ms"],
    )
    return {
        "endpoints":     ", ".join(ENDPOINTS_NORMAL),
        "request_count": n,
        "burst_size":    n,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         f"burst {n} requests no-delay",
    }


def scenario_latency_spike() -> dict:
    """
    What it generates
    -----------------
    Prometheus  The latency histogram p95/p99 bands diverge upward from p50.
                The request rate itself does NOT change — only the shape of
                the latency distribution shifts. That is the key diagnostic.
    Loki        INFO lines from frontend-api show duration_ms values in the
                hundreds. Compare with lines from a steady_normal period.
    Tempo       Wide spans: a /slow request has a wide frontend SERVER span
                with an obvious gap. A /backend-slow request shows a nested
                wide backend span as the main contributor.

    Learning value
    --------------
    Teaches you to read p95 vs p50 divergence (the "tail latency" concept).
    Also teaches you to open a trace waterfall to find WHERE the time is
    going — which span is wide, and which service owns it.
    """
    n = random.randint(5, 12)
    log.info("[latency_spike]    Sending %d requests to slow endpoints %s", n, ENDPOINTS_SLOW)
    r = _fire_requests(ENDPOINTS_SLOW, n, delay_between=0.1)
    log.info(
        "[latency_spike]    Done  total=%d  ok=%d  err=%d  avg=%.0fms  max=%.0fms",
        r["total"], r["ok"], r["err"], r["avg_ms"], r["max_ms"],
    )
    return {
        "endpoints":     ", ".join(ENDPOINTS_SLOW),
        "request_count": n,
        "burst_size":    0,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         "p95/p99 latency spike",
    }


def scenario_error_spike() -> dict:
    """
    What it generates
    -----------------
    Prometheus  http_errors_total counter spikes. On the Error Rate panel in
                Grafana the graph will show a clear anomaly window.
                The error rate ratio (errors/requests) approaches 1.0.
    Loki        WARNING log lines from frontend-api, all showing status=500.
                The Error Log Volume panel shows a matching bar spike.
    Tempo       Spans with status=ERROR (shown in red in Tempo waterfall).
                Each span has the error attributes recorded on it.

    Learning value
    --------------
    The simplest correlated failure pattern. Pick any of these error spans in
    Tempo, then use the "View in Loki" link — you will land directly on the
    log line that matches the same trace_id. That is metrics + traces + logs
    correlation in one click.
    """
    n = random.randint(8, 20)
    log.info("[error_spike]      Sending %d requests to /error", n)
    r = _fire_requests(["/error"], n, delay_between=0.05)
    log.info(
        "[error_spike]      Done  total=%d  ok=%d  err=%d  avg=%.0fms",
        r["total"], r["ok"], r["err"], r["avg_ms"],
    )
    return {
        "endpoints":     "/error",
        "request_count": n,
        "burst_size":    0,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         "frontend error counter spike",
    }


def scenario_backend_failure() -> dict:
    """
    What it generates
    -----------------
    Prometheus  http_errors_total increments on BOTH frontend-api AND
                backend-api series. The downstream_call_duration_seconds
                metric captures the round-trip to the backend.
    Loki        ERROR-level log lines in BOTH service log streams. Because
                OTel propagates the trace_id through the HTTP call, BOTH log
                lines carry the SAME trace_id. This is where log correlation
                across services becomes powerful.
    Tempo       A 3-span waterfall where ALL three spans show status=ERROR:
                  1. frontend SERVER span  "GET /backend-error"
                  2. frontend CLIENT span  "frontend.backend_call"
                  3. backend  SERVER span  "GET /error"
                This is the "blast radius" view — you see in one trace exactly
                how an error in the backend propagated to the frontend.

    Learning value
    --------------
    The most important distributed tracing lesson. Open one of these traces
    in Tempo: you will see the error propagation chain visually. Then check
    Loki with the same trace_id — both services' logs appear even though
    they are different processes.
    """
    n = random.randint(5, 15)
    log.info("[backend_failure]  Sending %d requests to /backend-error", n)
    r = _fire_requests(["/backend-error"], n, delay_between=0.1)
    log.info(
        "[backend_failure]  Done  total=%d  ok=%d  err=%d  avg=%.0fms",
        r["total"], r["ok"], r["err"], r["avg_ms"],
    )
    return {
        "endpoints":     "/backend-error",
        "request_count": n,
        "burst_size":    0,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         "3-span ERROR waterfall in Tempo",
    }


def scenario_slow_backend() -> dict:
    """
    What it generates
    -----------------
    Prometheus  The frontend latency histogram rises even though the FRONTEND
                code itself executes instantly. The downstream_call_duration_
                seconds metric, however, points directly at the backend wait.
                This shows the diagnostic value of per-dependency metrics.
    Loki        Frontend logs report high duration_ms values on /backend-slow.
                Backend logs report their own slow duration on /slow.
                Both carry the same trace_id — you can cross-reference them.
    Tempo       Waterfall: the frontend span is wide, but when you expand it
                you see the nested backend /slow span occupying most of the
                width. The frontend CODE spans are narrow — the time is in
                the downstream.

    Learning value
    --------------
    Teaches you to answer "is MY service slow, or is my DEPENDENCY slow?"
    Compare downstream_call_duration_seconds (backend round-trip) with
    http_request_duration_seconds (total frontend time). Their ratio tells
    you where the bottleneck lives without opening a single trace.
    """
    n = random.randint(5, 10)
    log.info("[slow_backend]     Sending %d requests to /backend-slow", n)
    r = _fire_requests(["/backend-slow"], n, delay_between=0.2)
    log.info(
        "[slow_backend]     Done  total=%d  ok=%d  err=%d  avg=%.0fms  max=%.0fms",
        r["total"], r["ok"], r["err"], r["avg_ms"], r["max_ms"],
    )
    return {
        "endpoints":     "/backend-slow",
        "request_count": n,
        "burst_size":    0,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         "downstream latency attribution",
    }


def scenario_mixed_chaos() -> dict:
    """
    What it generates
    -----------------
    Prometheus  All metrics become noisy and realistic simultaneously.
                No single signal dominates — moderate error rate, variable
                latency, normal request rate baseline.
    Loki        Interleaved INFO, WARNING, and ERROR lines from both services
                in no predictable order — just like production.
    Tempo       Traces of all shapes mixed in the same time window: fast
                clean traces, wide slow traces, error traces, multi-service
                traces.

    Learning value
    --------------
    The hardest and most realistic scenario to analyse. The goal is to train
    your eye to distinguish genuine anomalies from background noise. This is
    the core skill in production observability. Practice filtering Loki by
    level, narrowing Prometheus queries by service_name, and sorting Tempo
    traces by duration or error status.
    """
    endpoints = list(ENDPOINTS_ALL)
    if not ENABLE_ERRORS:
        endpoints = [e for e in endpoints if e not in ENDPOINTS_ERROR]
    if not ENABLE_SLOW:
        endpoints = [e for e in endpoints if e not in ENDPOINTS_SLOW]
    if not endpoints:
        endpoints = ENDPOINTS_NORMAL  # safety fallback

    n = random.randint(STEADY_REQUESTS, STEADY_REQUESTS * 3)
    log.info("[mixed_chaos]      Sending %d random requests across %s", n, endpoints)

    ok = err = 0
    durations: list[float] = []

    for _ in range(n):
        path = random.choice(endpoints)
        status, elapsed = _get(path)
        durations.append(elapsed)
        if 200 <= status < 400:
            ok += 1
        else:
            err += 1
        time.sleep(random.uniform(0.05, 0.4))

    avg_ms = (sum(durations) / len(durations) * 1000) if durations else 0.0
    max_ms = (max(durations) * 1000) if durations else 0.0
    log.info(
        "[mixed_chaos]      Done  total=%d  ok=%d  err=%d  avg=%.0fms  max=%.0fms",
        n, ok, err, avg_ms, max_ms,
    )
    return {
        "endpoints":     ", ".join(endpoints),
        "request_count": n,
        "burst_size":    0,
        "ok":            ok,
        "err":           err,
        "avg_ms":        avg_ms,
        "max_ms":        max_ms,
        "notes":         "random mixed endpoints",
    }


def scenario_slow_burn() -> dict:
    """
    What it generates
    -----------------
    Prometheus  Latency shows a gradual upward drift rather than a sharp
                spike. The rate() slope changes slowly. This tests your
                ability to notice trend changes vs point anomalies.
    Loki        Alternating fast and slow log entries make the pattern
                visible in the logs panel — fast lines interspersed with
                slow ones, showing the degradation is intermittent.
    Tempo       A mix of narrow and wide traces in the same window, creating
                a "striped" pattern in the Trace Explorer timeline.

    Learning value
    --------------
    Teaches you to catch slow degradation — the kind that does not trigger
    a threshold alert but means something is slowly getting worse. Compare
    a 30-minute window to a 5-minute window in Grafana: the longer view
    will reveal the drift that the shorter view hides.
    """
    rounds = random.randint(3, 7)
    log.info("[slow_burn]        Running %d alternating normal/slow rounds", rounds)

    total_ok = total_err = total_requests = 0
    total_weighted_ms = 0.0
    max_ms = 0.0

    for i in range(rounds):
        r_normal = _fire_requests(ENDPOINTS_NORMAL, 3, delay_between=0.2)
        total_ok          += r_normal["ok"]
        total_err         += r_normal["err"]
        total_requests    += r_normal["total"]
        total_weighted_ms += r_normal["avg_ms"] * r_normal["total"]
        max_ms             = max(max_ms, r_normal["max_ms"])
        if ENABLE_SLOW:
            r_slow = _fire_requests(ENDPOINTS_SLOW, 2, delay_between=0.1)
            total_ok          += r_slow["ok"]
            total_err         += r_slow["err"]
            total_requests    += r_slow["total"]
            total_weighted_ms += r_slow["avg_ms"] * r_slow["total"]
            max_ms             = max(max_ms, r_slow["max_ms"])
            slow_avg = r_slow["avg_ms"]
        else:
            slow_avg = 0.0
        log.info(
            "[slow_burn]        Round %d/%d  normal_ok=%d  slow_avg=%.0fms",
            i + 1, rounds, r_normal["ok"], slow_avg,
        )
        time.sleep(random.uniform(1.0, 3.0))

    log.info("[slow_burn]        Done")
    avg_ms = total_weighted_ms / total_requests if total_requests else 0.0
    endpoints_used = ENDPOINTS_NORMAL + (ENDPOINTS_SLOW if ENABLE_SLOW else [])
    return {
        "endpoints":     ", ".join(endpoints_used),
        "request_count": total_requests,
        "burst_size":    0,
        "ok":            total_ok,
        "err":           total_err,
        "avg_ms":        avg_ms,
        "max_ms":        max_ms,
        "notes":         f"{rounds} alternating rounds gradual drift",
    }


def scenario_sick_api() -> dict:
    """
    "Sick but Not Dead" — API jitter and partial failure mix.

    What it generates
    -----------------
    Prometheus  p99/p50 ratio diverges above 8x — the bimodal latency fingerprint.
                Error rate sits in the 2–20% sick zone (not healthy, not dead).
                Request RATE stays normal — the service is still responding.
                Alerts that fire: PartialFailureDetected, LatencyPercentileDivergence,
                SickAPINotDead.
                Alerts that do NOT fire: CriticalErrorRate, HighErrorRate (above 20%).
    Loki        Mix of INFO (fast /sick-partial success), WARNING (/sick-partial
                failure lines at ~30%), and slow INFO (/sick with high delay_ms).
                This interleaving at the SAME timestamp is the sick log signature.
    Tempo       Two trace populations visible simultaneously:
                  - Narrow fast spans (70% of /sick-partial calls — fast 200)
                  - Wide slow spans (/sick calls with log-normal jitter delay)
                The bimodal span width distribution is the sick-but-not-dead
                fingerprint in the trace explorer timeline.

    Learning value
    --------------
    Health check returns 200. Error rate is below CriticalErrorRate threshold.
    P50 looks acceptable. Only percentile divergence (p99/p50 ratio > 8) and
    the latency heatmap reveal the problem. This teaches why you need p99
    monitoring, not just averages.
    """
    n = STEADY_REQUESTS
    all_sick = ENDPOINTS_SICK
    log.info(
        "[sick_api]   Sending %d sick-jitter + %d partial-failure requests",
        n, n,
    )
    r_jitter  = _fire_requests(["/sick", "/backend-sick"],           n, delay_between=0.3)
    r_partial = _fire_requests(["/sick-partial", "/backend-sick-partial"], n, delay_between=0.1)

    total  = r_jitter["total"] + r_partial["total"]
    ok     = r_jitter["ok"]    + r_partial["ok"]
    err    = r_jitter["err"]   + r_partial["err"]
    avg_ms = (
        r_jitter["avg_ms"] * r_jitter["total"]
        + r_partial["avg_ms"] * r_partial["total"]
    ) / total
    max_ms = max(r_jitter["max_ms"], r_partial["max_ms"])

    log.info(
        "[sick_api]   Done  total=%d  ok=%d  err=%d  avg=%.0fms  max=%.0fms",
        total, ok, err, avg_ms, max_ms,
    )
    return {
        "endpoints":     ", ".join(all_sick),
        "request_count": total,
        "burst_size":    0,
        "ok":            ok,
        "err":           err,
        "avg_ms":        avg_ms,
        "max_ms":        max_ms,
        "notes":         "sick-but-not-dead: bimodal latency + 2-20% error rate in sick zone",
    }


def scenario_sick_db() -> dict:
    """
    DB connection pool exhaustion — the hardest sick-but-not-dead to diagnose.

    What it generates
    -----------------
    Prometheus  downstream_call_duration_seconds p95 rises above 2s → triggers
                ConnectionPoolApproachingSaturation alert.
                http_requests_in_flight rises (requests queuing, not completing fast).
                Backend /health still returns 200 — traditional monitoring: service UP.
    Loki        Backend logs show: "path=pool_contention  queue_wait=3000ms" lines
                (85% of requests) and "path=pool_exhausted  error=DB_POOL_EXHAUSTED"
                lines (10% of requests). All carry the same trace_id chain.
    Tempo       Wide backend SERVER spans (2-4s pool wait) with a small width frontend
                CLIENT span wrapping them. The backend owns the latency completely.
                10% of traces show a red backend span (503 pool exhausted path).

    Learning value
    --------------
    The most deceptive sick-but-not-dead variant. The frontend /health and the
    backend /health both return 200 immediately (neither is DB-backed). Yet
    real user requests are queuing 2-4 seconds before completing. Only the
    downstream_call_duration_seconds p95 metric reveals the problem.
    """
    n = STEADY_REQUESTS
    log.info("[sick_db]    Sending %d requests to DB pool exhaustion pattern", n)
    # Mix: /backend-sick-db (pool exhaustion model) + /backend-sick (jitter)
    r = _fire_requests(["/backend-sick-db", "/backend-sick"], n, delay_between=0.5)
    log.info(
        "[sick_db]    Done  total=%d  ok=%d  err=%d  avg=%.0fms  max=%.0fms",
        r["total"], r["ok"], r["err"], r["avg_ms"], r["max_ms"],
    )
    return {
        "endpoints":     "/backend-sick-db, /backend-sick",
        "request_count": r["total"],
        "burst_size":    0,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         "sick-but-not-dead: DB pool contention (slow queuing + 10% 503s)",
    }


def scenario_sick_storage() -> dict:
    """
    Storage brownout — the most deceptive sick-but-not-dead pattern.

    Activates the sick_storage scenario on the storage-simulator:
      OSDs: all 3 UP  →  CephOSDDown does NOT fire
      cluster_health_score: 2 (OK)  →  CephClusterDegraded does NOT fire
      IO latency: 8–18x normal  →  SickStorageBrownout DOES fire
      Pool fill: 65%  →  CephPoolNearFull does NOT fire

    Then sends normal frontend traffic to prove the API layer is still
    healthy — reinforcing the "not dead" part of "sick but not dead".
    Auto-reverts to healthy after 90 seconds so the sick→recovery arc
    is visible as a dip and recovery on the Grafana IO latency panel.

    What it generates
    -----------------
    Prometheus  storage_io_latency_seconds{pvc="pvc-db-0"} rises to 0.018 (18x)
                storage_osd_up{osd_id="osd.0/1/2"} stays at 1 (all green)
                storage_cluster_health_score stays at 2 (OK)
                SickStorageBrownout alert fires (IO > 15ms + all OSDs up + health=2)
    Loki        Normal frontend API logs continue — the web tier is unaffected
    Grafana     Row 5 "Storage Sick Pattern" panel: IO latency rising, OSD count flat

    Learning value
    --------------
    The storage cluster reports healthy. The Grafana storage health dashboard shows
    green everywhere. But the IO latency time series panel tells the truth.
    Health != Performance. This is the core lesson of storage brownouts.
    """
    log.info(
        "[sick_storage]  Activating sick_storage on storage-simulator "
        "(OSDs stay green, IO latency rises 10-18x)..."
    )
    try:
        resp = requests.post(f"{STORAGE_SIM_URL}/scenario/sick_storage", timeout=10)
        if resp.status_code == 200:
            log.info("[sick_storage]  Scenario activated: sick_storage")
        else:
            log.warning(
                "[sick_storage]  Storage simulator returned HTTP %d — continuing",
                resp.status_code,
            )
    except requests.exceptions.RequestException as exc:
        log.warning(
            "[sick_storage]  Could not reach storage-simulator at %s: %s — continuing",
            STORAGE_SIM_URL, exc,
        )

    # Send normal frontend traffic — proves API is still responding (not dead)
    log.info(
        "[sick_storage]  Sending %d normal API requests to demonstrate "
        "web tier is healthy while storage is sick...",
        STEADY_REQUESTS,
    )
    r = _fire_requests(ENDPOINTS_NORMAL, STEADY_REQUESTS, delay_between=0.5)
    log.info(
        "[sick_storage]  API ok=%d err=%d avg=%.0fms — web tier healthy, storage sick",
        r["ok"], r["err"], r["avg_ms"],
    )

    # Hold for 90 seconds so the brownout is visible in Grafana time series
    log.info(
        "[sick_storage]  Holding sick_storage for 90s "
        "— watch Grafana 'Storage Sick Pattern' row for IO latency rise..."
    )
    time.sleep(90)

    # Auto-revert so the sick → recovery transition is visible
    log.info("[sick_storage]  Reverting to healthy scenario...")
    try:
        requests.post(f"{STORAGE_SIM_URL}/scenario/healthy", timeout=10)
        log.info("[sick_storage]  Storage reverted to healthy")
    except requests.exceptions.RequestException as exc:
        log.warning("[sick_storage]  Could not revert: %s", exc)

    return {
        "endpoints":     ", ".join(ENDPOINTS_NORMAL),
        "request_count": r["total"],
        "burst_size":    0,
        "ok":            r["ok"],
        "err":           r["err"],
        "avg_ms":        r["avg_ms"],
        "max_ms":        r["max_ms"],
        "notes":         "sick-but-not-dead: storage brownout 90s (IO 10-18x, OSDs green, health=OK)",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Scenario registry
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class ScenarioEntry:
    name: str
    fn: Callable[[], dict]
    enabled: bool
    weight: int  # higher weight = more likely to be chosen


def _build_registry() -> list[ScenarioEntry]:
    """
    Build the active scenario list, honouring the ENABLE_* flags.

    Weights control relative frequency:
      steady_normal  runs most often (weight 4) to keep a healthy baseline.
      mixed_chaos    runs often (weight 3) for realistic background noise.
      all others     run occasionally (weight 2) to create clear anomalies.
      slow_burn      runs rarely (weight 1) as it is the most time-consuming.
    """
    return [
        ScenarioEntry("steady_normal",   scenario_steady_normal,   enabled=True,          weight=4),
        ScenarioEntry("burst_traffic",   scenario_burst_traffic,   enabled=ENABLE_BURSTS, weight=2),
        ScenarioEntry("latency_spike",   scenario_latency_spike,   enabled=ENABLE_SLOW,   weight=2),
        ScenarioEntry("error_spike",     scenario_error_spike,     enabled=ENABLE_ERRORS, weight=2),
        ScenarioEntry("backend_failure", scenario_backend_failure, enabled=ENABLE_ERRORS, weight=2),
        ScenarioEntry("slow_backend",    scenario_slow_backend,    enabled=ENABLE_SLOW,   weight=2),
        ScenarioEntry("mixed_chaos",     scenario_mixed_chaos,     enabled=True,          weight=3),
        ScenarioEntry("slow_burn",       scenario_slow_burn,       enabled=ENABLE_SLOW,   weight=1),
        # Sick but not dead scenarios
        ScenarioEntry("sick_api",        scenario_sick_api,        enabled=True,          weight=2),
        ScenarioEntry("sick_db",         scenario_sick_db,         enabled=True,          weight=2),
        ScenarioEntry("sick_storage",    scenario_sick_storage,    enabled=True,          weight=1),
    ]


def _pick_scenario(registry: list[ScenarioEntry]) -> ScenarioEntry:
    active = [s for s in registry if s.enabled]
    if not active:
        # Fallback: all scenarios disabled except steady_normal
        return ScenarioEntry("steady_normal", scenario_steady_normal, enabled=True, weight=1)
    weights = [s.weight for s in active]
    return random.choices(active, weights=weights, k=1)[0]


# ─────────────────────────────────────────────────────────────────────────────
# Startup gate — wait for the frontend to be ready
# ─────────────────────────────────────────────────────────────────────────────
def _wait_for_frontend(max_wait_seconds: int = 120) -> None:
    """
    Poll GET /health until a 200 is received.
    This prevents the chaos loop from starting before the app is instrumented.
    """
    url = BASE_URL + "/health"
    log.info("Waiting for frontend-api to become ready at %s …", url)
    deadline = time.monotonic() + max_wait_seconds

    while time.monotonic() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                log.info("Frontend-api is ready — starting chaos loop")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)

    log.error(
        "Frontend-api did not become ready within %ds — starting anyway",
        max_wait_seconds,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    separator = "─" * 60
    log.info(separator)
    log.info("  Troublemaker  starting up")
    log.info("  base_url         = %s", BASE_URL)
    log.info("  sleep_range      = %.0f – %.0f s", MIN_SLEEP, MAX_SLEEP)
    log.info("  burst_range      = %d – %d requests", BURST_MIN, BURST_MAX)
    log.info("  steady_requests  = %d", STEADY_REQUESTS)
    log.info("  enable_bursts    = %s", ENABLE_BURSTS)
    log.info("  enable_errors    = %s", ENABLE_ERRORS)
    log.info("  enable_slow      = %s", ENABLE_SLOW)
    log.info("  schedule_csv     = %s", CSV_PATH)
    log.info("  http_port        = %d", HTTP_PORT)
    log.info(separator)

    _init_csv()
    _start_http_server()
    _wait_for_frontend()

    registry = _build_registry()
    active_names = [s.name for s in registry if s.enabled]
    log.info("Active scenarios (%d): %s", len(active_names), ", ".join(active_names))
    log.info(separator)

    round_num = 0
    while True:
        round_num += 1
        scenario  = _pick_scenario(registry)
        sleep_s   = random.uniform(MIN_SLEEP, MAX_SLEEP)

        log.info(
            "┌── Round %d  scenario=%s  next_sleep=%.0fs",
            round_num, scenario.name, sleep_s,
        )

        t_start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        result: dict = {}
        try:
            result = scenario.fn()
        except Exception as exc:  # noqa: BLE001 — keep the loop alive on any error
            log.error("[%s]  Unhandled error: %s", scenario.name, exc, exc_info=True)
            result = {
                "endpoints": "", "request_count": 0, "burst_size": 0,
                "ok": 0, "err": 0, "avg_ms": 0.0, "max_ms": 0.0,
                "notes": f"ERROR: {exc}",
            }
        finally:
            t_end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _append_row({
                "timestamp_start": t_start,
                "timestamp_end":   t_end,
                "scenario":        scenario.name,
                "endpoints":       result.get("endpoints", ""),
                "request_count":   result.get("request_count", 0),
                "burst_size":      result.get("burst_size", 0),
                "ok":              result.get("ok", 0),
                "err":             result.get("err", 0),
                "avg_ms":          f"{result.get('avg_ms', 0.0):.0f}",
                "max_ms":          f"{result.get('max_ms', 0.0):.0f}",
                "notes":           result.get("notes", ""),
            })
            log.info(
                "  ✓ schedule row   scenario=%-18s  start=%s  requests=%d  ok=%d  err=%d",
                scenario.name, t_start,
                result.get("request_count", 0), result.get("ok", 0), result.get("err", 0),
            )

        log.info("└── Round %d complete — sleeping %.0fs\n", round_num, sleep_s)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
