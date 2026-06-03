"""
frontend-api/app/main.py
------------------------
Frontend API service for the Observability Learning project.

This version adds OpenTelemetry instrumentation.  Endpoint behaviour is
unchanged.  Every instrumentation decision is explained in a comment.

Endpoints
~~~~~~~~~
  GET /health       — liveness probe  (excluded from traces)
  GET /ok           — fast frontend success
  GET /slow         — intentional frontend delay
  GET /error        — HTTP 500, frontend only
  GET /backend-ok   — proxies to backend /ok
  GET /backend-slow — proxies to backend /slow
  GET /backend-error— proxies to backend /error

Telemetry emitted
~~~~~~~~~~~~~~~~~
  Traces  — auto via FastAPIInstrumentor for all inbound requests
           + auto client spans and W3C header injection via HTTPXClientInstrumentor
           + manual "frontend.backend_call" child span in _call_backend()
  Metrics — http_requests_total, http_errors_total,
            http_request_duration_seconds, http_requests_in_flight,
            downstream_call_duration_seconds
  Logs    — every log record enriched with trace_id + span_id
"""

import asyncio
import logging
import math
import os
import random
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry import trace
from opentelemetry.trace import StatusCode

# ── Telemetry bootstrap ────────────────────────────────────────────────────────
import app.telemetry as _tel
from app.telemetry import get_tracer, setup_telemetry

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# The format will be upgraded by telemetry.py's _install_log_enrichment()
# to include trace_id and span_id once setup_telemetry() is called.
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("frontend-api")


# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
SLOW_MS: int = int(os.getenv("FRONTEND_SLOW_MS", "800"))
BACKEND_URL: str = os.getenv("BACKEND_URL", "http://backend-api:8081")
logger.info("Frontend configuration: SLOW_MS=%d  BACKEND_URL=%s", SLOW_MS, BACKEND_URL)


# ──────────────────────────────────────────────────────────────────────────────
# FastAPI app
# setup_telemetry(app) wires FastAPIInstrumentor + HTTPXClientInstrumentor.
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Frontend API",
    version="1.0.0",
    description="Frontend service for the Observability Learning project.",
)

setup_telemetry(app)


# ──────────────────────────────────────────────────────────────────────────────
# Middleware — metrics recording + request logging
# ──────────────────────────────────────────────────────────────────────────────
@app.middleware("http")
async def record_metrics_and_log(request: Request, call_next):
    attrs = {
        "http.method":  request.method,
        "http.route":   request.url.path,
        "service.name": "frontend-api",
    }
    _tel.in_flight_gauge.add(1, attrs)
    start = time.perf_counter()
    response = await call_next(request)
    duration_s = time.perf_counter() - start
    attrs_with_status = {**attrs, "http.status_code": str(response.status_code)}
    _tel.request_counter.add(1, attrs_with_status)
    _tel.latency_histogram.record(duration_s, attrs_with_status)
    _tel.in_flight_gauge.add(-1, attrs)
    if response.status_code >= 500:
        _tel.error_counter.add(1, attrs_with_status)
    logger.info(
        "request  method=%s  path=%s  status=%d  duration_ms=%.1f",
        request.method,
        request.url.path,
        response.status_code,
        duration_s * 1000,
    )
    return response


# ──────────────────────────────────────────────────────────────────────────────
# Shutdown — flush telemetry buffers
# ──────────────────────────────────────────────────────────────────────────────
@app.on_event("shutdown")
async def shutdown_telemetry():
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()
        logger.info("TracerProvider flushed on shutdown")


# ──────────────────────────────────────────────────────────────────────────────
# Internal helper — call the backend service
#
# This is where distributed trace propagation happens.
#
# The HTTPXClientInstrumentor (wired in telemetry.py) automatically:
#   1. Creates a CLIENT span wrapping the httpx.get() call.
#   2. Injects the W3C `traceparent` header into the outgoing request.
#
# The backend receives that header, extracts the trace context, and
# creates its SERVER span as a CHILD of this CLIENT span.  Both spans
# share the same trace_id.  In Tempo they appear as one waterfall.
#
# The manual span below adds frontend-specific attributes on top of what
# HTTPXClientInstrumentor provides, and records the downstream latency
# to the downstream_histogram metric.
# ──────────────────────────────────────────────────────────────────────────────
async def _call_backend(path: str) -> dict:
    url = f"{BACKEND_URL}{path}"
    tracer = get_tracer()

    # Manual span: adds frontend context to the trace that HTTPXClientInstrumentor
    # alone does not provide — e.g. which frontend endpoint triggered this call.
    with tracer.start_as_current_span(
        "frontend.backend_call",
        kind=trace.SpanKind.CLIENT,
    ) as span:
        span.set_attribute("backend.url",  url)
        span.set_attribute("backend.path", path)

        logger.info("Outbound call  GET %s", url)
        ds_start = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # HTTPXClientInstrumentor injects traceparent here automatically.
                resp = await client.get(url)
            ds_duration_s = time.perf_counter() - ds_start

            span.set_attribute("http.status_code", resp.status_code)

            # Record downstream latency separately from total request latency.
            # This lets you answer: "is MY service slow or is the backend slow?"
            _tel.downstream_histogram.record(
                ds_duration_s,
                {"backend.path": path, "http.status_code": str(resp.status_code)},
            )

            if resp.status_code >= 500:
                span.set_status(
                    StatusCode.ERROR,
                    f"Backend returned HTTP {resp.status_code}",
                )

            logger.info("Outbound result  GET %s  status=%d", url, resp.status_code)
            return {"status_code": resp.status_code, "body": resp.json()}

        except httpx.RequestError as exc:
            ds_duration_s = time.perf_counter() - ds_start
            _tel.downstream_histogram.record(
                ds_duration_s,
                {"backend.path": path, "http.status_code": "503"},
            )
            # Record the exception on the span so it appears in the trace viewer.
            span.record_exception(exc)
            span.set_status(StatusCode.ERROR, str(exc))
            logger.error("Backend unreachable  url=%s  error=%s", url, exc)
            return {
                "status_code": 503,
                "body": {
                    "status": "error",
                    "message": f"Could not reach backend service: {exc}",
                    "error_code": "BACKEND_UNREACHABLE",
                },
            }


# ──────────────────────────────────────────────────────────────────────────────
# GET /health
# Excluded from traces (excluded_urls="/health" in FastAPIInstrumentor).
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health():
    return {"service": "frontend-api", "status": "healthy"}


# ──────────────────────────────────────────────────────────────────────────────
# GET /ok
#
# Telemetry emitted
# ~~~~~~~~~~~~~~~~~
# Auto trace: SERVER span "GET /ok",  status=OK
# Manual metric: request_counter +1,  latency_histogram record
# Log: INFO with trace_id and span_id
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/ok", tags=["demo"])
async def ok():
    logger.info("Handling /ok — returning normal response")
    return {
        "service": "frontend-api",
        "endpoint": "/ok",
        "status": "success",
        "message": "Frontend is operating normally.",
    }


# ──────────────────────────────────────────────────────────────────────────────
# GET /slow
#
# Telemetry emitted
# ~~~~~~~~~~~~~~~~~
# Auto trace: SERVER span "GET /slow" — span width = FRONTEND_SLOW_MS
# Manual metric: latency_histogram shows a spike on this route
# Log: INFO with trace_id/span_id
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/slow", tags=["demo"])
async def slow():
    delay_s = SLOW_MS / 1000.0
    logger.info("Handling /slow — sleeping %.3fs (%dms)", delay_s, SLOW_MS)
    await asyncio.sleep(delay_s)
    return {
        "service": "frontend-api",
        "endpoint": "/slow",
        "status": "success",
        "message": f"Response intentionally delayed by {SLOW_MS}ms.",
        "delay_ms": SLOW_MS,
    }


# ──────────────────────────────────────────────────────────────────────────────
# GET /error
#
# Telemetry emitted
# ~~~~~~~~~~~~~~~~~
# Auto trace: SERVER span "GET /error" with status=ERROR (5xx triggers it)
# Manual metric: error_counter +1 (in middleware), request_counter +1
# Log: WARNING with trace_id/span_id
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/error", tags=["demo"])
async def error():
    logger.warning("Handling /error — returning simulated HTTP 500")
    return JSONResponse(
        status_code=500,
        content={
            "service": "frontend-api",
            "endpoint": "/error",
            "status": "error",
            "message": "Simulated internal frontend error.",
            "error_code": "SIMULATED_FRONTEND_FAILURE",
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# GET /backend-ok
#
# Telemetry emitted
# ~~~~~~~~~~~~~~~~~
# Auto trace:   SERVER span "GET /backend-ok"  (frontend)
# Manual trace: CLIENT span "frontend.backend_call"  (inside _call_backend)
# Auto trace:   CLIENT span from HTTPXClientInstrumentor  (the actual HTTP call)
# Auto trace:   SERVER span "GET /ok"  (backend — child of above via traceparent)
# Manual metric: downstream_histogram records backend call duration
# Both services log with THE SAME trace_id — this is cross-service correlation.
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/backend-ok", tags=["demo"])
async def backend_ok():
    result = await _call_backend("/ok")
    status_code = result["status_code"]
    return JSONResponse(
        status_code=status_code,
        content={
            "service": "frontend-api",
            "endpoint": "/backend-ok",
            "status": "success" if status_code == 200 else "error",
            "message": "Proxied request to backend /ok.",
            "downstream_result": result["body"],
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# GET /backend-slow
#
# Telemetry emitted
# ~~~~~~~~~~~~~~~~~
# Distributed trace: 3 spans in the waterfall:
#   1. SERVER  frontend "GET /backend-slow"   (wide, same duration as backend)
#   2. CLIENT  frontend "frontend.backend_call"  (child of 1)
#   3. SERVER  backend  "GET /slow"  (~BACKEND_SLOW_MS wide, child of 2)
# Metric: downstream_histogram shows the full backend wait time
# Key lesson: frontend latency_histogram rises even though frontend code is fast.
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/backend-slow", tags=["demo"])
async def backend_slow():
    result = await _call_backend("/slow")
    status_code = result["status_code"]
    return JSONResponse(
        status_code=status_code,
        content={
            "service": "frontend-api",
            "endpoint": "/backend-slow",
            "status": "success" if status_code == 200 else "error",
            "message": "Proxied request to backend /slow.",
            "downstream_result": result["body"],
        },
    )


# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
# GET /backend-error
#
# Telemetry emitted
# ~~~~~~~~~~~~~~~~~
# Distributed trace: 3 spans, both frontend.backend_call AND backend span
#   show status=ERROR.  This teaches "blast radius" — you can see exactly
#   how far the error propagated through the system.
# Both services' error_counter metrics increment.
# Both services log a WARNING/ERROR with the SAME trace_id.
# This is the clearest demonstration of metrics + logs + traces correlation.
# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
@app.get("/backend-error", tags=["demo"])
async def backend_error():
    result = await _call_backend("/error")
    status_code = result["status_code"]
    return JSONResponse(
        status_code=status_code,
        content={
            "service": "frontend-api",
            "endpoint": "/backend-error",
            "status": "error",
            "message": "Proxied request to backend /error.",
            "downstream_result": result["body"],
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# SICK BUT NOT DEAD — Endpoints
#
# These endpoints simulate the "sick but not dead" failure pattern:
# the service keeps responding (health check = green, HTTP 200 in most cases)
# but it is NOT delivering a healthy experience to users.
#
# Three distinct sub-patterns are modelled:
#
#  /sick               — Latency jitter (log-normal bimodal distribution).
#                        p50 looks acceptable; p99 is alarming.
#                        Detection: p99/p50 divergence ratio > 8.
#
#  /sick-partial       — 30% of requests return HTTP 500.
#                        Error rate sits in the 2–20% "sick zone".
#                        Health check still passes (not queried for every req).
#                        Detection: PartialFailureDetected alert.
#
#  /backend-sick*      — Proxy the above patterns through to the backend,
#                        teaching WHERE in the call chain the sickness lives.
#                        The trace waterfall shows which span is wide/red.
#
#  /backend-sick-db    — Proxy to backend /sick-db (connection pool exhaustion):
#                        85% of calls have 2–4 s delay; 10% return 503; 5% fast.
#                        Health check on /health is NOT DB-backed → stays green.
# ══════════════════════════════════════════════════════════════════════════════


@app.get("/sick", tags=["sick-but-not-dead"])
async def sick():
    """
    Log-normal jitter delay — always HTTP 200.

    The log-normal distribution (mu=5.7, sigma=1.1) produces:
      p50  ≈  300 ms   (most requests feel OK)
      p95  ≈ 2.5 s
      p99  ≈ 6–8 s    (the sick tail that hurts users)

    Prometheus captures this in http_request_duration_seconds_bucket.
    In Grafana the latency HEATMAP shows TWO humps — the bimodal fingerprint.
    The p99/p50 RATIO rises above 8 — the SickAPINotDead alert fires.
    The /health endpoint still returns 200 — traditional monitoring is blind.
    """
    # Log-normal: median ≈ 300 ms, long tail up to 10 s
    delay_s = min(math.exp(random.gauss(5.7, 1.1)) / 1000.0, 10.0)
    logger.info(
        "Handling /sick  delay=%.0fms  pattern=sick-but-not-dead",
        delay_s * 1000,
    )
    await asyncio.sleep(delay_s)
    return {
        "service": "frontend-api",
        "endpoint": "/sick",
        "status": "success",        # ← always 200 — the service is NOT dead
        "message": "Response delayed by IO/DB jitter. Sick but not dead.",
        "delay_ms": round(delay_s * 1000, 1),
        "pattern": "sick-but-not-dead",
    }


@app.get("/sick-partial", tags=["sick-but-not-dead"])
async def sick_partial():
    """
    Partial failure — 70% fast HTTP 200, 30% HTTP 500.

    Models: connection pool where 30% of acquire() calls time out,
    or a flaky downstream dependency rejecting a fraction of requests.

    Error rate sits in the 2–20% sick zone:
      • NOT healthy  (healthy = 0% errors)
      • NOT dead     (dead = 100% errors or unreachable)
    The PartialFailureDetected alert fires; CriticalErrorRate does not.
    Health check at /health always returns 200.
    """
    if random.random() < 0.30:   # 30% failure rate — the sick zone
        logger.warning(
            "Handling /sick-partial  outcome=failure  pattern=sick-but-not-dead  rate=30pct"
        )
        return JSONResponse(
            status_code=500,
            content={
                "service": "frontend-api",
                "endpoint": "/sick-partial",
                "status": "error",
                "message": "Partial failure — connection pool slot unavailable (simulated).",
                "error_code": "SICK_PARTIAL_FAILURE",
                "pattern": "sick-but-not-dead",
            },
        )
    logger.info(
        "Handling /sick-partial  outcome=success  pattern=sick-but-not-dead  rate=70pct"
    )
    return {
        "service": "frontend-api",
        "endpoint": "/sick-partial",
        "status": "success",
        "message": "Request succeeded. Note: 30 pct of requests to this endpoint fail.",
        "pattern": "sick-but-not-dead",
    }


@app.get("/backend-sick", tags=["sick-but-not-dead"])
async def backend_sick():
    """
    Proxy the /sick jitter pattern through to the backend service.

    In the trace waterfall:
      - Frontend SERVER span is WIDE (waiting on downstream)
      - Backend SERVER span is WIDE (the jitter delay lives HERE)
      - Frontend code spans are NARROW — it is just waiting

    This teaches "where is the time going?" via distributed traces.
    The downstream_call_duration_seconds metric also shows the backend wait.
    """
    result = await _call_backend("/sick")
    status_code = result["status_code"]
    return JSONResponse(
        status_code=status_code,
        content={
            "service": "frontend-api",
            "endpoint": "/backend-sick",
            "status": "success" if status_code == 200 else "error",
            "message": "Proxied to backend /sick — downstream jitter pattern.",
            "downstream_result": result["body"],
            "pattern": "sick-but-not-dead",
        },
    )


@app.get("/backend-sick-partial", tags=["sick-but-not-dead"])
async def backend_sick_partial():
    """
    Proxy the /sick-partial pattern through to the backend service.

    In the trace waterfall:
      - 70% traces: both spans green (backend succeeded)
      - 30% traces: backend span RED — error propagates to frontend CLIENT span

    Shows how partial failure in a downstream service creates a visible but
    non-fatal error rate in the caller — the upstream is "sick by association".
    """
    result = await _call_backend("/sick-partial")
    status_code = result["status_code"]
    return JSONResponse(
        status_code=status_code,
        content={
            "service": "frontend-api",
            "endpoint": "/backend-sick-partial",
            "status": "success" if status_code == 200 else "error",
            "message": "Proxied to backend /sick-partial — downstream partial failure pattern.",
            "downstream_result": result["body"],
            "pattern": "sick-but-not-dead",
        },
    )


@app.get("/backend-sick-db", tags=["sick-but-not-dead"])
async def backend_sick_db():
    """
    Proxy the /sick-db DB pool exhaustion pattern through to the backend.

    The backend /sick-db endpoint models:
      85% — slow (2–4 s pool contention), HTTP 200
      10% — pool exhausted, HTTP 503
       5% — fast lucky slot, HTTP 200

    Health check at /health is NOT DB-backed → always returns 200.
    Traditional monitoring: service UP.
    Observability: p95 latency 3 s, 10% error rate, in_flight requests rising.
    """
    result = await _call_backend("/sick-db")
    status_code = result["status_code"]
    return JSONResponse(
        status_code=status_code,
        content={
            "service": "frontend-api",
            "endpoint": "/backend-sick-db",
            "status": "success" if status_code == 200 else "error",
            "message": "Proxied to backend /sick-db — DB pool exhaustion pattern.",
            "downstream_result": result["body"],
            "pattern": "sick-but-not-dead",
        },
    )
