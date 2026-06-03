"""
loadgen/loadgen.py
------------------
Simple traffic generator for the Observability Learning project.

Continuously sends GET requests to the frontend-api at a fixed interval.
Requests are chosen randomly from a weighted list of endpoints so the
traffic mix looks realistic: mostly healthy, occasionally slow or broken.

This is intentionally a plain script — no frameworks, no async, nothing fancy.
Its only job is to make sure there is always something flowing through the
system so you have data to observe.

Configuration (environment variables)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  FRONTEND_URL         Base URL of the frontend service
                       Default: http://frontend-api:8080

  LOADGEN_INTERVAL_MS  Pause between requests in milliseconds
                       Default: 2000  (one request every 2 seconds)

Usage
~~~~~
  # Start the full stack including the load generator:
  docker compose --profile loadgen up --build

  # Stop the load generator only:
  docker compose stop loadgen
"""

import logging
import os
import random
import time

import requests

# ──────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] loadgen - %(message)s",
)
logger = logging.getLogger("loadgen")


# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://frontend-api:8080")
INTERVAL_MS: int  = int(os.getenv("LOADGEN_INTERVAL_MS", "2000"))


# ──────────────────────────────────────────────────────────────
# Endpoint weight table
#
# Each tuple is (path, weight).  Higher weight = chosen more often.
# Weights do not need to sum to 100; random.choices normalises them.
#
# This distribution creates realistic traffic:
#   - Most traffic is healthy  → keeps success-rate high but not 100 %
#   - A small fraction is slow → keeps latency percentiles non-trivial
#   - A small fraction errors  → keeps error rate visible but not alarming
#
# Learning value: change these weights to simulate different incident
# scenarios (e.g. set error weight to 50 to simulate an outage).
# ──────────────────────────────────────────────────────────────
_ENDPOINTS: list[tuple[str, int]] = [
    ("/health",              10),   # liveness checks
    ("/ok",                 35),   # fast frontend success
    ("/backend-ok",         28),   # cross-service success
    ("/slow",                5),   # frontend latency spike
    ("/backend-slow",        5),   # backend latency spike
    ("/error",               7),   # frontend error
    ("/backend-error",      10),   # cross-service error
    # ──────────────────────────────────────────────────────────
    # Sick but not dead — always present in baseline traffic.
    # Weights are intentionally small so these routes are visible
    # in metrics without dominating the overall error/latency picture.
    # This ensures the bimodal latency fingerprint is always there to
    # observe, even without the troublemaker running.
    # ──────────────────────────────────────────────────────────
    ("/sick",                3),   # log-normal jitter, always 200 — bimodal latency source
    ("/sick-partial",        3),   # 70/30 split — partial failure source
    ("/backend-sick",        2),   # downstream jitter — trace shows backend owns it
]

_PATHS   = [ep[0] for ep in _ENDPOINTS]
_WEIGHTS = [ep[1] for ep in _ENDPOINTS]


# ──────────────────────────────────────────────────────────────
# Helper — send one request and log the result
# ──────────────────────────────────────────────────────────────
def call_endpoint(path: str) -> None:
    url = f"{FRONTEND_URL}{path}"
    try:
        start = time.perf_counter()
        response = requests.get(url, timeout=15)
        duration_ms = (time.perf_counter() - start) * 1000

        logger.info(
            "GET %-20s  HTTP %d  %.0fms",
            path,
            response.status_code,
            duration_ms,
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("GET %-20s  FAILED  %s", path, exc)


# ──────────────────────────────────────────────────────────────
# Helper — wait until the frontend /health returns 200
#
# The load generator starts at the same time as the application
# containers. This function retries until the frontend is ready
# so we don't flood logs with connection-refused errors at startup.
# ──────────────────────────────────────────────────────────────
def wait_for_frontend(max_retries: int = 30, retry_delay_s: float = 2.0) -> None:
    health_url = f"{FRONTEND_URL}/health"
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(health_url, timeout=5)
            if resp.status_code == 200:
                logger.info("Frontend is ready — starting traffic generation")
                return
        except requests.exceptions.RequestException:
            pass
        logger.info(
            "Waiting for frontend to become ready  attempt=%d/%d",
            attempt,
            max_retries,
        )
        time.sleep(retry_delay_s)

    logger.warning(
        "Frontend did not respond after %d attempts — starting anyway",
        max_retries,
    )


# ──────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Load generator initialising")
    logger.info("Target   : %s", FRONTEND_URL)
    logger.info("Interval : %dms between requests", INTERVAL_MS)

    wait_for_frontend()

    interval_s = INTERVAL_MS / 1000.0
    while True:
        path = random.choices(_PATHS, weights=_WEIGHTS, k=1)[0]
        call_endpoint(path)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
