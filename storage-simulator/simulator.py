"""
storage-simulator/simulator.py
──────────────────────────────────────────────────────────────────────────────
Ceph / PVC storage failure simulator for the Observability Learning project.

Exposes Prometheus metrics on GET /metrics (port 9200).
Allows scenario triggering via POST /scenario/{name}.

Scenarios
─────────
  healthy          Normal operation — all OSDs up, pool at 40% fill, low latency.
  osd_down         One OSD goes offline — cluster WARN state, 32 degraded PGs.
  multi_osd_failure Two OSDs down — cluster ERROR state, 128 degraded PGs.
  pool_full        SSD pool at 95% capacity — writes will be throttled.
  latency_spike    All PVC latency 10-40x normal — IO pressure detected.
  noisy_pvc        pvc-app-0 consuming 10x normal IOPS — noisy neighbour.

Usage
─────
  docker compose up --build
  curl http://localhost:9200/health
  curl http://localhost:9200/scenarios
  curl http://localhost:9200/metrics
  curl -X POST http://localhost:9200/scenario/osd_down
  curl -X POST http://localhost:9200/scenario/healthy
──────────────────────────────────────────────────────────────────────────────
"""

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest, REGISTRY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("storage-simulator")

# ─────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ─────────────────────────────────────────────────────────────────────────────

osd_up = Gauge("storage_osd_up", "Ceph OSD up (1) or down (0)", ["osd_id"])
pool_used = Gauge("storage_pool_used_bytes", "Used bytes in storage pool", ["pool"])
pool_capacity = Gauge("storage_pool_capacity_bytes", "Total pool capacity in bytes", ["pool"])
io_latency = Gauge("storage_io_latency_seconds", "Average IO latency for a PVC", ["pvc"])
pvc_iops = Gauge("storage_pvc_iops", "IOPS for a PVC", ["pvc", "operation"])
cluster_health = Gauge(
    "storage_cluster_health_score",
    "Ceph cluster health: 2=OK  1=WARN  0=ERROR",
)
degraded_pgs = Gauge(
    "storage_degraded_placement_groups",
    "Number of degraded placement groups",
)
active_scenario_info = Gauge(
    "storage_simulator_active_scenario",
    "Currently active scenario (label identifies it; value=1 for active)",
    ["scenario"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Scenario definitions
# ─────────────────────────────────────────────────────────────────────────────

GB = 1024 ** 3

SCENARIOS: dict[str, dict[str, Any]] = {
    "healthy": {
        "osd_up":        {"osd.0": 1, "osd.1": 1, "osd.2": 1},
        "pool_used":     {"ssd-pool": 400 * GB, "hdd-pool": 1_200 * GB},
        "pool_capacity": {"ssd-pool": 1_000 * GB, "hdd-pool": 3_000 * GB},
        "io_latency":    {"pvc-app-0": 0.002, "pvc-app-1": 0.003, "pvc-db-0": 0.001},
        "pvc_iops": {
            ("pvc-app-0", "read"): 200, ("pvc-app-0", "write"): 80,
            ("pvc-app-1", "read"): 150, ("pvc-app-1", "write"): 60,
            ("pvc-db-0",  "read"): 500, ("pvc-db-0",  "write"): 300,
        },
        "cluster_health": 2,
        "degraded_pgs": 0,
    },
    "osd_down": {
        "osd_up":        {"osd.0": 0, "osd.1": 1, "osd.2": 1},  # one OSD down
        "pool_used":     {"ssd-pool": 400 * GB, "hdd-pool": 1_200 * GB},
        "pool_capacity": {"ssd-pool": 1_000 * GB, "hdd-pool": 3_000 * GB},
        "io_latency":    {"pvc-app-0": 0.012, "pvc-app-1": 0.010, "pvc-db-0": 0.008},
        "pvc_iops": {
            ("pvc-app-0", "read"): 140, ("pvc-app-0", "write"): 55,
            ("pvc-app-1", "read"): 130, ("pvc-app-1", "write"): 50,
            ("pvc-db-0",  "read"): 380, ("pvc-db-0",  "write"): 230,
        },
        "cluster_health": 1,
        "degraded_pgs": 32,
    },
    "multi_osd_failure": {
        "osd_up":        {"osd.0": 0, "osd.1": 0, "osd.2": 1},  # two OSDs down
        "pool_used":     {"ssd-pool": 400 * GB, "hdd-pool": 1_200 * GB},
        "pool_capacity": {"ssd-pool": 1_000 * GB, "hdd-pool": 3_000 * GB},
        "io_latency":    {"pvc-app-0": 0.050, "pvc-app-1": 0.045, "pvc-db-0": 0.040},
        "pvc_iops": {
            ("pvc-app-0", "read"):  60, ("pvc-app-0", "write"):  20,
            ("pvc-app-1", "read"):  55, ("pvc-app-1", "write"):  18,
            ("pvc-db-0",  "read"): 120, ("pvc-db-0",  "write"):  70,
        },
        "cluster_health": 0,
        "degraded_pgs": 128,
    },
    "pool_full": {
        "osd_up":        {"osd.0": 1, "osd.1": 1, "osd.2": 1},
        "pool_used":     {"ssd-pool": 950 * GB, "hdd-pool": 2_850 * GB},   # ssd at 95%
        "pool_capacity": {"ssd-pool": 1_000 * GB, "hdd-pool": 3_000 * GB},
        "io_latency":    {"pvc-app-0": 0.030, "pvc-app-1": 0.028, "pvc-db-0": 0.025},
        "pvc_iops": {
            ("pvc-app-0", "read"): 100, ("pvc-app-0", "write"):  10,  # writes starved
            ("pvc-app-1", "read"):  90, ("pvc-app-1", "write"):   8,
            ("pvc-db-0",  "read"): 300, ("pvc-db-0",  "write"):  30,
        },
        "cluster_health": 1,
        "degraded_pgs": 0,
    },
    "latency_spike": {
        "osd_up":        {"osd.0": 1, "osd.1": 1, "osd.2": 1},
        "pool_used":     {"ssd-pool": 400 * GB, "hdd-pool": 1_200 * GB},
        "pool_capacity": {"ssd-pool": 1_000 * GB, "hdd-pool": 3_000 * GB},
        "io_latency":    {"pvc-app-0": 0.080, "pvc-app-1": 0.075, "pvc-db-0": 0.090},  # 10-40x normal
        "pvc_iops": {
            ("pvc-app-0", "read"):  30, ("pvc-app-0", "write"):  10,
            ("pvc-app-1", "read"):  25, ("pvc-app-1", "write"):   8,
            ("pvc-db-0",  "read"): 100, ("pvc-db-0",  "write"):  40,
        },
        "cluster_health": 1,
        "degraded_pgs": 0,
    },
    "noisy_pvc": {
        "osd_up":        {"osd.0": 1, "osd.1": 1, "osd.2": 1},
        "pool_used":     {"ssd-pool": 400 * GB, "hdd-pool": 1_200 * GB},
        "pool_capacity": {"ssd-pool": 1_000 * GB, "hdd-pool": 3_000 * GB},
        # pvc-app-0 is thrashing; neighbors see elevated latency
        "io_latency":    {"pvc-app-0": 0.002, "pvc-app-1": 0.018, "pvc-db-0": 0.015},
        "pvc_iops": {
            ("pvc-app-0", "read"):  2_500, ("pvc-app-0", "write"):  1_800,  # 10x noisy
            ("pvc-app-1", "read"):     80, ("pvc-app-1", "write"):     30,
            ("pvc-db-0",  "read"):    200, ("pvc-db-0",  "write"):    120,
        },
        "cluster_health": 2,
        "degraded_pgs": 0,
    },

    # ──────────────────────────────────────────────────────────────────────
    # SICK BUT NOT DEAD — Storage Brownout
    #
    # The most pedagogically powerful scenario: ALL health checks report OK
    # while IO latency is 8–18x above the normal baseline.
    #
    # What traditional monitoring sees:
    #   storage_osd_up        →  all 3 OSDs = 1 (UP)         ← GREEN
    #   storage_cluster_health →  2 = OK                      ← GREEN
    #   storage_degraded_pgs  →  0                            ← GREEN
    #   pool fill %           →  65% (below 85% warn thresh)  ← GREEN
    #   PVCHighLatency alert  →  threshold is 50ms; sick is ~18-30ms ← GREEN
    #
    # What observability detects:
    #   storage_io_latency_seconds{pvc="pvc-db-0"} → 0.018 (vs 0.001 normal = 18x)
    #   SickStorageBrownout alert fires (threshold 15ms, all OSDs up, health=2)
    #
    # This gap is the brownout signature.
    # Real-world causes: NVMe queue depth saturation, shared SAS bus contention,
    # Ceph scrub running at max priority during business hours,
    # background rebalance after adding a new OSD consuming IO bandwidth.
    # ──────────────────────────────────────────────────────────────────────
    "sick_storage": {
        # OSDs: ALL UP — health check passes, CephOSDDown does NOT fire
        "osd_up":        {"osd.0": 1, "osd.1": 1, "osd.2": 1},
        # Pool at 65% — below the 85% CephPoolNearFull warning threshold
        "pool_used":     {"ssd-pool": 650 * GB, "hdd-pool": 1_950 * GB},
        "pool_capacity": {"ssd-pool": 1_000 * GB, "hdd-pool": 3_000 * GB},
        # IO latency 8–18x normal:
        #   pvc-app-0: 0.002 → 0.025 s  (12x)   CephPVCHighLatency threshold=0.050 → no alert!
        #   pvc-app-1: 0.003 → 0.030 s  (10x)
        #   pvc-db-0:  0.001 → 0.018 s  (18x)   SickStorageBrownout threshold=0.015 → FIRES
        "io_latency":    {"pvc-app-0": 0.025, "pvc-app-1": 0.030, "pvc-db-0": 0.018},
        # IOPS elevated but below the 2000 NoisyPVCDetected threshold
        "pvc_iops": {
            ("pvc-app-0", "read"): 420, ("pvc-app-0", "write"): 180,
            ("pvc-app-1", "read"): 380, ("pvc-app-1", "write"): 150,
            ("pvc-db-0",  "read"): 950, ("pvc-db-0",  "write"): 580,
        },
        # Cluster health = 2 (OK) — the deception: reports HEALTHY
        "cluster_health": 2,
        "degraded_pgs": 0,
    },
}

# Active scenario name (module-level mutable string)
_active_scenario: list[str] = ["healthy"]  # list so nested function can mutate


def _apply(name: str) -> None:
    """Write scenario values to all registered Prometheus gauges."""
    sc = SCENARIOS[name]

    for osd_id, val in sc["osd_up"].items():
        osd_up.labels(osd_id=osd_id).set(val)

    for pool, val in sc["pool_used"].items():
        pool_used.labels(pool=pool).set(val)

    for pool, val in sc["pool_capacity"].items():
        pool_capacity.labels(pool=pool).set(val)

    for pvc, val in sc["io_latency"].items():
        io_latency.labels(pvc=pvc).set(val)

    for (pvc, op), val in sc["pvc_iops"].items():
        pvc_iops.labels(pvc=pvc, operation=op).set(val)

    cluster_health.set(sc["cluster_health"])
    degraded_pgs.set(sc["degraded_pgs"])

    for sc_name in SCENARIOS:
        active_scenario_info.labels(scenario=sc_name).set(1 if sc_name == name else 0)

    _active_scenario[0] = name
    logger.info("Scenario applied: %s", name)


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI application
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(application: FastAPI):
    _apply("healthy")
    logger.info("Storage simulator ready  scenario=healthy  port=9200")
    yield


app = FastAPI(
    title="storage-simulator",
    description="Simulates Ceph/PVC storage failures and exposes Prometheus metrics.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "scenario": _active_scenario[0]}


@app.get("/scenarios")
async def list_scenarios() -> dict:
    return {
        "active": _active_scenario[0],
        "available": list(SCENARIOS.keys()),
        "descriptions": {
            "healthy":          "All OSDs up, pool at 40% fill, normal latency",
            "osd_down":         "OSD osd.0 down — cluster WARN, 32 degraded PGs",
            "multi_osd_failure":"OSD osd.0 + osd.1 down — cluster ERROR, 128 degraded PGs",
            "pool_full":        "SSD pool at 95% capacity — writes throttled",
            "latency_spike":    "All PVC latency 10-40x normal — IO pressure",
            "noisy_pvc":        "pvc-app-0 at 10x IOPS — noisy neighbour affecting pvc-app-1 and pvc-db-0",
            "sick_storage":     "SICK BUT NOT DEAD: all OSDs up, cluster_health=2 (OK), IO latency 8-18x normal — storage brownout",
        },
    }


@app.post("/scenario/{name}")
async def set_scenario(name: str) -> dict:
    if name not in SCENARIOS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown scenario '{name}'. Available: {list(SCENARIOS.keys())}",
        )
    _apply(name)
    return {"status": "ok", "scenario": name}


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus text format scrape endpoint."""
    return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
