# Release Notes

---

## v15.0.0 — Pattern Intelligence Library (PostgreSQL + pgvector)

**Release date:** April 19, 2026

Implements the foundational **Pattern → Decision → Action** layer from `NewProject.md` — a structured failure pattern library backed by PostgreSQL 16 + pgvector that becomes the shared brain connecting the observability lab, the public discovery pipeline, and the compute agent reasoning engine.

### New Services

| Service | Port | Image | Purpose |
|---|---|---|---|
| `pattern-db` | 5432 | `pgvector/pgvector:pg16` | PostgreSQL 16 with pgvector extension; 7-table schema; HNSW cosine-similarity indexes on 384-dim embeddings |
| `pattern-library` | 9300 | `./pattern-library` | FastAPI service — pattern CRUD, semantic search, hybrid incident matching, n8n ingestion endpoint |

### New Files

| File/Dir | Purpose |
|---|---|
| `pattern-library/Dockerfile` | Python 3.11-slim image; asyncpg + FastAPI + httpx |
| `pattern-library/requirements.txt` | fastapi, uvicorn, asyncpg, httpx, pydantic |
| `pattern-library/migrations/001_init.sql` | 7-table schema + HNSW indexes; auto-applied by PostgreSQL on first start |
| `pattern-library/app/main.py` | FastAPI app — 20 endpoints across CRUD, search, matching, assessments, admin |
| `pattern-library/app/db.py` | asyncpg connection pool (text-based vector handling — no numpy required) |
| `pattern-library/app/embedder.py` | Ollama nomic-embed-text embeddings; graceful degradation when offline |
| `pattern-library/app/models.py` | Pydantic v2 request/response models |
| `pattern-library/app/seeder.py` | 5 foundational patterns with full signal/fix records; idempotent startup seed |

### 7-Table Schema

| Table | Records |
|---|---|
| `raw_public_issues` | Raw content from n8n public discovery (GitHub, SO, Reddit, HN, blogs) |
| `enriched_issues` | LLM-processed pain points + 384-dim embeddings |
| `patterns` | Core library — severity, recurrence score, confidence, OSS angle, source refs, embedding |
| `pattern_signals` | Detection signals: metric thresholds (`threshold_operator`, `threshold_value`), log patterns, TraceQL, alert names |
| `pattern_fixes` | Ordered fixes: autonomous → approval_required → manual; includes full YAML/Ansible content |
| `lab_validations` | Lab reproduction results; increments `evidence_count` on positive detection |
| `agent_assessments` | Session → matched patterns → outcome; incident embedding for future discovery |

### Hybrid Matching (`POST /patterns/match-incident`)

The core intelligence endpoint used by compute/storage agents:

```
combined_score = 0.6 × rule_score + 0.4 × vector_similarity
```

- **rule_score**: fraction of `pattern_signals` thresholds satisfied by caller-supplied `signals` dict
- **vector_similarity**: cosine similarity between incident embedding and pattern embedding
- Falls back to pure rule scoring when Ollama is offline (embeddings are NULL)

### 5 Foundational Patterns

| Pattern | Severity | Recurrence | Automation |
|---|---|---|---|
| `cpu_throttling_resource_limits` | high | 0.85 | risky |
| `collector_pipeline_backpressure` | high | 0.78 | manual |
| `prometheus_scrape_miss` | medium | 0.72 | safe |
| `oom_kill_resource_misconfiguration` | critical | 0.80 | risky |
| `misleading_p99_histogram_bucket_saturation` | low | 0.68 | safe |

Each pattern has 3–5 detection signals and 2–3 fixes with full PromQL/config/Ansible content.

### Modified Files

| File | Change |
|---|---|
| `docker-compose.yml` | Added `pattern-db`, `pattern-library` services; `pattern-db-data` volume; `PATTERN_LIBRARY_URL` env var in compute-agent and streamlit-dashboard |
| `README.md` | Phase 15 section; pattern-db and pattern-library in service map |

---

## v8.0.0 — Local LLM Validation, Knowledge Store & AIOps Command Center UI

**Release date:** March 21, 2026

This release completes the full agentic AIOps loop with two major additions:

1. **Block F — Local LLM Validation + ChromaDB Knowledge Store**: A self-hosted Ollama instance (`llama3.2:3b`) performs a second-opinion validation of every external LLM (GPT-4/Claude) analysis, while ChromaDB stores resolved incident embeddings for cosine-similarity retrieval.  Verdict — `corroborated`, `weak_support`, `divergent`, or `insufficient_context` — is propagated through the full pipeline to the xyOps ticket and Grafana dashboards.

2. **AIOps Command Center UI (Phase 9)**: A React + TypeScript single-page application (`command-center`) with a BFF aggregator (`ui-backend`) provides real-time pipeline visualisation, session playback, incident dashboards, scenario explorer, trust-score tracking, and the new Local LLM Validation drawer panel.

All 20+ containers are verified running and healthy.  Unit tests: 35/35 green.

---

### New Services

| Service | Port | Image | Purpose |
|---|---|---|---|
| `local-llm` | 11434 | `ollama/ollama:latest` | Hosts `llama3.2:3b` (corroboration) + `nomic-embed-text` (embeddings); 2.3 GB pulled at startup |
| `knowledge-store` | 8020→8000 | `chromadb/chroma:latest` | Vector store for incident resolutions; persisted in `chroma-data` volume |
| `ui-backend` | 9005 | `./ui-backend` | BFF aggregator — unified REST API for the React frontend; aggregates compute-agent, storage-agent, obs-intelligence, Gitea, xyOps |
| `aiops-ui` | 3005 | `nginx:1.25-alpine` | Production Nginx serving the React build; proxies `/api/*` to ui-backend |
| `command-center` | 3500 | `./command-center` | Vite dev server for the React Command Center UI |

---

### New Files

| File | Purpose |
|---|---|
| `obs-intelligence/app/obs_intelligence/local_llm_enricher.py` | ChromaDB + Ollama module; 6 public methods: `query_similar_incidents`, `validate_external_result`, `store_incident_resolution`, `update_incident_outcome`, `list_entries`, `knowledge_stats` |
| `obs-intelligence/app/obs_intelligence/incident_coordinator.py` | Cross-domain incident state tracker |
| `obs-intelligence/app/obs_intelligence/learning_store.py` | Outcome learning persistence |
| `obs-intelligence/app/obs_intelligence/outcome_store.py` | Outcome record CRUD |
| `obs-intelligence/tests/test_local_llm_enricher.py` | 35 unit tests for `local_llm_enricher.py` — 7 test classes, runs fully offline |
| `compute-agent/app/autonomy_engine.py` | Centralized autonomy decision engine |
| `compute-agent/app/tier_registry.py` | Service tier + trust score registry |
| `compute-agent/app/approval_history.py` | Persistent approval history management |
| `ui-backend/` | BFF aggregator service (FastAPI) |
| `command-center/` | React + TypeScript Command Center frontend |
| `aiops-ui/nginx.conf` | Production Nginx configuration |
| `grafana/provisioning/dashboards/ui-backend.json` | Grafana dashboard for UI Backend metrics |
| `LEARNING-LAYER-ARCHITECTURE.md` | Deep-dive architecture doc for the learning/feedback layer |
| `SIMULATION-SCENARIO.md` | Guide for triggering simulation scenarios end-to-end |

---

### Modified Files

| File | Change |
|---|---|
| `obs-intelligence/app/obs_intelligence/llm_enricher.py` | Inline dual-validation: external LLM result → ChromaDB similarity search → local Ollama corroboration; result fields `local_validation_status`, `local_validation_confidence`, `local_validation_reason`, `local_validation_completed`, `knowledge_top_similarity`, `local_model`, `source`, `validation_mode`, `validated_by`, `local_similar_count` added to `LLMEnrichment` |
| `obs-intelligence/app/main.py` | New endpoints: `GET /knowledge/stats`, `GET /knowledge/entries`, `POST /knowledge/store`, `POST /knowledge/outcome`; Block F env var wiring |
| `obs-intelligence/requirements.txt` | Added `chromadb>=0.4.22`, `ollama>=0.1.7` |
| `compute-agent/app/pipeline.py` | Full 6-step pipeline with Block F; `GET /session/{id}` returns all Block F fields |
| `compute-agent/app/approval_workflow.py` | Outcome feedback via `_record_llm_outcome` calls ChromaDB on resolution |
| `storage-agent/app/pipeline.py` | `GET /session/{session_id}` extended with all Block F fields mirroring compute-agent shape |
| `grafana/provisioning/dashboards/obs-intelligence-detail.json` | New "Block F — Local LLM Validation" row with 3 panels: External LLM Validations rate, Local Validation Outcomes (by verdict with colour overrides), Local Validation Latency p50/p95 |
| `alertmanager/alertmanager.yml` | Fixed duplicate `http_config` + `max_alerts` under `obs-intelligence` receiver |
| `docker-compose.yml` | Added `local-llm`, `knowledge-store`, `ui-backend`, `aiops-ui`, `command-center` services; Block F env vars added to all three agents |

---

### Deleted Files

| File | Reason |
|---|---|
| `obs-intelligence/app/obs_intelligence/local_validator.py` | Orphaned predecessor — replaced by `local_llm_enricher.py`; no imports anywhere |

---

### Block F — Local LLM Validation Details

#### How It Works

```
External LLM result (GPT-4o / Claude)
    │
    ▼
ChromaDB similarity search (top-K similar past incidents)
    │  cosine similarity via nomic-embed-text embeddings
    ▼
Ollama llama3.2:3b corroboration prompt
    │  "Given these similar past incidents, does this analysis hold?"
    ▼
LocalValidationResult
    verdict:    corroborated | weak_support | divergent | insufficient_context
    confidence: 0.0–1.0
    reason:     one-paragraph rationale
    │
    ▼
Enriched LLMEnrichment (passed to xyOps ticket + Grafana + UI drawer)
```

#### Verdict Semantics

| Verdict | Meaning | UI Color |
|---|---|---|
| `corroborated` | Local and external analyses agree | Green |
| `weak_support` | Partial agreement; minor divergence | Amber |
| `divergent` | Significant contradiction; manual review recommended | Red |
| `insufficient_context` | Not enough past incidents to validate | Purple |
| `unavailable` | Ollama/ChromaDB service offline; graceful degradation | Grey |

#### ChromaDB Knowledge Accumulation

- Each resolved incident is stored as a vector embedding (via `nomic-embed-text`) with metadata: `service_name`, `alert_name`, `scenario_id`, `root_cause`, `resolution`, `outcome`, `timestamp`.
- The collection grows over time — more incidents → better similarity matches → higher validation confidence.
- Use `GET http://localhost:9100/knowledge/stats` to inspect collection size and corroboration rate.

---

### AIOps Command Center UI Details

#### Architecture

```
Browser
  └── http://localhost:3005 (aiops-ui / Nginx)
       └── /api/* → ui-backend:9005
            ├── GET /pipeline/history          (all sessions)
            ├── GET /pipeline/session/{id}     (live state)
            ├── GET /scenarios                 (20 scenarios)
            ├── GET /autonomy/status           (trust tiers)
            └── ...

  OR http://localhost:3500 (command-center, Vite dev mode)
```

#### UI Features

- **Pipeline Graph** — horizontal React Flow node graph; 6 agent steps colour-coded by status (idle / running / success / failed / skipped)
- **Agent Details Drawer** — click any node to inspect full JSON output; Agent 4 (Analyze) shows:
  - External LLM root cause narrative
  - Local LLM Validation section (verdict badge, confidence progress bar, similarity stats, reasoning box)
- **Incident Dashboard** — risk score, risk level badge, scenario match, autonomy decision, trust score
- **Session Selector** — switch between active and historical incidents
- **Scenario Explorer** — all 20 scenarios with match counts and confidence history
- **Trust Score Tracker** — progress bar showing approvals toward next autonomy tier graduation
- **Playback Mode** — `GET /pipeline/session/{id}/snapshot` replays a historical session step-by-step

---

### Grafana Dashboard Changes

#### `obs-intelligence-detail.json` — New Row

**Row: "🧠 Block F — Local LLM Validation"** (y = 35)

| Panel | Metric | Visualisation |
|---|---|---|
| External LLM Validations | `obs_intelligence_external_validation_total` | Stacked bars by status |
| Local LLM Validation Outcomes | `obs_intelligence_local_validation_total{status}` | Bars with colour overrides per verdict |
| Local Validation Latency | `histogram_quantile(0.5|0.95, obs_intelligence_local_validation_duration_seconds_bucket)` | Line chart p50 + p95 |

---

### Unit Tests

**`obs-intelligence/tests/test_local_llm_enricher.py`** — 35 tests, 7 classes

| Class | Tests | What It Covers |
|---|---|---|
| `TestKnowledgeEntry` | 2 | `similarity()` method and cosine distance boundary |
| `TestLocalValidationResult` | 2 | `to_dict()` serialization, default field values |
| `TestQuerySimilarIncidents` | 6 | Happy path, domain filtering, error resilience, collection caching |
| `TestValidateExternalResult` | 6 | Corroborated verdict, no similar incidents, disabled flag, invalid verdict JSON, JSON parse error, Ollama error |
| `TestStoreIncidentResolution` | 5 | Happy path, no embedding generated, sensitive key stripping, service unavailable, upsert error |
| `TestUpdateIncidentOutcome` | 4 | Direct ID lookup, metadata fallback, not found, unavailable |
| `TestListEntries` | 4 | No filters, `service_name` filter, combined filters, unavailable |
| `TestKnowledgeStats` + `TestGetCollection` | 6 | Count aggregation, corroboration rate, empty collection, unavailable, import fail, caching |

Run with:
```bash
cd obs-intelligence
pip install -r requirements.txt pytest pytest-asyncio
pytest tests/test_local_llm_enricher.py -v
```

---

### Environment Variables Added

| Variable | Service | Default | Description |
|---|---|---|---|
| `LOCAL_LLM_URL` | compute-agent, storage-agent, obs-intelligence | `http://local-llm:11434` | Ollama REST API base URL |
| `CHROMA_URL` | compute-agent, storage-agent, obs-intelligence | `http://knowledge-store:8000` | ChromaDB HTTP API URL |
| `LOCAL_LLM_MODEL` | compute-agent, storage-agent, obs-intelligence | `llama3.2:3b` | Ollama model used for corroboration |
| `LOCAL_LLM_ENABLED` | compute-agent, storage-agent, obs-intelligence | `true` | Set `false` to disable local validation entirely |
| `LOCAL_LLM_MIN_SIMILARITY` | obs-intelligence | `0.82` | Minimum cosine similarity to count a past incident as relevant |
| `LOCAL_LLM_TOP_K` | obs-intelligence | `5` | Max similar incidents retrieved from ChromaDB |
| `LOCAL_LLM_VALIDATION_ONLY` | obs-intelligence | `true` | Use local LLM for validation only (not generation) |
| `COMPUTE_AGENT_URL` | ui-backend | `http://compute-agent:9000` | Compute agent base URL |
| `STORAGE_AGENT_URL` | ui-backend | `http://storage-agent:9001` | Storage agent base URL |
| `GITEA_TOKEN` | ui-backend | _(auto-generated)_ | Gitea API token for PR links |
| `DB_PATH` | ui-backend | `/data/pipeline_history.db` | SQLite path for pipeline session history |

---

### Known Limitations / Next Steps

- **Real Ansible targets**: Playbooks are generated and PR-branched but execute as dry-run. Set `ANSIBLE_LIVE_MODE=true` and provide real inventory for production use.
- **Trust tier accumulation**: Trust scores require real approved remediations to graduate. On a fresh stack all agents start at `low_trust` (human-only).
- **ChromaDB cold start**: The knowledge store is empty on first run. Local validation returns `insufficient_context` until enough incidents have been stored and resolved.
- **GPU acceleration**: `local-llm` runs on CPU by default. For faster inference add `deploy: resources: reservations: devices: - driver: nvidia` to the service definition.
- **Authentication**: Agent webhooks and obs-intelligence `/analyze` have no bearer token validation. Add API key middleware before exposing to untrusted networks.

---

## v7.0.0 — SRE Reasoning Layer

See [README.md — Phase 7](README.md#phase-7--sre-reasoning-layer) for details.

Key additions:
- `sre_reasoning_agent.py` — fully deterministic structured `SREAssessment`
- LLM enricher redesigned as a narrative writer receiving the SREAssessment as context
- OpenAI → Claude automatic failover
- Outcome recording (`POST /intelligence/record-outcome`) and alert resolution hooks
- Grafana "SRE Incident Timeline" dashboard (4 rows, 12 panels)
- `CONTRIBUTING-SCENARIOS.md` and `DEMO-RUNBOOK.md` added

---

## v6.0.0 — Continuous Intelligence & Predictive Alerts

Key additions:
- Background analysis loops: anomaly scan every 60 s, forecast + predictive dispatch every 5 min
- `POST /predictive-alert` on both agents — `[PREDICTIVE]`-tagged xyOps tickets
- New Prometheus alert rules: `NoAlertFiringButHighRisk`, `HighAnomalyZScore`
- Predictive Alert Workflow in xyOps for both domains

---

## v5.0.0 — Risk Scoring, Evidence Builder & LLM Enrichment

Key additions:
- `risk_scorer.py`, `recommender.py`, `evidence_builder.py`, `llm_enricher.py`
- `anomaly_detector.py` (Z-score), `forecaster.py` (numpy linear regression)
- Full 6-step intelligence pipeline in both agents

---

## v4.0.0 — Shared Obs-Intelligence Engine

Key additions:
- `obs_intelligence` Python package with 20 scenario YAML files
- FastAPI service exposing `/analyze`, `/intelligence/current`
- Two Grafana dashboards: `agentic-ai-overview.json`, `obs-intelligence-detail.json`

---

## v3.0.0 — Domain Split & Storage Agent

Key additions:
- `compute-agent` evolved from `aiops-bridge`
- New `storage-agent` with storage-domain autonomy rules
- `storage-simulator` (Ceph scenario emulator)

---

## v2.0.0 — Alert Pipeline & AIOps Integration

Key additions:
- Prometheus alert rules, Alertmanager routing
- `aiops-bridge` webhook → xyOps ticket → Ansible playbook
- Gitea auto-provisioned

---

## v1.0.0 — Core Observability Stack

Initial release:
- OTel Collector, Prometheus, Tempo, Loki, Grafana
- `frontend-api`, `backend-api`, `loadgen`, `troublemaker`
