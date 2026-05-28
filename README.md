# Observability Learning — Multi-Agent AIOps Platform

A full-stack, containerised **AIOps observability platform** that closes the loop from Prometheus alert → AI analysis → local LLM corroboration → enriched incident ticket → human-approved Ansible remediation — with a shared intelligence engine, ChromaDB knowledge store, and a **Streamlit Command Center** with live agent mesh topology.

> **Latest:** v14.0.0 — Multi-page dashboard + SSE + edge bandwidth labels + notification integrations.
> Streamlit split into 7 page files (`pages/*.py`) with shared config (`shared.py`); dark/light theme toggle;
> SSE `/pipeline/events` on compute-agent and storage-agent; vis.js click-to-drill and edge bandwidth labels;
> Slack/PagerDuty/email alertmanager receivers routed by severity.  See [RELEASE-NOTES.md](RELEASE-NOTES.md).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Service Map & Ports](#service-map--ports)
- [What Has Been Built](#what-has-been-built)
  - [Phase 1 — Core Observability Stack](#phase-1--core-observability-stack)
  - [Phase 2 — Alert Pipeline & AIOps Integration](#phase-2--alert-pipeline--aiops-integration)
  - [Phase 3 — Domain Split & Storage Agent](#phase-3--domain-split--storage-agent)
  - [Phase 4 — Shared Obs-Intelligence Engine](#phase-4--shared-obs-intelligence-engine-oie)
  - [Phase 5 — Risk Scoring, Evidence Builder & LLM Enrichment](#phase-5--risk-scoring-evidence-builder--llm-enrichment)
  - [Phase 6 — Continuous Intelligence & Predictive Alerts](#phase-6--continuous-intelligence--predictive-alerts)
  - [Phase 7 — SRE Reasoning Layer](#phase-7--sre-reasoning-layer)
  - [Phase 8 — Local LLM Validation & ChromaDB Knowledge Store](#phase-8--local-llm-validation--chromadb-knowledge-store-block-f)
  - [Phase 9 — AIOps Command Center UI (React)](#phase-9--aiops-command-center-ui-react)
  - [Phase 10 — Streamlit Command Center + Agent Mesh + qwen3.5](#phase-10--streamlit-command-center--agent-mesh--qwen35)
  - [Phase 11 — Multi-agent Cross-Domain Correlation](#phase-11--multi-agent-cross-domain-correlation)
  - [Phase 12 — SREAssessment Feedback Loop (RL-lite)](#phase-12--sreassessment-feedback-loop-rl-lite)
  - [Phase 13 — Persistence, Cold-Start & Fine-Tuning](#phase-13--persistence-cold-start--fine-tuning)
  - [Phase 14 — Multi-Page Dashboard, SSE & Notifications](#phase-14--multi-page-dashboard-sse--notifications)
- [What Is Still To Add](#what-is-still-to-add)
  - [Phase 14 — Dashboard Enhancements (remaining)](#phase-14--dashboard-enhancements-remaining)
  - [Phase 14 — Intelligence & Autonomy](#phase-14--intelligence--autonomy)
  - [Phase 14 — Integrations & Hardening](#phase-14--integrations--hardening)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Key Workflows](#key-workflows)
- [Environment Variables Reference](#environment-variables-reference)
- [Release Notes](#release-notes)

---

## Architecture Overview

```
                         ┌─────────────────────────────────────────────┐
                         │         Observability Stack                  │
                         │  Prometheus · Loki · Tempo · Grafana · OTEL  │
                         └───────────────┬─────────────────────────────┘
                                         │  alerts
                                         ▼
                                   Alertmanager
                                  /             \
                    domain=compute               domain=storage
                         │                            │
                         ▼                            ▼
                  Compute Agent                 Storage Agent
                  (port 9000)                   (port 9001)
                         │                            │
                         └──────────┬─────────────────┘
                                    │  POST /analyze
                                    ▼
                        Obs-Intelligence Engine
                              (port 9100)
                    ┌──────────────────────────────────┐
                    │  Scenario Correlator              │
                    │  Feature Extractor                │
                    │  Risk Scorer                      │
                    │  Anomaly Detector (Z-score)       │
                    │  Forecaster (numpy linreg)        │
                    │  Recommender                      │
                    │  Evidence Builder                 │
                    │  SRE Reasoning Agent (det.)       │
                    │  LLM Enricher (OpenAI → Claude)   │
                    │  Local LLM Enricher ◄─────────────┼──► Ollama :11434
                    │    ChromaDB similarity search ◄───┼──► ChromaDB :8020
                    │  Background: predictive dispatch  │
                    └──────────────────────────────────┘
                                    │
                                    ▼
                               xyOps (5522)
                    ┌──────────────────────────────┐
                    │  Incident Ticket              │
                    │  Approval Workflow Canvas     │
                    │  Job Executor                 │
                    └──────────────────────────────┘
                                    │ approved
                                    ▼
                           ansible-runner (8090)
                                    │
                                    ▼
                             Gitea PR / Audit (3002)

                    ─────── Command Center (Streamlit) ──────────────
                    Browser → streamlit-dashboard :3500
                    ┌─────────────────────────────────────────────────┐
                    │  📊 Overview       — health grid + activity feed │
                    │  🔴 Live Pipeline  — 6-agent flow + AI analysis  │
                    │  ⏳ Approvals      — pending PRs + approve/reject │
                    │  ✅ Outcomes       — per-run E2E results          │
                    │  📜 History        — filterable session table     │
                    │  🤖 Autonomy       — trust tiers + decision dist  │
                    │  🌐 Agent Mesh     — live vis.js topology graph   │
                    └─────────────────┬───────────────────────────────┘
                                      │
                              ui-backend :9005
                    ┌──────────────────────────────┐
                    │  BFF Aggregator (FastAPI)     │
                    │  Pipeline history + playback  │
                    │  Scenario explorer            │
                    │  Autonomy / trust-score API   │
                    └──────────────────────────────┘
```

**Alert routing** (alertmanager):
- Labels `domain: compute` → Compute Agent
- Labels `domain: storage` → Storage Agent
- Both agents call the shared Obs-Intelligence Engine for scenario matching, risk scoring, and LLM-enriched recommendations before creating a xyOps ticket.

---

## Service Map & Ports

| Service | Port(s) | Purpose |
|---|---|---|
| Service | Port(s) | Purpose |
|---|---|---|
| **otel-collector** | 4317 (gRPC), 4318 (HTTP), 8889 (Prom scrape), 13133 (health) | Receives all OTLP telemetry, exports to Prometheus / Tempo / Loki |
| **prometheus** | 9090 | Metrics TSDB + alert rule evaluation |
| **loki** | 3100 | Log aggregation (LogQL) |
| **tempo** | 3200 | Distributed trace storage (TraceQL) |
| **grafana** | 3001 | Dashboards — metrics, traces, logs, agent decisions |
| **alertmanager** | 9093 | Alert grouping/dedup + webhook routing |
| **compute-agent** | 9000 | Compute AIOps agent — `POST /webhook`, `GET /health`, `GET /session/{id}` |
| **storage-agent** | 9001 | Storage AIOps agent — `POST /webhook`, `GET /health`, `GET /session/{id}` |
| **obs-intelligence** | 9100 | Shared intelligence engine — `/analyze`, `/intelligence/current`, `/knowledge/*` |
| **storage-simulator** | 9200 | Ceph scenario emulator — `POST /scenario/{name}`, `GET /metrics` |
| **local-llm** | 11434 | Ollama — hosts `qwen3.5` (corroboration) + `nomic-embed-text` (embeddings) |
| **knowledge-store** | 8020 | ChromaDB vector store — incident embeddings for similarity retrieval |
| **xyops** | 5522, 5523 | AIOps platform — tickets, workflows, job scheduler |
| **ansible-runner** | 8090 | Playbook executor — `POST /run` |
| **gitea** | 3002 | Self-hosted Git — Ansible PR audit trail |
| **ui-backend** | 9005 | BFF aggregator — unified API for Command Center (`/pipeline/*`, `/scenarios`, `/autonomy`) |
| **streamlit-dashboard** | 3500 | Streamlit Command Center — 7-tab live dashboard (replaces React `aiops-ui` + `command-center`) |
| **pattern-db** | 5432 | PostgreSQL 16 + pgvector — Pattern Intelligence Library (7-table schema, HNSW indexes) |
| **pattern-library** | 9300 | Pattern Library API — CRUD, semantic search, hybrid incident-to-pattern matching |
| **frontend-api** | 8080 | Demo app — emits OTel spans/metrics/logs |
| **backend-api** | 8081 | Demo app — emits OTel spans/metrics/logs |
| **loadgen** | — | Steady traffic generator (opt-in profile) |
| **troublemaker** | 8088 | Compute chaos engine — CPUSpike, MemoryLeak, etc. (opt-in profile) |

---

## What Has Been Built

### Phase 1 — Core Observability Stack
- **OTel Collector** with Prometheus, Tempo, and Loki exporters
- **Grafana** with pre-provisioned datasources (Prometheus, Tempo, Loki) and cross-linking (exemplar → trace → log)
- **frontend-api / backend-api** demo services instrumented with OTel SDK (spans + metrics + logs)
- **loadgen** and **troublemaker** for realistic traffic and chaos scenarios

### Phase 2 — Alert Pipeline & AIOps Integration
- **Prometheus alert rules** (`prometheus/alert-rules.yml`) for CPU, memory, latency, error rate, storage metrics
- **Alertmanager** routing: compute vs storage domain labels, inhibition rules
- **aiops-bridge** (now `compute-agent`) — webhook receiver, AI analysis, xyOps ticket creation, Ansible execution
- **xyOps** — AIOps platform provisioned with incident workflows and approval canvas
- **ansible-runner** — dry-run playbook executor; set `ANSIBLE_LIVE_MODE=true` for real hosts
- **Gitea** — auto-provisioned with org `aiops-org` / repo `ansible-playbooks`; every approval creates a PR branch

### Phase 3 — Domain Split & Storage Agent
- **compute-agent** — evolved from aiops-bridge; now emits `compute_agent_*` Prometheus metrics
- **storage-agent** — new storage-domain agent mirroring compute-agent architecture
  - Autonomy rules: `HUMAN_ONLY` actions (multi-OSD), `APPROVAL_REQUIRED` (OSD reweight, pool expand), `AUTONOMOUS` (PVC throttle)
- **storage-simulator** — Ceph scenario emulator (healthy / osd_down / multi_osd_failure / pool_full / latency_spike / noisy_pvc)

### Phase 4 — Shared Obs-Intelligence Engine (OIE)
- **obs_intelligence Python package** (`obs-intelligence/app/obs_intelligence/`):
  - `scenario_loader.py` — loads YAML scenario definitions from `obs-intelligence/scenarios/`
  - `scenario_correlator.py` — matches features against scenario thresholds with confidence weighting
  - `feature_extractor.py` — maps Prometheus metrics + Loki logs → typed `ObsFeatures`
  - `models.py` — shared Pydantic models (`ObsFeatures`, `ScenarioMatch`, `RiskAssessment`, `Recommendation`, `EvidenceReport`)
  - `telemetry_client.py` — cached Prometheus + Loki query client
- **20 scenario YAML files** — 10 compute (`cpu_saturation`, `memory_leak_emergence`, `error_spike`, `latency_regression`, `noisy_neighbor_effect`, `queue_backlog`, `cascading_timeout_chain`, `baseline_shift_after_deploy`, `collector_overload`, `recurring_failure_signature`) + 10 storage (`ceph_rebalance_storm`, `cluster_degraded_health`, `multi_osd_failure`, `pool_full_critical`, `pool_near_full`, `pool_fill_forecast_breach`, `single_osd_down`, `pvc_latency_degradation`, `noisy_pvc_iops`, `storage_io_brownout`)
- **obs-intelligence FastAPI service** (`main.py`) — exposes `GET /health`, `GET /metrics`, `GET /intelligence/current`, `POST /analyze`
- **Background analysis loops** (`background.py`) — APScheduler: anomaly scan every 60 s, forecasting every 5 min
- Two Grafana dashboards: `agentic-ai-overview.json` and `obs-intelligence-detail.json`

### Phase 5 — Risk Scoring, Evidence Builder & LLM Enrichment
- `risk_scorer.py` — weighted scoring: 30% severity + 40% scenario confidence + 15% log anomaly + 15% forecast urgency → 0.0–1.0 `RiskAssessment`
- `recommender.py` — maps best scenario → `Recommendation` with playbook, MTTR, autonomy clamping
- `evidence_builder.py` — assembles `EvidenceReport` (matched signals, metric snapshots, log counts, confidence factors)
- `llm_enricher.py` — optional GPT-4o / Claude enrichment; returns `LLMEnrichment` with root cause hypothesis, investigation steps, rollback hint
- `anomaly_detector.py` — Z-score anomaly detection via PromQL `avg_over_time` / `stddev_over_time`
- `forecaster.py` — numpy linear regression + exponential growth detection; returns `ForecastResult` with trend and predicted time-to-critical
- `metrics_publisher.py` — publishes `obs_intelligence_*` Prometheus gauges and counters
- Both agents' `pipeline.py` updated with full 6-step intelligence pipeline:
  1. Feature extraction
  2. Scenario correlation
  3. Risk scoring
  4. Recommendation
  5. Evidence building
  6. LLM enrichment
- Enriched xyOps tickets now include: risk level badge, risk score, evidence observations, LLM root cause narrative

### Phase 6 — Continuous Intelligence & Predictive Alerts
- **Predictive alert dispatch** — obs-intelligence background loop now fires `POST /predictive-alert` on compute-agent and storage-agent when `risk_score > 0.75 AND confidence > 0.7` and **no active Prometheus alert exists** — catching degradation before it pages
- **`POST /predictive-alert`** endpoint on both agents — creates `[PREDICTIVE]`-tagged xyOps tickets; always `approval_gated` (human must approve proactive remediations)
- **`background.py`** fully realised — APScheduler drives two loops:
  - Anomaly scan every 60 s (Z-score per metric/service)
  - Forecast + predictive dispatch every 5 min
- **New Prometheus alert rules** — `NoAlertFiringButHighRisk` and `HighAnomalyZScore` for surfacing intelligence-layer signals in the alerting pipeline
- **Predictive Alert Workflow** provisioned in xyOps for both compute and storage domains — canvas nodes distinguish predictive from reactive alerts
- **`agentic-ai-overview` Grafana dashboard** updated with "Active Intelligence Signals" row showing predictive alert rates and anomaly Z-scores
- **Predictive alert counters** added to `telemetry.py` for all three services

### Phase 7 — SRE Reasoning Layer
- **`sre_reasoning_agent.py`** — fully deterministic (no LLM calls, unit-testable) `SREReasoningAgent` that produces a structured `SREAssessment`:
  - `degradation_summary` — one-liner per domain derived from live signal values
  - `causal_chain` — root cause chain built from top scenario match + `contributing_factors` + domain inferences
  - `predicted_impact` — maps risk level → blast radius + time-to-impact
  - `recommended_actions` — scenario playbook actions + domain-specific steps
  - `autonomy_recommendation` — `human_only` / `approval_gated` / `autonomous`
  - `urgency` — `critical` / `high` / `medium` / `low` from risk + time-to-impact
  - `evidence_strength` — scored from scenario confidence + log anomaly + error metrics
  - `to_prompt_block()` — serialises the assessment as a structured block injected into the LLM prompt
- **LLM enricher redesigned** — LLM receives SREAssessment as structured context and **writes the narrative** around it; it no longer generates its own reasoning (narrative writer, not reasoner)
- **OpenAI → Claude automatic failover** — `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` are independent; if OpenAI call raises an exception, Claude is tried automatically; returns `None` and falls back to deterministic analysis only if both fail
- **Outcome tracking**:
  - `obs_intelligence_scenario_outcome_total{scenario_id, outcome}` Prometheus counter added to `metrics_publisher.py`
  - `POST /intelligence/record-outcome` endpoint on obs-intelligence — agents call it on alert resolution/escalation
  - Alert resolution hooks in both compute-agent and storage-agent automatically POST the outcome when a firing alert resolves
- **`recurring_failure_signature` scenario** — upgraded to `human_only` autonomy; fires when `recurrence_count >= 3` within 6 h; `recurrence_count` field added to `ObsFeatures`
- **Grafana "SRE Incident Timeline" dashboard** (`sre-incident-timeline.json`) — 4 rows, 12 panels: risk gauges, firing alerts, scenario outcomes barchart, HTTP error rate / latency timeseries, storage pool fill, risk score timeline, outcome rate, scenario confidence heatmap
- **`CONTRIBUTING-SCENARIOS.md`** — full scenario YAML authoring guide covering all fields, available `ObsFeatures` fields, confidence scoring formula, autonomy selection guidance, and two worked examples
- **`DEMO-RUNBOOK.md`** — step-by-step end-to-end rehearsal guide from `docker compose up` through chaos injection, predictive alert observation, approval workflows, Ansible execution, and outcome recording

### Phase 8 — Local LLM Validation & ChromaDB Knowledge Store (Block F)
- **`local_llm_enricher.py`** — ChromaDB + Ollama module; 6 public methods for similarity retrieval, external result validation, incident storage, outcome updates, entry listing, and stats
- **Dual-validation pipeline** — every external LLM result (GPT-4o / Claude) is validated by `llama3.2:3b`; verdict (`corroborated`, `weak_support`, `divergent`, `insufficient_context`) written to the xyOps ticket and surfaced in Grafana
- **`local-llm` service** (Ollama) — pulls `llama3.2:3b` + `nomic-embed-text` on startup; ~2.3 GB; graceful degradation when offline
- **`knowledge-store` service** (ChromaDB) — vector store persisted in `chroma-data` volume; grows through every resolved incident
- **New obs-intelligence endpoints** — `GET /knowledge/stats`, `GET /knowledge/entries`, `POST /knowledge/store`, `POST /knowledge/outcome`
- **Storage-agent `GET /session/{id}`** extended to return all Block F fields (mirroring compute-agent)
- **Grafana Block F row** — 3 new panels in `obs-intelligence-detail.json`: External LLM Validations, Local Validation Outcomes (colour-coded by verdict), Local Validation Latency p50/p95
- **Unit tests** — `obs-intelligence/tests/test_local_llm_enricher.py` — 35 tests, 7 classes, fully offline (no Ollama/ChromaDB needed); 35/35 passing
- **`local_validator.py` deleted** — orphaned predecessor removed

### Phase 9 — AIOps Command Center UI (React)
- **`command-center/`** — React + TypeScript SPA with React Flow pipeline graph, agent details drawer, incident dashboard, scenario explorer, trust-score tracker, and playback mode
- **`ui-backend/`** — FastAPI BFF aggregator; consolidates compute-agent, storage-agent, obs-intelligence, Gitea, xyOps into a unified REST API
- **`aiops-ui/`** — Production Nginx service serving the React build (port 3005); proxies `/api/*` to ui-backend
- **Agent 4 (Analyze) drawer** — dedicated Local LLM Validation section: verdict badge (colour-coded), confidence `LinearProgress` bar, similarity / count / mode row, reasoning summary box
- **Grafana `ui-backend.json`** — new dashboard for BFF aggregator metrics
- **`LEARNING-LAYER-ARCHITECTURE.md`** — deep-dive doc for the feedback/learning layer
- **`SIMULATION-SCENARIO.md`** — end-to-end scenario trigger guide

### Phase 10 — Streamlit Command Center + Agent Mesh + qwen3.5

#### Streamlit Dashboard (`streamlit-dashboard/`)
Replaced the React `command-center` + `aiops-ui` services with a single Python Streamlit app on **port 3500**.  Key improvements that the React app could not surface:
- **26+ pending approvals** in Gitea were invisible in the React UI — now a dedicated ⏳ Approvals tab with one-click Approve/Reject buttons posted back to compute-agent
- **Workflow outcomes** (`/autonomy/history`) were never called by React — now shown in a ✅ Outcomes tab with success/failure trend chart
- **Full pipeline history** with per-session drill-down into AI analysis, risk score, evidence, LLM verdict
- **Autonomy & Trust** tab showing per-service tier progress, decision distribution pie, and the path to the next tier

**Seven tabs:**

| Tab | Content |
|---|---|
| 📊 Overview | Service health grid, active sessions, alert counts, recent activity |
| 🔴 Live Pipeline | Current session: 6 agent status cards, AI analysis narrative, risk gauge, LLM verdict |
| ⏳ Pending Approvals | All approval-gated sessions with Approve / Reject actions, Gitea PR links |
| ✅ Workflow Outcomes | Per-run E2E result table + success-rate trend chart + executor details |
| 📜 Pipeline History | Filterable session table; click to expand full analysis per run |
| 🤖 Autonomy & Trust | Per-service autonomy tier, trust progress bar, decision distribution chart |
| 🌐 Agent Mesh | Live vis.js topology graph (see below) |

#### Agent Mesh Visualisation (`🌐 Agent Mesh` tab)
- **vis.js Network** graph (CDN, dark theme `#0a0e27`) embedded via `st.components.v1.html()`
- **9 nodes**: AlertManager · Compute Agent · Prometheus · Loki · Obs-Intelligence · Local LLM (qwen3.5) · xyOps · Gitea · Ansible Runner
- **Node colour** driven by current pipeline `stage` — orange glow = actively communicating, dark green = step done, dark = idle, red = unhealthy
- **Animated edges** (dashed orange) light up only on the links active right now; edges to completed nodes turn green
- **Stage-to-active-nodes map**: `logs` → {compute, loki} · `metrics` → {compute, prometheus} · `analyzed` → {compute, obs_intel, local_llm} · `awaiting_approval` → {compute, gitea} · `autonomous_executing` → {compute, ansible}
- **Info bar** below graph: current stage badge, session ID, service name, alert name, severity, risk score, autonomy decision
- **Narrative strip**: one-line plain-English sentence for each stage (e.g. `"🧠 Obs-Intelligence running AI analysis + Local LLM corroboration."`)
- **Live Agent Activity Feed** below graph: per-agent status cards (left) + platform detail expanders (right) showing AI analysis text, Prometheus metric table, Loki log lines, Gitea PR status, trust metrics
- **Pending approvals mini-feed** at bottom of tab

#### Bug Fixes (Phase 10)
- **`compute-agent` DB fallback** — `_require_session("default")` now queries SQLite when in-memory `_sessions` is empty (sessions were being GC'd after the 1-hour TTL, causing 404s on restart)
- **`ui-backend` Pydantic stripping** — removed `response_model=PipelineState` from `GET /pipeline/session/{id}`; the Pydantic model was silently dropping `created_at`, `stage`, and other critical fields that caused the React app to crash with `new Date(undefined * 1000)`

#### qwen3.5 LLM Upgrade
- `LOCAL_LLM_MODEL` default changed from `llama3.2:3b` → **`qwen3.5`** across all services and agents
- Updated in: `docker-compose.yml`, `compute-agent/app/ai_analyst.py`, `compute-agent/app/main.py`, `obs-intelligence/app/main.py`, `obs-intelligence/app/obs_intelligence/llm_enricher.py`, `obs-intelligence/app/obs_intelligence/local_llm_enricher.py`

---

### Phase 11 — Multi-agent Cross-Domain Correlation

When compute and storage alerts fire simultaneously (within a 120 s co-occurrence window), the platform now detects the cascading failure relationship and produces a unified **`UnifiedSREAssessment`** — a full causal-chain analysis with urgency, combined risk score, and ordered remediation actions across both domains.

#### Architecture

```
Compute Agent  ──┐
                 ├─ POST /intelligence/record-incident (with signals snapshot)
Storage Agent  ──┘
                        │
                  IncidentCoordinator
                  (120 s co-occurrence ring buffer)
                        │ cross_domain_event
                  CrossDomainCorrelator.assess()
                        │
                  UnifiedSREAssessment ──► stored in IncidentCoordinator
                        │
                  GET /intelligence/correlation/current  ◄── Streamlit polling
                        │
              both agent tickets get cross-domain comment
```

#### `CrossDomainCorrelator` (`obs-intelligence/app/obs_intelligence/cross_domain_correlator.py`)

A fully **deterministic, rule-based** correlator (no LLM needed — fast and auditable):

| Correlation Type | Detection Logic | Primary Domain |
|---|---|---|
| `STORAGE_ROOT` | Storage IO latency ≥ 0.2 s **or** OSD fraction < 90% — compute errors follow | `storage` |
| `COMPUTE_ROOT` | Compute CPU ≥ 85% — storage thread starvation follows | `compute` |
| `SHARED_INFRASTRUCTURE` | Both domains degraded similarly, no single root cause signal | `shared` |
| `INDEPENDENT_CONCURRENT` | Signal patterns don't cross-correlate | `compute` |

**Combined risk formula:** `min(1.0, max(compute_risk, storage_risk) × 1.25)` — boosted because two simultaneous incidents always imply shared-impact blast radius.

#### Signals carried by agents (v2 `record-incident` payload)

| Domain | Signal Keys |
|---|---|
| Compute | `error_rate`, `latency_p99_ms`, `cpu_usage_pct`, `risk_level` |
| Storage | `io_latency_s`, `pool_usage_pct`, `osd_up`, `osd_total` |

#### New REST endpoints (obs-intelligence :9100)

| Endpoint | Purpose |
|---|---|
| `POST /intelligence/record-incident` | Extended — now accepts `signals` dict, returns `unified_assessment` when correlation fires |
| `GET /intelligence/correlation/current` | Returns latest `UnifiedSREAssessment` (10-min TTL) for Streamlit polling |

#### Agent Mesh tab — Cross-Domain Panel
The `🌐 Agent Mesh` tab now contains a **🔗 Cross-Domain Correlation** expander that auto-refreshes alongside the rest of the tab.  When active it shows:
- Correlation type badge + risk badge + urgency + primary domain
- Compute vs storage service / scenario row
- Narrative paragraph
- Causal chain (numbered steps)
- Unified recommended actions
- Evidence bullet list
- Raw JSON expander for debugging

#### xyOps ticket integration
Both the compute-agent and storage-agent tickets receive an out-of-band comment with the full causal chain and recommended actions the moment a cross-domain correlation is detected.

#### Prometheus metric
`obs_intelligence_cross_domain_correlations_total{correlation_type}` — counts correlations detected per type.

---

### Phase 12 — SREAssessment Feedback Loop (RL-lite)

Closes the model learning cycle: every recorded remediation outcome — and every LLM corroboration/divergence verdict — updates a per-scenario confidence weight stored in SQLite, so the `ScenarioCorrelator` gets smarter over time without retraining.

**`obs-intelligence/app/obs_intelligence/outcome_store.py`** — complete rewrite

| Capability | Detail |
|---|---|
| **Exponential time decay** | `decay_weight = exp(−λ·days_ago)`, half-life 30 days (env: `OUTCOME_DECAY_HALF_LIFE_DAYS`) |
| **Dynamic evidence tiers** | none(0)→±0.00 · low(1-4)→±0.10 · medium(5-14)→±0.15 · high(15-29)→±0.20 · strong(30+)→±0.25 |
| **Outcome normalisation** | positive(1.0): `success/resolved/auto_resolved` · partial(0.5): `timedout/declined` · negative(0.0): `failure/escalated/…` |
| **Multi-signal inputs** | `signal_strength=1.0` for real outcomes; `0.3` for LLM validation signals |
| **Fast-resolution bonus** | `resolution_time_seconds < 300` → +0.1 boost before clamping |
| **Trend computation** | last-5 vs prior-5 decay-weighted success rate → `improving / degrading / stable` |
| **Schema migration** | `ALTER TABLE … ADD COLUMN` wrapped in try/except — no data loss on upgrade |
| **Stale-weight refresh** | `_refresh_stale_weights()` at startup re-runs `_recalculate()` for rows miscounted by the old formula |

**`obs-intelligence/app/main.py`** — 3 targeted changes
- `POST /intelligence/record-outcome` passes `resolution_time_seconds`; returns `weight_adjustment`, `evidence_tier`, `trend`, `total_seen`
- `POST /intelligence/validate-external-analysis` routes `corroborated`/`divergent` verdicts to `record_validation_signal()` as 0.3-weight implicit signals
- New `GET /intelligence/feedback-stats` — all scenarios with sparkline history + global learning stats

**`streamlit-dashboard/app.py`** — Outcomes tab learning panel
- 5-metric KPI row: scenarios tracked · total outcomes · avg Δ · improving · degrading
- Bar chart of weight adjustments colour-coded green/red/grey
- Per-scenario detail table: tier badge, trend arrow, decay rate, raw rate, signals
- Sparkline history charts for top scenarios with ≥3 outcomes

---

### Phase 13 — Persistence, Cold-Start & Fine-Tuning

Hardened obs-intelligence against restarts and cold-start gaps; added opt-in LoRA fine-tuning pipeline.

**`obs-intelligence/app/obs_intelligence/state_store.py`** — new SQLite store (`/data/state.db`)

| Capability | Detail |
|---|---|
| **Intelligence state persistence** | `save_intelligence(data)` + `load_intelligence()` — full `current_intelligence` dict (anomalies, forecasts, loop counters, timestamps) persisted after every background loop; restored to dict on startup so dashboard is never blank after restart |
| **Recurrence counter persistence** | `record_alert_fired(alert_name) → int` — append-only log of firing timestamps; prunes old rows on insert; returns count in 6h window for `ObsFeatures.recurrence_count` |
| **Hourly cleanup** | `cleanup_old_firings()` called by a third APScheduler job every hour to keep `alert_recurrence` table small |
| **Thread-safe** | Each method opens a fresh SQLite connection with commit/rollback; safe for concurrent FastAPI + APScheduler access |

**`obs-intelligence/app/background.py`** — `StateStore` wired in

- `_state_store = StateStore()` created at module level (shared with `main.py` via import)
- `current_intelligence` initialised from `_state_store.load_intelligence()` at import time
- `_state_store.save_intelligence(current_intelligence)` called after every analysis loop + forecast loop
- New `run_state_cleanup()` APScheduler job (hourly)

**`obs-intelligence/app/main.py`** — webhook recurrence tracking + seeder launch

- Webhook handler: for every `firing` alert → `_state_store.record_alert_fired(alert_name)`; logs recurrence count with a `WARNING` when ≥ 3 (recurring_failure_signature threshold)
- Lifespan: `asyncio.create_task(seed_chromadb_if_empty())` launched at startup

**`obs-intelligence/app/obs_intelligence/cold_start_seeder.py`** — ChromaDB cold-start seeder

- 10 synthetic incident templates (5 compute + 5 storage) covering all major scenario IDs
- Only seeds when `knowledge_entries_total < CHROMA_SEED_MIN_ENTRIES` (default 5)
- Each entry embedded + upserted via `local_llm_enricher.store_incident_resolution()` with `validation_status="corroborated"` and `provider="synthetic_seed"`
- 0.5s inter-call delay; all errors caught as warnings (never blocks startup)

**`obs-intelligence/finetune/`** — LoRA fine-tuning pipeline (opt-in)

| File | Purpose |
|---|---|
| `export_training_data.py` | Reads `learning.db` → chat-format JSONL (`train.jsonl` + `eval.jsonl`); quality-scores rows (confirmed verdict + resolved outcome = highest quality) |
| `train_lora.py` | Fine-tunes Qwen2.5 with PEFT LoRA (r=8, alpha=16); saves adapter to `/output/lora_adapter/`; writes Ollama `Modelfile` |
| `Dockerfile` | Minimal Python 3.11 image with torch + transformers + peft + trl |
| `requirements.txt` | Fine-tune-only deps (not part of the main image) |
| `README.md` | Step-by-step guide: export → train → merge → GGUF → `ollama create` → `LOCAL_LLM_MODEL` swap |

Run the pipeline with:
```bash
docker compose --profile finetune run --rm finetuner python export_training_data.py
docker compose --profile finetune run --rm finetuner python train_lora.py --use-4bit
```

---

### Phase 14 — Multi-Page Dashboard, SSE & Notifications

Refactored the Streamlit Command Center into a multi-page app, added real-time SSE push, and wired Slack/PagerDuty/email notification receivers.

**`streamlit-dashboard/shared.py`** — new shared config + helper module

| Capability | Detail |
|---|---|
| **Centralised config** | All `os.getenv(...)` constants (agent URLs, Gitea org/repo, external service URLs) defined once and imported by all pages |
| **HTTP helpers** | `api_get()`, `api_post()`, `gitea_get()` with try/except returning `None` on error |
| **UI formatters** | `since_str()`, `sev_icon()`, `status_icon()` for consistent display across pages |
| **Dark/light CSS** | `inject_theme_css()` reads `st.session_state["theme"]` and injects either `_DARK_CUSTOM` or `_LIGHT_CUSTOM` CSS block |
| **Page scaffolding** | `page_header(title)` (theme + sidebar + title), `page_footer(key, fragment_mode)` (auto-refresh or fragment caption) |
| **Sidebar controls** | Dark/light toggle + quick-links to all external service UIs |

**`streamlit-dashboard/pages/`** — 7 page files (auto-discovered by Streamlit)

| Page | File | Key Feature |
|---|---|---|
| Live Pipeline | `1_Live_Pipeline.py` | `@st.fragment(run_every="1s")` — independent 1-second refresh of compute-agent pipeline |
| Storage Pipeline | `2_Storage_Pipeline.py` | Mirrors compute view for storage-agent; inline Approve/Decline; history table with LinkColumn |
| Pending Approvals | `3_Pending_Approvals.py` | Combined approval queue from both agents + Gitea open PRs table |
| Workflow Outcomes | `4_Workflow_Outcomes.py` | xyOps ticket + Grafana drill-down via `st.column_config.LinkColumn` |
| Pipeline History | `5_Pipeline_History.py` | Merged compute + storage history with agent/severity filters |
| Autonomy & Trust | `6_Autonomy_Trust.py` | Trust-tier config, decision distribution, scenario confidence learning |
| Agent Mesh | `7_Agent_Mesh.py` | vis.js topology with edge bandwidth labels + click-to-drill node navigation |

**`compute-agent/app/pipeline.py`** — SSE endpoint

| Endpoint | Detail |
|---|---|
| `GET /pipeline/events` | `StreamingResponse(media_type="text/event-stream")` — emits `data: {json}\n\n` on stage change, `: heartbeat\n\n` otherwise; polls every 0.5s |

**`storage-agent/app/pipeline.py`** — 3 new endpoints

| Endpoint | Detail |
|---|---|
| `GET /pipeline/session/default` | "default" alias — returns most recently created session (mirrors compute-agent) |
| `GET /pipeline/history` | Returns `{"history": [...], "count": n}` from in-memory `_sessions` dict |
| `GET /pipeline/events` | SSE stream identical to compute-agent pattern |

**`alertmanager/alertmanager.yml`** — notification integrations

| Receiver | Detail |
|---|---|
| `slack-critical` | `slack_configs` with webhook URL placeholder; posts to `#incidents-critical` channel |
| `pagerduty-critical` | `pagerduty_configs` with Events v2 routing key placeholder |
| `email-all` | `email_configs` to on-call distribution list; warning/info severity routing |

Routes: critical alerts fan out to `slack-critical` + `pagerduty-critical` via `continue: true`; warning/info alerts also copied to `email-all`.

---

### Phase 15 — Pattern Intelligence Library (PostgreSQL + pgvector)

> **Latest:** v15.0.0 — Pattern Library service with hybrid incident matching and n8n ingestion endpoint.

Adds a structured failure pattern library as the foundational layer of the Pattern → Decision → Action intelligence system described in `NewProject.md`.

**New services:**

| Service | Port | Purpose |
|---|---|---|
| `pattern-db` | 5432 | PostgreSQL 16 + pgvector 0.7 — Pattern Intelligence Library (HNSW cosine-similarity indexes) |
| `pattern-library` | 9300 | FastAPI Pattern Library — CRUD, semantic search, hybrid matching, n8n ingestion |

**7-table schema** (`pattern-library/migrations/001_init.sql`):

| Table | Purpose |
|---|---|
| `raw_public_issues` | Raw content from n8n (GitHub, SO, Reddit, HN, blogs) |
| `enriched_issues` | LLM-processed issues with pain point + 384-dim embedding |
| `patterns` | Core pattern library — name, description, severity, recurrence score, confidence, embedding |
| `pattern_signals` | Detection signals per pattern (metric thresholds, log patterns, TraceQL, alert names) |
| `pattern_fixes` | Ordered fixes per pattern (autonomous → approval-required → manual) |
| `lab_validations` | Records of patterns reproduced and detected in the local lab |
| `agent_assessments` | Links compute/storage sessions to matched patterns with outcomes |

**Key endpoints** (`http://localhost:9300/docs`):

| Endpoint | Purpose |
|---|---|
| `GET /health` | Service health + active pattern count |
| `GET /stats` | Full library statistics |
| `GET /patterns` | List patterns (filter by severity, environment, deprecated) |
| `POST /patterns` | Create a new pattern (embedding auto-generated via Ollama) |
| `GET /patterns/{id}` | Pattern detail with signals, fixes, and recent lab validations |
| `POST /patterns/search` | Semantic search — text → embedding → cosine similarity |
| `POST /patterns/match-incident` | **Hybrid matching** — 0.6 × rule_score + 0.4 × vector_similarity |
| `POST /patterns/{id}/signals` | Add a detection signal to a pattern |
| `POST /patterns/{id}/fixes` | Add a fix to a pattern |
| `POST /patterns/{id}/validations` | Record a lab validation result |
| `POST /issues/raw` | n8n ingestion — receive raw public issue |
| `POST /assessments` | Record agent session → matched patterns |
| `POST /assessments/{id}/outcome` | Record resolved / escalated / false_positive outcome |
| `POST /admin/seed` | Re-trigger seeder (force=true to overwrite) |
| `POST /admin/embed-all` | Backfill NULL embeddings after Ollama starts |

**5 foundational patterns seeded on startup:**

| Pattern | Severity | Automation | Recurrence |
|---|---|---|---|
| `cpu_throttling_resource_limits` | high | risky | 0.85 |
| `collector_pipeline_backpressure` | high | manual | 0.78 |
| `prometheus_scrape_miss` | medium | safe | 0.72 |
| `oom_kill_resource_misconfiguration` | critical | risky | 0.80 |
| `misleading_p99_histogram_bucket_saturation` | low | safe | 0.68 |

**Hybrid matching algorithm:**
1. If `incident_text` supplied → embed → cosine search returns top_k×3 candidates
2. For each candidate → score thresholds in `pattern_signals` against caller's `signals` dict
3. `combined_score = 0.6 × rule_score + 0.4 × vector_similarity`
4. Return top_k sorted by `combined_score` descending

**Embedding:** 384-dim nomic-embed-text via shared `local-llm` Ollama instance.
Gracefully degrades to NULL embeddings (pure rule matching) when Ollama is offline.
Run `POST /admin/embed-all` to backfill after Ollama comes online.

---

## What Is Still To Add - to follow

### Phase 14 — Dashboard Enhancements (remaining)
| Item | Description | Effort |
|---|---|---|
| ~~**Streamlit Storage Agent tab**~~ | ~~Add a second Live Pipeline tab for the storage-agent pipeline, mirroring the compute-agent view~~ | ✅ Done (v14.0.0) |
| ~~**WebSocket / SSE real-time push**~~ | ~~Replace 5-second polling with SSE stream from compute-agent~~ | ✅ Done (v14.0.0) |
| ~~**Agent Mesh — edge bandwidth labels**~~ | ~~Show estimated data size / latency on each active edge~~ | ✅ Done (v14.0.0) |
| ~~**Agent Mesh — click-to-drill**~~ | ~~Click a node in the vis.js graph → open the relevant service UI~~ | ✅ Done (v14.0.0) |
| ~~**Streamlit multi-page**~~ | ~~Split the 7 tabs into Streamlit multi-page app~~ | ✅ Done (v14.0.0) |
| ~~**Dark/light theme toggle**~~ | ~~Allow users to switch between dark and light themes~~ | ✅ Done (v14.0.0) |

### Phase 14 — Intelligence & Autonomy
| Item | Description | Effort |
|---|---|---|
| ~~**Multi-agent correlation**~~ | ~~When compute and storage alerts fire simultaneously, detect cross-domain cascading failures~~ | ✅ Done (v11.0.0) |
| ~~**SREAssessment feedback loop**~~ | ~~Feed recorded outcomes back into scenario confidence weights over time (reinforcement learning lite)~~ | ✅ Done (v12.0.0) |
| ~~**Recurrence counter persistence**~~ | ~~`recurrence_count` is currently in-memory; persist it in Redis or SQLite so obs-intelligence survives restarts~~ | ✅ Done (v13.0.0) |
| ~~**Persistent intelligence state**~~ | ~~Replace in-memory current state with Redis or SQLite so obs-intelligence survives restarts~~ | ✅ Done (v13.0.0) |
| ~~**ChromaDB cold-start seeding**~~ | ~~Pre-seed the knowledge store with synthetic past incidents so local validation works on day one~~ | ✅ Done (v13.0.0) |
| ~~**qwen3.5 fine-tuning**~~ | ~~Fine-tune a LoRA adapter on recorded incident outcomes to improve corroboration accuracy over time~~ | ✅ Done (v13.0.0) |

### Phase 14 — Integrations & Hardening
| Item | Description | Effort |
|---|---|---|
| ~~**Notification integrations**~~ | ~~Slack / PagerDuty / email receivers beyond xyOps webhook; notification routing by risk level~~ | ✅ Done (v14.0.0) |
| ~~**Outcome dashboard drill-down**~~ | ~~Grafana panel linking scenario outcome bars to the originating xyOps ticket and Ansible run log~~ | ✅ Done (v14.0.0) |
| **Auth / API key middleware** | Bearer token validation on agent webhook endpoints and obs-intelligence `/analyze`; currently open | Medium |
| **Ansible live mode** | Real playbook execution against actual infrastructure targets (set `ANSIBLE_LIVE_MODE=true`) | Medium |
| **Ollama GPU acceleration** | Add `deploy.resources.reservations.devices` to `local-llm` service for CUDA/ROCm GPU inference | Low |
| **ChromaDB auth** | Enable ChromaDB token authentication before exposing to untrusted networks | Low |

---

## Prerequisites

### Required Software

| Tool | Minimum Version | Purpose |
|---|---|---|
| **Docker Desktop** | 4.x (Docker Engine 24+) | Container runtime — all services run in Docker |
| **Docker Compose** | v2.x (bundled with Docker Desktop) | Multi-container orchestration |
| **Git** | 2.x | Clone the repository |

> Docker Desktop for Windows requires WSL 2 or Hyper-V. Ensure at least **8 GB RAM** allocated to Docker.

### Optional (for AI enrichment)

| Credential | Where to set | Purpose |
|---|---|---|
| **OpenAI API key** | `OPENAI_API_KEY` env var in `docker-compose.yml` or `.env` file | GPT-4o LLM enrichment in agent pipelines |
| **Anthropic API key** | `ANTHROPIC_API_KEY` env var | Claude-3 alternative to OpenAI |

> Without an API key the agents fall back to deterministic analysis — full scenario matching, risk scoring, and evidence building still work; only the LLM narrative is skipped.

### Network & Ports

Ensure the following host ports are free before starting:

```
3000  Grafana
3002  Gitea
3100  Loki
3200  Tempo
4317  OTel Collector gRPC
5522  xyOps
8080  frontend-api
8081  backend-api
8090  ansible-runner
9000  compute-agent
9001  storage-agent
9090  Prometheus
9093  Alertmanager
9100  obs-intelligence
9200  storage-simulator
```

### First-Time xyOps Setup (after first `docker compose up`)

1. Open [http://localhost:5522](http://localhost:5522) — login: `admin / admin`
2. **Admin → API Keys → Create Key** → copy the key
3. Paste the key as `XYOPS_API_KEY` in `docker-compose.yml` for both `compute-agent` and `storage-agent`
4. `docker compose restart compute-agent storage-agent`

---

## Quick Start

```bash
# Clone
git clone https://github.com/omkarumarani/obseransiblerepo.git
cd obseransiblerepo

# Start the full stack (all 20+ services)
docker compose up --build -d

# With load generator + chaos (troublemaker)
docker compose --profile loadgen --profile troublemaker up --build -d

# Health checks
curl http://localhost:9000/health      # compute-agent
curl http://localhost:9001/health      # storage-agent
curl http://localhost:9100/health      # obs-intelligence
curl http://localhost:9005/health      # ui-backend

# AIOps Command Center (Streamlit)
open http://localhost:3500             # 7-tab Streamlit dashboard

# Ollama model status
docker compose exec local-llm ollama list

# ChromaDB knowledge store stats
curl http://localhost:9100/knowledge/stats

# Manually trigger a compute test alert
curl -X POST http://localhost:9000/webhook \
  -H "Content-Type: application/json" \
  -d '{"status":"firing","alerts":[{"status":"firing","labels":{"alertname":"HighMemoryUsage","service_name":"frontend-api","severity":"warning","domain":"compute"},"annotations":{"summary":"Memory above 80%","description":"Test","dashboard_url":"http://localhost:3001"},"startsAt":"2026-03-20T10:00:00Z"}]}'

# Trigger a storage scenario
curl -X POST http://localhost:9200/scenario/osd_down

# Check pre-computed intelligence state
curl http://localhost:9100/intelligence/current

# View obs-intelligence metrics (Prometheus format)
curl http://localhost:9100/metrics

# Stop everything (preserves data volumes)
docker compose down

# Stop and wipe all data (including Ollama models and ChromaDB embeddings)
docker compose down -v
```

---

## Key Workflows

### Compute Alert → Ticket

```
Prometheus fires HighMemoryUsage (domain=compute)
  → Alertmanager → POST compute-agent:9000/webhook
    → pipeline.py step 1: extract_features (memory_growth_rate, error_rate, p99_latency)
    → step 2: match_scenarios (top match: memory_leak_emergence, confidence 0.82)
    → step 3: score_risk (risk_score=0.74, level=high)
    → step 4: recommend (action: restart_service, approval_required=true)
    → step 5: build_evidence (3 matched signals, 12 log errors)
    → step 6: LLM enrich (root cause narrative, investigation steps)
  → POST xyops:5522/api/app/create_ticket → ticket with risk badge + evidence
  → Approval workflow canvas (human approves)
  → POST ansible-runner:8080/run → Ansible playbook
  → Gitea PR created for audit trail
```

### Storage Alert → Ticket

Same as above via `storage-agent:9001/webhook` with storage-domain scenarios (CephOSDDown, pool_full_critical, etc.).

### Background Intelligence

```
obs-intelligence (every 60 s):
  → query Prometheus for known service metrics
  → Z-score anomaly detection per metric
  → publish obs_intelligence_anomaly_zscore{metric, service}

obs-intelligence (every 5 min):
  → query Prometheus query_range for 1h history
  → numpy linear regression per metric
  → publish obs_intelligence_forecast_value{metric, service, horizon}

Domain agents (on each alert):
  → GET obs-intelligence:9100/intelligence/current
  → merge pre-computed anomalies + forecasts into LLM context
```

---

## Environment Variables Reference

| Variable | Service | Default | Description |
|---|---|---|---|
| `XYOPS_URL` | compute-agent, storage-agent | `http://xyops:5522` | xyOps base URL |
| `XYOPS_API_KEY` | compute-agent, storage-agent | _(set after first login)_ | xyOps REST API key |
| `OPENAI_API_KEY` | obs-intelligence | _(optional)_ | OpenAI enrichment (tried first) |
| `ANTHROPIC_API_KEY` | obs-intelligence | _(optional)_ | Anthropic Claude fallback (used if OpenAI fails) |
| `OPENAI_MODEL` | obs-intelligence | `gpt-4o-mini` | OpenAI model override |
| `CLAUDE_MODEL` | obs-intelligence | `claude-3-5-haiku-20241022` | Claude model override |
| `PROMETHEUS_URL` | all agents | `http://prometheus:9090` | Prometheus query URL |
| `LOKI_URL` | all agents | `http://loki:3100` | Loki query URL |
| `OBS_INTELLIGENCE_URL` | compute-agent, storage-agent | `http://obs-intelligence:9100` | Intelligence engine URL |
| `REQUIRE_APPROVAL` | compute-agent | `true` | Set `false` to auto-execute without human sign-off |
| `STORAGE_REQUIRE_APPROVAL` | storage-agent | `true` | Set `false` for auto-execute |
| `ANSIBLE_LIVE_MODE` | ansible-runner | `false` | Set `true` for real playbook execution |
| `ANOMALY_Z_THRESHOLD` | obs-intelligence | `2.5` | Z-score threshold to flag anomalies |
| `FORECAST_HORIZON_MINUTES` | obs-intelligence | `60` | Forecast window in minutes |
| `GITHUB_REPO` | compute-agent | _(your repo)_ | For Ansible playbook PR audit trail |
| `GITEA_ENABLED` | compute-agent | `true` | Enable Gitea PR creation on approval |
| `WORKFLOW_STEP_DELAY_SECONDS` | compute-agent, storage-agent | `5` | Seconds each xyOps canvas node stays highlighted |
| `LOCAL_LLM_URL` | compute-agent, storage-agent, obs-intelligence | `http://local-llm:11434` | Ollama REST API base URL |
| `CHROMA_URL` | compute-agent, storage-agent, obs-intelligence | `http://knowledge-store:8000` | ChromaDB HTTP API URL |
| `LOCAL_LLM_MODEL` | compute-agent, storage-agent, obs-intelligence | `qwen3.5` | Ollama model for local corroboration |
| `LOCAL_LLM_ENABLED` | compute-agent, storage-agent, obs-intelligence | `true` | Set `false` to disable local validation |
| `LOCAL_LLM_MIN_SIMILARITY` | obs-intelligence | `0.82` | Min cosine similarity to count a past incident as relevant |
| `LOCAL_LLM_TOP_K` | obs-intelligence | `5` | Max similar incidents retrieved from ChromaDB per query |
| `COMPUTE_AGENT_URL` | ui-backend | `http://compute-agent:9000` | Compute agent base URL |
| `STORAGE_AGENT_URL` | ui-backend | `http://storage-agent:9001` | Storage agent base URL |
| `DB_PATH` | ui-backend | `/data/pipeline_history.db` | SQLite path for pipeline session history |

---

## Release Notes

See [RELEASE-NOTES.md](RELEASE-NOTES.md) for the full versioned changelog.

| Version | Highlights |
|---|---|
| **v14.0.0** | Multi-page dashboard (7 pages + shared.py) — Streamlit multi-page architecture; dark/light theme toggle; SSE `/pipeline/events` on compute-agent & storage-agent; vis.js edge bandwidth labels + click-to-drill; storage pipeline default/history/SSE endpoints; Slack/PagerDuty/email alertmanager receivers; outcome drill-down to xyOps + Grafana |
| **v13.0.0** | Persistence + cold-start hardening — `StateStore` SQLite for `current_intelligence` + `recurrence_count`; 10-entry ChromaDB cold-start seeder; LoRA fine-tuning `finetuner` Docker service (`--profile finetune`) |
| **v12.0.0** | SREAssessment RL-lite feedback loop — exponential decay, dynamic evidence tiers, outcome normalisation, validation signals, `feedback-stats` API, Streamlit learning panel |
| **v11.0.0** | Multi-agent cross-domain correlation engine (`CrossDomainCorrelator`) + unified `SREAssessment` + agent signals + Streamlit cross-domain panel + xyOps ticket comments |
| **v10.0.0** | Streamlit Command Center (7 tabs) + Agent Mesh vis.js topology + qwen3.5 LLM + compute-agent DB fallback fix |
| **v9.0.0** | AIOps Command Center UI (React) — ui-backend BFF, pipeline playback, scenario explorer, trust-score tracker |
| **v8.0.0** | Block F: Local LLM validation + ChromaDB knowledge store |
| **v7.0.0** | SRE Reasoning Layer — deterministic `SREAssessment`, OpenAI→Claude failover, outcome tracking |
| **v6.0.0** | Continuous intelligence — predictive alert dispatch, background APScheduler loops |
| **v5.0.0** | Risk scoring, evidence builder, LLM enrichment, anomaly detection, forecasting |
| **v4.0.0** | Shared Obs-Intelligence Engine — 20 scenario YAMLs, `/analyze` API |
| **v3.0.0** | Domain split — compute-agent + storage-agent + storage-simulator |
| **v2.0.0** | Alert pipeline — Alertmanager + xyOps + Ansible + Gitea |
| **v1.0.0** | Core observability stack — OTel, Prometheus, Loki, Tempo, Grafana |
