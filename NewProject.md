Act as a **Senior Observability Architect, AIOps Platform Designer, and OSS Strategy Advisor** with deep experience in OpenTelemetry, Kubernetes observability, and agentic systems.

---

## 🎯 CONTEXT

I already have an observability lab running with:

* OpenTelemetry Collector
* Prometheus
* Loki
* Tempo
* Grafana
* Alertmanager
* Instrumented frontend/backend services
* Optional chaos/troublemaker scenarios

In parallel, I am designing a **public-source pattern discovery pipeline using n8n** that collects recurring observability pain points from:

* GitHub (especially OpenTelemetry repos)
* Stack Overflow
* Reddit (DevOps/Kubernetes)
* Hacker News
* CNCF / vendor blogs (Grafana, Datadog, Splunk, etc.)

---

## 🚨 IMPORTANT DIRECTION

This is NOT a problem aggregation system.

This is a:

👉 **Pattern → Decision → Action Observability Intelligence System**

I do NOT want separate systems.

I want ONE unified system where:

1. **Observability Lab**

   * Controlled environment to reproduce and validate failure patterns

2. **Public Discovery Pipeline (n8n)**

   * Finds recurring real-world problems
   * Converts them into structured failure patterns

3. **Compute Agent**

   * Uses:

     * live telemetry (metrics/logs/traces/alerts)
     * pattern library
   * To:

     * diagnose incidents
     * provide evidence-based reasoning
     * recommend actions
     * optionally generate automation

---

## 🧠 CORE GOAL

Build an:

👉 **AI-powered Observability Pattern Intelligence + Compute Incident Reasoning System**

That can:

* detect recurring failure patterns
* map live incidents to those patterns
* guide investigation
* suggest fixes
* evolve into agentic automation

---

## 🔥 WHAT I NEED (DETAILED DESIGN)

---

### A. 🔷 Combined Architecture (End-to-End)

Design a production-grade architecture including:

* Telemetry Plane (lab)
* Public Discovery Plane (n8n)
* Pattern Intelligence Layer (shared brain)
* Compute Agent Runtime
* Output / Action Plane

Include:

* clear data flow
* boundaries of each component
* where n8n is sufficient vs where custom services are required
* how the system evolves from MVP → advanced

Provide a **text-based architecture diagram**

---

### B. 🧠 Pattern Library (MOST IMPORTANT)

Design the **Pattern Intelligence Layer** as the core of the system.

Each pattern must include:

* pattern name
* description
* symptoms (metrics/logs/traces)
* signals to check
* environment (Kubernetes, VM, etc.)
* impacted layers (app / infra / collector / network)
* multiple possible root causes
* detection logic
* recurrence score
* severity
* known fixes
* suggested fixes
* automation readiness
* OSS contribution angle
* source references (with URLs)

Explain:

* how patterns are created from raw problems
* how duplicates are avoided
* how patterns evolve over time
* how confidence is calculated

---

### C. 🤖 Compute Agent Reasoning Design

Design how the compute agent uses:

* metrics (Prometheus)
* logs (Loki)
* traces (Tempo)
* alerts (Alertmanager)
* pattern library

To perform:

1. hypothesis generation
2. evidence collection
3. pattern matching
4. confidence scoring
5. recommendation generation

Output format must include:

* incident summary
* observed evidence
* matched patterns (ranked)
* reasoning trace
* next investigation steps
* safe actions
* automation candidates

---

### D. 🧪 First 5 Compute Failure Patterns

Define the first 5 patterns I should implement in my lab.

For EACH pattern provide:

* pattern name
* symptoms
* signals (metrics/logs/traces)
* likely causes (multiple)
* reproduction strategy in lab
* detection logic
* fixes
* automation feasibility (safe / risky / manual)

Focus on:

* CPU saturation (different root causes)
* collector bottlenecks
* telemetry drops
* resource misconfiguration
* misleading alerts

---

### E. 🗄️ Data Architecture (MANDATORY)

Design schema for:

1. raw_public_issues
2. enriched_issues
3. patterns
4. pattern_signals
5. pattern_fixes
6. lab_validations
7. agent_assessments

Use:

* PostgreSQL for structured data
* pgvector (PostgreSQL vector extension) for semantic search

Explain:

* how embeddings are generated
* how similarity search is used
* how pattern matching uses vector + rules hybrid approach

---

### F. 🔌 MCP (Model Context Protocol) Integration

Include a design where:

* Compute agent accesses:

  * Prometheus
  * Loki
  * Tempo
  * Pattern DB
    using MCP-style tool interfaces

Define:

* tool schema for:

  * query_metrics
  * query_logs
  * query_traces
  * search_patterns
* how context is passed to LLM safely
* how to avoid overloading context window

---

### G. 🧠 AI Prompt Chains

Design prompts for:

1. public issue summarization (strict grounding)
2. pain extraction
3. pattern creation
4. clustering
5. incident-to-pattern matching
6. recommendation generation

Include:

* anti-hallucination constraints
* citation enforcement
* structured outputs (JSON preferred)

---

### H. 📊 Scoring Model

Design scoring for patterns based on:

* recurrence across sources
* business impact
* technical depth
* feasibility
* OSS potential
* career visibility

Provide a weighted formula.

---

### I. ⚙️ Action Layer (Key Differentiator)

Design how patterns convert into:

* OpenTelemetry configs
* alert rules
* Grafana dashboards
* Ansible playbooks
* runbooks

Clearly define:

* what is safe to automate
* what must require human approval
* how to gradually move toward agentic automation

---

### J. 📅 30-Day Execution Plan

Provide a **realistic solo-developer plan**:

Week 1:

* lab pattern scenarios

Week 2:

* n8n ingestion + filtering

Week 3:

* pattern DB + enrichment

Week 4:

* compute agent + first end-to-end run

---

### K. 🚀 MVP vs Advanced Architecture

Define:

**MVP:**

* minimal components
* manual review included

**Advanced:**

* pattern clustering
* automated scoring
* agentic actions
* OSS integration

---

### L. 🌍 Expansion Strategy (Non-IT Future)

Explain how this architecture generalizes to:

* healthcare
* finance
* manufacturing

What remains same:

* pattern → decision → action

What changes:

* data sources
* signals
* domain patterns

---

### M. 📦 Deliverables

Provide:

1. architecture diagram (text)
2. example pattern record
3. example DB schema (SQL)
4. example prompts
5. example compute agent output
6. example monthly report
7. top 10 failure risks + mitigation

---

## ⚠️ CONSTRAINTS

* Prefer APIs/RSS over scraping
* Keep MVP buildable by one person
* Focus on observability (OpenTelemetry, metrics/logs/traces)
* Ensure real-world practicality
* Avoid generic explanations

---

## 🎯 FINAL EXPECTATION

This should feel like designing:

👉 **A real Observability Intelligence Platform + Compute Reasoning System**

NOT:

* a dashboard project
* a data collection tool
* a generic AI workflow

It should demonstrate:

* system thinking
* pattern intelligence
* observability expertise
* path to OSS contribution
* foundation for agentic systems

---

Think deeply. Be structured. Be practical.



N. 🔁 Feedback Loop & Learning System

Design how the system learns from real usage:

how to track whether suggested fixes worked or not
how to update pattern confidence over time
how failed recommendations are handled
how human feedback improves the system
O. 🎯 Confidence & Uncertainty Modeling

Design how the compute agent:

assigns confidence scores to each hypothesis
handles multiple competing root causes
identifies when data is insufficient
avoids overconfident recommendations
P. 🔗 Multi-Pattern Correlation

Design how the system:

detects multiple patterns in a single incident
correlates them across metrics/logs/traces
prioritizes dominant vs secondary patterns
Q. ⏱️ Time-Series Reasoning

Include:

baseline vs anomaly detection
before/after comparison
trend-based reasoning
correlation with deployment/events
R. 🚀 Deployment & Integration

Explain how this system runs in real environments:

Kubernetes deployment model
integration with CI/CD pipelines
integration with alerting systems
API exposure for external systems
S. 💰 Cost & Performance Design

Include:

LLM usage optimization
caching strategies
query performance
scaling considerations
T. 🧹 Data Quality & Noise Handling

Design:

spam filtering
low-quality signal detection
deduplication improvements
bias mitigation
U. 📏 Evaluation Framework

Define:

how to measure system accuracy
how to compare against manual debugging
success metrics (precision, usefulness, adoption)
V. 🔄 Pattern Lifecycle Management

Include:

pattern versioning
updates and deprecations
pattern evolution tracking
W. 🧑‍💻 User Interaction Model

Design:

how engineers interact with the system
CLI / UI / chat interface
how results are presented
how decisions are consumed