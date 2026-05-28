-- Pattern Library — PostgreSQL + pgvector schema
-- Phase 15: Pattern Intelligence Layer
-- Run automatically by Docker at first startup (docker-entrypoint-initdb.d)
-- Idempotent: all statements use CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS

-- Enable pgvector and pgcrypto (UUID generation)
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─────────────────────────────────────────────────────────────
-- 1. raw_public_issues
--    Raw content scraped/fetched from public sources.
--    Source: n8n public discovery pipeline (GitHub, Stack Overflow,
--    Reddit, Hacker News, vendor blogs).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_public_issues (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source      VARCHAR(50) NOT NULL,           -- github | stackoverflow | reddit | hackernews | blog
    source_id   VARCHAR(255) NOT NULL,          -- original post/issue ID in source system
    url         TEXT        NOT NULL,
    title       TEXT,
    body        TEXT,
    author      VARCHAR(255),
    score       INTEGER     DEFAULT 0,          -- upvotes / stars / answer count
    tags        TEXT[]      DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    fetched_at  TIMESTAMPTZ DEFAULT NOW(),
    processed   BOOLEAN     DEFAULT FALSE,
    UNIQUE (source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_issues_source     ON raw_public_issues (source);
CREATE INDEX IF NOT EXISTS idx_raw_issues_processed  ON raw_public_issues (processed);

-- ─────────────────────────────────────────────────────────────
-- 2. enriched_issues
--    LLM-processed version of raw issues:
--    pain point extracted, component identified, embedding generated.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS enriched_issues (
    id                  UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_issue_id        UUID    REFERENCES raw_public_issues(id) ON DELETE CASCADE,
    pain_point          TEXT    NOT NULL,
    affected_component  VARCHAR(100),           -- collector | prometheus | loki | kubernetes | application
    environment         VARCHAR(50),            -- kubernetes | vm | docker | bare-metal | any
    symptoms            TEXT[]  DEFAULT '{}',
    is_duplicate        BOOLEAN DEFAULT FALSE,
    duplicate_of        UUID    REFERENCES enriched_issues(id),
    quality_score       FLOAT   DEFAULT 0.5,    -- 0.0–1.0; used for pattern confidence weighting
    embedding           vector(768),            -- nomic-embed-text (768-dim)
    processed_at        TIMESTAMPTZ DEFAULT NOW(),
    llm_model           VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_enriched_raw         ON enriched_issues (raw_issue_id);
CREATE INDEX IF NOT EXISTS idx_enriched_embedding   ON enriched_issues USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_enriched_component   ON enriched_issues (affected_component);

-- ─────────────────────────────────────────────────────────────
-- 3. patterns
--    Core failure pattern library — the shared brain.
--    Each pattern synthesises multiple raw/enriched issues into
--    a reusable, scored, versionable intelligence unit.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS patterns (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name                    VARCHAR(255) NOT NULL UNIQUE,
    description             TEXT        NOT NULL,
    environment             VARCHAR(50) DEFAULT 'any',      -- kubernetes | vm | docker | any
    impacted_layers         TEXT[]      DEFAULT '{}',       -- app | infra | collector | network
    recurrence_score        FLOAT       DEFAULT 0.5,        -- 0.0–1.0; updated by feedback loop
    severity                VARCHAR(20) DEFAULT 'medium',   -- critical | high | medium | low
    automation_readiness    VARCHAR(20) DEFAULT 'manual',   -- safe | risky | manual
    oss_contribution_angle  TEXT,                           -- how to contribute this back to OSS
    source_references       JSONB       DEFAULT '[]',       -- [{title, url, source}]
    confidence              FLOAT       DEFAULT 0.5,        -- 0.0–1.0; weighted by evidence_count
    evidence_count          INTEGER     DEFAULT 0,          -- lab validations + real outcomes
    version                 INTEGER     DEFAULT 1,
    embedding               vector(768),                    -- embedding of name+description
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    deprecated              BOOLEAN     DEFAULT FALSE,
    deprecated_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_patterns_embedding   ON patterns USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_patterns_severity    ON patterns (severity);
CREATE INDEX IF NOT EXISTS idx_patterns_deprecated  ON patterns (deprecated);
CREATE INDEX IF NOT EXISTS idx_patterns_recurrence  ON patterns (recurrence_score DESC);

-- ─────────────────────────────────────────────────────────────
-- 4. pattern_signals
--    Detection signals per pattern: metric thresholds, log patterns,
--    trace characteristics, alert name patterns.
--    Used in hybrid rule+vector matching.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pattern_signals (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_id          UUID        REFERENCES patterns(id) ON DELETE CASCADE,
    signal_type         VARCHAR(20) NOT NULL,       -- metric | log | trace | alert
    name                VARCHAR(255) NOT NULL,      -- metric name / log pattern / alert name
    description         TEXT,
    query_template      TEXT,                       -- PromQL / LogQL / TraceQL template
    threshold_operator  VARCHAR(10),                -- > | < | >= | <= | =
    threshold_value     FLOAT,                      -- numeric threshold for rule scoring
    severity            VARCHAR(20) DEFAULT 'medium',
    weight              FLOAT       DEFAULT 1.0,    -- contribution to confidence score (0.0–2.0)
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signals_pattern ON pattern_signals (pattern_id);

-- ─────────────────────────────────────────────────────────────
-- 5. pattern_fixes
--    Known fixes per pattern, from safe config tweaks to
--    Ansible playbooks and runbooks.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pattern_fixes (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_id              UUID        REFERENCES patterns(id) ON DELETE CASCADE,
    title                   VARCHAR(255) NOT NULL,
    description             TEXT,
    fix_type                VARCHAR(30) NOT NULL,       -- config_change | restart | scale | alert_rule | playbook | runbook
    automation_level        VARCHAR(20) DEFAULT 'manual',   -- autonomous | approval_required | manual
    content                 TEXT,                       -- actual YAML / Ansible / shell content
    risk_level              VARCHAR(20) DEFAULT 'medium',   -- critical | high | medium | low
    estimated_mttr_seconds  INTEGER,
    requires_restart        BOOLEAN     DEFAULT FALSE,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fixes_pattern ON pattern_fixes (pattern_id);

-- ─────────────────────────────────────────────────────────────
-- 6. lab_validations
--    Records whether a pattern was successfully reproduced and
--    detected in the local observability lab.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS lab_validations (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_id                  UUID        REFERENCES patterns(id) ON DELETE CASCADE,
    validated_at                TIMESTAMPTZ DEFAULT NOW(),
    reproducer_scenario         VARCHAR(255),   -- troublemaker / storage-simulator scenario name
    reproduction_steps          TEXT,
    detected                    BOOLEAN     NOT NULL,
    detection_latency_seconds   FLOAT,          -- time from trigger to alert/detection
    false_positive_rate         FLOAT,          -- fraction of detection attempts that were FP
    notes                       TEXT,
    validator                   VARCHAR(100) DEFAULT 'manual'
);

CREATE INDEX IF NOT EXISTS idx_validations_pattern ON lab_validations (pattern_id);

-- ─────────────────────────────────────────────────────────────
-- 7. agent_assessments
--    Links compute/storage agent sessions to matched patterns.
--    Enables tracking which patterns were matched in production
--    and whether the recommendations worked.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_assessments (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id              VARCHAR(255) NOT NULL,
    agent                   VARCHAR(50) NOT NULL,       -- compute | storage
    alert_name              VARCHAR(255),
    assessed_at             TIMESTAMPTZ DEFAULT NOW(),
    matched_patterns        JSONB       DEFAULT '[]',   -- [{pattern_id, score, rank}]
    top_pattern_id          UUID        REFERENCES patterns(id),
    top_pattern_score       FLOAT,
    risk_score              FLOAT,
    recommendation          TEXT,
    outcome                 VARCHAR(50),    -- resolved | escalated | false_positive | unknown
    outcome_recorded_at     TIMESTAMPTZ,
    embedding               vector(768)    -- embedding of incident description for future discovery
);

CREATE INDEX IF NOT EXISTS idx_assessments_session      ON agent_assessments (session_id);
CREATE INDEX IF NOT EXISTS idx_assessments_agent        ON agent_assessments (agent);
CREATE INDEX IF NOT EXISTS idx_assessments_top_pattern  ON agent_assessments (top_pattern_id);
CREATE INDEX IF NOT EXISTS idx_assessments_embedding    ON agent_assessments USING hnsw (embedding vector_cosine_ops);
