-- Pattern Library — Migration 002: Pattern Lifecycle History
-- Idempotent: uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS
--
-- Provides a full audit trail for every pattern mutation:
--   created | updated | deprecated | evidence_added | merged
--
-- Written by: pattern-library API (system) on every mutating operation.

CREATE TABLE IF NOT EXISTS pattern_history (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_id   UUID         NOT NULL REFERENCES patterns(id) ON DELETE CASCADE,
    event_type   VARCHAR(30)  NOT NULL,       -- created | updated | deprecated | evidence_added | merged
    version      INTEGER      NOT NULL DEFAULT 1,   -- pattern.version at event time
    changed_by   VARCHAR(100) DEFAULT 'system',     -- 'system' | 'enrichment-worker' | 'manual'
    changes      JSONB        DEFAULT '{}',          -- {field: new_value, ...} for updates
    reason       TEXT,                               -- optional human/LLM-supplied note
    occurred_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ph_pattern  ON pattern_history (pattern_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_ph_event    ON pattern_history (event_type);
CREATE INDEX IF NOT EXISTS idx_ph_time     ON pattern_history (occurred_at DESC);
