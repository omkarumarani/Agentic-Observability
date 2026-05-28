-- Pattern Library — Migration 003: Evaluation Snapshots
-- Idempotent: uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS
--
-- Stores periodic evaluation snapshots so the team can track whether the
-- system is improving over time.  Each row is a point-in-time roll-up of
-- the live metrics computed by GET /evaluation/metrics.

CREATE TABLE IF NOT EXISTS evaluation_snapshots (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    label       VARCHAR(100) DEFAULT 'periodic',  -- 'periodic' | 'manual' | 'release-v15.1' etc.
    window_days INTEGER      NOT NULL DEFAULT 30,  -- assessment look-back window
    metrics     JSONB        NOT NULL,             -- full metrics payload (same shape as /evaluation/metrics)
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eval_snap_time ON evaluation_snapshots (created_at DESC);
