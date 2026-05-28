"""Pattern Library API — Phase 15
==================================
FastAPI service providing:
  - CRUD for the 7-table pattern intelligence schema
  - Semantic search via pgvector (nomic-embed-text 768-dim)
  - Hybrid incident-to-pattern matching (rule threshold + vector similarity)
  - Agent assessment recording (links sessions → matched patterns)

Port: 9300
DB:   PostgreSQL + pgvector (pattern-db:5432)
Docs: http://localhost:9300/docs
"""
import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .db import close_pool, get_pool, init_pool
from .embedder import embed_text, vec_to_str
from .scorer import score_pattern_from_row
from .models import (
    AssessmentCreate,
    AssessmentOutcome,
    FixCreate,
    IncidentMatchRequest,
    IncidentMatchResult,
    PatternCreate,
    PatternDetail,
    PatternListItem,
    PatternUpdate,
    SearchRequest,
    SearchResult,
    SignalCreate,
    ValidationCreate,
)
from .seeder import seed_patterns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

SEED_ON_STARTUP = os.getenv("SEED_ON_STARTUP", "true").lower() == "true"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    if SEED_ON_STARTUP:
        await asyncio.sleep(2)  # brief wait for schema to apply on cold start
        try:
            await seed_patterns()
        except Exception as exc:
            logger.warning("Pattern seeder failed (non-fatal): %s", exc)
    yield
    await close_pool()


app = FastAPI(
    title="Pattern Library",
    version="1.0.0",
    description=(
        "Observability failure pattern intelligence store. "
        "PostgreSQL + pgvector semantic search — Phase 15 of the AIOps Platform."
    ),
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────
# Health & Stats
# ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
async def health():
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM patterns WHERE NOT deprecated"
            )
        return {"status": "ok", "active_patterns": count, "db": "connected"}
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "error": str(exc)},
        )


@app.get("/stats", tags=["meta"])
async def stats():
    pool = get_pool()
    async with pool.acquire() as conn:
        totals = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE NOT deprecated)              AS active_patterns,
                COUNT(*) FILTER (WHERE deprecated)                  AS deprecated_count,
                COUNT(*) FILTER (WHERE severity='critical' AND NOT deprecated) AS critical_count,
                COUNT(*) FILTER (WHERE severity='high' AND NOT deprecated)     AS high_count,
                COUNT(*) FILTER (WHERE severity='medium' AND NOT deprecated)   AS medium_count,
                COUNT(*) FILTER (WHERE severity='low' AND NOT deprecated)      AS low_count,
                COUNT(*) FILTER (WHERE automation_readiness='safe' AND NOT deprecated) AS safe_auto,
                AVG(confidence)       FILTER (WHERE NOT deprecated)  AS avg_confidence,
                AVG(recurrence_score) FILTER (WHERE NOT deprecated)  AS avg_recurrence,
                COUNT(*) FILTER (WHERE embedding IS NOT NULL AND NOT deprecated) AS embedded_count
            FROM patterns
            """
        )
        signal_count     = await conn.fetchval("SELECT COUNT(*) FROM pattern_signals")
        fix_count        = await conn.fetchval("SELECT COUNT(*) FROM pattern_fixes")
        validation_count = await conn.fetchval("SELECT COUNT(*) FROM lab_validations")
        assessment_count = await conn.fetchval("SELECT COUNT(*) FROM agent_assessments")
        issue_count      = await conn.fetchval("SELECT COUNT(*) FROM raw_public_issues")

    return {
        "patterns": {
            "active":              totals["active_patterns"],
            "deprecated":          totals["deprecated_count"],
            "embedded":            totals["embedded_count"],
            "by_severity": {
                "critical": totals["critical_count"],
                "high":     totals["high_count"],
                "medium":   totals["medium_count"],
                "low":      totals["low_count"],
            },
            "safe_automation_ready": totals["safe_auto"],
            "avg_confidence":     round(float(totals["avg_confidence"] or 0), 3),
            "avg_recurrence_score": round(float(totals["avg_recurrence"] or 0), 3),
        },
        "signals":          signal_count,
        "fixes":            fix_count,
        "lab_validations":  validation_count,
        "agent_assessments": assessment_count,
        "raw_public_issues": issue_count,
    }


# ─────────────────────────────────────────────────────────────
# Pattern CRUD
# ─────────────────────────────────────────────────────────────

@app.get("/patterns", response_model=list[PatternListItem], tags=["patterns"])
async def list_patterns(
    severity: Optional[str] = None,
    environment: Optional[str] = None,
    deprecated: bool = False,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    pool = get_pool()
    clauses: list[str] = ["deprecated = $1"]
    params: list[Any] = [deprecated]
    idx = 2

    if severity:
        clauses.append(f"severity = ${idx}")
        params.append(severity)
        idx += 1

    if environment and environment != "any":
        clauses.append(f"(environment = ${idx} OR environment = 'any')")
        params.append(environment)
        idx += 1

    where = "WHERE " + " AND ".join(clauses)
    params += [limit, offset]

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, name, description, severity, environment, impacted_layers,
                   recurrence_score, confidence, automation_readiness,
                   evidence_count, version, deprecated, created_at, updated_at
            FROM patterns
            {where}
            ORDER BY recurrence_score DESC, created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params,
        )
    return [_row_to_list_item(r) for r in rows]


@app.post("/patterns", status_code=201, tags=["patterns"])
async def create_pattern(body: PatternCreate):
    pool = get_pool()
    embedding = await embed_text(f"{body.name} {body.description}")
    embed_val = vec_to_str(embedding) if embedding else None

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO patterns (
                name, description, environment, impacted_layers,
                recurrence_score, severity, automation_readiness,
                oss_contribution_angle, source_references,
                confidence, embedding
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::vector)
            RETURNING id, name, created_at
            """,
            body.name,
            body.description,
            body.environment,
            body.impacted_layers,
            body.recurrence_score,
            body.severity,
            body.automation_readiness,
            body.oss_contribution_angle,
            json.dumps([r.model_dump() for r in body.source_references]),
            body.confidence,
            embed_val,
        )
        await _write_history(conn, row["id"], "created", 1)
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "created_at": row["created_at"].isoformat(),
    }


@app.get("/patterns/{pattern_id}", response_model=PatternDetail, tags=["patterns"])
async def get_pattern(pattern_id: str):
    pool = get_pool()
    try:
        pid = uuid.UUID(pattern_id)
    except ValueError:
        raise HTTPException(400, "Invalid UUID format")

    async with pool.acquire() as conn:
        pattern = await conn.fetchrow(
            """
            SELECT id, name, description, environment, impacted_layers,
                   recurrence_score, severity, automation_readiness,
                   oss_contribution_angle, source_references, confidence,
                   evidence_count, version, deprecated, created_at, updated_at
            FROM patterns WHERE id = $1
            """,
            pid,
        )
        if not pattern:
            raise HTTPException(404, "Pattern not found")

        signals = await conn.fetch(
            """
            SELECT id, signal_type, name, description, query_template,
                   threshold_operator, threshold_value, severity, weight
            FROM pattern_signals WHERE pattern_id = $1 ORDER BY weight DESC
            """,
            pid,
        )
        fixes = await conn.fetch(
            """
            SELECT id, title, description, fix_type, automation_level,
                   content, risk_level, estimated_mttr_seconds, requires_restart
            FROM pattern_fixes WHERE pattern_id = $1 ORDER BY risk_level
            """,
            pid,
        )
        validations = await conn.fetch(
            """
            SELECT id, validated_at, reproducer_scenario, detected,
                   detection_latency_seconds, false_positive_rate, notes, validator
            FROM lab_validations WHERE pattern_id = $1 ORDER BY validated_at DESC LIMIT 10
            """,
            pid,
        )

    return PatternDetail(
        id=str(pattern["id"]),
        name=pattern["name"],
        description=pattern["description"],
        environment=pattern["environment"],
        impacted_layers=list(pattern["impacted_layers"] or []),
        recurrence_score=pattern["recurrence_score"],
        severity=pattern["severity"],
        automation_readiness=pattern["automation_readiness"],
        oss_contribution_angle=pattern["oss_contribution_angle"],
        source_references=pattern["source_references"],
        confidence=pattern["confidence"],
        evidence_count=pattern["evidence_count"],
        version=pattern["version"],
        deprecated=pattern["deprecated"],
        created_at=pattern["created_at"].isoformat(),
        updated_at=pattern["updated_at"].isoformat(),
        signals=[_record_to_dict(s) for s in signals],
        fixes=[_record_to_dict(f) for f in fixes],
        validations=[_record_to_dict(v) for v in validations],
    )


@app.put("/patterns/{pattern_id}", tags=["patterns"])
async def update_pattern(pattern_id: str, body: PatternUpdate):
    try:
        pid = uuid.UUID(pattern_id)
    except ValueError:
        raise HTTPException(400, "Invalid UUID format")

    changed = body.model_dump(exclude_none=True)

    updates: list[str] = []
    params: list[Any] = []
    idx = 1

    for field in ("name", "description", "environment", "severity",
                  "automation_readiness", "oss_contribution_angle",
                  "recurrence_score", "confidence"):
        if field in changed:
            updates.append(f"{field} = ${idx}")
            params.append(changed[field])
            idx += 1

    if not updates:
        raise HTTPException(400, "No updatable fields provided")

    pool = get_pool()

    # Fetch current state (name, description, version) — needed for embedding
    # regen *and* for the history version number.  Release the connection before
    # the (potentially slow) embed_text call.
    async with pool.acquire() as conn:
        current = await conn.fetchrow(
            "SELECT name, description, version FROM patterns WHERE id = $1 AND NOT deprecated",
            pid,
        )
    if not current:
        raise HTTPException(404, "Pattern not found or already deprecated")

    # Regenerate embedding if name or description changed
    if "name" in changed or "description" in changed:
        cur_name = changed.get("name", current["name"])
        cur_desc = changed.get("description", current["description"])
        embedding = await embed_text(f"{cur_name} {cur_desc}")
        if embedding:
            updates.append(f"embedding = ${idx}::vector")
            params.append(vec_to_str(embedding))
            idx += 1

    new_version = current["version"] + 1
    updates.append(f"version = ${idx}");      params.append(new_version); idx += 1
    updates.append(f"updated_at = ${idx}");   params.append(datetime.now(timezone.utc)); idx += 1
    params.append(pid)

    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.execute(
                f"UPDATE patterns SET {', '.join(updates)} WHERE id = ${idx} AND NOT deprecated",
                *params,
            )
            if result == "UPDATE 0":
                raise HTTPException(404, "Pattern not found or already deprecated")
            await _write_history(conn, pid, "updated", new_version, changes=changed)

    return {"updated": True, "version": new_version}


@app.delete("/patterns/{pattern_id}", tags=["patterns"])
async def deprecate_pattern(pattern_id: str):
    try:
        pid = uuid.UUID(pattern_id)
    except ValueError:
        raise HTTPException(400, "Invalid UUID format")

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE patterns SET deprecated = TRUE, deprecated_at = NOW()
                WHERE id = $1 AND NOT deprecated
                RETURNING version
                """,
                pid,
            )
            if not row:
                raise HTTPException(404, "Pattern not found or already deprecated")
            await _write_history(conn, pid, "deprecated", row["version"])
    return {"deprecated": True}


@app.get("/patterns/{pattern_id}/history", tags=["patterns"])
async def get_pattern_history(
    pattern_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    """
    Return the full audit trail for a pattern ordered newest-first.

    Each record has:
      id, event_type, version, changed_by, changes, reason, occurred_at
    """
    pid = _parse_uuid(pattern_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        # Verify pattern exists (any state — deprecated patterns still have history)
        exists = await conn.fetchval("SELECT id FROM patterns WHERE id = $1", pid)
        if not exists:
            raise HTTPException(404, "Pattern not found")
        rows = await conn.fetch(
            """
            SELECT id, event_type, version, changed_by, changes, reason, occurred_at
            FROM pattern_history
            WHERE pattern_id = $1
            ORDER BY occurred_at DESC
            LIMIT $2
            """,
            pid, limit,
        )
    return [_record_to_dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Signals, Fixes, Lab Validations
# ─────────────────────────────────────────────────────────────

@app.post("/patterns/{pattern_id}/signals", status_code=201, tags=["patterns"])
async def add_signal(pattern_id: str, body: SignalCreate):
    pid = _parse_uuid(pattern_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        _ensure_pattern_exists(await conn.fetchval(
            "SELECT id FROM patterns WHERE id = $1 AND NOT deprecated", pid
        ))
        row = await conn.fetchrow(
            """
            INSERT INTO pattern_signals (
                pattern_id, signal_type, name, description,
                query_template, threshold_operator, threshold_value, severity, weight
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            RETURNING id
            """,
            pid, body.signal_type, body.name, body.description,
            body.query_template, body.threshold_operator, body.threshold_value,
            body.severity, body.weight,
        )
    return {"id": str(row["id"])}


@app.post("/patterns/{pattern_id}/fixes", status_code=201, tags=["patterns"])
async def add_fix(pattern_id: str, body: FixCreate):
    pid = _parse_uuid(pattern_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        _ensure_pattern_exists(await conn.fetchval(
            "SELECT id FROM patterns WHERE id = $1 AND NOT deprecated", pid
        ))
        row = await conn.fetchrow(
            """
            INSERT INTO pattern_fixes (
                pattern_id, title, description, fix_type,
                automation_level, content, risk_level, estimated_mttr_seconds, requires_restart
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            RETURNING id
            """,
            pid, body.title, body.description, body.fix_type,
            body.automation_level, body.content, body.risk_level,
            body.estimated_mttr_seconds, body.requires_restart,
        )
    return {"id": str(row["id"])}


@app.post("/patterns/{pattern_id}/validations", status_code=201, tags=["patterns"])
async def add_validation(pattern_id: str, body: ValidationCreate):
    pid = _parse_uuid(pattern_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        _ensure_pattern_exists(await conn.fetchval(
            "SELECT id FROM patterns WHERE id = $1", pid
        ))
        row = await conn.fetchrow(
            """
            INSERT INTO lab_validations (
                pattern_id, reproducer_scenario, reproduction_steps,
                detected, detection_latency_seconds, false_positive_rate, notes, validator
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            RETURNING id, validated_at
            """,
            pid, body.reproducer_scenario, body.reproduction_steps,
            body.detected, body.detection_latency_seconds,
            body.false_positive_rate, body.notes, body.validator,
        )
        # Increment evidence_count when detection was positive
        if body.detected:
            await conn.execute(
                "UPDATE patterns SET evidence_count = evidence_count + 1, updated_at = NOW() WHERE id = $1",
                pid,
            )
    return {"id": str(row["id"]), "validated_at": row["validated_at"].isoformat()}


# ─────────────────────────────────────────────────────────────
# Semantic Search (vector similarity)
# ─────────────────────────────────────────────────────────────

@app.post("/patterns/search", response_model=list[SearchResult], tags=["intelligence"])
async def search_patterns(body: SearchRequest):
    embedding = await embed_text(body.query)
    if not embedding:
        raise HTTPException(
            503,
            "Embedding service unavailable — POST /admin/embed-all to backfill, "
            "or use POST /patterns/match-incident with signals dict for rule-only matching",
        )

    embed_val = vec_to_str(embedding)
    severity_clause = f"AND severity = '{body.severity}'" if body.severity else ""

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, name, description, severity, environment,
                   recurrence_score, confidence,
                   1 - (embedding <=> $1::vector) AS similarity
            FROM patterns
            WHERE NOT deprecated AND embedding IS NOT NULL {severity_clause}
            ORDER BY embedding <=> $1::vector
            LIMIT $2
            """,
            embed_val,
            body.limit,
        )

    return [
        SearchResult(
            id=str(r["id"]),
            name=r["name"],
            description=r["description"],
            severity=r["severity"],
            environment=r["environment"],
            recurrence_score=r["recurrence_score"],
            confidence=r["confidence"],
            similarity=round(float(r["similarity"]), 4),
        )
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────
# Hybrid Incident Matching
# ─────────────────────────────────────────────────────────────

@app.post("/patterns/match-incident", response_model=list[IncidentMatchResult], tags=["intelligence"])
async def match_incident(body: IncidentMatchRequest):
    """
    Match live incident signals against the pattern library using a hybrid approach:

    1. If incident_text is supplied → generate embedding → cosine similarity top-K*3 candidates
    2. For each candidate → score against signal thresholds in body.signals
    3. Combined score = 0.6 * rule_score + 0.4 * vector_similarity
       (pure rule_score when no embedding is available)
    4. Return top_k results sorted by combined_score descending
    """
    pool = get_pool()

    # Step 1: vector candidate retrieval
    vec_scores: dict[str, float] = {}
    if body.incident_text:
        embedding = await embed_text(body.incident_text)
        if embedding:
            embed_val = vec_to_str(embedding)
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id::text AS id,
                           1 - (embedding <=> $1::vector) AS similarity
                    FROM patterns
                    WHERE NOT deprecated AND embedding IS NOT NULL
                    ORDER BY embedding <=> $1::vector
                    LIMIT $2
                    """,
                    embed_val,
                    body.top_k * 3,
                )
            vec_scores = {r["id"]: float(r["similarity"]) for r in rows}

    # Step 2: fetch candidate pattern metadata
    async with pool.acquire() as conn:
        if vec_scores:
            id_list = list(vec_scores.keys())
            patterns = await conn.fetch(
                """
                SELECT id::text AS id, name, severity, environment, confidence, recurrence_score
                FROM patterns
                WHERE id = ANY($1::uuid[]) AND NOT deprecated
                """,
                id_list,
            )
        else:
            patterns = await conn.fetch(
                """
                SELECT id::text AS id, name, severity, environment, confidence, recurrence_score
                FROM patterns WHERE NOT deprecated
                ORDER BY recurrence_score DESC
                LIMIT $1
                """,
                body.top_k * 3,
            )

        # Step 3: rule-based scoring for each candidate
        results: list[IncidentMatchResult] = []
        for p in patterns:
            pid = p["id"]
            rule_score = await _score_rules(conn, pid, body.signals)
            v_score = vec_scores.get(pid, 0.5 if not vec_scores else 0.0)

            combined = (0.6 * rule_score + 0.4 * v_score) if vec_scores else rule_score

            results.append(
                IncidentMatchResult(
                    pattern_id=pid,
                    pattern_name=p["name"],
                    severity=p["severity"],
                    environment=p["environment"],
                    rule_score=round(rule_score, 4),
                    vector_similarity=round(v_score, 4),
                    combined_score=round(combined, 4),
                    confidence=p["confidence"],
                    recurrence_score=p["recurrence_score"],
                )
            )

    results.sort(key=lambda x: x.combined_score, reverse=True)
    return results[: body.top_k]


async def _score_rules(conn, pattern_id: str, signals: dict[str, float]) -> float:
    """
    Score a pattern against a dict of live signal values.
    Returns 0.5 (neutral) when the pattern has no threshold rules.
    """
    rows = await conn.fetch(
        """
        SELECT name, threshold_operator, threshold_value, weight
        FROM pattern_signals
        WHERE pattern_id = $1
          AND threshold_operator IS NOT NULL
          AND threshold_value IS NOT NULL
        """,
        uuid.UUID(pattern_id),
    )

    if not rows:
        return 0.5  # no rules → neutral score

    total_weight = sum(float(r["weight"]) for r in rows)
    matched_weight = 0.0

    for r in rows:
        # Normalise signal name for lookup (dots and dashes → underscores)
        canonical = r["name"].lower().replace(".", "_").replace("-", "_")
        value = signals.get(r["name"]) if r["name"] in signals else signals.get(canonical)
        if value is None:
            continue

        op = r["threshold_operator"]
        threshold = float(r["threshold_value"])
        try:
            fval = float(value)
        except (TypeError, ValueError):
            continue

        match = {
            ">":  fval > threshold,
            "<":  fval < threshold,
            ">=": fval >= threshold,
            "<=": fval <= threshold,
            "=":  abs(fval - threshold) < 1e-9,
        }.get(op, False)

        if match:
            matched_weight += float(r["weight"])

    return matched_weight / total_weight if total_weight > 0 else 0.0


# ─────────────────────────────────────────────────────────────
# Agent Assessments
# ─────────────────────────────────────────────────────────────

@app.post("/assessments", status_code=201, tags=["assessments"])
async def create_assessment(body: AssessmentCreate):
    pool = get_pool()

    embedding = await embed_text(body.incident_summary) if body.incident_summary else None
    embed_val = vec_to_str(embedding) if embedding else None

    top_pattern = (
        max(body.matched_patterns, key=lambda x: float(x.get("score", 0)), default=None)
        if body.matched_patterns
        else None
    )

    top_pid = None
    top_score = None
    if top_pattern and top_pattern.get("pattern_id"):
        try:
            top_pid = uuid.UUID(top_pattern["pattern_id"])
            top_score = float(top_pattern.get("score", 0))
        except (ValueError, TypeError):
            pass

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO agent_assessments (
                session_id, agent, alert_name,
                matched_patterns, top_pattern_id, top_pattern_score,
                risk_score, recommendation, embedding
            ) VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9::vector)
            RETURNING id, assessed_at
            """,
            body.session_id,
            body.agent,
            body.alert_name,
            json.dumps(body.matched_patterns),
            top_pid,
            top_score,
            body.risk_score,
            body.recommendation,
            embed_val,
        )

    return {"id": str(row["id"]), "assessed_at": row["assessed_at"].isoformat()}


@app.get("/assessments", tags=["assessments"])
async def list_assessments(
    agent: Optional[str] = None,
    session_id: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
):
    pool = get_pool()
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1

    if agent:
        clauses.append(f"agent = ${idx}")
        params.append(agent)
        idx += 1
    if session_id:
        clauses.append(f"session_id = ${idx}")
        params.append(session_id)
        idx += 1

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, session_id, agent, alert_name, assessed_at,
                   matched_patterns, top_pattern_id, top_pattern_score,
                   risk_score, recommendation, outcome, outcome_recorded_at
            FROM agent_assessments
            {where}
            ORDER BY assessed_at DESC
            LIMIT ${idx}
            """,
            *params,
        )

    return [_assessment_to_dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Admin endpoints
# ─────────────────────────────────────────────────────────────

@app.post("/admin/seed", tags=["admin"])
async def admin_seed(force: bool = False):
    """Manually trigger the pattern seeder. Use force=true to re-seed even if patterns exist."""
    try:
        count = await seed_patterns(force=force)
        return {"seeded": count}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.post("/admin/embed-all", tags=["admin"])
async def embed_all():
    """
    Regenerate embeddings for all active patterns with NULL embeddings.
    Run this after Ollama comes online following a cold-start without embeddings.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, description FROM patterns WHERE embedding IS NULL AND NOT deprecated"
        )

    updated = 0
    for r in rows:
        embedding = await embed_text(f"{r['name']} {r['description']}")
        if embedding:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE patterns SET embedding = $1::vector, updated_at = NOW() WHERE id = $2",
                    vec_to_str(embedding),
                    r["id"],
                )
            updated += 1

    return {"total_without_embeddings": len(rows), "updated": updated, "skipped": len(rows) - updated}


# ─────────────────────────────────────────────────────────────
# Pattern Lifecycle History — shared write helper
# ─────────────────────────────────────────────────────────────

async def _write_history(
    conn,
    pattern_id: uuid.UUID,
    event_type: str,
    version: int,
    changes: Optional[dict] = None,
    reason: Optional[str] = None,
    changed_by: str = "system",
) -> None:
    """Insert one row into pattern_history inside an existing connection/transaction."""
    await conn.execute(
        """
        INSERT INTO pattern_history
            (pattern_id, event_type, version, changed_by, changes, reason)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """,
        pattern_id,
        event_type,
        version,
        changed_by,
        json.dumps(changes or {}),
        reason,
    )


# ─────────────────────────────────────────────────────────────
# Raw Public Issues (n8n ingestion endpoint)
# ─────────────────────────────────────────────────────────────

# Minimum engagement score required per source before a raw issue is stored.
_SOURCE_MIN_SCORE: dict[str, int] = {
    "github":        0,   # GitHub engagement already filtered by n8n (score ≥ 2)
    "stackoverflow": 3,
    "reddit":        8,
    "hackernews":    8,
    "blog":          0,   # RSS blogs have no numeric score
}

# At least one of these tokens must appear in title+body (case-insensitive).
_OBS_KEYWORDS: frozenset[str] = frozenset({
    "opentelemetry", "otel", "prometheus", "grafana", "loki", "tempo",
    "jaeger", "zipkin", "metric", "trace", "span", "log", "alert",
    "observability", "telemetry", "exporter", "collector", "otlp",
    "instrumentation", "scrape", "cardinality", "sampling", "baggage",
})

# If any of these appear in title (case-insensitive) the issue is rejected.
_SPAM_PHRASES: frozenset[str] = frozenset({
    "hiring", "we are hiring", "job opening", "salary", "referral",
    "coupon", "discount", "[ad]", "sponsored", "affiliate",
})

_MIN_TITLE_LEN = 10   # characters
_MIN_BODY_LEN  = 40   # characters


def _quality_gate(source: str, title: str, body: str, score: int) -> tuple[bool, str]:
    """
    Returns (True, "") when the issue passes quality checks.
    Returns (False, reason) when it should be rejected.
    """
    title_clean = (title or "").strip()
    body_clean  = (body  or "").strip()
    text_lower  = (title_clean + " " + body_clean).lower()

    # 1. Minimum title length
    if len(title_clean) < _MIN_TITLE_LEN:
        return False, f"title too short ({len(title_clean)} chars, min {_MIN_TITLE_LEN})"

    # 2. Minimum body length
    if len(body_clean) < _MIN_BODY_LEN:
        return False, f"body too short ({len(body_clean)} chars, min {_MIN_BODY_LEN})"

    # 3. Spam / off-topic title check
    title_lower = title_clean.lower()
    for phrase in _SPAM_PHRASES:
        if phrase in title_lower:
            return False, f"spam phrase detected: '{phrase}'"

    # 4. Observability relevance — at least one OBS keyword must be present
    if not any(kw in text_lower for kw in _OBS_KEYWORDS):
        return False, "no observability keywords found in title or body"

    # 5. Source-specific minimum engagement score
    min_score = _SOURCE_MIN_SCORE.get(source, 0)
    if score < min_score:
        return False, f"score {score} below minimum {min_score} for source '{source}'"

    return True, ""


@app.post("/issues/raw", status_code=201, tags=["ingestion"])
async def ingest_raw_issue(body: dict):
    """
    Receive a raw public issue from the n8n discovery pipeline.
    Required fields: source, source_id, url
    Optional: title, body, author, score, tags

    Runs a data quality gate before writing to the DB.  Issues that fail
    the gate are rejected with HTTP 422 and a machine-readable reason so
    n8n can log them without retrying.
    """
    for required in ("source", "source_id", "url"):
        if not body.get(required):
            raise HTTPException(400, f"Missing required field: {required}")

    passed, reason = _quality_gate(
        source=body.get("source", ""),
        title=body.get("title", ""),
        body=body.get("body", ""),
        score=int(body.get("score", 0)),
    )
    if not passed:
        raise HTTPException(422, detail={"quality_gate": "rejected", "reason": reason})

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO raw_public_issues (source, source_id, url, title, body, author, score, tags)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (source, source_id) DO NOTHING
            RETURNING id
            """,
            body["source"],
            body["source_id"],
            body["url"],
            body.get("title"),
            body.get("body"),
            body.get("author"),
            int(body.get("score", 0)),
            body.get("tags", []),
        )
    if row:
        return {"id": str(row["id"]), "created": True}
    return {"created": False, "reason": "duplicate"}


@app.get("/issues/raw", tags=["ingestion"])
async def list_raw_issues(
    source: Optional[str] = None,
    processed: Optional[bool] = None,
    limit: int = Query(20, ge=1, le=100),
):
    pool = get_pool()
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1

    if source:
        clauses.append(f"source = ${idx}"); params.append(source); idx += 1
    if processed is not None:
        clauses.append(f"processed = ${idx}"); params.append(processed); idx += 1

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, source, source_id, url, title, body, score, tags, fetched_at, processed
            FROM raw_public_issues {where}
            ORDER BY fetched_at DESC
            LIMIT ${idx}
            """,
            *params,
        )
    return [_record_to_dict(r) for r in rows]


@app.get("/issues/raw/{issue_id}", tags=["ingestion"])
async def get_raw_issue(issue_id: str):
    """Get a single raw issue including full body text."""
    rid = _parse_uuid(issue_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, source, source_id, url, title, body, author, score, tags, fetched_at, processed FROM raw_public_issues WHERE id = $1",
            rid,
        )
    if not row:
        raise HTTPException(404, "Raw issue not found")
    return _record_to_dict(row)


# ─────────────────────────────────────────────────────────────
# Section — Enrichment Pipeline
#   raw_public_issues  →  enriched_issues
#   enriched_issues    →  patterns  (via /issues/enriched/{id}/promote)
# ─────────────────────────────────────────────────────────────

@app.post("/issues/raw/{issue_id}/enrich", status_code=200, tags=["enrichment"])
async def enrich_raw_issue(issue_id: str, body: dict = {}):
    """
    Promote a raw issue to enriched_issues.

    Accepts an already-processed pain extraction payload (produced by
    the enrichment worker using Section G prompt chains).  If the worker
    calls this endpoint with the LLM output, the pain point + embedding
    are stored and the raw issue is marked processed=true.

    Expected body:
      {
        "pain_point":          "...",
        "affected_component":  "collector | prometheus | ...",
        "environment":         "kubernetes | vm | any",
        "symptoms":            ["...", "..."],
        "quality_score":       0.0–1.0,
        "llm_model":           "llama3.2:3b"
      }
    """
    rid = _parse_uuid(issue_id)
    pool = get_pool()

    async with pool.acquire() as conn:
        raw = await conn.fetchrow(
            "SELECT id, title, body FROM raw_public_issues WHERE id = $1", rid
        )
        if not raw:
            raise HTTPException(404, "Raw issue not found")

        pain_point = body.get("pain_point") or raw["title"] or ""
        component  = body.get("affected_component", "unknown")
        environment = body.get("environment", "any")
        symptoms   = body.get("symptoms", [])
        quality    = float(body.get("quality_score", 0.5))
        llm_model  = body.get("llm_model", "manual")

        # Generate embedding for the pain point
        embedding = await embed_text(pain_point)
        embed_val = vec_to_str(embedding) if embedding else None

        # Check for semantic duplicates in enriched_issues
        is_duplicate  = False
        duplicate_of  = None
        if embed_val:
            dup_row = await conn.fetchrow(
                """
                SELECT id FROM enriched_issues
                WHERE embedding IS NOT NULL
                  AND NOT is_duplicate
                  AND 1 - (embedding <=> $1::vector) > 0.92
                ORDER BY embedding <=> $1::vector
                LIMIT 1
                """,
                embed_val,
            )
            if dup_row:
                is_duplicate = True
                duplicate_of = dup_row["id"]

        enriched = await conn.fetchrow(
            """
            INSERT INTO enriched_issues (
                raw_issue_id, pain_point, affected_component, environment,
                symptoms, is_duplicate, duplicate_of, quality_score,
                embedding, llm_model
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::vector,$10)
            RETURNING id
            """,
            rid, pain_point, component, environment,
            symptoms, is_duplicate, duplicate_of, quality,
            embed_val, llm_model,
        )

        # Mark raw issue as processed
        await conn.execute(
            "UPDATE raw_public_issues SET processed = TRUE WHERE id = $1", rid
        )

    return {
        "enriched_id":  str(enriched["id"]),
        "is_duplicate": is_duplicate,
        "duplicate_of": str(duplicate_of) if duplicate_of else None,
    }


@app.get("/issues/enriched", tags=["enrichment"])
async def list_enriched_issues(
    component: Optional[str] = None,
    is_duplicate: Optional[bool] = None,
    limit: int = Query(20, ge=1, le=100),
):
    """List enriched issues, optionally filtered by component or duplicate status."""
    pool = get_pool()
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1

    if component:
        clauses.append(f"affected_component = ${idx}"); params.append(component); idx += 1
    if is_duplicate is not None:
        clauses.append(f"is_duplicate = ${idx}"); params.append(is_duplicate); idx += 1

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, raw_issue_id, pain_point, affected_component, environment,
                   symptoms, is_duplicate, quality_score, llm_model, processed_at
            FROM enriched_issues {where}
            ORDER BY processed_at DESC
            LIMIT ${idx}
            """,
            *params,
        )
    return [_record_to_dict(r) for r in rows]


@app.get("/issues/enriched/{enriched_id}", tags=["enrichment"])
async def get_enriched_issue(enriched_id: str):
    """Get a single enriched issue by ID."""
    eid = _parse_uuid(enriched_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT e.id, e.raw_issue_id, e.pain_point, e.affected_component,
                   e.environment, e.symptoms, e.is_duplicate, e.duplicate_of,
                   e.quality_score, e.llm_model, e.processed_at,
                   r.source, r.url, r.title AS raw_title, r.score AS raw_score
            FROM enriched_issues e
            JOIN raw_public_issues r ON r.id = e.raw_issue_id
            WHERE e.id = $1
            """,
            eid,
        )
        if not row:
            raise HTTPException(404, "Enriched issue not found")
    return _record_to_dict(row)


@app.post("/issues/enriched/{enriched_id}/promote", status_code=200, tags=["enrichment"])
async def promote_enriched_to_pattern(enriched_id: str, body: dict = {}):
    """
    Create or merge a pattern from an enriched issue.

    The enrichment worker calls this after running Chains 3+4 (pattern_creator +
    pattern_clusterer) to either:
      - create a new pattern if no similar pattern exists (similarity < 0.80)
      - increment recurrence_score + evidence_count on an existing matched pattern

    Expected body:
      {
        "pattern_name":        "...",
        "description":         "...",
        "severity":            "critical|high|medium|low",
        "environment":         "kubernetes|vm|any",
        "impacted_layers":     ["collector", "app"],
        "automation_readiness": "safe|risky|manual",
        "oss_contribution_angle": "...",
        "recurrence_delta":    0.05   # how much to bump recurrence on merge
      }
    """
    eid = _parse_uuid(enriched_id)
    pool = get_pool()

    async with pool.acquire() as conn:
        enriched = await conn.fetchrow(
            "SELECT id, pain_point, affected_component, environment, embedding FROM enriched_issues WHERE id = $1",
            eid,
        )
        if not enriched:
            raise HTTPException(404, "Enriched issue not found")

        if enriched["is_duplicate"] if "is_duplicate" in enriched.keys() else False:
            return {"action": "skipped", "reason": "duplicate enriched issue"}

        embed_val = enriched["embedding"]

        # Check if a similar pattern already exists (merge candidate)
        existing = None
        if embed_val:
            existing = await conn.fetchrow(
                """
                SELECT id, name, recurrence_score, evidence_count
                FROM patterns
                WHERE embedding IS NOT NULL AND NOT deprecated
                  AND 1 - (embedding <=> $1::vector) > 0.80
                ORDER BY embedding <=> $1::vector
                LIMIT 1
                """,
                embed_val,
            )

        if existing:
            # Merge: bump recurrence and evidence count
            delta = float(body.get("recurrence_delta", 0.03))
            new_recurrence = min(1.0, float(existing["recurrence_score"]) + delta)
            merged_version = existing["evidence_count"] + 2  # proxy: evidence_count + 1 after update
            row = await conn.fetchrow(
                """
                UPDATE patterns
                SET recurrence_score = $1,
                    evidence_count  = evidence_count + 1,
                    version         = version + 1,
                    updated_at      = NOW()
                WHERE id = $2
                RETURNING version
                """,
                new_recurrence,
                existing["id"],
            )
            await _write_history(
                conn, existing["id"], "evidence_added", row["version"],
                changes={"recurrence_score": new_recurrence, "evidence_count": "+1"},
                reason=f"merged from enriched_issue {str(eid)}",
                changed_by="enrichment-worker",
            )
            return {
                "action":      "merged",
                "pattern_id":  str(existing["id"]),
                "pattern_name": existing["name"],
                "new_recurrence": new_recurrence,
            }

        # Create new pattern
        pattern_name = body.get("pattern_name") or enriched["pain_point"][:80]
        description  = body.get("description") or enriched["pain_point"]
        severity     = body.get("severity", "medium")
        environment  = body.get("environment") or enriched["environment"] or "any"
        layers       = body.get("impacted_layers", [enriched["affected_component"] or "unknown"])
        auto         = body.get("automation_readiness", "manual")
        oss_angle    = body.get("oss_contribution_angle")

        pattern_embed = embed_val  # reuse enriched issue embedding

        new_pid = await conn.fetchval(
            """
            INSERT INTO patterns (
                name, description, environment, impacted_layers,
                recurrence_score, severity, automation_readiness,
                oss_contribution_angle, confidence, embedding
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::vector)
            RETURNING id
            """,
            pattern_name, description, environment, layers,
            0.10,   # initial low recurrence — builds via merges
            severity, auto, oss_angle,
            0.40,   # initial low confidence — builds via feedback
            embed_val,
        )
        await _write_history(
            conn, new_pid, "created", 1,
            reason=f"promoted from enriched_issue {str(eid)}",
            changed_by="enrichment-worker",
        )
        return {
            "action":     "created",
            "pattern_id": str(new_pid),
            "pattern_name": pattern_name,
        }



def _parse_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError:
        raise HTTPException(400, f"Invalid UUID: {value}")


def _ensure_pattern_exists(row):
    if not row:
        raise HTTPException(404, "Pattern not found or deprecated")


def _row_to_list_item(r) -> PatternListItem:
    return PatternListItem(
        id=str(r["id"]),
        name=r["name"],
        description=r["description"],
        severity=r["severity"],
        environment=r["environment"],
        impacted_layers=list(r["impacted_layers"] or []),
        recurrence_score=r["recurrence_score"],
        confidence=r["confidence"],
        automation_readiness=r["automation_readiness"],
        evidence_count=r["evidence_count"],
        version=r["version"],
        deprecated=r["deprecated"],
        created_at=r["created_at"].isoformat(),
        updated_at=r["updated_at"].isoformat(),
    )


def _record_to_dict(r) -> dict:
    """Convert an asyncpg Record to a JSON-serialisable dict."""
    result = {}
    for key in r.keys():
        val = r[key]
        if isinstance(val, uuid.UUID):
            result[key] = str(val)
        elif isinstance(val, datetime):
            result[key] = val.isoformat()
        else:
            result[key] = val
    return result


def _assessment_to_dict(r) -> dict:
    d = _record_to_dict(r)
    if d.get("top_pattern_id"):
        d["top_pattern_id"] = str(d["top_pattern_id"])
    return d


# ─────────────────────────────────────────────────────────────
# Section H — Pattern Scoring (GET /patterns/{id}/score)
# ─────────────────────────────────────────────────────────────

@app.get("/patterns/{pattern_id}/score", tags=["scoring"])
async def get_pattern_score(pattern_id: str):
    """
    Compute the composite importance score for a single pattern.

    Uses the Section H weighted formula:
      30% recurrence · 25% business impact · 20% technical depth
      15% automation feasibility · 10% OSS potential
    """
    pid = _parse_uuid(pattern_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, name, severity, environment, recurrence_score, confidence,
                   automation_readiness, oss_contribution_angle, evidence_count
            FROM patterns WHERE id = $1
            """,
            pid,
        )
        _ensure_pattern_exists(row)

        signal_count     = await conn.fetchval(
            "SELECT COUNT(*) FROM pattern_signals WHERE pattern_id = $1", pid
        )
        fix_count        = await conn.fetchval(
            "SELECT COUNT(*) FROM pattern_fixes WHERE pattern_id = $1", pid
        )
        validation_count = await conn.fetchval(
            "SELECT COUNT(*) FROM lab_validations WHERE pattern_id = $1", pid
        )
        evidence_count   = await conn.fetchval(
            "SELECT COUNT(*) FROM agent_assessments WHERE top_pattern_id = $1", pid
        )

    ps = score_pattern_from_row(
        dict(row),
        counts={
            "signals":     signal_count,
            "fixes":       fix_count,
            "validations": validation_count,
            "assessments": evidence_count,
        },
    )
    return {
        "pattern_id":     pattern_id,
        "pattern_name":   row["name"],
        "composite_score": ps.composite,
        "priority_tier":  ps.priority_tier,
        "breakdown":      ps.breakdown,
    }


@app.get("/patterns/leaderboard", tags=["scoring"])
async def pattern_leaderboard(top_n: int = Query(10, ge=1, le=50)):
    """
    Return the top-N patterns ranked by composite importance score (Section H).
    Includes signal/fix/validation counts and score breakdown.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.name, p.severity, p.environment, p.recurrence_score,
                   p.confidence, p.automation_readiness, p.oss_contribution_angle,
                   p.evidence_count,
                   COUNT(DISTINCT ps.id)  AS signal_count,
                   COUNT(DISTINCT pf.id)  AS fix_count,
                   COUNT(DISTINCT lv.id)  AS validation_count,
                   COUNT(DISTINCT aa.id)  AS assessment_count
            FROM patterns p
            LEFT JOIN pattern_signals  ps ON ps.pattern_id = p.id
            LEFT JOIN pattern_fixes    pf ON pf.pattern_id = p.id
            LEFT JOIN lab_validations  lv ON lv.pattern_id = p.id
            LEFT JOIN agent_assessments aa ON aa.top_pattern_id = p.id
            WHERE NOT p.deprecated
            GROUP BY p.id
            ORDER BY p.recurrence_score DESC
            LIMIT 100
            """
        )

    scored = []
    for r in rows:
        ps = score_pattern_from_row(
            dict(r),
            counts={
                "signals":     r["signal_count"],
                "fixes":       r["fix_count"],
                "validations": r["validation_count"],
                "assessments": r["assessment_count"],
            },
        )
        scored.append({
            "pattern_id":     str(r["id"]),
            "pattern_name":   r["name"],
            "severity":       r["severity"],
            "composite_score": ps.composite,
            "priority_tier":  ps.priority_tier,
            "recurrence_score": float(r["recurrence_score"]),
            "automation_readiness": r["automation_readiness"],
        })

    scored.sort(key=lambda x: x["composite_score"], reverse=True)
    return scored[:top_n]


# ─────────────────────────────────────────────────────────────
# Section I — Action Layer (GET /patterns/{id}/actions)
# ─────────────────────────────────────────────────────────────

@app.get("/patterns/{pattern_id}/actions", tags=["actions"])
async def get_pattern_actions(pattern_id: str):
    """
    Generate ready-to-use action artifacts from a pattern's documented fixes.

    Returns:
      - prometheus_alert_rule   YAML alert rule snippet
      - otel_collector_snippet  YAML processor/filter config
      - ansible_playbook        YAML playbook stub
      - runbook_markdown        Incident runbook in Markdown
      - grafana_panel_hint      Panel description for dashboard creation
    """
    pid = _parse_uuid(pattern_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, name, description, severity, environment, impacted_layers,
                   automation_readiness, confidence, recurrence_score
            FROM patterns WHERE id = $1
            """,
            pid,
        )
        _ensure_pattern_exists(row)

        signals = await conn.fetch(
            """
            SELECT name, signal_type, threshold_operator, threshold_value, unit, weight
            FROM pattern_signals WHERE pattern_id = $1
            ORDER BY weight DESC
            """,
            pid,
        )
        fixes = await conn.fetch(
            """
            SELECT title, description, automation_safe, config_snippet
            FROM pattern_fixes WHERE pattern_id = $1
            ORDER BY automation_safe DESC
            """,
            pid,
        )

    name        = row["name"]
    description = row["description"]
    severity    = row["severity"]
    safe_fixes  = [f for f in fixes if f["automation_safe"]]

    # ── Prometheus alert rule ─────────────────────────────────────────────
    alert_exprs = []
    for sig in signals:
        if sig["threshold_operator"] and sig["threshold_value"] is not None:
            op_map = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<=", "eq": "=="}
            op = op_map.get(sig["threshold_operator"], sig["threshold_operator"])
            # Best-effort metric name → PromQL expression
            metric = sig["name"]
            alert_exprs.append(f"  # {metric} {op} {sig['threshold_value']}{' ' + sig['unit'] if sig['unit'] else ''}")

    expr_block = "\n".join(alert_exprs) if alert_exprs else "  # (define PromQL expression here)"
    prom_severity = "critical" if severity in ("critical", "high") else "warning"

    prometheus_alert_rule = f"""\
# Auto-generated alert rule for pattern: {name}
# Tune thresholds and expr before production use
groups:
  - name: pattern.{name}
    rules:
      - alert: {_to_alert_name(name)}
        expr: |
{expr_block}
        for: 5m
        labels:
          severity: {prom_severity}
          pattern: "{name}"
        annotations:
          summary: "{description[:100]}"
          runbook: "https://wiki.internal/runbooks/{name}"
"""

    # ── OTel Collector snippet ────────────────────────────────────────────
    affected = list(row["impacted_layers"] or [])
    otel_snippet = f"""\
# OTel Collector config snippet for pattern: {name}
# Add to your otel-collector config.yaml processors section
processors:
  filter/{name}_guard:
    error_mode: ignore
    # Drop or tag telemetry that matches the known bad pattern
    # Adjust metric_statements to match your instrumentation
    metrics:
      datapoint:
        - 'attributes["service.name"] != ""'  # passthrough — define drop rules here
  attributes/{name}_enrich:
    actions:
      - key: aiops.pattern
        value: "{name}"
        action: insert
      - key: aiops.pattern_severity
        value: "{severity}"
        action: insert
"""

    # ── Ansible playbook stub ─────────────────────────────────────────────
    tasks_yaml = ""
    for i, fix in enumerate(safe_fixes[:3], start=1):
        task_name = fix["title"]
        snippet   = (fix["config_snippet"] or "").strip()
        tasks_yaml += f"""\
  - name: "Fix {i}: {task_name}"
    # {fix['description'][:120]}
    debug:
      msg: "TODO: implement fix — {task_name}"
    # {snippet[:80] if snippet else '# (no config snippet available)'}

"""

    if not tasks_yaml:
        tasks_yaml = """\
  - name: "Manual remediation required"
    debug:
      msg: "No safe automation available for this pattern — follow runbook"
"""

    ansible_playbook = f"""\
---
# Auto-generated Ansible playbook stub for pattern: {name}
# Generated by Pattern Library action layer — Section I
# Automation safety: {row['automation_readiness']}
# Review all tasks before running in production!

- name: "Remediate: {name}"
  hosts: "{{{{ target_hosts | default('localhost') }}}}"
  gather_facts: false
  vars:
    pattern_name: "{name}"
    pattern_severity: "{severity}"

  tasks:
{tasks_yaml}\
  - name: "Post-fix validation"
    debug:
      msg: "Verify: {{{{ pattern_name }}}} signals have returned to normal"
"""

    # ── Runbook Markdown ──────────────────────────────────────────────────
    signals_table = "| Signal | Type | Threshold |\n|--------|------|-----------|\n"
    for sig in signals:
        op   = sig["threshold_operator"] or "—"
        val  = sig["threshold_value"]
        unit = sig["unit"] or ""
        threshold_str = f"{op} {val}{unit}" if val is not None else "—"
        signals_table += f"| `{sig['name']}` | {sig['signal_type']} | {threshold_str} |\n"

    fixes_section = ""
    for i, fix in enumerate(fixes, start=1):
        auto_icon = "✅" if fix["automation_safe"] else "🛑"
        fixes_section += f"\n### Fix {i}: {fix['title']} {auto_icon}\n\n{fix['description']}\n"
        if fix["config_snippet"]:
            fixes_section += f"\n```yaml\n{fix['config_snippet']}\n```\n"

    runbook_markdown = f"""\
# Runbook: {name}

**Severity:** {severity.upper()}  |  **Environment:** {row['environment']}  |  **Confidence:** {row['confidence']:.0%}

## Description

{description}

## Signals to Check

{signals_table}

## Investigation Steps

1. Confirm alert is firing — check Alertmanager and Grafana
2. Pull last 30 min of metrics for signals listed above
3. Correlate with logs: search for ERROR/WARN patterns in Loki
4. Check recent deployments (git log, CI/CD history)
5. Assess blast radius — which downstream services are affected?
{fixes_section}

## Escalation

If no fix resolves the pattern within 30min at critical severity, escalate to on-call SRE.

## Related Patterns

- Check Pattern Library for similar patterns using semantic search
- Run: `POST /patterns/search` with this pattern's description as query

---
*Generated by AIOps Pattern Library — Section I Action Layer*
"""

    # ── Grafana panel hint ────────────────────────────────────────────────
    panel_signals = [sig["name"] for sig in signals[:4]]
    grafana_panel_hint = {
        "panel_type":     "timeseries",
        "title":          f"Pattern: {name}",
        "description":    description[:150],
        "suggested_metrics": panel_signals,
        "threshold_lines": [
            {"metric": sig["name"], "value": sig["threshold_value"]}
            for sig in signals if sig["threshold_value"] is not None
        ][:3],
        "alert_rule_name": _to_alert_name(name),
    }

    return {
        "pattern_id":            pattern_id,
        "pattern_name":          name,
        "automation_readiness":  row["automation_readiness"],
        "prometheus_alert_rule": prometheus_alert_rule,
        "otel_collector_snippet": otel_snippet,
        "ansible_playbook":      ansible_playbook,
        "runbook_markdown":      runbook_markdown,
        "grafana_panel_hint":    grafana_panel_hint,
    }


def _to_alert_name(pattern_name: str) -> str:
    """Convert snake_case pattern name to PascalCase alert name."""
    return "".join(word.capitalize() for word in pattern_name.split("_"))


# ─────────────────────────────────────────────────────────────
# Section N — Feedback Loop: outcome-driven confidence update
# ─────────────────────────────────────────────────────────────

@app.post("/assessments/{assessment_id}/outcome", tags=["feedback"])
async def record_assessment_outcome_with_learning(
    assessment_id: str,
    body: AssessmentOutcome,
):
    """
    Section N — Feedback Loop.

    Record the real-world outcome of an agent assessment AND update pattern
    confidence scores based on aggregated historical outcomes.

    Outcome values:
      resolved          — fix worked, incident cleared
      escalated         — fix did not work, required human escalation
      false_positive     — incident was not real
      unknown           — outcome unknown / still open
    """
    aid = _parse_uuid(assessment_id)
    pool = get_pool()

    async with pool.acquire() as conn:
        assessment = await conn.fetchrow(
            "SELECT id, top_pattern_id FROM agent_assessments WHERE id = $1", aid
        )
        if not assessment:
            raise HTTPException(404, "Assessment not found")

        # Record outcome on assessment
        await conn.execute(
            """
            UPDATE agent_assessments
            SET outcome = $1, outcome_recorded_at = NOW()
            WHERE id = $2
            """,
            body.outcome,
            aid,
        )

        # ── Section N: update pattern confidence if a top pattern was matched ──
        top_pid = assessment["top_pattern_id"]
        if top_pid and body.outcome in ("resolved", "escalated", "false_positive"):
            # Aggregate all outcomes for this pattern
            outcome_rows = await conn.fetch(
                """
                SELECT outcome, COUNT(*) AS cnt
                FROM agent_assessments
                WHERE top_pattern_id = $1 AND outcome IS NOT NULL
                GROUP BY outcome
                """,
                top_pid,
            )
            counts = {r["outcome"]: int(r["cnt"]) for r in outcome_rows}
            resolved       = counts.get("resolved", 0)
            total_decisive = resolved + counts.get("escalated", 0) + counts.get("false_positive", 0)

            if total_decisive >= 2:
                # Bayesian-style confidence update:
                #   new_confidence = (resolved + 1) / (total_decisive + 2)  [Laplace smoothing]
                new_confidence = (resolved + 1) / (total_decisive + 2)
                new_confidence = round(min(0.99, max(0.10, new_confidence)), 4)

                # Bump evidence_count on each resolved outcome
                if body.outcome == "resolved":
                    await conn.execute(
                        """
                        UPDATE patterns
                        SET confidence = $1,
                            evidence_count = evidence_count + 1,
                            updated_at = NOW()
                        WHERE id = $2
                        """,
                        new_confidence,
                        top_pid,
                    )
                else:
                    await conn.execute(
                        "UPDATE patterns SET confidence = $1, updated_at = NOW() WHERE id = $2",
                        new_confidence,
                        top_pid,
                    )

                return {
                    "recorded": True,
                    "pattern_confidence_updated": True,
                    "new_confidence": new_confidence,
                    "total_decisive_outcomes": total_decisive,
                    "resolved_count": resolved,
                }

    return {"recorded": True, "pattern_confidence_updated": False}


@app.get("/patterns/{pattern_id}/feedback-summary", tags=["feedback"])
async def get_pattern_feedback_summary(pattern_id: str):
    """
    Section N — Return outcome statistics for a pattern to show how reliable
    the fix recommendations have been in the field.
    """
    pid = _parse_uuid(pattern_id)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, confidence, recurrence_score FROM patterns WHERE id = $1", pid
        )
        _ensure_pattern_exists(row)

        outcome_rows = await conn.fetch(
            """
            SELECT outcome, COUNT(*) AS cnt
            FROM agent_assessments
            WHERE top_pattern_id = $1 AND outcome IS NOT NULL
            GROUP BY outcome
            """,
            pid,
        )
        total_assessments = await conn.fetchval(
            "SELECT COUNT(*) FROM agent_assessments WHERE top_pattern_id = $1", pid
        )

    outcomes = {r["outcome"]: int(r["cnt"]) for r in outcome_rows}
    resolved   = outcomes.get("resolved", 0)
    escalated  = outcomes.get("escalated", 0)
    fp         = outcomes.get("false_positive", 0)
    total_dec  = resolved + escalated + fp
    resolution_rate = round(resolved / total_dec, 3) if total_dec > 0 else None

    return {
        "pattern_id":       pattern_id,
        "pattern_name":     row["name"],
        "current_confidence": float(row["confidence"]),
        "total_assessments":  total_assessments,
        "outcome_counts":     outcomes,
        "resolution_rate":    resolution_rate,
        "data_quality":       "sufficient" if total_dec >= 5 else "insufficient",
    }


# ─────────────────────────────────────────────────────────────
# Section U — Evaluation Framework
#
# Measures whether the system is actually working:
#   • Pattern matching precision (true-positive rate)
#   • Resolution rate (recommendations that fixed the incident)
#   • False-positive rate
#   • Pipeline throughput & conversion rates
#   • Adoption (assessments per day)
#   • Pattern utilisation (% of patterns actually matched)
#   • Mean time between assessment and outcome recording
# ─────────────────────────────────────────────────────────────

@app.get("/evaluation/metrics", tags=["evaluation"])
async def evaluation_metrics(
    window_days: int = Query(30, ge=1, le=365),
):
    """
    Compute live evaluation metrics over the last *window_days* days.

    Returns a single JSON object with five blocks:
      accuracy   — precision, resolution rate, false-positive rate
      pipeline   — raw→enriched→pattern conversion funnel
      adoption   — assessments per day, unique sessions
      coverage   — patterns utilised at least once, top/bottom performers
      timeliness — median hours to record an outcome after assessment
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        # ── Accuracy (from agent_assessments with recorded outcomes) ──────
        outcome_rows = await conn.fetch(
            """
            SELECT outcome, COUNT(*) AS cnt
            FROM agent_assessments
            WHERE assessed_at >= NOW() - ($1 || ' days')::interval
              AND outcome IS NOT NULL
            GROUP BY outcome
            """,
            str(window_days),
        )
        oc = {r["outcome"]: int(r["cnt"]) for r in outcome_rows}
        resolved   = oc.get("resolved", 0)
        escalated  = oc.get("escalated", 0)
        fp         = oc.get("false_positive", 0)
        unknown_oc = oc.get("unknown", 0)
        total_with_outcome = resolved + escalated + fp + unknown_oc
        decisive           = resolved + escalated + fp  # excludes "unknown"

        resolution_rate  = round(resolved / decisive, 4) if decisive else None
        false_pos_rate   = round(fp / decisive, 4) if decisive else None
        escalation_rate  = round(escalated / decisive, 4) if decisive else None

        # Precision proxy: an assessment is "correct" if the top pattern led
        # to a resolved outcome.  resolved / (resolved + false_positive).
        precision_denom = resolved + fp
        precision = round(resolved / precision_denom, 4) if precision_denom else None

        accuracy = {
            "window_days":       window_days,
            "total_with_outcome": total_with_outcome,
            "decisive_outcomes":  decisive,
            "outcome_counts":     oc,
            "resolution_rate":    resolution_rate,
            "false_positive_rate": false_pos_rate,
            "escalation_rate":    escalation_rate,
            "precision":          precision,
            "data_quality":       "sufficient" if decisive >= 10 else (
                                  "marginal" if decisive >= 3 else "insufficient"),
        }

        # ── Pipeline funnel ──────────────────────────────────────────────
        raw_total = await conn.fetchval(
            "SELECT COUNT(*) FROM raw_public_issues WHERE fetched_at >= NOW() - ($1 || ' days')::interval",
            str(window_days),
        )
        enriched_total = await conn.fetchval(
            "SELECT COUNT(*) FROM enriched_issues WHERE processed_at >= NOW() - ($1 || ' days')::interval",
            str(window_days),
        )
        enriched_dupes = await conn.fetchval(
            """SELECT COUNT(*) FROM enriched_issues
               WHERE processed_at >= NOW() - ($1 || ' days')::interval AND is_duplicate = true""",
            str(window_days),
        )
        patterns_created = await conn.fetchval(
            "SELECT COUNT(*) FROM patterns WHERE created_at >= NOW() - ($1 || ' days')::interval",
            str(window_days),
        )
        patterns_total = await conn.fetchval(
            "SELECT COUNT(*) FROM patterns WHERE NOT deprecated"
        )

        pipeline = {
            "raw_ingested":       raw_total,
            "enriched":           enriched_total,
            "enriched_duplicates": enriched_dupes,
            "patterns_created":   patterns_created,
            "patterns_active":    patterns_total,
            "raw_to_enriched_rate": round(enriched_total / raw_total, 4) if raw_total else None,
            "enriched_to_pattern_rate": round(patterns_created / enriched_total, 4) if enriched_total else None,
        }

        # ── Adoption ─────────────────────────────────────────────────────
        assess_total = await conn.fetchval(
            "SELECT COUNT(*) FROM agent_assessments WHERE assessed_at >= NOW() - ($1 || ' days')::interval",
            str(window_days),
        )
        unique_sessions = await conn.fetchval(
            """SELECT COUNT(DISTINCT session_id) FROM agent_assessments
               WHERE assessed_at >= NOW() - ($1 || ' days')::interval""",
            str(window_days),
        )
        assessments_per_day = round(assess_total / window_days, 2) if window_days else 0

        adoption = {
            "total_assessments":    assess_total,
            "unique_sessions":      unique_sessions,
            "assessments_per_day":  assessments_per_day,
            "outcome_recording_rate": round(total_with_outcome / assess_total, 4) if assess_total else None,
        }

        # ── Coverage — which patterns are actually being matched? ────────
        utilised_patterns = await conn.fetchval(
            """SELECT COUNT(DISTINCT top_pattern_id) FROM agent_assessments
               WHERE assessed_at >= NOW() - ($1 || ' days')::interval
                 AND top_pattern_id IS NOT NULL""",
            str(window_days),
        )
        coverage_rate = round(utilised_patterns / patterns_total, 4) if patterns_total else None

        top_patterns = await conn.fetch(
            """
            SELECT p.id, p.name, COUNT(a.id) AS match_count,
                   AVG(a.top_pattern_score) AS avg_score
            FROM agent_assessments a
            JOIN patterns p ON p.id = a.top_pattern_id
            WHERE a.assessed_at >= NOW() - ($1 || ' days')::interval
            GROUP BY p.id, p.name
            ORDER BY match_count DESC
            LIMIT 5
            """,
            str(window_days),
        )
        never_matched = await conn.fetch(
            """
            SELECT p.id, p.name, p.severity, p.created_at
            FROM patterns p
            WHERE NOT p.deprecated
              AND p.id NOT IN (
                SELECT DISTINCT top_pattern_id FROM agent_assessments
                WHERE top_pattern_id IS NOT NULL
              )
            ORDER BY p.created_at
            LIMIT 10
            """,
        )

        coverage = {
            "patterns_active":      patterns_total,
            "patterns_utilised":    utilised_patterns,
            "coverage_rate":        coverage_rate,
            "top_matched_patterns": [
                {"id": str(r["id"]), "name": r["name"],
                 "match_count": int(r["match_count"]),
                 "avg_score": round(float(r["avg_score"]), 4) if r["avg_score"] else None}
                for r in top_patterns
            ],
            "never_matched_patterns": [
                {"id": str(r["id"]), "name": r["name"],
                 "severity": r["severity"],
                 "created_at": r["created_at"].isoformat()}
                for r in never_matched
            ],
        }

        # ── Timeliness — how fast are outcomes recorded? ─────────────────
        median_hours_row = await conn.fetchrow(
            """
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (outcome_recorded_at - assessed_at)) / 3600.0
                ) AS median_hours,
                PERCENTILE_CONT(0.9) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (outcome_recorded_at - assessed_at)) / 3600.0
                ) AS p90_hours
            FROM agent_assessments
            WHERE assessed_at >= NOW() - ($1 || ' days')::interval
              AND outcome_recorded_at IS NOT NULL
            """,
            str(window_days),
        )
        timeliness = {
            "median_hours_to_outcome": round(float(median_hours_row["median_hours"]), 2) if median_hours_row["median_hours"] else None,
            "p90_hours_to_outcome":    round(float(median_hours_row["p90_hours"]), 2) if median_hours_row["p90_hours"] else None,
        }

        # ── Lab validation coverage ──────────────────────────────────────
        validated_patterns = await conn.fetchval(
            "SELECT COUNT(DISTINCT pattern_id) FROM lab_validations"
        )
        lab_success_rate_row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE detected) AS detected,
                COUNT(*) AS total
            FROM lab_validations
            """
        )
        lab_detected = int(lab_success_rate_row["detected"]) if lab_success_rate_row else 0
        lab_total    = int(lab_success_rate_row["total"]) if lab_success_rate_row else 0

        lab = {
            "patterns_validated":  validated_patterns,
            "validation_attempts": lab_total,
            "detection_rate":      round(lab_detected / lab_total, 4) if lab_total else None,
        }

    return {
        "accuracy":   accuracy,
        "pipeline":   pipeline,
        "adoption":   adoption,
        "coverage":   coverage,
        "timeliness": timeliness,
        "lab":        lab,
    }


@app.post("/evaluation/snapshot", status_code=201, tags=["evaluation"])
async def create_evaluation_snapshot(
    window_days: int = Query(30, ge=1, le=365),
    label: str = Query("periodic"),
):
    """
    Persist the current evaluation metrics as a snapshot.

    Call this on a schedule (e.g. weekly cron) or manually before/after
    a configuration change to track system improvement over time.
    """
    metrics = await evaluation_metrics(window_days=window_days)

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO evaluation_snapshots (label, window_days, metrics)
            VALUES ($1, $2, $3::jsonb)
            RETURNING id, created_at
            """,
            label,
            window_days,
            json.dumps(metrics),
        )
    return {
        "id":          str(row["id"]),
        "label":       label,
        "window_days": window_days,
        "created_at":  row["created_at"].isoformat(),
    }


@app.get("/evaluation/snapshots", tags=["evaluation"])
async def list_evaluation_snapshots(
    limit: int = Query(20, ge=1, le=100),
):
    """Return recent evaluation snapshots newest-first."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, label, window_days, metrics, created_at
            FROM evaluation_snapshots
            ORDER BY created_at DESC
            LIMIT $1
            """,
            limit,
        )
    return [
        {
            "id":          str(r["id"]),
            "label":       r["label"],
            "window_days": r["window_days"],
            "metrics":     json.loads(r["metrics"]) if isinstance(r["metrics"], str) else r["metrics"],
            "created_at":  r["created_at"].isoformat(),
        }
        for r in rows
    ]

