"""
enrichment-worker/worker.py
════════════════════════════════════════════════════════════════════════════════
Background worker that closes the loop between n8n ingestion and patterns.

Pipeline
────────
  raw_public_issues (processed=false)
      ↓  Chain 1+2: issue_summarizer + pain_extractor  (via local LLM)
  enriched_issues
      ↓  Chain 3+4: pattern_creator + pattern_clusterer (via local LLM)
  patterns  (created or merged)

The worker polls GET /issues/raw?processed=false every POLL_INTERVAL seconds.
For each unprocessed issue it:
  1. Calls the local LLM with Chain 1 (summarize) + Chain 2 (extract pain)
  2. POSTs the result to POST /issues/raw/{id}/enrich
  3. If not a duplicate, calls Chain 3 (create pattern) + Chain 4 (cluster check)
  4. POSTs to POST /issues/enriched/{id}/promote

Environment variables
─────────────────────
  PATTERN_LIBRARY_URL   default: http://pattern-library:9300
  LOCAL_LLM_URL         default: http://local-llm:11434
  LLM_MODEL             default: llama3.2:3b
  POLL_INTERVAL         default: 60 (seconds)
  BATCH_SIZE            default: 5  (issues per poll cycle)
  MIN_ISSUE_SCORE       default: 1  (skip issues with score below this)
"""
import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Optional

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("enrichment-worker")

PATTERN_LIBRARY_URL  = os.getenv("PATTERN_LIBRARY_URL",  "http://pattern-library:9300")
LOCAL_LLM_URL        = os.getenv("LOCAL_LLM_URL",        "http://local-llm:11434")
LLM_MODEL            = os.getenv("LLM_MODEL",            "llama3.2:3b")
POLL_INTERVAL        = int(os.getenv("POLL_INTERVAL",    "60"))
BATCH_SIZE           = int(os.getenv("BATCH_SIZE",       "5"))
MIN_ISSUE_SCORE      = int(os.getenv("MIN_ISSUE_SCORE",  "1"))
# LLM response cache — in-process, content-addressed by sha256(prompt).
# Same title+body from different sources or a re-run hits the cache instead of Ollama.
CACHE_MAX_ENTRIES    = int(os.getenv("CACHE_MAX_ENTRIES", "500"))
CACHE_TTL_SECONDS    = int(os.getenv("CACHE_TTL_SECONDS", "86400"))  # 24 h default

# Tracks issue IDs seen this run to avoid re-queuing after poll overlap.
_SEEN_ISSUE_IDS: set = set()

# {sha256(prompt): (parsed_result: dict, stored_at: float)}
_llm_cache: dict[str, tuple[dict, float]] = {}
_cache_hits   = 0
_cache_misses = 0


def _cache_key(prompt: str) -> str:
    return hashlib.sha256(prompt.encode()).hexdigest()


def _cache_get(key: str) -> Optional[dict]:
    """Return cached result if present and not expired; None otherwise."""
    entry = _llm_cache.get(key)
    if entry is None:
        return None
    result, stored_at = entry
    if time.monotonic() - stored_at > CACHE_TTL_SECONDS:
        del _llm_cache[key]
        return None
    return result


def _cache_put(key: str, result: dict) -> None:
    """Store result; evict oldest entries when over capacity."""
    if len(_llm_cache) >= CACHE_MAX_ENTRIES:
        # Remove the oldest CACHE_MAX_ENTRIES//10 entries (10 % eviction)
        evict_count = max(1, CACHE_MAX_ENTRIES // 10)
        oldest_keys = sorted(_llm_cache, key=lambda k: _llm_cache[k][1])[:evict_count]
        for k in oldest_keys:
            del _llm_cache[k]
    _llm_cache[key] = (result, time.monotonic())

# ─── Prompt templates (aligned with Section G prompt_chains.py) ────────────

SYSTEM_PROMPT = (
    "You are a senior SRE and OpenTelemetry expert. "
    "Respond ONLY with valid JSON. Do not add prose, markdown, or explanation. "
    "Base every field strictly on the provided text. "
    "If a field cannot be determined, use null."
)

def _chain1_summarize(title: str, body: str) -> str:
    return (
        f"{SYSTEM_PROMPT}\n\n"
        "Summarize this observability issue into a structured digest.\n\n"
        f"TITLE: {title}\n\nBODY:\n{body[:3000]}\n\n"
        "Return JSON with exactly these fields:\n"
        '{"component": "<collector|prometheus|loki|tempo|kubernetes|application|unknown>",\n'
        ' "environment": "<kubernetes|vm|docker|bare-metal|any>",\n'
        ' "pain_summary": "<one sentence describing the core pain>",\n'
        ' "symptom_keywords": ["<word>", ...],\n'
        ' "confidence": <0.0-1.0>,\n'
        ' "is_observability_issue": <true|false>}'
    )


def _chain2_extract_pain(summary: dict, title: str) -> str:
    return (
        f"{SYSTEM_PROMPT}\n\n"
        "Extract a structured pain record from this issue summary.\n\n"
        f"TITLE: {title}\n"
        f"SUMMARY: {json.dumps(summary)}\n\n"
        "Return JSON with exactly these fields:\n"
        '{"pain_point": "<precise technical description of the failure pain>",\n'
        ' "affected_component": "<component name>",\n'
        ' "environment": "<environment>",\n'
        ' "symptoms": ["<observable symptom>", ...],\n'
        ' "quality_score": <0.0-1.0>}'
    )


def _chain3_create_pattern(pain: dict, source_url: str) -> str:
    return (
        f"{SYSTEM_PROMPT}\n\n"
        "Convert this pain record into a pattern record for the pattern library.\n\n"
        f"PAIN RECORD: {json.dumps(pain)}\n"
        f"SOURCE URL: {source_url}\n\n"
        "Return JSON with exactly these fields:\n"
        '{"pattern_name": "<snake_case_name_max_60_chars>",\n'
        ' "description": "<2-3 sentence technical description>",\n'
        ' "severity": "<critical|high|medium|low>",\n'
        ' "environment": "<kubernetes|vm|any>",\n'
        ' "impacted_layers": ["<collector|app|infra|network>"],\n'
        ' "automation_readiness": "<safe|risky|manual>",\n'
        ' "oss_contribution_angle": "<null or brief OSS opportunity>",\n'
        ' "recurrence_delta": <0.02-0.10>}'
    )


# ─── LLM helper ────────────────────────────────────────────────────────────

async def _llm_call(http: httpx.AsyncClient, prompt: str) -> Optional[dict]:
    """Call Ollama local LLM; return parsed JSON or None on failure.

    Checks the in-process content-addressed cache before hitting Ollama.
    Cache key = sha256(prompt).  TTL = CACHE_TTL_SECONDS (default 24 h).
    """
    global _cache_hits, _cache_misses

    key = _cache_key(prompt)
    cached = _cache_get(key)
    if cached is not None:
        _cache_hits += 1
        log.debug("LLM cache HIT (hits=%d misses=%d)", _cache_hits, _cache_misses)
        return cached

    _cache_misses += 1
    try:
        resp = await http.post(
            f"{LOCAL_LLM_URL}/api/generate",
            json={
                "model":  LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.1, "num_predict": 512},
            },
            timeout=60.0,
        )
        resp.raise_for_status()
        raw = resp.json().get("response", "")
        result = json.loads(raw)
        _cache_put(key, result)
        return result
    except Exception as exc:
        log.warning("LLM call failed: %s", exc)
        return None


# ─── Pattern Library helpers ───────────────────────────────────────────────

async def _get_unprocessed(http: httpx.AsyncClient) -> list:
    try:
        r = await http.get(
            f"{PATTERN_LIBRARY_URL}/issues/raw",
            params={"processed": "false", "limit": str(BATCH_SIZE)},
            timeout=10.0,
        )
        r.raise_for_status()
        issues = r.json()
        # Apply minimum score filter (pattern-library doesn't filter by score)
        return [i for i in issues if (i.get("score") or 0) >= MIN_ISSUE_SCORE]
    except Exception as exc:
        log.warning("Failed to fetch unprocessed issues: %s", exc)
        return []


async def _get_raw_body(http: httpx.AsyncClient, issue_id: str) -> Optional[str]:
    """Fetch the full body of a raw issue via GET /issues/raw/{id}."""
    try:
        r = await http.get(f"{PATTERN_LIBRARY_URL}/issues/raw/{issue_id}", timeout=10.0)
        r.raise_for_status()
        return r.json().get("body") or ""
    except Exception as exc:
        log.warning("Could not fetch body for issue %s: %s", issue_id, exc)
        return None


async def _post_enrich(http: httpx.AsyncClient, issue_id: str, payload: dict) -> Optional[dict]:
    try:
        r = await http.post(
            f"{PATTERN_LIBRARY_URL}/issues/raw/{issue_id}/enrich",
            json=payload,
            timeout=15.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("Enrich POST failed for %s: %s", issue_id, exc)
        return None


async def _post_promote(http: httpx.AsyncClient, enriched_id: str, payload: dict) -> Optional[dict]:
    try:
        r = await http.post(
            f"{PATTERN_LIBRARY_URL}/issues/enriched/{enriched_id}/promote",
            json=payload,
            timeout=15.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("Promote POST failed for %s: %s", enriched_id, exc)
        return None


# ─── Core processing loop ──────────────────────────────────────────────────

async def _process_issue(http: httpx.AsyncClient, issue: dict) -> None:
    issue_id = issue.get("id")
    if not issue_id or issue_id in _SEEN_ISSUE_IDS:
        return

    title  = issue.get("title", "")
    # List endpoint doesn't return body — use title as the content.
    # The full body would require a separate GET /issues/raw/{id} endpoint.
    body   = issue.get("body") or title
    source = issue.get("source", "unknown")
    url    = issue.get("url", "")

    if not title:
        log.info("Skip issue %s: no title", issue_id)
        _SEEN_ISSUE_IDS.add(issue_id)
        return

    log.info("Processing issue %s [%s]: %s", issue_id, source, title[:60])

    # Fetch full body text via detail endpoint
    full_body = await _get_raw_body(http, issue_id)
    body = full_body if full_body else (issue.get("body") or title)

    # ── Chain 1: summarize ────────────────────────────────────────────────
    summary = await _llm_call(http, _chain1_summarize(title, body))
    if not summary:
        log.warning("Chain 1 failed for %s — skipping", issue_id)
        return

    # Skip non-observability content detected by LLM
    if not summary.get("is_observability_issue", True):
        log.info("Issue %s not observability-related — marking processed", issue_id)
        await _post_enrich(http, issue_id, {
            "pain_point":         title,
            "affected_component": "unknown",
            "environment":        "any",
            "symptoms":           [],
            "quality_score":      0.0,
            "llm_model":          LLM_MODEL,
        })
        _SEEN_ISSUE_IDS.add(issue_id)
        return

    # ── Chain 2: extract pain ─────────────────────────────────────────────
    pain = await _llm_call(http, _chain2_extract_pain(summary, title))
    if not pain:
        # Fallback: use Chain 1 summary fields as pain record
        pain = {
            "pain_point":         summary.get("pain_summary", title),
            "affected_component": summary.get("component", "unknown"),
            "environment":        summary.get("environment", "any"),
            "symptoms":           summary.get("symptom_keywords", []),
            "quality_score":      summary.get("confidence", 0.4),
        }

    # ── POST → /issues/raw/{id}/enrich ───────────────────────────────────
    enrich_result = await _post_enrich(http, issue_id, {
        "pain_point":         pain.get("pain_point", title),
        "affected_component": pain.get("affected_component", "unknown"),
        "environment":        pain.get("environment", "any"),
        "symptoms":           pain.get("symptoms", []),
        "quality_score":      float(pain.get("quality_score", 0.5)),
        "llm_model":          LLM_MODEL,
    })

    _SEEN_ISSUE_IDS.add(issue_id)

    if not enrich_result:
        log.warning("Enrichment failed for issue %s", issue_id)
        return

    enriched_id = enrich_result.get("enriched_id")
    if not enriched_id:
        return

    # Skip promotion if semantic duplicate detected
    if enrich_result.get("is_duplicate"):
        log.info("Issue %s is a duplicate enriched issue — skipping pattern promotion", issue_id)
        return

    # ── Chain 3: create pattern payload ───────────────────────────────────
    pattern_payload = await _llm_call(http, _chain3_create_pattern(pain, url))
    if not pattern_payload:
        # Fallback: derive minimal pattern from pain record
        pattern_payload = {
            "pattern_name":        pain.get("pain_point", title)[:60].lower().replace(" ", "_"),
            "description":         pain.get("pain_point", title),
            "severity":            "medium",
            "environment":         pain.get("environment", "any"),
            "impacted_layers":     [pain.get("affected_component", "unknown")],
            "automation_readiness": "manual",
            "oss_contribution_angle": None,
            "recurrence_delta":    0.03,
        }

    # ── POST → /issues/enriched/{id}/promote ──────────────────────────────
    promote_result = await _post_promote(http, enriched_id, pattern_payload)
    if promote_result:
        action = promote_result.get("action", "unknown")
        name   = promote_result.get("pattern_name", "?")
        log.info("Pattern %s: %s (%s)", action.upper(), name, promote_result.get("pattern_id", ""))
    else:
        log.warning("Promotion failed for enriched issue %s", enriched_id)


async def main() -> None:
    log.info("Enrichment worker starting")
    log.info("  Pattern Library : %s", PATTERN_LIBRARY_URL)
    log.info("  LLM             : %s (%s)", LOCAL_LLM_URL, LLM_MODEL)
    log.info("  Poll interval   : %ss  |  Batch size: %s  |  Min score: %s",
             POLL_INTERVAL, BATCH_SIZE, MIN_ISSUE_SCORE)
    log.info("  LLM cache       : max %d entries  |  TTL %ds",
             CACHE_MAX_ENTRIES, CACHE_TTL_SECONDS)

    async with httpx.AsyncClient() as http:
        while True:
            issues = await _get_unprocessed(http)

            if issues:
                log.info("Fetched %d unprocessed issue(s) — enriching...", len(issues))
                for issue in issues:
                    try:
                        await _process_issue(http, issue)
                    except Exception as exc:
                        log.error("Unexpected error processing issue %s: %s",
                                  issue.get("id", "?"), exc)
                    # Small delay between LLM calls to avoid overloading Ollama
                    await asyncio.sleep(2)

                total = _cache_hits + _cache_misses
                ratio = (_cache_hits / total * 100) if total else 0.0
                log.info(
                    "LLM cache — hits: %d  misses: %d  hit-rate: %.1f%%  entries: %d/%d",
                    _cache_hits, _cache_misses, ratio, len(_llm_cache), CACHE_MAX_ENTRIES,
                )
            else:
                log.debug("No unprocessed issues found")

            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
