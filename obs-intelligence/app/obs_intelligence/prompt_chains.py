"""
obs_intelligence/prompt_chains.py
────────────────────────────────────────────────────────────────────────────────
Section G — AI Prompt Chains

Structured prompt templates for every stage of the Pattern → Decision → Action
pipeline.  All prompts enforce:

  • Strict JSON output (no markdown fences, no prose wrappers)
  • Anti-hallucination constraints — LLM must cite which observation justifies
    each claim; if data is absent it returns an explicit "insufficient_data" flag
  • Citation enforcement — source references must be preserved verbatim from
    the input, never invented
  • Uncertainty expression — every field has a "confidence" key so callers can
    filter before acting

Usage pattern
─────────────
    from obs_intelligence.prompt_chains import PromptChains
    prompt = PromptChains.issue_summarizer(raw_issue)
    response_json = await call_llm(prompt)

Chain index
──────────
1. issue_summarizer       — convert raw public issue to clean summary
2. pain_extractor         — identify the specific observability pain point
3. pattern_creator        — build a structured pattern record from pains
4. pattern_clusterer      — find whether a new pain duplicates an existing pattern
5. incident_matcher       — map a live incident to the closest patterns
6. recommendation_builder — generate ranked, evidence-bound fix recommendations
"""

from __future__ import annotations

import json


# ═══════════════════════════════════════════════════════════════════════════════
# System prompt (shared across all chains)
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = (
    "You are a senior SRE and observability expert specialising in OpenTelemetry, "
    "Prometheus, Loki, and Kubernetes production operations. "
    "You ONLY respond with valid JSON. "
    "You NEVER invent facts — if required information is missing from the input, "
    "set the relevant field to null and set confidence='insufficient_data'. "
    "You NEVER add markdown fences (``` blocks) around your output. "
    "You cite your observations verbatim from the provided input — never paraphrase "
    "a source URL or issue ID."
)


# ═══════════════════════════════════════════════════════════════════════════════
# Chain 1 — Issue Summarizer
# ═══════════════════════════════════════════════════════════════════════════════

def issue_summarizer(raw_issue: dict) -> str:
    """
    Chain 1: Convert a raw public issue (GitHub/SO/Reddit/HN) into a
    clean, normalised summary.

    Input keys used: title, body, source, url, author, score, tags
    Output schema: IssueDigest JSON
    """
    title   = raw_issue.get("title", "(no title)")
    body    = (raw_issue.get("body") or "")[:3000]
    source  = raw_issue.get("source", "unknown")
    url     = raw_issue.get("url", "")
    score   = raw_issue.get("score", 0)
    tags    = raw_issue.get("tags", [])

    return f"""You are processing a raw community issue from {source}.

RAW ISSUE
━━━━━━━━━
Title:  {title}
URL:    {url}
Score:  {score}
Tags:   {", ".join(tags) if tags else "(none)"}

Body (first 3000 chars):
{body}

Produce a JSON IssueDigest. CONSTRAINTS:
- is_observability_related: true only if the issue is about metrics, logs,
  traces, alerting, or telemetry collection — false otherwise
- pain_category must be one of:
  "collection_gap" | "cardinality" | "storage" | "alert_quality" | "latency" |
  "resource_saturation" | "data_loss" | "correlation" | "cost" | "other"
- confidence: "high" if the issue is clearly technical with reproduction steps;
  "medium" if vague but clearly observability; "low" if tangential or speculative
- NEVER invent URLs — preserve the input URL verbatim

{{
  "title":                  "<cleaned, max 120 chars>",
  "one_sentence_summary":   "<single sentence, ≤ 180 chars>",
  "is_observability_related": true,
  "pain_category":          "collection_gap",
  "affected_components":    ["otel-collector", "prometheus"],
  "environment_hints":      ["kubernetes", "docker"],
  "severity_hint":          "medium",
  "source":                 "{source}",
  "source_url":             "{url}",
  "confidence":             "high",
  "reasoning":              "<1-2 sentences explaining why you classified it this way>"
}}"""


# ═══════════════════════════════════════════════════════════════════════════════
# Chain 2 — Pain Extractor
# ═══════════════════════════════════════════════════════════════════════════════

def pain_extractor(issue_digest: dict, raw_body: str = "") -> str:
    """
    Chain 2: Extract the precise, actionable observability pain from a
    classified issue digest.  Produces a PainRecord with structured
    signals that can drive pattern detection logic.

    Input: IssueDigest from Chain 1 + optionally the raw body for context.
    """
    digest_str = json.dumps(issue_digest, indent=2)
    body_excerpt = raw_body[:1500] if raw_body else "(not provided)"

    return f"""Extract the precise observability pain point from this issue digest.

ISSUE DIGEST
━━━━━━━━━━━━
{digest_str}

RAW BODY EXCERPT (for grounding)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{body_excerpt}

CONSTRAINTS:
- observable_symptoms: list real, observable signals (metric names, log patterns,
  error codes) — NOT generic phrases like "slow" or "broken"
- root_cause_hypotheses: ranked from most to least likely
- grounding_quote: a verbatim excerpt from the raw body that supports your claim;
  if none exists set to null and set confidence="insufficient_data"
- Every hypothesis must cite which symptom it explains

{{
  "pain_statement":          "<precise, ≤ 200 chars>",
  "observed_symptoms": [
    {{
      "signal_type":   "metric|log|trace|alert",
      "signal_name":   "<exact metric or pattern>",
      "threshold":     "<threshold or pattern that triggers this>",
      "observed_value": "<value from issue, or null>"
    }}
  ],
  "root_cause_hypotheses": [
    {{
      "hypothesis":    "<single root cause>",
      "explains_symptoms": ["<symptom signal_name>"],
      "confidence":    "high|medium|low",
      "evidence_quote": "<verbatim from body, or null>"
    }}
  ],
  "affected_telemetry_path": "<e.g. app → otel-collector → prometheus>",
  "reproduction_complexity": "easy|medium|hard",
  "grounding_quote":         "<verbatim from raw body>",
  "confidence":              "high|medium|low|insufficient_data"
}}"""


# ═══════════════════════════════════════════════════════════════════════════════
# Chain 3 — Pattern Creator
# ═══════════════════════════════════════════════════════════════════════════════

def pattern_creator(pain_records: list[dict], source_urls: list[str]) -> str:
    """
    Chain 3: Synthesise one or more PainRecords into a canonical Pattern record.

    This is called after 2+ similar pains are confirmed as the same problem.
    Input: list of PainRecords (Chain 2 output) + their source URLs.
    Output: PatternRecord matching the pattern-library DB schema.
    """
    pains_str = json.dumps(pain_records, indent=2)
    urls_str  = "\n".join(f"  - {u}" for u in source_urls)

    return f"""You are synthesising {len(pain_records)} related observability pain report(s)
into a single canonical Pattern record for the pattern intelligence library.

PAIN RECORDS
━━━━━━━━━━━━
{pains_str}

SOURCE URLS (must appear verbatim in references)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{urls_str}

CONSTRAINTS:
- name: snake_case, ≤ 80 chars, unique and descriptive
- severity: critical | high | medium | low
- environment: kubernetes | vm | bare-metal | any
- automation_readiness: safe | risky | manual
- confidence score 0.0–1.0 based on how many independent sources confirm this
- recurrence_score 0.0–1.0: how frequently this appears across the web
- All source URLs must appear verbatim in "references" — never paraphrase them
- If you cannot determine a field from the evidence, set it to null

{{
  "name":                "<snake_case_pattern_name>",
  "description":         "<2-3 sentences, precise technical description>",
  "severity":            "high",
  "environment":         "kubernetes",
  "impacted_layers":     ["app", "collector"],
  "root_causes": [
    {{
      "cause":           "<specific root cause>",
      "confidence":      "high|medium|low",
      "supporting_evidence": "<brief quote or observation>"
    }}
  ],
  "symptoms": [
    {{
      "signal_type":     "metric|log|trace|alert",
      "signal_name":     "<exact>",
      "threshold":       "<value or pattern>"
    }}
  ],
  "known_fixes": [
    {{
      "title":           "<fix title>",
      "description":     "<what to do>",
      "automation_safe": true
    }}
  ],
  "automation_readiness": "safe|risky|manual",
  "recurrence_score":    0.78,
  "confidence":          0.82,
  "references":          {json.dumps(source_urls)},
  "oss_angle":           "<contribution angle, or null>"
}}"""


# ═══════════════════════════════════════════════════════════════════════════════
# Chain 4 — Pattern Clusterer
# ═══════════════════════════════════════════════════════════════════════════════

def pattern_clusterer(new_pain: dict, existing_patterns: list[dict]) -> str:
    """
    Chain 4: Determine if a new PainRecord is already covered by an existing
    pattern, or whether it warrants a new pattern.  Prevents duplication.

    Input: new PainRecord + top-K existing patterns (from pgvector search).
    Output: ClusterDecision JSON.
    """
    new_str      = json.dumps(new_pain, indent=2)
    existing_str = json.dumps(existing_patterns[:5], indent=2)

    return f"""Decide whether a new observability pain duplicates an existing pattern
or requires a new pattern entry.

NEW PAIN RECORD
━━━━━━━━━━━━━━━
{new_str}

TOP EXISTING PATTERNS (from semantic search)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{existing_str}

CONSTRAINTS:
- decision: "duplicate" | "variant" | "new"
  * duplicate: same root cause, same signals, same fix → merge/skip
  * variant: same family but different root cause or environment → new pattern linked to parent
  * new: genuinely novel pattern
- matching_pattern_id: UUID of matching pattern if decision != "new", else null
- reasoning: cite specific signal names or root causes that match/differ

{{
  "decision":             "duplicate|variant|new",
  "matching_pattern_id":  "<uuid or null>",
  "similarity_score":     0.0,
  "reasoning":            "<specific comparison, ≤ 200 chars>",
  "suggested_action":     "skip|merge_into_existing|create_variant|create_new",
  "confidence":           "high|medium|low"
}}"""


# ═══════════════════════════════════════════════════════════════════════════════
# Chain 5 — Incident-to-Pattern Matcher
# ═══════════════════════════════════════════════════════════════════════════════

def incident_matcher(
    incident: dict,
    candidate_patterns: list[dict],
    evidence_lines: list[str],
) -> str:
    """
    Chain 5: Map a live incident to the best-matching patterns from the library.
    Called AFTER the hybrid rule+vector search narrows candidates to top-K.

    Input:
      incident         — alert metadata + current ObsFeatures
      candidate_patterns — PatternListItems from hybrid search
      evidence_lines   — human-readable evidence observations
    Output: MatchResult JSON with ranked pattern matches + reasoning trace.
    """
    incident_str  = json.dumps(incident, indent=2)
    patterns_str  = json.dumps(candidate_patterns, indent=2)
    ev_str        = "\n".join(f"  • {l}" for l in evidence_lines[:20])

    return f"""Match a live production incident to known observability failure patterns.

LIVE INCIDENT
━━━━━━━━━━━━━
{incident_str}

OBSERVED EVIDENCE
━━━━━━━━━━━━━━━━━
{ev_str}

CANDIDATE PATTERNS (pre-filtered by hybrid search)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{patterns_str}

CONSTRAINTS:
- matched_patterns: rank by combined evidence alignment, NOT by score field alone
- For each match, cite which observed signal matches which pattern signal
- unmatched_signals: list signals in the incident that no pattern explains
  (important for gap detection and future pattern creation)
- If NO pattern matches well (all alignment < 0.5), set dominant_pattern to null
  and set investigation_required=true

{{
  "dominant_pattern": {{
    "pattern_id":     "<uuid>",
    "pattern_name":   "<name>",
    "alignment_score": 0.0,
    "matched_signals": [
      {{"incident_signal": "<name>", "pattern_signal": "<name>", "match_type": "threshold|anomaly|log"}}
    ],
    "reasoning":       "<why this is the best match, cite evidence>"
  }},
  "secondary_patterns": [
    {{
      "pattern_id":     "<uuid>",
      "pattern_name":   "<name>",
      "alignment_score": 0.0,
      "role":           "contributing|confounding"
    }}
  ],
  "unmatched_signals":      ["<signal names not explained by any pattern>"],
  "investigation_required": false,
  "gap_pattern_hint":       "<brief description of unmatched signals for future pattern creation, or null>",
  "confidence":             "high|medium|low|insufficient_data"
}}"""


# ═══════════════════════════════════════════════════════════════════════════════
# Chain 6 — Recommendation Builder
# ═══════════════════════════════════════════════════════════════════════════════

def recommendation_builder(
    incident: dict,
    matched_pattern: dict,
    risk_assessment: dict,
    evidence_lines: list[str],
    pattern_fixes: list[dict],
) -> str:
    """
    Chain 6: Generate ranked, evidence-bound fix recommendations from a
    matched pattern and its known fixes.

    Input:
      incident       — alert metadata
      matched_pattern — PatternDetail (includes fixes)
      risk_assessment — RiskAssessment dict
      evidence_lines  — observations from evidence builder
      pattern_fixes   — known fix records for this pattern
    Output: RecommendationSet JSON with actions ranked by safety and impact.
    """
    incident_str  = json.dumps(incident, indent=2)
    pattern_str   = json.dumps(matched_pattern, indent=2)
    risk_str      = json.dumps(risk_assessment, indent=2)
    ev_str        = "\n".join(f"  • {l}" for l in evidence_lines[:15])
    fixes_str     = json.dumps(pattern_fixes, indent=2)

    return f"""Generate ranked, actionable fix recommendations for a production incident.

INCIDENT
━━━━━━━━
{incident_str}

EVIDENCE
━━━━━━━━
{ev_str}

MATCHED PATTERN
━━━━━━━━━━━━━━━
{pattern_str}

RISK ASSESSMENT
━━━━━━━━━━━━━━━
{risk_str}

KNOWN PATTERN FIXES
━━━━━━━━━━━━━━━━━━━
{fixes_str}

CONSTRAINTS:
- recommendations ranked safest → riskiest
- Every claim in "justification" must cite a specific evidence line or pattern signal
- If risk_level is "critical", the top recommendation MUST be a safe/reversible action
- automation_safety: "safe" = no downtime risk; "risky" = potential disruption;
  "manual" = requires human judgement
- Do NOT recommend actions not supported by the pattern fixes or evidence
- next_investigation_steps: for cases where confidence is low

{{
  "recommendations": [
    {{
      "rank":              1,
      "title":             "<short action title>",
      "description":       "<what to do and why>",
      "automation_safety": "safe|risky|manual",
      "estimated_fix_time_minutes": 10,
      "justification":     "<cite specific evidence + pattern signals>",
      "rollback_steps":    ["<step 1>", "<step 2>"]
    }}
  ],
  "next_investigation_steps": ["<step if confidence is low>"],
  "automation_candidate":     true,
  "ansible_playbook_hint":    "<brief description of what the playbook would do, or null>",
  "confidence":               "high|medium|low|insufficient_data"
}}"""


# ═══════════════════════════════════════════════════════════════════════════════
# Convenience accessor
# ═══════════════════════════════════════════════════════════════════════════════

class PromptChains:
    """Namespace for all prompt-chain builder functions."""
    system = SYSTEM_PROMPT
    issue_summarizer      = staticmethod(issue_summarizer)
    pain_extractor        = staticmethod(pain_extractor)
    pattern_creator       = staticmethod(pattern_creator)
    pattern_clusterer     = staticmethod(pattern_clusterer)
    incident_matcher      = staticmethod(incident_matcher)
    recommendation_builder = staticmethod(recommendation_builder)
