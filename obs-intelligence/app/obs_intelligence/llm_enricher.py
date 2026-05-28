"""
obs_intelligence/llm_enricher.py
────────────────────────────────────────────────────────────────────────────────
LLM enrichment layer.

Wraps the raw OpenAI / Claude API call and converts an EvidenceReport +
Recommendation into a rich LLMEnrichment that agents use to write incident
tickets and approval requests.

  ┌───────────────────────────────────────────────┐
  │  EvidenceReport  ──┐                          │
  │  Recommendation  ──┤─► enrich() ─► LLMEnrichment | None
  │  RiskAssessment  ──┘                          │
  └───────────────────────────────────────────────┘

When AI is disabled (no API key), enrich() returns None — callers fall back
to the Recommendation and RiskAssessment that the deterministic pipeline
already produced.

Provider auto-detection (identical to agent-side ai_analyst.py):
  OPENAI_API_KEY set  → OpenAI (priority)
  CLAUDE_API_KEY set  → Anthropic Claude
  Neither set         → AI disabled → returns None

Environment variables
─────────────────────
  OPENAI_API_KEY    OpenAI API key
  CLAUDE_API_KEY    Anthropic API key (legacy)
  AI_MODEL          Model override (default: gpt-4o-mini / claude-3-5-haiku)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any

import httpx

from obs_intelligence.models import EvidenceReport, RiskAssessment, Recommendation
from obs_intelligence.evidence_builder import evidence_lines
from obs_intelligence.sre_reasoning_agent import SREAssessment, SREReasoningAgent
from obs_intelligence.local_llm_enricher import local_llm_enricher

logger = logging.getLogger("obs_intelligence.llm_enricher")

# ── Provider config ───────────────────────────────────────────────────────────
# Both providers can be enabled simultaneously; OpenAI is tried first and
# Claude is used as automatic fallback if OpenAI fails or is unavailable.
_OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
_CLAUDE_API_KEY: str = os.getenv("CLAUDE_API_KEY", "")
_USE_OPENAI: bool = bool(_OPENAI_API_KEY)
_USE_CLAUDE: bool = bool(_CLAUDE_API_KEY)   # independent of OpenAI — used as fallback
AI_ENABLED: bool = _USE_OPENAI or _USE_CLAUDE

# Per-provider model names — override via env if needed
_OPENAI_MODEL: str = os.getenv("OPENAI_MODEL") or os.getenv("AI_MODEL") or "gpt-4o-mini"
_CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL") or os.getenv("AI_MODEL") or "claude-3-5-haiku-20241022"

_OPENAI_URL = "https://api.openai.com/v1/chat/completions"
_CLAUDE_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_HEADERS = {
    "Content-Type": "application/json",
    "anthropic-version": "2023-06-01",
}

_providers_active = (
    (["openai"] if _USE_OPENAI else [])
    + (["claude (fallback)" if _USE_OPENAI else "claude"] if _USE_CLAUDE else [])
)
logger.info(
    "llm_enricher providers: %s",
    ", ".join(_providers_active) if _providers_active else "disabled",
)


# ═══════════════════════════════════════════════════════════════════════════════
# Output model
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class LLMEnrichment:
    """
    Structured output from the LLM enrichment step.

    Mirrors the JSON schema expected from OpenAI/Claude and used by
    build_enriched_ticket_body() in both domain agents.
    """

    rca_summary: str
    recommended_action: str
    autonomy_level: str
    confidence: str                            # "high" | "medium" | "low"
    provider: str                              # "openai" | "claude"

    ansible_playbook: str = ""
    ansible_description: str = ""
    test_plan: list[str] = field(default_factory=list)
    test_cases: list[dict] = field(default_factory=list)

    rca_detail: dict = field(default_factory=dict)
    pr_title: str | None = None
    pr_description: str | None = None
    rollback_steps: list[str] = field(default_factory=list)
    estimated_fix_time_minutes: int | None = None
    external_model: str | None = None
    root_cause: str | None = None
    knowledge_entry_id: str | None = None
    local_validation_status: str | None = None
    local_validation_confidence: float | None = None
    local_validation_reason: str | None = None
    local_validation_completed: bool = False
    knowledge_top_similarity: float | None = None
    local_model: str | None = None

    # Block F dual-validation metadata
    source: str = "external_llm"          # always "external_llm" — authoritative
    validation_mode: str = "external_only" # "dual" when local validator ran
    validated_by: list[str] = field(default_factory=list)
    local_similar_count: int = 0

    # The raw dict returned by the LLM (for downstream use / debugging)
    raw: dict = field(default_factory=dict)

    def to_analysis_dict(self) -> dict:
        """
        Convert to the 'analysis' dict schema expected by existing
        build_enriched_ticket_body() implementations.

        This preserves backward compatibility with Agent 5 (ticket scribe)
        in both domain pipelines.
        """
        result: dict = {
            "rca_summary":          self.rca_summary,
            "root_cause":           self.root_cause or self.rca_summary,
            "recommended_action":   self.recommended_action,
            "autonomy_level":       self.autonomy_level,
            "confidence":           self.confidence,
            "provider":             self.provider,
            "model":                self.external_model,
            "ansible_playbook":     self.ansible_playbook,
            "ansible_description":  self.ansible_description,
            "test_plan":            self.test_plan,
        }
        if self.test_cases:
            result["test_cases"] = self.test_cases
        if self.rca_detail:
            result["rca_detail"] = self.rca_detail
        if self.pr_title:
            result["pr_title"] = self.pr_title
        if self.pr_description:
            result["pr_description"] = self.pr_description
        if self.rollback_steps:
            result["rollback_steps"] = self.rollback_steps
        if self.estimated_fix_time_minutes is not None:
            result["estimated_fix_time_minutes"] = self.estimated_fix_time_minutes
        if self.knowledge_entry_id:
            result["knowledge_entry_id"] = self.knowledge_entry_id
        if self.local_validation_status:
            result["local_validation_status"] = self.local_validation_status
        if self.local_validation_confidence is not None:
            result["local_validation_confidence"] = self.local_validation_confidence
        if self.local_validation_reason:
            result["local_validation_reason"] = self.local_validation_reason
        if self.knowledge_top_similarity is not None:
            result["knowledge_top_similarity"] = self.knowledge_top_similarity
        if self.local_model:
            result["local_model"] = self.local_model
        result["local_validation_completed"] = self.local_validation_completed
        result["source"]            = self.source
        result["validation_mode"]   = self.validation_mode
        result["validated_by"]      = list(self.validated_by)
        result["local_similar_count"] = self.local_similar_count
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# Public
# ═══════════════════════════════════════════════════════════════════════════════

async def enrich(
    evidence: EvidenceReport,
    recommendation: Recommendation,
    risk: RiskAssessment,
    http: httpx.AsyncClient,
    sre_assessment: SREAssessment | None = None,
    pattern_context: str | None = None,
) -> LLMEnrichment | None:
    """
    Call the LLM to produce a rich incident narrative from the evidence bundle.

    Builds a prompt from the EvidenceReport (features, scenario matches, risk,
    evidence lines) and the top Recommendation, then requests:
      - A detailed RCA narrative
      - An Ansible playbook for remediation
      - Pre/post test cases
      - A GitHub PR description
      - Rollback steps

    The sre_assessment argument (optional) contains pre-computed structured
    SRE reasoning that is injected into the prompt as verified facts.  If not
    provided it is computed automatically from the evidence before the LLM call.
    The LLM writes narrative FROM this structure — it does not re-derive it.

    Provider failover: tries OpenAI first; if OpenAI is unavailable or returns
    an error, retries with Claude (if CLAUDE_API_KEY is set).

    Returns None when AI is disabled or all providers fail.
    """
    if not AI_ENABLED:
        logger.debug("AI disabled — skipping LLM enrichment")
        return None

    # Run deterministic SRE reasoning first (fast, no network)
    if sre_assessment is None:
        sre_assessment = SREReasoningAgent().assess(
            evidence.features,
            evidence.scenario_matches,
            risk,
        )

    prompt = _build_prompt(evidence, recommendation, risk, sre_assessment,
                            pattern_context=pattern_context)

    # ── Provider failover: OpenAI → Claude → deterministic fallback ───────────
    raw: dict | None = None
    provider_used: str | None = None

    if _USE_OPENAI:
        try:
            raw = await _call_openai(prompt, http)
            if raw:
                provider_used = "openai"
        except Exception as exc:
            logger.warning(
                "OpenAI enrichment failed — trying Claude fallback: %s", exc
            )

    if raw is None and _USE_CLAUDE:
        try:
            raw = await _call_claude(prompt, http)
            if raw:
                provider_used = "claude"
        except Exception as exc:
            logger.warning(
                "Claude enrichment also failed — returning deterministic result: %s",
                exc,
            )

    if not raw:
        return None

    enrichment = _parse_enrichment(raw, recommendation, provider_used or "unknown")
    enrichment.external_model = _provider_model(provider_used or "unknown")
    enrichment.root_cause = (
        enrichment.rca_detail.get("probable_cause")
        or enrichment.raw.get("root_cause")
        or enrichment.rca_summary
    )

    # ── Block F: dual validation ──────────────────────────────────────────────
    features = evidence.features
    run_id   = evidence.trace_id or ""
    scenario_matches = evidence.scenario_matches

    incident_text = (
        f"Service: {features.service_name} "
        f"Alert: {features.alert_name} "
        f"Severity: {features.severity} "
        f"Domain: {features.domain} "
        f"Error rate: {features.error_rate:.2%} "
        f"P99: {features.latency_p99:.3f}s "
        f"CPU: {features.cpu_usage:.0%} "
        f"Memory: {features.memory_usage:.0%}"
    )
    incident_context: dict = {
        "service_name":        features.service_name,
        "alert_name":          features.alert_name,
        "domain":              features.domain,
        "scenario_id":         scenario_matches[0].scenario_id if scenario_matches else "",
        "scenario_confidence": scenario_matches[0].confidence  if scenario_matches else 0.0,
        "risk_score":          risk.risk_score,
        "description":         incident_text,
        "run_id":              run_id,
    }

    logger.info(
        "External LLM result authoritative — running local dual validation  run_id=%s", run_id
    )

    # Mark as external_llm authoritative result; local validation is advisory
    enrichment.source          = "external_llm"
    enrichment.validation_mode = "external_only"
    enrichment.validated_by    = ["external_llm"]

    local_validation = None
    similar: list = []
    try:
        similar = await asyncio.wait_for(
            local_llm_enricher.query_similar_incidents(incident_text, features.domain),
            timeout=10.0,
        )
        local_validation = await asyncio.wait_for(
            local_llm_enricher.validate_external_result(
                incident_context=incident_context,
                external_result=enrichment,
                similar=similar,
            ),
            timeout=10.0,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Local LLM validation timed out; keeping external result  run_id=%s", run_id
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Local LLM validation error: %s  run_id=%s", exc, run_id)

    # Fire-and-forget ChromaDB store (never blocks the pipeline)
    asyncio.create_task(
        local_llm_enricher.store_incident_resolution(
            incident_context=incident_context,
            external_result=enrichment,
            local_validation=local_validation,
            similar=similar,
            outcome="pending",
            run_id=run_id,
        )
    )

    if local_validation is not None:
        enrichment.validated_by.append("local_llm")
        enrichment.validation_mode          = "dual"
        enrichment.local_validation_status  = local_validation.validation_status
        enrichment.local_validation_confidence = local_validation.confidence
        enrichment.local_validation_reason  = local_validation.reasoning_summary
        enrichment.local_validation_completed = True
        enrichment.knowledge_top_similarity  = local_validation.top_similarity
        enrichment.local_similar_count       = local_validation.similar_count

    enrichment.local_similar_count = enrichment.local_similar_count or len(similar)
    enrichment.local_model = os.getenv("LOCAL_LLM_MODEL", "qwen3.5")

    if local_validation and local_validation.validation_status == "divergent":
        logger.warning(
            "Local LLM diverged from external result  run_id=%s  top_similarity=%.2f",
            run_id,
            similar[0].similarity() if similar else 0.0,
        )

    return enrichment


# ─────────────────────────────────────────────────────────────────────────────
# Internal: prompt builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_prompt(
    evidence: EvidenceReport,
    recommendation: Recommendation,
    risk: RiskAssessment,
    sre_assessment: SREAssessment | None = None,
    pattern_context: str | None = None,
) -> str:
    """
    Build the LLM user prompt from the EvidenceReport, top Recommendation,
    and (optionally) a pre-computed SREAssessment.

    When sre_assessment is provided, its structured reasoning block is injected
    before the JSON schema instruction.  The LLM is instructed to write a
    narrative FROM the pre-computed facts rather than re-deriving them.
    """
    f = evidence.features
    ev_lines = "\n".join(evidence_lines(evidence))

    scenario_context = ""
    if evidence.scenario_matches:
        top = evidence.scenario_matches[0]
        scenario_context = (
            f"\nMatched scenario: {top.display_name} (confidence: {top.confidence:.0%})"
        )
        if top.matched_features:
            scenario_context += f"\nMatched conditions: {', '.join(top.matched_features)}"

    playbook_name = recommendation.ansible_playbook or "(none identified)"
    rollback_hint = recommendation.rollback_plan or "(none)"

    sre_block = (
        f"\n{sre_assessment.to_prompt_block()}\n"
        if sre_assessment is not None
        else ""
    )
    pattern_block = (
        f"\n{pattern_context}\n"
        if pattern_context
        else ""
    )

    return f"""Analyze this production incident and produce a structured JSON response.

INCIDENT SUMMARY
━━━━━━━━━━━━━━━━
Alert:     {f.alert_name}
Service:   {f.service_name}
Severity:  {f.severity.upper()}
Domain:    {f.domain}
{scenario_context}

EVIDENCE OBSERVATIONS
━━━━━━━━━━━━━━━━━━━━━
{ev_lines}

RISK ASSESSMENT
━━━━━━━━━━━━━━━
Risk level: {risk.risk_level.upper()} (score: {risk.risk_score:.2f})
Contributing factors: {'; '.join(risk.contributing_factors[:5])}
Blast radius: {risk.blast_radius}
Time to impact: {risk.time_to_impact or 'unknown'}

RECOMMENDED ACTION
━━━━━━━━━━━━━━━━━━
Action type:   {recommendation.action_type}
Display name:  {recommendation.display_name}
Playbook:      {playbook_name}
Rollback hint: {rollback_hint}
{sre_block}
{pattern_block}
Respond with ONLY valid JSON (no markdown fences) matching this schema:
{{
  "rca_summary": "2-3 sentence root cause analysis written from the SRE Reasoning Layer facts above",
  "rca_detail": {{
    "symptoms": ["observed symptoms"],
    "probable_cause": "most likely root cause",
    "contributing_factors": ["secondary factors"],
    "blast_radius": "impact scope description"
  }},
  "confidence": "high|medium|low",
  "ansible_playbook": "Complete YAML playbook string",
  "ansible_description": "1-sentence playbook description",
  "test_cases": [
    {{"id": "TC-PRE-1", "name": "Check name", "assertion": "What to verify", "phase": "pre"}},
    {{"id": "TC-POST-1", "name": "Check name", "assertion": "What to verify", "phase": "post"}}
  ],
  "pr_title": "Short PR title under 72 chars (or null)",
  "pr_description": "Markdown PR description (or null)",
  "rollback_steps": ["Step 1", "Step 2"],
  "estimated_fix_time_minutes": 10
}}"""


_SYSTEM_PROMPT = (
    "You are a senior SRE and DevOps expert specializing in production incident analysis. "
    "Respond ONLY with valid JSON matching the provided schema. No markdown, no extra text."
)


# ─────────────────────────────────────────────────────────────────────────────
# Internal: API callers
# ─────────────────────────────────────────────────────────────────────────────

async def _call_openai(prompt: str, http: httpx.AsyncClient) -> dict | None:
    resp = await http.post(
        _OPENAI_URL,
        headers={
            "Authorization": f"Bearer {_OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": _OPENAI_MODEL,
            "max_tokens": 2000,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
        },
        timeout=30.0,
    )
    if resp.status_code != 200:
        logger.warning("OpenAI returned HTTP %d: %s", resp.status_code, resp.text[:200])
        return None
    content = resp.json()["choices"][0]["message"]["content"]
    return json.loads(content)


async def _call_claude(prompt: str, http: httpx.AsyncClient) -> dict | None:
    resp = await http.post(
        _CLAUDE_URL,
        headers={**_CLAUDE_HEADERS, "x-api-key": _CLAUDE_API_KEY},
        json={
            "model": _CLAUDE_MODEL,
            "max_tokens": 2000,
            "system": _SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30.0,
    )
    if resp.status_code != 200:
        logger.warning("Claude returned HTTP %d: %s", resp.status_code, resp.text[:200])
        return None
    content = resp.json()["content"][0]["text"]
    clean = content.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(clean)


# ─────────────────────────────────────────────────────────────────────────────
# Internal: response parser
# ─────────────────────────────────────────────────────────────────────────────

def _provider_model(provider_used: str) -> str:
    if provider_used == "openai":
        return _OPENAI_MODEL
    if provider_used == "claude":
        return _CLAUDE_MODEL
    return ""

def _parse_enrichment(
    raw: dict,
    recommendation: Recommendation,
    provider: str = "unknown",
) -> LLMEnrichment:
    """
    Parse the raw LLM JSON into a typed LLMEnrichment.

    Uses the Recommendation as a fallback for fields the LLM may have omitted.
    The provider parameter records which API actually produced the response
    (important for failover scenarios where the active provider differs from
    _USE_OPENAI / _USE_CLAUDE flags).
    """
    return LLMEnrichment(
        rca_summary         = raw.get("rca_summary", "No RCA generated."),
        recommended_action  = recommendation.action_type,
        autonomy_level      = "autonomous" if recommendation.autonomous else "approval_gated",
        confidence          = raw.get("confidence", "low"),
        provider            = provider,
        ansible_playbook    = raw.get("ansible_playbook", ""),
        ansible_description = raw.get("ansible_description", ""),
        test_plan           = raw.get("test_plan", []),
        test_cases          = raw.get("test_cases", []),
        rca_detail          = raw.get("rca_detail", {}),
        pr_title            = raw.get("pr_title"),
        pr_description      = raw.get("pr_description"),
        rollback_steps      = raw.get("rollback_steps", []),
        estimated_fix_time_minutes = raw.get("estimated_fix_time_minutes"),
        raw                 = raw,
    )
