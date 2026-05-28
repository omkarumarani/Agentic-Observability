"""
pattern-library/app/scorer.py
────────────────────────────────────────────────────────────────
Section H — Pattern Scoring Model

Weighted composite scoring formula for ranking observability patterns by their
overall importance.  Used to drive the pattern library's default sort order and
to surface the highest-value patterns for SRE attention.

Scoring formula
───────────────
  composite = Σ (weight_i × normalised_component_i)

  Component             Weight  Description
  ────────────────────  ──────  ───────────────────────────────────────────
  recurrence_score       0.30   How often this pattern appears in the wild
  business_impact        0.25   Blast radius + severity mapping
  technical_depth        0.20   Signal richness (fixes, signals, validations)
  automation_feasibility 0.15   safe > risky > manual
  oss_potential          0.10   Whether a contribution angle exists

Score range: 0.0 – 1.0  (higher = more important to action)

Priority tiers:
  0.80 – 1.00  →  P0  (action immediately)
  0.60 – 0.79  →  P1  (action this sprint)
  0.40 – 0.59  →  P2  (queue for next sprint)
  0.00 – 0.39  →  P3  (backlog / monitor)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ── Weight constants ────────────────────────────────────────────────────────
_W_RECURRENCE   = 0.30
_W_IMPACT       = 0.25
_W_DEPTH        = 0.20
_W_AUTOMATION   = 0.15
_W_OSS          = 0.10

# ── Severity → base impact score ───────────────────────────────────────────
_SEVERITY_IMPACT: dict[str, float] = {
    "critical": 1.00,
    "high":     0.75,
    "medium":   0.45,
    "low":      0.20,
}

# ── Automation readiness → score ───────────────────────────────────────────
_AUTOMATION_SCORES: dict[str, float] = {
    "safe":   1.00,
    "risky":  0.55,
    "manual": 0.20,
}

# ── Priority tiers ──────────────────────────────────────────────────────────
_TIERS = [
    (0.80, "P0"),
    (0.60, "P1"),
    (0.40, "P2"),
    (0.00, "P3"),
]


# ═══════════════════════════════════════════════════════════════════════════════
# Public types
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class PatternScore:
    composite:            float
    priority_tier:        str
    recurrence_component: float
    impact_component:     float
    depth_component:      float
    automation_component: float
    oss_component:        float
    breakdown:            dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def score_pattern(
    *,
    recurrence_score: float,
    severity: str,
    signal_count: int,
    fix_count: int,
    validation_count: int,
    automation_readiness: str,
    has_oss_angle: bool,
    evidence_count: int = 0,
    confidence: float = 0.5,
) -> PatternScore:
    """
    Compute a composite importance score for a pattern record.

    Parameters
    ──────────
    recurrence_score      : float 0–1; from the pattern's DB field
    severity              : "critical" | "high" | "medium" | "low"
    signal_count          : number of threshold-defined signals for this pattern
    fix_count             : number of documented fix entries
    validation_count      : number of lab validation runs
    automation_readiness  : "safe" | "risky" | "manual"
    has_oss_angle         : whether the pattern has an OSS contribution angle
    evidence_count        : cumulative number of agent assessments (recurrence proxy)
    confidence            : pattern-level confidence from the DB
    """
    # ── Component 1: recurrence (0–1) ────────────────────────────────────────
    # Blend DB recurrence_score (objective) with evidence_count (experiential)
    evidence_boost = min(1.0, evidence_count / 20.0)  # saturates at 20 assessments
    recurrence_component = min(1.0,
        recurrence_score * 0.7 + evidence_boost * 0.3
    )

    # ── Component 2: business impact (0–1) ───────────────────────────────────
    base_impact = _SEVERITY_IMPACT.get(severity.lower(), 0.30)
    # Confidence modulates impact slightly (low confidence → less certain impact)
    impact_component = base_impact * (0.7 + confidence * 0.3)

    # ── Component 3: technical depth (0–1) ───────────────────────────────────
    # Rich patterns (many signals + fixes + lab validations) score higher
    signal_score     = min(1.0, signal_count / 8.0)      # saturates at 8 signals
    fix_score        = min(1.0, fix_count / 5.0)         # saturates at 5 fixes
    validation_score = min(1.0, validation_count / 3.0)  # saturates at 3 validations
    depth_component  = (signal_score * 0.50 + fix_score * 0.35 + validation_score * 0.15)

    # ── Component 4: automation feasibility (0–1) ────────────────────────────
    automation_component = _AUTOMATION_SCORES.get(automation_readiness.lower(), 0.20)

    # ── Component 5: OSS potential (0–1) ─────────────────────────────────────
    oss_component = 1.0 if has_oss_angle else 0.0

    # ── Composite ────────────────────────────────────────────────────────────
    composite = (
        _W_RECURRENCE  * recurrence_component
        + _W_IMPACT    * impact_component
        + _W_DEPTH     * depth_component
        + _W_AUTOMATION * automation_component
        + _W_OSS       * oss_component
    )
    composite = round(min(1.0, max(0.0, composite)), 4)
    priority_tier = _to_tier(composite)

    return PatternScore(
        composite=composite,
        priority_tier=priority_tier,
        recurrence_component=round(recurrence_component, 4),
        impact_component=round(impact_component, 4),
        depth_component=round(depth_component, 4),
        automation_component=round(automation_component, 4),
        oss_component=round(oss_component, 4),
        breakdown={
            "weights": {
                "recurrence":   _W_RECURRENCE,
                "impact":       _W_IMPACT,
                "depth":        _W_DEPTH,
                "automation":   _W_AUTOMATION,
                "oss":          _W_OSS,
            },
            "components": {
                "recurrence":   round(recurrence_component, 4),
                "impact":       round(impact_component, 4),
                "depth":        round(depth_component, 4),
                "automation":   round(automation_component, 4),
                "oss":          round(oss_component, 4),
            },
            "inputs": {
                "recurrence_score":     recurrence_score,
                "severity":             severity,
                "signal_count":         signal_count,
                "fix_count":            fix_count,
                "validation_count":     validation_count,
                "automation_readiness": automation_readiness,
                "has_oss_angle":        has_oss_angle,
                "evidence_count":       evidence_count,
                "confidence":           confidence,
            },
        },
    )


def score_pattern_from_row(row: dict, counts: dict | None = None) -> PatternScore:
    """
    Convenience wrapper that accepts a raw DB row dict and an optional
    counts dict: {"signals": n, "fixes": n, "validations": n, "assessments": n}
    """
    c = counts or {}
    return score_pattern(
        recurrence_score=float(row.get("recurrence_score", 0.5)),
        severity=row.get("severity", "medium"),
        signal_count=int(c.get("signals", 0)),
        fix_count=int(c.get("fixes", 0)),
        validation_count=int(c.get("validations", 0)),
        automation_readiness=row.get("automation_readiness", "manual"),
        has_oss_angle=bool(row.get("oss_contribution_angle")),
        evidence_count=int(c.get("assessments", row.get("evidence_count", 0))),
        confidence=float(row.get("confidence", 0.5)),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal
# ─────────────────────────────────────────────────────────────────────────────

def _to_tier(score: float) -> str:
    for threshold, tier in _TIERS:
        if score >= threshold:
            return tier
    return "P3"
