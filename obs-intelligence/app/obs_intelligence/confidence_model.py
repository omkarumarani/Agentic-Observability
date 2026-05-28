"""
obs_intelligence/confidence_model.py
────────────────────────────────────────────────────────────────────────────────
Section O — Confidence & Uncertainty Modeling

Extends the deterministic risk scorer with explicit uncertainty quantification.
Answers:
  • How confident are we in each hypothesis?
  • Are there multiple competing hypotheses?
  • Is there insufficient data to make a recommendation?
  • Should the system stay silent rather than make an overconfident claim?

Design
──────
  UncertaintyProfile encapsulates per-hypothesis confidence + an overall
  "decision confidence" for the agent's recommendation.

  Confidence signals used:
    • scenario match count & spread (multiple matches → uncertainty)
    • scenario top-1 confidence score
    • number of signals with values vs missing (data completeness)
    • log anomaly presence (corroborating evidence)
    • recurrence count (seen before → more confident)
    • error rate magnitude (strong signal vs noise)
    • pattern library match score (external calibration)

Decision confidence tiers
─────────────────────────
  ≥ 0.75  →  ACT       — evidence is clear; recommend action
  0.50 – 0.74 → REVIEW — plausible hypothesis; human review recommended
  0.25 – 0.49 → INVESTIGATE — data insufficient; collect more signals first
  < 0.25  →  DEFER     — too uncertain; do not recommend automated action
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from obs_intelligence.models import ObsFeatures, RiskAssessment, ScenarioMatch

logger = logging.getLogger("obs_intelligence.confidence_model")

# ── Decision tier thresholds ─────────────────────────────────────────────────
_TIERS = [
    (0.75, "ACT"),
    (0.50, "REVIEW"),
    (0.25, "INVESTIGATE"),
    (0.00, "DEFER"),
]

# ── Weights for confidence components ───────────────────────────────────────
_W_MATCH_CONFIDENCE  = 0.35   # top scenario match confidence
_W_SIGNAL_COVERAGE   = 0.25   # fraction of key signals with real values
_W_CORROBORATING     = 0.20   # corroborating evidence (logs, errors)
_W_RECURRENCE        = 0.10   # seen before reduces uncertainty
_W_PATTERN_LIBRARY   = 0.10   # external pattern match adds confidence


# ═══════════════════════════════════════════════════════════════════════════════
# Data types
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class HypothesisConfidence:
    """Confidence model for a single root-cause hypothesis."""
    scenario_id:   str
    display_name:  str
    raw_confidence: float           # from ScenarioMatch.confidence
    evidence_count: int             # how many signals support it
    competing:     bool = False     # True if another hypothesis is nearly as confident


@dataclass
class UncertaintyProfile:
    """Full uncertainty picture for one incident."""
    decision_confidence:   float          # 0.0–1.0; drives tier
    decision_tier:         str            # ACT | REVIEW | INVESTIGATE | DEFER

    primary_hypothesis:    HypothesisConfidence | None
    competing_hypotheses:  list[HypothesisConfidence] = field(default_factory=list)

    # Diagnostic flags
    data_insufficient:     bool = False   # key metrics are missing
    multiple_causes:       bool = False   # ≥2 hypotheses with confidence > 0.4
    overconfidence_risk:   bool = False   # single dominant match but low signal coverage
    recommend_silent:      bool = False   # True when DEFER tier — do not recommend action

    # Component scores (for auditability)
    match_confidence_score:  float = 0.0
    signal_coverage_score:   float = 0.0
    corroborating_score:     float = 0.0
    recurrence_score:        float = 0.0
    pattern_library_score:   float = 0.0

    # Human-readable notes
    uncertainty_notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "decision_confidence": round(self.decision_confidence, 4),
            "decision_tier":       self.decision_tier,
            "recommend_silent":    self.recommend_silent,
            "data_insufficient":   self.data_insufficient,
            "multiple_causes":     self.multiple_causes,
            "overconfidence_risk": self.overconfidence_risk,
            "uncertainty_notes":   self.uncertainty_notes,
            "primary_hypothesis": {
                "scenario_id":    self.primary_hypothesis.scenario_id,
                "display_name":   self.primary_hypothesis.display_name,
                "confidence":     round(self.primary_hypothesis.raw_confidence, 4),
                "evidence_count": self.primary_hypothesis.evidence_count,
                "competing":      self.primary_hypothesis.competing,
            } if self.primary_hypothesis else None,
            "competing_hypotheses": [
                {
                    "scenario_id":  h.scenario_id,
                    "display_name": h.display_name,
                    "confidence":   round(h.raw_confidence, 4),
                }
                for h in self.competing_hypotheses
            ],
            "score_breakdown": {
                "match_confidence":  round(self.match_confidence_score, 4),
                "signal_coverage":   round(self.signal_coverage_score, 4),
                "corroborating":     round(self.corroborating_score, 4),
                "recurrence":        round(self.recurrence_score, 4),
                "pattern_library":   round(self.pattern_library_score, 4),
            },
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def build_uncertainty_profile(
    features: "ObsFeatures",
    scenario_matches: list["ScenarioMatch"],
    risk: "RiskAssessment",
    domain: str,
    pattern_library_top_score: float = 0.0,
) -> UncertaintyProfile:
    """
    Build an UncertaintyProfile for one incident.

    Parameters
    ──────────
    features               ObsFeatures snapshot
    scenario_matches       All ScenarioMatch results (ranked by confidence)
    risk                   RiskAssessment from risk_scorer
    domain                 "compute" | "storage"
    pattern_library_top_score  optional: combined_score from pattern-library search
    """
    notes: list[str] = []

    # ── Component 1: match confidence ────────────────────────────────────────
    if scenario_matches:
        top_conf        = scenario_matches[0].confidence
        match_conf_score = top_conf
        if len(scenario_matches) >= 2:
            second_conf = scenario_matches[1].confidence
            spread = top_conf - second_conf
            if spread < 0.15:
                # Very similar top-2 → genuine ambiguity
                match_conf_score = top_conf * 0.75
                notes.append(
                    f"Top-2 scenarios close: {top_conf:.0%} vs {second_conf:.0%} "
                    f"(spread={spread:.0%}) — ambiguous root cause"
                )
    else:
        match_conf_score = 0.05
        notes.append("No scenario matched — pure heuristic analysis")

    # ── Component 2: signal coverage ─────────────────────────────────────────
    signal_coverage_score = _signal_coverage(features, domain, notes)

    # ── Component 3: corroborating evidence ──────────────────────────────────
    corroborating_score = _corroborating_evidence(features, notes)

    # ── Component 4: recurrence ───────────────────────────────────────────────
    recurrence_score = min(1.0, features.recurrence_count / 5.0)
    if features.recurrence_count >= 3:
        notes.append(f"Recurring alert ({features.recurrence_count}×) — higher confidence in pattern")

    # ── Component 5: pattern library ─────────────────────────────────────────
    plib_score = pattern_library_top_score
    if plib_score >= 0.70:
        notes.append(f"Pattern Library strong match ({plib_score:.0%}) — external validation")

    # ── Composite confidence ─────────────────────────────────────────────────
    decision_confidence = (
        _W_MATCH_CONFIDENCE * match_conf_score
        + _W_SIGNAL_COVERAGE  * signal_coverage_score
        + _W_CORROBORATING    * corroborating_score
        + _W_RECURRENCE       * recurrence_score
        + _W_PATTERN_LIBRARY  * plib_score
    )
    decision_confidence = round(min(1.0, max(0.0, decision_confidence)), 4)
    tier = _to_tier(decision_confidence)

    # ── Hypothesis list ───────────────────────────────────────────────────────
    primary    = None
    competing  = []
    multi_flag = False

    if scenario_matches:
        primary = HypothesisConfidence(
            scenario_id=scenario_matches[0].scenario_id,
            display_name=scenario_matches[0].display_name,
            raw_confidence=scenario_matches[0].confidence,
            evidence_count=len(scenario_matches[0].matched_features),
        )
        for m in scenario_matches[1:]:
            if m.confidence >= 0.40:
                competing.append(HypothesisConfidence(
                    scenario_id=m.scenario_id,
                    display_name=m.display_name,
                    raw_confidence=m.confidence,
                    evidence_count=len(m.matched_features),
                ))
        if competing:
            multi_flag = True
            primary.competing = True
            notes.append(
                f"{len(competing)} competing hypothesis(es) above 40% — "
                "multi-cause incident possible"
            )

    # ── Safety flags ─────────────────────────────────────────────────────────
    data_insufficient  = signal_coverage_score < 0.40
    overconfidence_risk = (
        match_conf_score > 0.80
        and signal_coverage_score < 0.50
        and not corroborating_score
    )
    recommend_silent = tier == "DEFER"

    if data_insufficient:
        notes.append("Key signal values missing — expand metric collection before acting")
    if overconfidence_risk:
        notes.append("High match confidence but low signal coverage — verify before acting")
    if recommend_silent:
        notes.append("Decision confidence too low — hold recommendation; collect more data")

    logger.info(
        "Uncertainty profile  alert=%s  tier=%s  confidence=%.3f  "
        "matches=%d  competing=%d",
        features.alert_name, tier, decision_confidence,
        len(scenario_matches), len(competing),
    )

    return UncertaintyProfile(
        decision_confidence=decision_confidence,
        decision_tier=tier,
        primary_hypothesis=primary,
        competing_hypotheses=competing,
        data_insufficient=data_insufficient,
        multiple_causes=multi_flag,
        overconfidence_risk=overconfidence_risk,
        recommend_silent=recommend_silent,
        match_confidence_score=match_conf_score,
        signal_coverage_score=signal_coverage_score,
        corroborating_score=corroborating_score,
        recurrence_score=recurrence_score,
        pattern_library_score=plib_score,
        uncertainty_notes=notes,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

_COMPUTE_KEY_SIGNALS = ["error_rate", "latency_p99", "cpu_usage", "memory_usage"]
_STORAGE_KEY_SIGNALS = ["pool_usage_pct", "osd_up_count", "degraded_pgs", "io_latency"]


def _signal_coverage(
    features: "ObsFeatures",
    domain: str,
    notes: list[str],
) -> float:
    """Fraction of domain-critical signals that have non-zero values."""
    key_signals = _COMPUTE_KEY_SIGNALS if domain == "compute" else _STORAGE_KEY_SIGNALS
    present = 0
    missing = []
    for sig in key_signals:
        val = getattr(features, sig, 0)
        if isinstance(val, bool):
            present += 1
        elif isinstance(val, (int, float)) and val > 0:
            present += 1
        else:
            missing.append(sig)
    coverage = present / len(key_signals)
    if missing:
        notes.append(f"Missing signals: {', '.join(missing)}")
    return coverage


def _corroborating_evidence(
    features: "ObsFeatures",
    notes: list[str],
) -> float:
    """Score based on logs + error counts that corroborate the metric signal."""
    score = 0.0
    if features.log_anomaly_detected:
        score += 0.60
    if features.recent_error_count >= 5:
        score += 0.30
    elif features.recent_error_count >= 1:
        score += 0.10
    if features.error_rate > 0.01:
        score += 0.10
    score = min(1.0, score)
    if score == 0.0:
        notes.append("No log corroboration available (consider enabling log anomaly detection)")
    return score


def _to_tier(score: float) -> str:
    for threshold, tier in _TIERS:
        if score >= threshold:
            return tier
    return "DEFER"
