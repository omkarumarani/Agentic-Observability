"""
obs_intelligence/multi_pattern_correlator.py
────────────────────────────────────────────────────────────────────────────────
Section P — Multi-Pattern Correlation

Detects when a single incident is explained by multiple simultaneous patterns
and determines which is dominant vs secondary/contributing.

Design
──────
  In complex production incidents, two or more failure modes co-occur:
    e.g. CPU saturation AND log pipeline backpressure at the same time.

  The correlator:
    1. Classifies each pattern match as DOMINANT, CONTRIBUTING, or CONFOUNDING
    2. Detects cross-signal causal chains (A → B → alerting)
    3. Produces a CorrelationResult with recommended investigation order

  Classification rules
  ─────────────────────
    DOMINANT     combined_score ≥ 0.60 and score gap from next ≥ 0.15
    CONTRIBUTING combined_score ≥ 0.40 and not dominant
    CONFOUNDING  combined_score < 0.40 but signals are noisy / misleading
    NOISE        combined_score < 0.25

  Causal chain detection
  ──────────────────────
    Heuristic: if Pattern A involves a "producer" layer (app, service) and
    Pattern B involves a "consumer" layer (collector, storage, pipeline) and
    both match simultaneously, infer A → B causality.

  Signal overlap detection
  ────────────────────────
    Two patterns that fire on the same signal (e.g. cpu_usage) are flagged as
    potentially correlated — one may be a symptom of the other.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("obs_intelligence.multi_pattern_correlator")

# ── Role constants ─────────────────────────────────────────────────────────
DOMINANT     = "dominant"
CONTRIBUTING = "contributing"
CONFOUNDING  = "confounding"
NOISE        = "noise"

# ── Layer ordering for causal chain inference ─────────────────────────────
_LAYER_ORDER = ["app", "collector", "network", "storage", "infra"]


# ═══════════════════════════════════════════════════════════════════════════════
# Data types
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class CorrelatedPattern:
    pattern_id:    str
    pattern_name:  str
    combined_score: float
    severity:      str
    role:          str              # dominant | contributing | confounding | noise
    causal_note:   str = ""        # e.g. "may be caused by dominant pattern"
    overlapping_signals: list[str] = field(default_factory=list)


@dataclass
class CorrelationResult:
    """Multi-pattern correlation result for one incident."""
    incident_summary:   str
    dominant:           CorrelatedPattern | None
    contributors:       list[CorrelatedPattern] = field(default_factory=list)
    confounders:        list[CorrelatedPattern] = field(default_factory=list)
    causal_chain:       list[str] = field(default_factory=list)   # ordered pattern names
    multi_cause:        bool = False
    investigation_order: list[str] = field(default_factory=list)  # pattern names

    def to_dict(self) -> dict:
        return {
            "multi_cause":    self.multi_cause,
            "causal_chain":   self.causal_chain,
            "investigation_order": self.investigation_order,
            "dominant": {
                "pattern_id":   self.dominant.pattern_id,
                "pattern_name": self.dominant.pattern_name,
                "score":        round(self.dominant.combined_score, 4),
                "severity":     self.dominant.severity,
                "role":         self.dominant.role,
                "causal_note":  self.dominant.causal_note,
            } if self.dominant else None,
            "contributors": [
                {
                    "pattern_id":    c.pattern_id,
                    "pattern_name":  c.pattern_name,
                    "score":         round(c.combined_score, 4),
                    "severity":      c.severity,
                    "role":          c.role,
                    "causal_note":   c.causal_note,
                    "overlapping_signals": c.overlapping_signals,
                }
                for c in self.contributors
            ],
            "confounders": [
                {
                    "pattern_id":  c.pattern_id,
                    "pattern_name": c.pattern_name,
                    "score":        round(c.combined_score, 4),
                    "role":         c.role,
                }
                for c in self.confounders
            ],
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def correlate_patterns(
    pattern_matches: list[dict],
    alert_name: str = "",
    service_name: str = "",
) -> CorrelationResult:
    """
    Classify and correlate a list of pattern match dicts (from pattern-library).

    Each dict must have:
      pattern_id, pattern_name, combined_score, severity
    Optional (used for causal inference if present):
      environment, impacted_layers

    Returns a CorrelationResult describing the multi-pattern landscape.
    """
    if not pattern_matches:
        return CorrelationResult(
            incident_summary=f"{alert_name} on {service_name}",
            dominant=None,
        )

    sorted_matches = sorted(
        pattern_matches,
        key=lambda m: m.get("combined_score", 0.0),
        reverse=True,
    )

    # ── Classify each match ───────────────────────────────────────────────
    classified: list[CorrelatedPattern] = []
    for i, m in enumerate(sorted_matches):
        score = m.get("combined_score", 0.0)
        cp = CorrelatedPattern(
            pattern_id=m.get("pattern_id", ""),
            pattern_name=m.get("pattern_name", "unknown"),
            combined_score=score,
            severity=m.get("severity", "medium"),
            role=_classify_role(score, i, sorted_matches),
        )
        classified.append(cp)

    # ── Detect overlapping signals ────────────────────────────────────────
    _mark_overlapping_signals(classified, pattern_matches)

    # ── Infer causal chain ────────────────────────────────────────────────
    causal_chain = _infer_causal_chain(classified, pattern_matches)
    _annotate_causal_notes(classified, causal_chain)

    # ── Build result ──────────────────────────────────────────────────────
    dominant_item  = next((c for c in classified if c.role == DOMINANT), None)
    contributors   = [c for c in classified if c.role == CONTRIBUTING]
    confounders    = [c for c in classified if c.role == CONFOUNDING]

    multi_cause = len(contributors) >= 1 and dominant_item is not None

    # Investigation order: dominant first, then contributors by score
    investigation_order = (
        ([dominant_item.pattern_name] if dominant_item else [])
        + [c.pattern_name for c in contributors]
    )

    logger.info(
        "Multi-pattern correlation  alert=%s  dominant=%s  contributors=%d  causal_chain=%s",
        alert_name,
        dominant_item.pattern_name if dominant_item else "none",
        len(contributors),
        " → ".join(causal_chain) if causal_chain else "none",
    )

    return CorrelationResult(
        incident_summary=f"{alert_name} on {service_name}",
        dominant=dominant_item,
        contributors=contributors,
        confounders=confounders,
        causal_chain=causal_chain,
        multi_cause=multi_cause,
        investigation_order=investigation_order,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _classify_role(score: float, rank: int, sorted_matches: list[dict]) -> str:
    """Determine whether a pattern is dominant, contributing, confounding, or noise."""
    if score < 0.25:
        return NOISE
    if score < 0.40:
        return CONFOUNDING

    if rank == 0 and score >= 0.60:
        # Check gap to next
        if len(sorted_matches) > 1:
            gap = score - sorted_matches[1].get("combined_score", 0.0)
            if gap >= 0.15:
                return DOMINANT
        else:
            return DOMINANT

    return CONTRIBUTING


def _mark_overlapping_signals(
    classified: list[CorrelatedPattern],
    raw_matches: list[dict],
) -> None:
    """
    Mark patterns that share the same triggering signal names.
    Uses pattern names as proxies — full signal overlap requires pattern DB data.
    """
    # Heuristic: patterns with "cpu" in name all share cpu_usage signal
    signal_buckets: dict[str, list[int]] = {}
    signal_keywords = {
        "cpu": "cpu_usage",
        "memory": "memory_usage",
        "latency": "latency_p99",
        "error": "error_rate",
        "backpressure": "collector_queue_depth",
        "oom": "memory_usage",
        "histogram": "histogram_sum",
    }
    for idx, cp in enumerate(classified):
        for kw, signal in signal_keywords.items():
            if kw in cp.pattern_name.lower():
                signal_buckets.setdefault(signal, []).append(idx)

    for signal, indices in signal_buckets.items():
        if len(indices) >= 2:
            for idx in indices:
                classified[idx].overlapping_signals.append(signal)


def _infer_causal_chain(
    classified: list[CorrelatedPattern],
    raw_matches: list[dict],
) -> list[str]:
    """
    Simple heuristic causal chain inference.
    If a dominant pattern is app-layer and a contributor is collector-layer,
    infer: app → collector chain.
    """
    if not classified:
        return []

    dominant = next((c for c in classified if c.role == DOMINANT), None)
    if not dominant:
        return []

    chain = [dominant.pattern_name]
    for c in classified:
        if c.role == CONTRIBUTING and c.pattern_name != dominant.pattern_name:
            # Heuristic: "backpressure" or "collector" patterns are downstream
            if any(kw in c.pattern_name.lower() for kw in ("backpressure", "collector", "pipeline", "scrape")):
                chain.append(c.pattern_name)

    return chain if len(chain) > 1 else []


def _annotate_causal_notes(
    classified: list[CorrelatedPattern],
    causal_chain: list[str],
) -> None:
    """Add causal notes to patterns in the chain."""
    if len(causal_chain) < 2:
        return
    chain_set = set(causal_chain)
    dominant_name = causal_chain[0]
    for cp in classified:
        if cp.pattern_name in chain_set and cp.pattern_name != dominant_name:
            cp.causal_note = f"may be a downstream effect of '{dominant_name}'"
