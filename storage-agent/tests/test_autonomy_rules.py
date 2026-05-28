"""
tests/test_autonomy_rules.py
─────────────────────────────────────────────────────────────────────────────
Tests for the storage autonomy rules engine.
"""

from app.autonomy_rules import (
    HUMAN_ONLY,
    APPROVAL_REQUIRED,
    AUTONOMOUS_ALLOWED,
    FORCE_APPROVAL_ABOVE_RISK,
    DOMAIN,
)


class TestAutonomyRuleSets:
    """Verify rule sets are correctly defined and mutually exclusive."""

    def test_domain_is_storage(self):
        assert DOMAIN == "storage"

    def test_human_only_non_empty(self):
        assert len(HUMAN_ONLY) > 0
        assert "multi_osd_escalate" in HUMAN_ONLY

    def test_approval_required_non_empty(self):
        assert len(APPROVAL_REQUIRED) > 0
        assert "osd_reweight" in APPROVAL_REQUIRED

    def test_autonomous_allowed_non_empty(self):
        assert len(AUTONOMOUS_ALLOWED) > 0
        assert "pvc_throttle" in AUTONOMOUS_ALLOWED

    def test_no_overlap_human_only_and_autonomous(self):
        overlap = HUMAN_ONLY & AUTONOMOUS_ALLOWED
        assert overlap == set(), f"Overlap found: {overlap}"

    def test_no_overlap_human_only_and_approval(self):
        overlap = HUMAN_ONLY & APPROVAL_REQUIRED
        assert overlap == set(), f"Overlap found: {overlap}"

    def test_no_overlap_approval_and_autonomous(self):
        overlap = APPROVAL_REQUIRED & AUTONOMOUS_ALLOWED
        assert overlap == set(), f"Overlap found: {overlap}"

    def test_force_approval_threshold(self):
        assert 0.0 < FORCE_APPROVAL_ABOVE_RISK < 1.0
        assert FORCE_APPROVAL_ABOVE_RISK == 0.65

    def test_all_actions_are_strings(self):
        for action_set in [HUMAN_ONLY, APPROVAL_REQUIRED, AUTONOMOUS_ALLOWED]:
            for action in action_set:
                assert isinstance(action, str)
                assert len(action) > 0
