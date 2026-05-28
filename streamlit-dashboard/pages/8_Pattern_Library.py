"""
AIOps Command Center — 🧠 Pattern Library Browser
===================================================
Section W — User Interaction Model

Interactive browser for the Pattern Intelligence Library (Phase 15 / Section E).
Allows SREs to:
  • Browse all patterns with filtering by severity / automation readiness
  • View the leaderboard (Section H composite scoring)
  • Inspect a single pattern: signals, fixes, feedback history, actions
  • Download generated action artifacts (Ansible playbook, runbook, alert rule)
  • Submit outcome feedback to improve pattern confidence (Section N)
  • Inspect matched patterns from the latest pipeline session

Layout
──────
  Sidebar: filter controls + stats summary
  Main: tabbed view
    Tab 1 — Leaderboard (top-N patterns by composite score)
    Tab 2 — Pattern Browser (filterable table + detail expander)
    Tab 3 — Incident Patterns (last pipeline session's matched patterns)
    Tab 4 — Action Artifacts (generate and download per-pattern)
    Tab 5 — Submit Feedback (mark assessment outcome to train confidence)
"""

import json

import streamlit as st

from shared import (
    COMPUTE_AGENT_URL,
    PATTERN_LIBRARY_URL,
    api_get,
    api_post,
    page_footer,
    page_header,
    sev_icon,
    since_str,
)

st.set_page_config(
    page_title="Pattern Library — AIOps",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

page_header("🧠 Pattern Intelligence Library")

st.caption(
    "Browse, score, and action-ize observability failure patterns. "
    f"Pattern Library API: `{PATTERN_LIBRARY_URL}`"
)

# ─── Sidebar: controls & stats ─────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")
    sev_filter = st.selectbox(
        "Severity", ["all", "critical", "high", "medium", "low"]
    )
    auto_filter = st.selectbox(
        "Automation readiness", ["all", "safe", "risky", "manual"]
    )
    env_filter = st.selectbox(
        "Environment", ["all", "kubernetes", "vm", "bare-metal", "any"]
    )
    limit_val = st.slider("Max patterns", 10, 100, 50, 10)

    st.divider()
    st.subheader("Library Stats")
    stats = api_get(f"{PATTERN_LIBRARY_URL}/stats", timeout=5.0)
    if stats:
        p = stats.get("patterns", {})
        st.metric("Active Patterns", p.get("active", 0))
        sev_data = p.get("by_severity", {})
        cols = st.columns(2)
        cols[0].metric("🔴 Critical", sev_data.get("critical", 0))
        cols[0].metric("🟠 High",     sev_data.get("high", 0))
        cols[1].metric("🟡 Medium",   sev_data.get("medium", 0))
        cols[1].metric("🔵 Low",      sev_data.get("low", 0))
        st.metric("Safe Automation Ready", p.get("safe_automation_ready", 0))
        st.metric("Avg Confidence",
                  f"{p.get('avg_confidence', 0):.0%}")
        st.metric("Avg Recurrence",
                  f"{p.get('avg_recurrence_score', 0):.0%}")
    else:
        st.warning("Pattern Library unavailable")


# ─── Main tabs ─────────────────────────────────────────────────────────────────

tab_lb, tab_browse, tab_incident, tab_actions, tab_feedback = st.tabs([
    "🏆 Leaderboard",
    "📋 Browse Patterns",
    "🔗 Incident Matches",
    "⚙️ Action Artifacts",
    "📣 Submit Feedback",
])


# ══════════════════════════════════════════════════════════════════════════════
# Tab 1 — Leaderboard
# ══════════════════════════════════════════════════════════════════════════════

with tab_lb:
    st.subheader("Pattern Importance Leaderboard (Section H)")
    st.caption(
        "Composite score = 30% recurrence · 25% business impact · "
        "20% technical depth · 15% automation feasibility · 10% OSS potential"
    )

    top_n = st.slider("Show top N", 5, 50, 10, 5, key="lb_topn")
    lb_data = api_get(f"{PATTERN_LIBRARY_URL}/patterns/leaderboard?top_n={top_n}")

    if lb_data:
        tier_colors = {"P0": "🔴", "P1": "🟠", "P2": "🟡", "P3": "🔵"}
        for rank, p in enumerate(lb_data, start=1):
            tier = p.get("priority_tier", "P3")
            icon = tier_colors.get(tier, "⚪")
            score = p.get("composite_score", 0)
            with st.expander(
                f"{rank}. {icon} [{tier}] {p['pattern_name']} — score: {score:.2f}",
                expanded=(rank <= 3),
            ):
                cols = st.columns(4)
                cols[0].metric("Composite Score", f"{score:.3f}")
                cols[1].metric("Severity", sev_icon(p.get("severity", "?")) + " " + p.get("severity", "?").upper())
                cols[2].metric("Recurrence", f"{p.get('recurrence_score', 0):.0%}")
                cols[3].metric("Automation", p.get("automation_readiness", "?"))

                # Detail score from /patterns/{id}/score
                pid = p.get("pattern_id")
                if pid and st.button(f"Load score breakdown", key=f"lb_score_{pid}"):
                    score_data = api_get(f"{PATTERN_LIBRARY_URL}/patterns/{pid}/score")
                    if score_data:
                        breakdown = score_data.get("breakdown", {}).get("components", {})
                        st.json(breakdown)
    else:
        st.info("No leaderboard data available — ensure Pattern Library is running.")


# ══════════════════════════════════════════════════════════════════════════════
# Tab 2 — Pattern Browser
# ══════════════════════════════════════════════════════════════════════════════

with tab_browse:
    st.subheader("Pattern Browser")

    params = f"limit={limit_val}&deprecated=false"
    if sev_filter != "all":
        params += f"&severity={sev_filter}"
    if env_filter != "all":
        params += f"&environment={env_filter}"

    patterns = api_get(f"{PATTERN_LIBRARY_URL}/patterns?{params}")

    if auto_filter != "all" and patterns:
        patterns = [p for p in patterns if p.get("automation_readiness") == auto_filter]

    if patterns:
        st.caption(f"Showing {len(patterns)} pattern(s)")
        for p in patterns:
            sev  = p.get("severity", "?")
            icon = sev_icon(sev)
            auto = p.get("automation_readiness", "?")
            rec  = p.get("recurrence_score", 0)
            conf = p.get("confidence", 0)
            pid  = p.get("id")

            with st.expander(
                f"{icon} **{p['name']}** — {sev.upper()} | "
                f"rec: {rec:.0%} | conf: {conf:.0%} | {auto}",
            ):
                st.write(p.get("description", ""))
                cols = st.columns(3)
                cols[0].metric("Severity",     sev.upper())
                cols[1].metric("Recurrence",   f"{rec:.0%}")
                cols[2].metric("Confidence",   f"{conf:.0%}")

                c2 = st.columns(3)
                c2[0].metric("Automation",    auto)
                c2[1].metric("Environment",   p.get("environment", "?"))
                c2[2].metric("Evidence Count", p.get("evidence_count", 0))

                layers = p.get("impacted_layers") or []
                if layers:
                    st.write("**Impacted layers:**", ", ".join(layers))

                if pid:
                    if st.button("Load full detail", key=f"browse_detail_{pid}"):
                        detail = api_get(f"{PATTERN_LIBRARY_URL}/patterns/{pid}")
                        if detail:
                            # Signals
                            sigs = detail.get("signals", [])
                            if sigs:
                                st.markdown("**Signals:**")
                                for sig in sigs:
                                    op  = sig.get("threshold_operator", "")
                                    val = sig.get("threshold_value")
                                    unit = sig.get("unit", "")
                                    thresh = f"{op} {val}{unit}" if val is not None else "(no threshold)"
                                    st.write(
                                        f"  • `{sig['name']}` [{sig['signal_type']}] — {thresh}"
                                    )
                            # Fixes
                            fixes = detail.get("fixes", [])
                            if fixes:
                                st.markdown("**Known fixes:**")
                                for fix in fixes:
                                    safe_icon = "✅" if fix.get("automation_safe") else "🛑"
                                    st.write(f"  {safe_icon} **{fix['title']}**: {fix['description']}")
                        else:
                            st.error("Could not load pattern detail")

                    # Feedback summary
                    if st.button("Load feedback history", key=f"feedback_hist_{pid}"):
                        fb = api_get(f"{PATTERN_LIBRARY_URL}/patterns/{pid}/feedback-summary")
                        if fb:
                            st.json(fb)
    else:
        st.info("No patterns match the current filters.")


# ══════════════════════════════════════════════════════════════════════════════
# Tab 3 — Incident Patterns (latest pipeline session)
# ══════════════════════════════════════════════════════════════════════════════

with tab_incident:
    st.subheader("Matched Patterns — Latest Pipeline Session")
    st.caption(
        "Shows Pattern Library matches from the most recent agent pipeline run. "
        "Refresh the pipeline by triggering a test alert."
    )

    session = api_get(f"{COMPUTE_AGENT_URL}/pipeline/session/default")
    if session:
        incident = session.get("incident", {})
        analysis = session.get("analysis", {})
        unc      = analysis.get("uncertainty", {})
        corr     = analysis.get("pattern_correlation", {})
        ts_ctx   = analysis.get("timeseries_context", {})
        matched  = session.get("matched_patterns", [])

        # Session header
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Service",     incident.get("service_name", "-"))
        col2.metric("Alert",       incident.get("alert_name", "-"))
        col3.metric("Risk",        f"{incident.get('risk_score', 0):.2f}")
        col4.metric("Stage",       session.get("stage", "-"))

        st.divider()

        # ── Uncertainty profile (Section O) ──────────────────────────────────
        if unc:
            tier   = unc.get("decision_tier", "?")
            dconf  = unc.get("decision_confidence", 0)
            tier_colors = {"ACT": "🟢", "REVIEW": "🟡", "INVESTIGATE": "🟠", "DEFER": "🔴"}
            tier_col = tier_colors.get(tier, "⚪")
            st.subheader(f"Decision Confidence  {tier_col} {tier}")
            uc1, uc2, uc3 = st.columns(3)
            uc1.metric("Decision Confidence", f"{dconf:.0%}")
            uc2.metric("Data Insufficient",   "Yes" if unc.get("data_insufficient") else "No")
            uc3.metric("Multiple Causes",      "Yes" if unc.get("multiple_causes") else "No")
            notes = unc.get("uncertainty_notes", [])
            if notes:
                for note in notes:
                    st.info(f"⚠️ {note}")

        st.divider()

        # ── Pattern Library matches ───────────────────────────────────────────
        st.subheader(f"Pattern Library: {len(matched)} Match(es)")
        if matched:
            for rank, m in enumerate(matched, start=1):
                score    = m.get("combined_score", 0)
                pat_name = m.get("pattern_name", "?")
                sev      = m.get("severity", "?")
                rec_sc   = m.get("recurrence_score", 0)

                # Role from correlation
                role     = "—"
                if corr.get("dominant", {}).get("pattern_name") == pat_name:
                    role = "🔴 DOMINANT"
                elif any(c.get("pattern_name") == pat_name for c in corr.get("contributors", [])):
                    role = "🟡 CONTRIBUTING"

                with st.expander(
                    f"{rank}. {sev_icon(sev)} {pat_name}  "
                    f"match={score:.0%}  recurrence={rec_sc:.0%}  {role}"
                ):
                    mc1, mc2, mc3 = st.columns(3)
                    mc1.metric("Combined Score",   f"{score:.3f}")
                    mc2.metric("Rule Score",       f"{m.get('rule_score', 0):.3f}")
                    mc3.metric("Vector Similarity", f"{m.get('vector_similarity', 0):.3f}")
                    st.write(f"**Pattern ID:** `{m.get('pattern_id', '?')}`")
        else:
            st.info("No Pattern Library matches for this session.")

        # ── Multi-pattern correlation (Section P) ─────────────────────────────
        if corr and corr.get("multi_cause"):
            st.divider()
            st.subheader("Multi-Pattern Correlation")
            chain = corr.get("causal_chain", [])
            if chain:
                st.write("**Causal chain:** " + " → ".join(chain))
            inv_order = corr.get("investigation_order", [])
            if inv_order:
                st.write("**Investigation order:**")
                for i, name in enumerate(inv_order, start=1):
                    st.write(f"  {i}. {name}")

        # ── Time-series context (Section Q) ──────────────────────────────────
        if ts_ctx:
            st.divider()
            st.subheader("Time-Series Baseline Context")
            ts1, ts2 = st.columns(2)
            ts1.write(f"**Trend summary:** {ts_ctx.get('trend_summary', '—')}")
            if ts_ctx.get("deployment_note"):
                ts2.write(f"**Deployment:** {ts_ctx['deployment_note']}")
            trends = ts_ctx.get("trends", [])
            if trends:
                for t in trends:
                    if t.get("is_anomalous"):
                        z = t.get("z_score", 0)
                        baseline = t.get("baseline")
                        pct = t.get("pct_vs_baseline")
                        label = f"{t['metric']}  [{t['direction'].upper()}]"
                        detail = f"z={z:.1f}" if z else ""
                        if pct is not None:
                            detail += f"  {pct:+.1f}% vs baseline"
                        st.warning(f"📈 `{label}` — {detail}")
    else:
        st.info(
            "No pipeline session found. Trigger a test alert:\n"
            "```bash\n"
            "curl -X POST http://localhost:9000/webhook \\\n"
            "  -H 'Content-Type: application/json' \\\n"
            "  -d '{\"status\":\"firing\",\"alerts\":[{\"status\":\"firing\","
            "\"labels\":{\"alertname\":\"HighCPU\",\"service_name\":\"frontend-api\","
            "\"severity\":\"warning\"},\"annotations\":{\"summary\":\"CPU high\"},"
            "\"startsAt\":\"2026-01-01T00:00:00Z\"}]}'\n"
            "```"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Tab 4 — Action Artifacts (Section I)
# ══════════════════════════════════════════════════════════════════════════════

with tab_actions:
    st.subheader("Generate Action Artifacts (Section I)")
    st.caption(
        "Select a pattern to generate ready-to-use Prometheus alert rules, "
        "OTel Collector snippets, Ansible playbooks, and runbooks."
    )

    patterns_for_actions = api_get(
        f"{PATTERN_LIBRARY_URL}/patterns?limit=50&deprecated=false"
    )
    if patterns_for_actions:
        pattern_names = {p["name"]: p["id"] for p in patterns_for_actions}
        selected_name = st.selectbox(
            "Select pattern", list(pattern_names.keys()), key="action_pattern"
        )
        selected_pid = pattern_names.get(selected_name)

        if selected_pid and st.button("Generate Actions", type="primary"):
            with st.spinner("Generating action artifacts..."):
                actions = api_get(
                    f"{PATTERN_LIBRARY_URL}/patterns/{selected_pid}/actions"
                )
            if actions:
                auto = actions.get("automation_readiness", "?")
                auto_icon = {"safe": "✅", "risky": "⚠️", "manual": "🛑"}.get(auto, "?")
                st.success(
                    f"Actions generated for **{selected_name}**  "
                    f"— Automation: {auto_icon} {auto.upper()}"
                )

                a1, a2 = st.tabs(["📜 Prometheus Alert Rule", "🔧 OTel Collector"])
                with a1:
                    prom = actions.get("prometheus_alert_rule", "")
                    st.code(prom, language="yaml")
                    st.download_button(
                        "Download alert rule",
                        prom,
                        file_name=f"alert_{selected_name}.yml",
                        mime="text/yaml",
                    )
                with a2:
                    otel = actions.get("otel_collector_snippet", "")
                    st.code(otel, language="yaml")
                    st.download_button(
                        "Download OTel snippet",
                        otel,
                        file_name=f"otel_{selected_name}.yml",
                        mime="text/yaml",
                    )

                b1, b2 = st.tabs(["🤖 Ansible Playbook", "📖 Runbook"])
                with b1:
                    playbook = actions.get("ansible_playbook", "")
                    st.code(playbook, language="yaml")
                    st.download_button(
                        "Download Ansible playbook",
                        playbook,
                        file_name=f"playbook_{selected_name}.yml",
                        mime="text/yaml",
                    )
                with b2:
                    runbook = actions.get("runbook_markdown", "")
                    st.markdown(runbook)
                    st.download_button(
                        "Download runbook",
                        runbook,
                        file_name=f"runbook_{selected_name}.md",
                        mime="text/markdown",
                    )

                with st.expander("Grafana Panel Hint"):
                    st.json(actions.get("grafana_panel_hint", {}))
            else:
                st.error("Failed to generate actions — check Pattern Library service.")
    else:
        st.info("No patterns available.")


# ══════════════════════════════════════════════════════════════════════════════
# Tab 5 — Submit Feedback (Section N)
# ══════════════════════════════════════════════════════════════════════════════

with tab_feedback:
    st.subheader("Submit Outcome Feedback (Section N — Feedback Loop)")
    st.caption(
        "Record whether a fix recommendation worked. "
        "Outcomes update pattern confidence scores to improve future recommendations."
    )

    st.info(
        "To submit feedback you need the **Assessment ID** from a completed pipeline run. "
        "Find it in `GET /assessments` on the Pattern Library API or in the pipeline session."
    )

    assessment_id = st.text_input(
        "Assessment ID (UUID)", placeholder="e.g. 3fa85f64-5717-4562-b3fc-2c963f66afa6"
    )
    outcome = st.selectbox(
        "Outcome",
        ["resolved", "escalated", "false_positive", "unknown"],
        index=0,
    )

    outcome_descriptions = {
        "resolved":       "✅ Fix worked — incident cleared without escalation",
        "escalated":      "🔺 Fix did not resolve — required manual escalation",
        "false_positive": "⚠️  Alert was not a real incident",
        "unknown":        "❓ Outcome not yet determined",
    }
    st.write(outcome_descriptions.get(outcome, ""))

    if st.button("Submit Feedback", type="primary", disabled=not assessment_id):
        result = api_post(
            f"{PATTERN_LIBRARY_URL}/assessments/{assessment_id}/outcome",
            {"outcome": outcome},
        )
        if result and result.get("recorded"):
            if result.get("pattern_confidence_updated"):
                st.success(
                    f"Outcome recorded! Pattern confidence updated to "
                    f"**{result.get('new_confidence', 0):.0%}** "
                    f"({result.get('total_decisive_outcomes', 0)} total decisive outcomes)"
                )
            else:
                st.success(
                    "Outcome recorded. Confidence update pending (need ≥2 decisive outcomes)."
                )
        else:
            error_msg = (result or {}).get("error", "Unknown error")
            st.error(f"Failed to submit feedback: {error_msg}")

    st.divider()
    st.subheader("Recent Assessments")

    recent_assessments = api_get(f"{PATTERN_LIBRARY_URL}/assessments?limit=10")
    if recent_assessments:
        for a in recent_assessments:
            with st.expander(
                f"Session: {a.get('session_id', '?')} — "
                f"Alert: {a.get('alert_name', '?')} — "
                f"Outcome: {a.get('outcome') or 'pending'}"
            ):
                ac1, ac2, ac3 = st.columns(3)
                ac1.write(f"**ID:** `{a.get('id', '?')}`")
                ac2.write(f"**Agent:** {a.get('agent', '?')}")
                ac3.write(f"**Risk:** {a.get('risk_score', '?')}")
                created = a.get("assessed_at") or a.get("created_at", "?")
                st.write(f"**Created:** {since_str(created)}")
                if a.get("top_pattern_id"):
                    st.write(f"**Matched pattern:** `{a['top_pattern_id']}`")
    else:
        st.info("No recent assessments found.")


page_footer()
