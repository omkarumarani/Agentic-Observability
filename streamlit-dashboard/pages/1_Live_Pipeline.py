"""
AIOps Command Center — 🔴 Live Pipeline (Compute)
===================================================
Real-time view of the compute-agent pipeline.

The pipeline stage refreshes every 1 second via @st.fragment — no full
page reload needed, so the rest of the UI stays responsive.

The compute-agent also exposes GET /pipeline/events as a proper
Server-Sent Events stream for external EventSource consumers.
"""

import streamlit as st

from shared import (
    COMPUTE_AGENT_URL, XYOPS_EXT, GRAFANA_EXT,
    api_get, since_str, sev_icon, status_icon,
    page_header, page_footer,
)

st.set_page_config(
    page_title="Live Pipeline — AIOps",
    page_icon="🔴",
    layout="wide",
    initial_sidebar_state="expanded",
)

page_header("🔴 Live Pipeline  (Compute Agent)")

st.caption(
    "⚡ Agent status boxes refresh every **1 second** via `@st.fragment`.  "
    f"SSE stream available at `GET {COMPUTE_AGENT_URL}/pipeline/events` "
    "for external EventSource consumers."
)


# ─── Fragment: independent 1-second refresh for the live pipeline view ──────────

@st.fragment(run_every="1s")
def _live_pipeline_view() -> None:
    live = api_get(f"{COMPUTE_AGENT_URL}/pipeline/session/default")

    if not live:
        st.warning("No pipeline session found — compute-agent returned no data.")
        st.info(
            "**Manually trigger a test alert:**\n"
            "```bash\n"
            "curl -X POST http://localhost:9000/webhook \\\n"
            "  -H 'Content-Type: application/json' \\\n"
            "  -d '{\"status\":\"firing\",\"alerts\":[{\"status\":\"firing\","
            "\"labels\":{\"alertname\":\"HighErrorRate\",\"service_name\":\"frontend-api\","
            "\"severity\":\"warning\"},\"annotations\":{\"summary\":\"Test alert\"},"
            "\"startsAt\":\"2026-03-22T10:00:00Z\"}]}'\n"
            "```"
        )
        return

    s     = live
    sev   = s.get("severity", "info")
    stage = s.get("stage", "unknown")

    # ── Session header ─────────────────────────────────────────────────────
    c = st.columns(4)
    c[0].metric("Session ID",  s.get("session_id", "-"))
    c[1].metric("Service",     s.get("service_name", "-"))
    c[2].metric(f"Severity {sev_icon(sev)}", sev.upper())
    c[3].metric("Stage",       stage)

    c2 = st.columns(4)
    c2[0].metric("Alert",      s.get("alert_name", "-"))
    c2[1].metric("Status",     s.get("status", "unknown").upper())
    c2[2].metric("Risk Score", f"{s.get('risk_score', 0):.2f}  ({s.get('risk_level', '-')})")
    c2[3].metric(
        "MTTR",
        f"{s.get('mttr_seconds', 0):.1f}s" if s.get("mttr_seconds") else "⏳ running",
    )

    st.divider()

    # ── 6-Agent Pipeline Visualisation ────────────────────────────────────
    st.subheader("Agent Pipeline  ⚡ live")

    AGENTS = [
        ("ticket-creator",   "1️⃣ Ticket\nCreator"),
        ("log-fetcher",      "2️⃣ Log\nFetcher"),
        ("metrics-fetcher",  "3️⃣ Metrics\nFetcher"),
        ("ai-analyst",       "4️⃣ AI\nAnalyst"),
        ("ticket-writer",    "5️⃣ Ticket\nWriter"),
        ("approval-gateway", "6️⃣ Approval\nGateway"),
    ]

    agents_data   = {a["name"]: a for a in s.get("agents", [])}
    completed_cnt = sum(1 for a in s.get("agents", []) if a.get("status") == "completed")
    total_agents  = len(s.get("agents", [])) or 6

    agent_cols = st.columns(6)
    for i, (agent_key, agent_label) in enumerate(AGENTS):
        agent = agents_data.get(agent_key, {"name": agent_key, "status": "idle"})
        ast   = agent.get("status", "idle")
        icon  = status_icon(ast)
        with agent_cols[i]:
            st.markdown(
                f'<div class="agent-box">'
                f'<div style="font-size:0.75rem;font-weight:600;margin-bottom:4px">'
                f'{agent_label}</div>'
                f'<div class="badge-{ast}">{icon}</div>'
                f'<div style="font-size:0.7rem;color:#8b949e;margin-top:4px">'
                f'{ast.upper()}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.progress(
        completed_cnt / total_agents,
        text=f"{completed_cnt} / {total_agents} agents completed",
    )

    st.divider()

    # ── Incident Info + AI Analysis ────────────────────────────────────────
    left_col, right_col = st.columns(2)

    with left_col:
        incident = s.get("incident", {})
        if incident:
            st.subheader("Incident Details")
            st.markdown(f"**Service:** `{incident.get('service_name', '-')}`")
            st.markdown(f"**Alert:** `{incident.get('alert_name', '-')}`")
            st.markdown(f"**Severity:** {sev_icon(incident.get('severity', ''))} "
                        f"{incident.get('severity', '').upper()}")
            st.markdown(f"**Risk Score:** `{incident.get('risk_score', 0):.2f}`")
            grafana = incident.get("grafana_url", "")
            if grafana:
                st.markdown(f"[📊 Open Grafana Dashboard]({grafana})")

        ticket_num = s.get("ticket_num", 0)
        ticket_id  = s.get("ticket_id", "")
        if ticket_num:
            st.markdown(f"**xyOps Ticket:** [#{ticket_num}]({XYOPS_EXT}) `{ticket_id}`")

        approval_id  = s.get("approval_id", "")
        approval_num = s.get("approval_ticket_num", 0)
        if approval_id:
            st.markdown(f"**Approval ID:** `{approval_id}`")
        if approval_num:
            st.markdown(f"**Approval Ticket:** [#{approval_num}]({XYOPS_EXT})")

        st.markdown(f"**Approval Required:** {'✅ Yes' if s.get('approval_required') else '❌ No'}")

    with right_col:
        analysis = s.get("analysis", {})
        if analysis:
            st.subheader("🧠 AI Analysis")
            root_cause = analysis.get("root_cause", "Analyzing...")
            action     = analysis.get("recommended_action", "Pending...")

            if root_cause and root_cause not in ("Analyzing...", ""):
                st.info(f"**Root Cause:** {root_cause}")
                st.success(f"**Recommended Action:** {action}")
                with st.expander("Analysis details"):
                    st.json({
                        "provider":              analysis.get("provider"),
                        "model":                 analysis.get("model"),
                        "confidence":            analysis.get("confidence"),
                        "scenario_id":           analysis.get("scenario_id"),
                        "scenario_confidence":   analysis.get("scenario_confidence"),
                        "local_validation":      analysis.get("local_validation_status"),
                        "local_validation_conf": analysis.get("local_validation_confidence"),
                        "local_model":           analysis.get("local_model"),
                        "knowledge_similarity":  analysis.get("knowledge_top_similarity"),
                    })
            else:
                st.caption("⏳ AI analysis in progress...")

    st.divider()

    # ── Trust & Autonomy ───────────────────────────────────────────────────
    trust = s.get("trust_metrics")
    if trust:
        st.subheader("🏆 Trust & Autonomy Progress")
        t = st.columns(4)
        t[0].metric("Autonomy Decision",  s.get("autonomy_decision", "-"))
        t[1].metric("Approvals Recorded", trust.get("approvals_recorded", 0))
        t[2].metric("Success Rate",       f"{trust.get('success_rate', 0) * 100:.1f}%")
        t[3].metric("Progress",           s.get("trust_progress", "-"))

        next_tier = trust.get("next_tier", {})
        if next_tier:
            needed   = max(next_tier.get("approvals_needed", 1), 1)
            recorded = trust.get("approvals_recorded", 0)
            pct      = min(recorded / needed, 1.0)
            st.progress(pct, text=trust.get("path_to_next_tier", ""))

    with st.expander("🔍 Full raw session JSON"):
        st.json(s)


_live_pipeline_view()

page_footer("live", fragment_mode=True)
