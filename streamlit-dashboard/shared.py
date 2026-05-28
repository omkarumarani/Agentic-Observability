"""
streamlit-dashboard/shared.py
──────────────────────────────────────────────────────────────────────────────
Shared configuration, HTTP helpers, formatting utilities, and page-setup
functions used by every page in the AIOps Command Center multi-page app.
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
import streamlit as st

# ─── Configuration ─────────────────────────────────────────────────────────────

COMPUTE_AGENT_URL    = os.getenv("COMPUTE_AGENT_URL",    "http://compute-agent:9000")
STORAGE_AGENT_URL    = os.getenv("STORAGE_AGENT_URL",    "http://storage-agent:9001")
OBS_INTELLIGENCE_URL = os.getenv("OBS_INTELLIGENCE_URL", "http://obs-intelligence:9100")
PATTERN_LIBRARY_URL  = os.getenv("PATTERN_LIBRARY_URL",  "http://pattern-library:9300")
GITEA_URL            = os.getenv("GITEA_URL",            "http://gitea:3000")
GITEA_USER           = os.getenv("GITEA_ADMIN_USER",     "aiops")
GITEA_PASS           = os.getenv("GITEA_ADMIN_PASS",     "")
GITEA_ORG            = os.getenv("GITEA_ORG",            "aiops-org")
GITEA_REPO           = os.getenv("GITEA_REPO",           "ansible-playbooks")
XYOPS_URL            = os.getenv("XYOPS_URL",            "http://xyops:5522")
POLL_INTERVAL        = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))

# External (browser-accessible) URLs — Docker-internal → localhost mapping
XYOPS_EXT        = "http://localhost:5522"
GITEA_EXT        = "http://localhost:3002"
GRAFANA_EXT      = "http://localhost:3001"
PROMETHEUS_EXT   = "http://localhost:9090"
ALERTMANAGER_EXT = "http://localhost:9093"
COMPUTE_EXT      = "http://localhost:9000"
STORAGE_EXT      = "http://localhost:9001"
OBS_INTEL_EXT    = "http://localhost:9100"
ANSIBLE_EXT      = "http://localhost:8080"
PATTERN_LIB_EXT  = "http://localhost:9300"

# ─── HTTP helpers ──────────────────────────────────────────────────────────────

def api_get(url: str, timeout: float = 5.0) -> Optional[Dict]:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def api_post(url: str, payload: Dict, timeout: float = 8.0) -> Optional[Dict]:
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def gitea_get(path: str) -> Optional[Any]:
    try:
        r = requests.get(
            f"{GITEA_URL}{path}",
            auth=(GITEA_USER, GITEA_PASS),
            timeout=5.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


# ─── Formatting utils ──────────────────────────────────────────────────────────

def since_str(val) -> str:
    """Return human-readable age string from unix timestamp or ISO string."""
    try:
        if isinstance(val, (int, float)) and val > 0:
            dt = datetime.fromtimestamp(val, tz=timezone.utc)
        else:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        secs = int((datetime.now(timezone.utc) - dt).total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        if secs < 3600:
            return f"{secs // 60}m {secs % 60}s ago"
        return f"{secs // 3600}h {(secs % 3600) // 60}m ago"
    except Exception:
        return str(val)


def sev_icon(sev: str) -> str:
    return {
        "critical": "🔴",
        "high":     "🟠",
        "warning":  "🟡",
        "medium":   "🟡",
        "info":     "🔵",
        "low":      "🔵",
    }.get(str(sev).lower(), "⚪")


def status_icon(st_: str) -> str:
    return {
        "completed": "✅",
        "running":   "⏳",
        "idle":      "⬜",
        "failed":    "❌",
        "skipped":   "⏭️",
    }.get(str(st_).lower(), "⬜")


# ─── CSS Themes ────────────────────────────────────────────────────────────────

_COMMON_CSS = """
div[data-testid="stHorizontalBlock"] { gap: 0.4rem; }
"""

_DARK_CUSTOM = """
.agent-box {
  background: #1a1f3a; border-radius: 8px; padding: 0.6rem 0.4rem;
  text-align: center; border: 1px solid rgba(255,255,255,0.08);
}
.badge-completed { color: #4caf50; font-size: 1.6rem; }
.badge-running   { color: #ff9800; font-size: 1.6rem; }
.badge-idle      { color: #555;    font-size: 1.6rem; }
.badge-failed    { color: #f44336; font-size: 1.6rem; }
.badge-skipped   { color: #9e9e9e; font-size: 1.6rem; }
.sev-critical { color: #f44336; font-weight: 700; }
.sev-warning  { color: #ff9800; font-weight: 700; }
.sev-info     { color: #2196f3; font-weight: 700; }
.pending-badge {
  background: #f44336; color: white; border-radius: 12px;
  padding: 2px 10px; font-weight: bold; font-size: 0.85rem;
}
"""

_LIGHT_CUSTOM = """
.agent-box {
  background: #f0f4f8; border-radius: 8px; padding: 0.6rem 0.4rem;
  text-align: center; border: 1px solid rgba(0,0,0,0.10); color: #1a202c;
}
.badge-completed { color: #276749; font-size: 1.6rem; }
.badge-running   { color: #c05621; font-size: 1.6rem; }
.badge-idle      { color: #718096; font-size: 1.6rem; }
.badge-failed    { color: #c53030; font-size: 1.6rem; }
.badge-skipped   { color: #4a5568; font-size: 1.6rem; }
.sev-critical { color: #c53030; font-weight: 700; }
.sev-warning  { color: #c05621; font-weight: 700; }
.sev-info     { color: #2b6cb0; font-weight: 700; }
.pending-badge {
  background: #c53030; color: white; border-radius: 12px;
  padding: 2px 10px; font-weight: bold; font-size: 0.85rem;
}
[data-testid="stAppViewContainer"] { background-color: #f8fafc !important; }
[data-testid="stSidebar"] { background-color: #f1f5f9 !important; }
.main .block-container { background-color: #f8fafc !important; }
[data-testid="stMetricLabel"] { color: #374151 !important; }
[data-testid="stMetricValue"] { color: #111827 !important; }
"""


def inject_theme_css() -> None:
    """Inject dark or light CSS based on st.session_state.theme (default: dark)."""
    dark = st.session_state.get("theme", "dark") == "dark"
    custom = _DARK_CUSTOM if dark else _LIGHT_CUSTOM
    st.markdown(f"<style>{_COMMON_CSS}{custom}</style>", unsafe_allow_html=True)


def sidebar_controls() -> None:
    """Render theme toggle and quick-links in the sidebar (called by each page)."""
    with st.sidebar:
        st.markdown("### 🤖 AIOps Controls")

        # ── Dark / Light theme toggle ──────────────────────────────────────
        dark = st.toggle(
            "🌙 Dark mode",
            value=st.session_state.get("theme", "dark") == "dark",
            key="_theme_toggle",
            help="Switch between dark (default) and light theme",
        )
        st.session_state["theme"] = "dark" if dark else "light"

        st.divider()

        # ── Quick-links to platform UIs ────────────────────────────────────
        st.markdown("**Quick links**")
        st.markdown(f"[📈 Prometheus]({PROMETHEUS_EXT})")
        st.markdown(f"[📊 Grafana]({GRAFANA_EXT})")
        st.markdown(f"[🔀 Gitea]({GITEA_EXT})")
        st.markdown(f"[🎫 xyOps]({XYOPS_EXT})")
        st.markdown(f"[🚨 Alertmanager]({ALERTMANAGER_EXT})")
        st.markdown(f"[🤖 Compute API]({COMPUTE_EXT}/docs)")
        st.markdown(f"[🗄️ Storage API]({STORAGE_EXT}/docs)")
        st.markdown(f"[🧠 Obs-Intel API]({OBS_INTEL_EXT}/docs)")
        st.markdown(f"[🔬 Pattern Library]({PATTERN_LIB_EXT}/docs)")


def page_header(title: str) -> None:
    """Inject theme CSS, render sidebar controls, and show page title + refresh time."""
    inject_theme_css()
    sidebar_controls()
    c1, c2 = st.columns([5, 1])
    c1.title(title)
    c2.markdown(
        f"<br><small style='color:#8b949e'>Refreshed {datetime.now().strftime('%H:%M:%S')}</small>",
        unsafe_allow_html=True,
    )


def page_footer(key_suffix: str = "", fragment_mode: bool = False) -> None:
    """Render auto-refresh footer.

    Args:
        key_suffix:    Unique suffix to avoid Streamlit widget key collisions.
        fragment_mode: When True, skip the 5-second full-page rerun and just
                       show a manual refresh button (fragment handles auto-refresh).
    """
    st.divider()
    fc1, fc2 = st.columns([4, 1])
    with fc1:
        if fragment_mode:
            st.caption("⚡ Fragment auto-refresh active (1 s) — full page reload not needed.")
        else:
            auto = st.toggle(
                "⏱ Auto-refresh every 5 seconds",
                value=True,
                key=f"auto_refresh_{key_suffix}",
            )
    with fc2:
        if st.button("🔄 Refresh now", use_container_width=True, key=f"refresh_btn_{key_suffix}"):
            st.rerun()

    if not fragment_mode and auto:
        time.sleep(POLL_INTERVAL)
        st.rerun()
