"""
aiops-bridge/app/git_client.py
────────────────────────────────────────────────────────────────
Gitea (local self-hosted Git) integration for the AIOps Bridge.

Flow
────
1. ensure_gitea_setup()   — called at bridge startup; idempotent
     a. Wait for Gitea to be ready
     b. POST /_/install to create admin user on first boot
     c. Create organisation "aiops-org" (if not exists)
     d. Create repository  "ansible-playbooks" with a README on main
     e. Issue an API token and cache it in _gitea_token

2. commit_playbook()       — creates a branch + commits playbook YAML
3. create_pull_request()   — opens a PR: branch → main
4. merge_pull_request()    — merges on human approval
5. close_pull_request()    — closes without merging on decline

Gitea is reachable at http://localhost:3002 (host) / http://gitea:3000 (container).
Login:  aiops / Aiops1234!
Repo:   http://localhost:3002/aiops-org/ansible-playbooks/pulls
────────────────────────────────────────────────────────────────
"""

import base64
import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("aiops-bridge.git_client")

# ── Config from environment ────────────────────────────────────────────────────
GITEA_URL:    str  = os.getenv("GITEA_URL",        "http://gitea:3000")
GITEA_USER:   str  = os.getenv("GITEA_ADMIN_USER", "aiops")
GITEA_PASS:   str  = os.getenv("GITEA_ADMIN_PASS", "")
GITEA_ORG:    str  = os.getenv("GITEA_ORG",        "aiops-org")
GITEA_REPO:   str  = os.getenv("GITEA_REPO",       "ansible-playbooks")
GITEA_ENABLED: bool = os.getenv("GITEA_ENABLED", "true").lower() == "true"

# Internal state — token populated once at startup
_gitea_token: str = ""


def _auth_header(token: str = "") -> dict[str, str]:
    """Return Authorization header — token auth preferred, basic auth fallback."""
    if token:
        return {"Authorization": f"token {token}"}
    creds = base64.b64encode(f"{GITEA_USER}:{GITEA_PASS}".encode()).decode()
    return {"Authorization": f"Basic {creds}"}


def get_token() -> str:
    """Return cached Gitea API token (empty string if not set up)."""
    return _gitea_token


# ═══════════════════════════════════════════════════════════════════════════════
# Startup: ensure Gitea is initialised
# ═══════════════════════════════════════════════════════════════════════════════

async def ensure_gitea_setup(http: httpx.AsyncClient) -> str:
    """
    Idempotent Gitea bootstrap called at bridge startup (as a background task).

    Steps:
      1. Wait for Gitea API (admin user created by gitea/init.sh entrypoint)
      2. Create org GITEA_ORG (skip if exists)
      3. Create repo GITEA_REPO with a README on main (skip if exists)
      4. Create API token "aiops-bridge-token" (or recreate to get value)
      5. Return the token string (also stored in _gitea_token module-level)

    Returns "" if Gitea is disabled or unreachable.
    """
    import asyncio
    global _gitea_token
    if not GITEA_ENABLED:
        logger.info("Gitea integration disabled (GITEA_ENABLED=false)")
        return ""

    # 1. Liveness check — wait for the API to respond with 200
    # The init.sh entrypoint handles admin user creation before the bridge first
    # tries to use the API.
    for attempt in range(24):  # up to ~2 min
        try:
            resp = await http.get(f"{GITEA_URL}/api/v1/version", timeout=5.0)
            if resp.status_code == 200:
                logger.info("Gitea API ready  version=%s", resp.json().get("version", "?"))
                break
        except Exception:
            pass
        await asyncio.sleep(5)
    else:
        logger.warning("Gitea not reachable after 2 min — git integration disabled")
        return ""

    # 2. Create org
    await _ensure_org(http)

    # 4. Create repo with README
    await _ensure_repo(http)

    # 5. Token
    token = await _ensure_token(http)
    _gitea_token = token
    logger.info(
        "Gitea ready  org=%s  repo=%s  token=%s",
        GITEA_ORG, GITEA_REPO, "***" if token else "(none)",
    )
    return token


async def _admin_exists(http: httpx.AsyncClient) -> bool:
    """Check if the admin user account already exists."""
    try:
        resp = await http.get(
            f"{GITEA_URL}/api/v1/users/{GITEA_USER}",
            headers=_auth_header(),
            timeout=5.0,
        )
        return resp.status_code == 200
    except Exception:
        return False


async def _do_install(http: httpx.AsyncClient) -> None:
    """POST to Gitea's install form to create admin user and lock the installer."""
    form = {
        "db_type":               "SQLite3",
        "db_path":               "/data/gitea/gitea.db",
        "app_name":              "AIOps Git",
        "repo_root_path":        "/data/gitea/repositories",
        "lfs_root_path":         "/data/gitea/lfs",
        "run_user":              "git",
        "domain":                "gitea",
        "ssh_port":              "22",
        "http_port":             "3000",
        "app_url":               f"{GITEA_URL}/",
        "log_root_path":         "/data/gitea/log",
        "smtp_host":             "",
        "smtp_from":             "",
        "smtp_user":             "",
        "smtp_passwd":           "",
        "enable_federated_avatar": "off",
        "enable_open_id_sign_in": "off",
        "enable_open_id_sign_up": "off",
        "admin_name":            GITEA_USER,
        "admin_passwd":          GITEA_PASS,
        "admin_confirm_passwd":  GITEA_PASS,
        "admin_email":           "aiops@local.test",
    }
    try:
        resp = await http.post(
            f"{GITEA_URL}/",
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
            timeout=30.0,
        )
        if resp.status_code in (200, 302):
            logger.info("Gitea install completed (HTTP %d)", resp.status_code)
        else:
            logger.warning("Gitea install returned HTTP %d: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        logger.warning("Gitea install failed: %s", exc)


async def _ensure_org(http: httpx.AsyncClient) -> None:
    """Create the AIOps organisation if it doesn't exist."""
    resp = await http.get(
        f"{GITEA_URL}/api/v1/orgs/{GITEA_ORG}",
        headers=_auth_header(),
        timeout=10.0,
    )
    if resp.status_code == 200:
        return
    resp = await http.post(
        f"{GITEA_URL}/api/v1/orgs",
        json={
            "username":    GITEA_ORG,
            "visibility":  "public",
            "description": "AIOps Bridge — automated remediation playbooks",
        },
        headers={**_auth_header(), "Content-Type": "application/json"},
        timeout=10.0,
    )
    if resp.status_code == 201:
        logger.info("Created Gitea org: %s", GITEA_ORG)
    elif resp.status_code == 422:
        logger.info("Gitea org %s already exists", GITEA_ORG)
    else:
        logger.warning("Could not create Gitea org: HTTP %d %s", resp.status_code, resp.text[:200])


async def _ensure_repo(http: httpx.AsyncClient) -> None:
    """Create the ansible-playbooks repo if it doesn't exist."""
    resp = await http.get(
        f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}",
        headers=_auth_header(),
        timeout=10.0,
    )
    if resp.status_code == 200:
        return
    resp = await http.post(
        f"{GITEA_URL}/api/v1/orgs/{GITEA_ORG}/repos",
        json={
            "name":          GITEA_REPO,
            "description":   "Ansible playbooks auto-generated by AIOps Bridge",
            "private":       False,
            "auto_init":     True,
            "default_branch": "main",
            "readme":        "Default",
        },
        headers={**_auth_header(), "Content-Type": "application/json"},
        timeout=10.0,
    )
    if resp.status_code == 201:
        logger.info("Created Gitea repo: %s/%s", GITEA_ORG, GITEA_REPO)
    elif resp.status_code == 409:
        logger.info("Gitea repo %s/%s already exists", GITEA_ORG, GITEA_REPO)
    else:
        logger.warning("Could not create Gitea repo: HTTP %d %s", resp.status_code, resp.text[:200])


async def _ensure_token(http: httpx.AsyncClient) -> str:
    """Create or fetch the aiops-bridge-token API token."""
    token_name = "aiops-bridge-token"

    # Check if token already exists
    resp = await http.get(
        f"{GITEA_URL}/api/v1/users/{GITEA_USER}/tokens",
        headers=_auth_header(),
        timeout=10.0,
    )
    if resp.status_code == 200:
        for tok in resp.json():
            if tok.get("name") == token_name:
                # Token exists but value is masked — recreate to get the value
                await http.delete(
                    f"{GITEA_URL}/api/v1/users/{GITEA_USER}/tokens/{tok['id']}",
                    headers=_auth_header(),
                    timeout=10.0,
                )
                break

    resp = await http.post(
        f"{GITEA_URL}/api/v1/users/{GITEA_USER}/tokens",
        json={
            "name": token_name,
            # Gitea 1.21+ requires explicit scopes; write:repository covers
            # branch/file/PR operations; write:issue covers PR comments.
            "scopes": ["write:repository", "write:issue", "read:user", "read:organization"],
        },
        headers={**_auth_header(), "Content-Type": "application/json"},
        timeout=10.0,
    )
    if resp.status_code == 201:
        token = resp.json().get("sha1", "")
        logger.info("Created Gitea API token (name=%s)", token_name)
        return token

    logger.warning("Could not create Gitea token: HTTP %d %s", resp.status_code, resp.text[:200])
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
# Playbook operations
# ═══════════════════════════════════════════════════════════════════════════════

async def commit_playbook(
    approval_id: str,
    playbook_yaml: str,
    service_name: str,
    alert_name: str,
    http: httpx.AsyncClient,
) -> dict[str, Any]:
    """
    Create a new branch and commit the playbook YAML.

    Returns:
      branch:   str — branch name (e.g. remediation/frontend-api-HighErrorRate-20260318-102245)
      filepath: str — path inside repo
      sha:      str — file blob SHA
      url:      str — Gitea HTML URL to the committed file
    """
    token = _gitea_token
    if not token:
        return {"error": "Gitea not configured — commit skipped"}

    safe_alert   = alert_name.lower().replace(" ", "-").replace("/", "-")[:40]
    safe_service = service_name.lower().replace(" ", "-")[:30]
    date_str     = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    branch       = f"remediation/{safe_service}-{safe_alert}-{date_str}"
    filepath     = f"playbooks/{safe_service}/{safe_alert}.yml"

    # 1. Create branch from main
    resp = await http.post(
        f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/branches",
        json={"new_branch_name": branch, "old_branch_name": "main"},
        headers={**_auth_header(token), "Content-Type": "application/json"},
        timeout=15.0,
    )
    if resp.status_code not in (200, 201):
        logger.warning("Could not create branch %s: HTTP %d", branch, resp.status_code)
        return {"error": f"branch creation failed: HTTP {resp.status_code}"}

    # 2. Commit file to that branch
    content_b64 = base64.b64encode(playbook_yaml.encode()).decode()
    commit_msg  = (
        f"feat(remediation): add playbook for {service_name}/{alert_name}\n\n"
        f"Approval ID: {approval_id}\n"
        f"Auto-generated by AIOps Bridge"
    )
    resp = await http.post(
        f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/contents/{filepath}",
        json={"message": commit_msg, "content": content_b64, "branch": branch},
        headers={**_auth_header(token), "Content-Type": "application/json"},
        timeout=15.0,
    )
    if resp.status_code == 201:
        data = resp.json()
        sha  = data.get("content", {}).get("sha", "")
        url  = data.get("content", {}).get("html_url", "")
        logger.info("Committed playbook  branch=%s  path=%s", branch, filepath)
        return {"branch": branch, "filepath": filepath, "sha": sha, "url": url}
    elif resp.status_code == 422:
        # File already exists on this branch (inherited from main) — fetch SHA and update
        logger.info("File already exists — fetching SHA to update  path=%s  branch=%s", filepath, branch)
        get_resp = await http.get(
            f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/contents/{filepath}",
            params={"ref": branch},
            headers=_auth_header(token),
            timeout=15.0,
        )
        if get_resp.status_code != 200:
            logger.warning("Could not fetch existing file SHA: HTTP %d", get_resp.status_code)
            return {"error": f"commit failed: HTTP {resp.status_code}"}
        existing_sha = get_resp.json().get("sha", "")
        put_resp = await http.put(
            f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/contents/{filepath}",
            json={"message": commit_msg, "content": content_b64, "branch": branch, "sha": existing_sha},
            headers={**_auth_header(token), "Content-Type": "application/json"},
            timeout=15.0,
        )
        if put_resp.status_code == 200:
            data = put_resp.json()
            sha  = data.get("content", {}).get("sha", "")
            url  = data.get("content", {}).get("html_url", "")
            logger.info("Updated playbook  branch=%s  path=%s", branch, filepath)
            return {"branch": branch, "filepath": filepath, "sha": sha, "url": url}
        else:
            logger.warning("Could not update playbook: HTTP %d %s", put_resp.status_code, put_resp.text[:200])
            return {"error": f"commit update failed: HTTP {put_resp.status_code}"}
    else:
        logger.warning("Could not commit playbook: HTTP %d %s", resp.status_code, resp.text[:200])
        return {"error": f"commit failed: HTTP {resp.status_code}"}


async def create_pull_request(
    branch: str,
    service_name: str,
    alert_name: str,
    rca_summary: str,
    approval_id: str,
    http: httpx.AsyncClient,
) -> dict[str, Any]:
    """
    Open a PR from branch → main with RCA context in the body.

    Returns:
      pr_number: int
      pr_url:    str — Gitea HTML URL for the PR
      pr_id:     int
    """
    token = _gitea_token
    if not token:
        return {"error": "Gitea not configured — PR creation skipped"}

    pr_body = (
        f"## 🔧 Automated Remediation Playbook\n\n"
        f"**Service:** `{service_name}`  \n"
        f"**Alert:** `{alert_name}`  \n"
        f"**Approval ID:** `{approval_id}`\n\n"
        f"### AI Root Cause Summary\n\n"
        f"{rca_summary or '_No AI analysis available_'}\n\n"
        f"---\n\n"
        f"*Auto-generated by AIOps Bridge — review the playbook YAML and "
        f"approve/decline via the linked xyOps change ticket.*"
    )
    resp = await http.post(
        f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/pulls",
        json={
            "title": f"[AIOps] Remediate {alert_name} on {service_name}",
            "body":  pr_body,
            "head":  branch,
            "base":  "main",
        },
        headers={**_auth_header(token), "Content-Type": "application/json"},
        timeout=15.0,
    )
    if resp.status_code == 201:
        data = resp.json()
        pr_num = data.get("number", 0)
        pr_url = data.get("html_url", "")
        logger.info("Created Gitea PR #%d  branch=%s", pr_num, branch)
        return {"pr_number": pr_num, "pr_url": pr_url, "pr_id": data.get("id", 0)}
    else:
        logger.warning("Could not create PR: HTTP %d %s", resp.status_code, resp.text[:200])
        return {"error": f"PR creation failed: HTTP {resp.status_code}"}


async def merge_pull_request(pr_number: int, http: httpx.AsyncClient) -> bool:
    """Merge the PR (called on human approval)."""
    token = _gitea_token
    if not token or not pr_number:
        return False
    resp = await http.post(
        f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/pulls/{pr_number}/merge",
        json={
            "Do":                   "merge",
            "merge_message_field":  "Approved and merged via AIOps Bridge",
        },
        headers={**_auth_header(token), "Content-Type": "application/json"},
        timeout=15.0,
    )
    ok = resp.status_code in (200, 204)
    logger.info("Merge PR #%d  ok=%s  status=%d", pr_number, ok, resp.status_code)
    return ok


async def close_pull_request(pr_number: int, http: httpx.AsyncClient) -> bool:
    """Close the PR without merging (called on decline)."""
    token = _gitea_token
    if not token or not pr_number:
        return False
    resp = await http.patch(
        f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/pulls/{pr_number}",
        json={"state": "closed"},
        headers={**_auth_header(token), "Content-Type": "application/json"},
        timeout=15.0,
    )
    ok = resp.status_code in (200, 201)
    logger.info("Close PR #%d  ok=%s  status=%d", pr_number, ok, resp.status_code)
    return ok


async def check_pr_merged(pr_number: int, http: httpx.AsyncClient) -> bool:
    """
    Return True if the Gitea PR has already been merged by the user.

    Used during approval decision: if the user merged the PR manually (as
    instructed), we skip calling merge_pull_request() and go straight to
    Ansible execution.
    """
    token = _gitea_token
    if not token or not pr_number:
        return False
    try:
        resp = await http.get(
            f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/pulls/{pr_number}",
            headers=_auth_header(token),
            timeout=10.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            merged = data.get("merged", False)
            state  = data.get("state", "open")
            logger.info("PR #%d  merged=%s  state=%s", pr_number, merged, state)
            return bool(merged)
    except Exception as exc:
        logger.warning("check_pr_merged PR #%d failed: %s", pr_number, exc)
    return False


async def auto_merge_pull_request(
    pr_number: int,
    approval_id: str,
    http: httpx.AsyncClient,
) -> bool:
    """
    Autonomously merge a Gitea PR — called by the autonomy engine when
    a (service, action) pair has earned enough trust to bypass the manual
    merge requirement.

    Adds a comment to the PR explaining it was merged autonomously before
    performing the actual merge, which ensures the Git history and Gitea
    PR timeline both show WHY it was auto-merged.

    Returns True on success.
    """
    token = _gitea_token
    if not token or not pr_number:
        logger.warning("auto_merge_pull_request: token or PR number missing")
        return False

    # 1. Leave audit comment on the PR so reviewers know what happened
    comment_body = (
        "🤖 **Autonomous Merge — AIOps Bridge**\n\n"
        f"This PR is being merged automatically by the AIOps Autonomy Engine.\n\n"
        f"**Approval ID:** `{approval_id}`  \n"
        f"**Reason:** Trust threshold met for this service/action combination — "
        "sufficient approved executions with a high enough success rate have been "
        "recorded in the approval history to permit autonomous execution.\n\n"
        "The associated Ansible playbook will execute immediately after merge. "
        "Check the linked xyOps incident ticket for execution results."
    )
    try:
        await http.post(
            f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/issues/{pr_number}/comments",
            json={"body": comment_body},
            headers={**_auth_header(token), "Content-Type": "application/json"},
            timeout=10.0,
        )
    except Exception as exc:
        logger.warning("auto_merge: could not post PR comment: %s", exc)

    # 2. Merge the PR
    resp = await http.post(
        f"{GITEA_URL}/api/v1/repos/{GITEA_ORG}/{GITEA_REPO}/pulls/{pr_number}/merge",
        json={
            "Do":                  "merge",
            "merge_message_field": (
                f"chore(autonomy): auto-merge remediation PR\n\n"
                f"Approval ID: {approval_id}\n"
                f"Merged autonomously by AIOps Bridge — trust threshold met."
            ),
        },
        headers={**_auth_header(token), "Content-Type": "application/json"},
        timeout=15.0,
    )
    ok = resp.status_code in (200, 204)
    logger.info(
        "auto_merge PR #%d  ok=%s  status=%d  approval_id=%s",
        pr_number, ok, resp.status_code, approval_id,
    )
    return ok
