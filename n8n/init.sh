#!/bin/sh
# ──────────────────────────────────────────────────────────────────────────────
# n8n startup script  —  AIOps Pattern Discovery Pipeline
#
# Sequence:
#   1. Import all workflow JSON files from /workflows/
#      (n8n import is idempotent — re-importing the same workflow ID is safe)
#   2. Activate every workflow while n8n is still stopped
#      (n8n publish:workflow sets active=true in the DB before startup,
#       so n8n picks them up as active on boot — no UI step required)
#   3. Start n8n normally
#
# Workflows are mounted read-only from ./n8n/workflows/ in docker-compose.
# ──────────────────────────────────────────────────────────────────────────────
set -e

echo "[n8n-init] Importing discovery workflows..."

IMPORTED=0
FAILED=0

for workflow_file in /workflows/*.json; do
    if [ -f "$workflow_file" ]; then
        name=$(basename "$workflow_file")
        if n8n import:workflow --input="$workflow_file" 2>/dev/null; then
            echo "[n8n-init]   ✓ Imported: $name"
            IMPORTED=$((IMPORTED + 1))
        else
            echo "[n8n-init]   ⚠ Skipped (already exists or error): $name"
            FAILED=$((FAILED + 1))
        fi
    fi
done

echo "[n8n-init] Import complete: $IMPORTED imported, $FAILED skipped"

# ── Activate all workflows while n8n is still stopped.
# publish:workflow + update:workflow both write active=true to the SQLite DB.
# Because n8n has not started yet, the change persists and n8n boots with
# all schedulers already live.
echo "[n8n-init] Activating workflows..."
for wf_id in github-otel-issues-v1 stackoverflow-otel-v1 reddit-devops-otel-v1 hackernews-otel-v1 rss-blogs-otel-v1; do
    if n8n update:workflow --id="$wf_id" --active=true 2>/dev/null; then
        echo "[n8n-init]   ✓ Activated: $wf_id"
    else
        echo "[n8n-init]   ⚠ Could not activate: $wf_id"
    fi
done

echo "[n8n-init] Starting n8n..."
exec n8n start
