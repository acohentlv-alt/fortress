#!/bin/sh
# QA helper — pre-push readiness check for the committer.
# Runs three checks before a push:
#   1. Origin drift — does origin/main have commits we don't have?
#   2. Active batches — would a Render redeploy kill an in-flight batch?
#   3. Worktree state — what's outstanding?
# Exits 0 on all clear, 1 if any check fails (drift or active batch).
# Usage: bash scripts/qa_pre_push_check.sh

set -e
cd "$(dirname "$0")/.."

if [ -z "$DATABASE_URL" ] && [ -f .env ]; then
    DATABASE_URL=$(awk -F= '/^DATABASE_URL=/{sub(/^DATABASE_URL=/,""); gsub(/^["'\'']|["'\'']$/,""); print; exit}' .env)
    export DATABASE_URL
fi
if [ -z "$DATABASE_URL" ]; then
    echo "FAIL: DATABASE_URL not set (expected in .env)"
    exit 2
fi

drift_fail=""
active_fail=""

echo "=== Pre-push readiness check ==="
echo

echo "--- 1. Origin drift ---"
git fetch origin --quiet
ahead=$(git log origin/main..HEAD --oneline 2>/dev/null | wc -l | tr -d ' ')
behind=$(git log HEAD..origin/main --oneline 2>/dev/null | wc -l | tr -d ' ')
echo "Ahead of origin/main: $ahead commit(s)"
echo "Behind origin/main:   $behind commit(s)"
if [ "$behind" -gt 0 ]; then
    echo "WARN: origin/main has commits not in local — rebase before push"
    git log HEAD..origin/main --oneline
    drift_fail=1
fi
echo

echo "--- 2. Active batches ---"
psql "$DATABASE_URL" -c "SELECT batch_id, LEFT(batch_name, 40) AS batch_name, workspace_id, status, updated_at FROM batch_data WHERE status IN ('queued','in_progress','triage') ORDER BY updated_at DESC;"
active_count=$(psql "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM batch_data WHERE status IN ('queued','in_progress','triage');" | tr -d ' ')
echo "Active batch count: $active_count"
if [ "$active_count" -gt 0 ]; then
    echo "WARN: $active_count active batch(es) — Render redeploy after push will kill them"
    active_fail=1
fi
echo

echo "--- 3. Worktrees ---"
git worktree list
worktree_count=$(git worktree list | wc -l | tr -d ' ')
echo "Total worktrees: $worktree_count"
echo

if [ -n "$drift_fail" ] || [ -n "$active_fail" ]; then
    echo "=== FAIL — see warnings above ==="
    [ -n "$drift_fail" ]  && echo "  - rebase needed (origin/main moved)"
    [ -n "$active_fail" ] && echo "  - active batches present (would die on redeploy)"
    exit 1
fi
echo "=== PASS — safe to push ==="
exit 0
