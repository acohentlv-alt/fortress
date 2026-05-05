#!/bin/sh
# QA helper — assert a batch is safe for destructive testing.
# Required by every QA brief that performs delete/cancel/destroy actions.
# Per CLAUDE.md "QA Testing Rules → Destructive selectors hard rule":
#   - workspace_id MUST = 174 (testing workspace)
#   - batch_name MUST start with "QA_" (test prefix)
# Writes the preflight result to /tmp/qa_preflight_<batch_id>_<timestamp>.txt
# (Layer B of the 3-layer enforcement — file persists for post-hoc audit).
# Usage: bash scripts/qa_destructive_preflight.sh <batch_id>
# Exit 0 = safe; exit 1 = abort; exit 2 = bad usage

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
if [ -z "$1" ]; then
    echo "Usage: $0 <batch_id>"
    exit 2
fi

batch_id="$1"
ts=$(date +%Y%m%d_%H%M%S)
audit_file="/tmp/qa_preflight_${batch_id}_${ts}.txt"

echo "=== Destructive preflight for batch_id=$batch_id ==="
echo "Audit file: $audit_file"
echo

result=$(psql "$DATABASE_URL" -t -A -F'|' -c "SELECT workspace_id, batch_name, status FROM batch_data WHERE batch_id = '$batch_id';")
{
    echo "batch_id=$batch_id"
    echo "timestamp=$ts"
    echo "result=$result"
} > "$audit_file"

echo "Result (workspace_id|batch_name|status):"
echo "  $result"
echo

if [ -z "$result" ]; then
    echo "FAIL: batch_id not found in batch_data"
    exit 1
fi

ws=$(echo "$result" | cut -d'|' -f1)
name=$(echo "$result" | cut -d'|' -f2)

if [ "$ws" != "174" ]; then
    echo "FAIL: workspace_id=$ws (MUST be 174 for destructive QA)"
    echo "      Cindy's workspace is 1 — anything else risks her data."
    exit 1
fi

case "$name" in
    QA_*)
        ;;
    *)
        echo "FAIL: batch_name='$name' does NOT start with QA_"
        echo "      Test batches must use the QA_ prefix (e.g. QA_DEL_admin_test)."
        exit 1
        ;;
esac

echo "PASS: ws174 + QA_ prefix confirmed — safe for destructive test"
exit 0
