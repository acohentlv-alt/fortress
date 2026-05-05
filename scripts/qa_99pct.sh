#!/bin/sh
# QA helper — print the ws174 wide-batches confirm rate (the 99% goal stat).
# Per CLAUDE.md QA Testing Rules: every QA brief must include this query.
# Strict-mode batches are excluded by design (they cap below 99% mathematically).
# Usage: bash scripts/qa_99pct.sh

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

echo "=== 99% goal tracking — ws174 wide-mode last 7 days ==="
psql "$DATABASE_URL" <<'SQL'
WITH recent AS (
    SELECT DISTINCT co.siren, co.linked_siren, co.link_confidence
    FROM batch_data bd
    JOIN batch_tags bt ON bt.batch_id = bd.batch_id
    JOIN companies co ON co.siren = bt.siren
    WHERE bd.workspace_id = 174 AND bd.status = 'completed'
      AND bd.strict_naf = false
      AND bd.created_at::date >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT
    COUNT(DISTINCT siren)                                                   AS total,
    SUM(CASE WHEN link_confidence = 'confirmed' THEN 1 ELSE 0 END)          AS confirmed,
    ROUND(100.0 * SUM(CASE WHEN link_confidence = 'confirmed' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(DISTINCT siren), 0), 1)                            AS confirmed_pct,
    ROUND(99.0 - 100.0 * SUM(CASE WHEN link_confidence = 'confirmed' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(DISTINCT siren), 0), 1)                            AS gap_to_99pp
FROM recent;
SQL
