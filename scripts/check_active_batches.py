#!/usr/bin/env python3
"""Pre-push gate: warn if any batch is currently active in production DB.

Exit codes:
  0 — safe to push (zero active batches OR DB unreachable — fail-open)
  1 — active batches found; user should reconsider

Run independently to inspect state:
  python3 scripts/check_active_batches.py
"""
import os
import sys


def main() -> int:
    try:
        import psycopg
        from dotenv import load_dotenv
    except ImportError:
        print("WARNING: psycopg/dotenv missing; skipping active-batch check", file=sys.stderr)
        return 0

    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("WARNING: DATABASE_URL not set; skipping active-batch check", file=sys.stderr)
        return 0

    try:
        conn = psycopg.connect(db_url, connect_timeout=10)
    except Exception as e:
        print(f"WARNING: DB unreachable ({e}); skipping active-batch check", file=sys.stderr)
        return 0

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT batch_id, batch_name, status, workspace_id, created_at
            FROM batch_data
            WHERE status IN ('queued', 'in_progress', 'triage')
            ORDER BY workspace_id, created_at DESC
        """)
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return 0

    print(f"\n⚠️  ACTIVE BATCH WARNING — {len(rows)} batch(es) currently running:\n", file=sys.stderr)
    for batch_id, name, status, ws, created in rows:
        print(f"  ws={ws} status={status:<12} created={str(created)[:19]}", file=sys.stderr)
        print(f"     {batch_id} — {name}", file=sys.stderr)
    print("\nPushing now will trigger a Render redeploy that KILLS these batches.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
