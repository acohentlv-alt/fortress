#!/usr/bin/env python3 -u
"""Fortress Stress Test — 1000 entities across diverse sectors/departments.

Runs batches of 50, analyzes results after each, commits+pushes findings.
Planned stops after batches 5, 10, 15, 20 for code review.

Usage:
    python3 stress_test.py              # Run all batches
    python3 stress_test.py --start 6    # Resume from batch 6
    python3 stress_test.py --batch 3    # Run only batch 3
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://localhost:8080"
USERNAME = "stress.test"
PASSWORD = "StressTest1234"
WORKSPACE_ID = 417
DATABASE_URL = os.environ["DATABASE_URL"]

# ── Batch schedule: Alan's priority sectors + geographic diversity ──
# Sectors: camping, transport, logistique, agriculture (Alan's picks)
# Plus: restaurant, hotel, boulangerie, plomberie for variety
BATCH_SCHEDULE = [
    # 4 departments: 66 (Perpignan), 33 (Bordeaux), 34 (Montpellier), 11 (Carcassonne)
    # 4 sectors: camping, transport, logistique, agriculture
    # = 16 combos, each batch of 50 → 800 entities
    # Then repeat sectors in same depts for round 2 to reach 1000

    # Block 1 (batches 1-5) → STOP for review
    {"sector": "camping",     "dept": "66", "query": "camping 66"},
    {"sector": "transport",   "dept": "66", "query": "transport 66"},
    {"sector": "logistique",  "dept": "66", "query": "logistique 66"},
    {"sector": "agriculture", "dept": "66", "query": "agriculture 66"},
    {"sector": "camping",     "dept": "33", "query": "camping 33"},
    # Block 2 (batches 6-10) → STOP for review
    {"sector": "transport",   "dept": "33", "query": "transport 33"},
    {"sector": "logistique",  "dept": "33", "query": "logistique 33"},
    {"sector": "agriculture", "dept": "33", "query": "agriculture 33"},
    {"sector": "camping",     "dept": "34", "query": "camping 34"},
    {"sector": "transport",   "dept": "34", "query": "transport 34"},
    # Block 3 (batches 11-15) → STOP for review
    {"sector": "logistique",  "dept": "34", "query": "logistique 34"},
    {"sector": "agriculture", "dept": "34", "query": "agriculture 34"},
    {"sector": "camping",     "dept": "11", "query": "camping 11"},
    {"sector": "transport",   "dept": "11", "query": "transport 11"},
    {"sector": "logistique",  "dept": "11", "query": "logistique 11"},
    # Block 4 (batches 16-20) → STOP for review
    {"sector": "agriculture", "dept": "11", "query": "agriculture 11"},
    # Round 2 — second pass on same depts to fill remaining
    {"sector": "camping",     "dept": "33", "query": "camping 33"},
    {"sector": "transport",   "dept": "34", "query": "transport 34"},
    {"sector": "camping",     "dept": "11", "query": "camping 11"},
    {"sector": "logistique",  "dept": "66", "query": "logistique 66"},
    # Extra if needed
    {"sector": "agriculture", "dept": "33", "query": "agriculture 33"},
    {"sector": "transport",   "dept": "11", "query": "transport 11"},
    {"sector": "camping",     "dept": "34", "query": "camping 34"},
    {"sector": "logistique",  "dept": "33", "query": "logistique 33"},
    {"sector": "agriculture", "dept": "66", "query": "agriculture 66"},
    {"sector": "transport",   "dept": "66", "query": "transport 66"},
]

PLANNED_STOPS = {5, 10, 15, 20}  # Pause after these batch numbers


def login() -> requests.Session:
    s = requests.Session()
    # Use curl for login (requests has cookie issues with this server)
    result = subprocess.run(
        ["curl", "-s", "-c", "/tmp/stress_cookies.txt",
         "-H", "Content-Type: application/json",
         "-d", json.dumps({"username": USERNAME, "password": PASSWORD}),
         f"{BASE_URL}/api/auth/login"],
        capture_output=True, text=True,
    )
    resp = json.loads(result.stdout)
    if resp.get("status") != "ok":
        raise RuntimeError(f"Login failed: {result.stdout}")
    return s


def launch_batch(batch_info: dict) -> str:
    result = subprocess.run(
        ["curl", "-s", "-b", "/tmp/stress_cookies.txt",
         "-H", "Content-Type: application/json",
         "-d", json.dumps({
             "sector": batch_info["sector"],
             "department": batch_info["dept"],
             "size": 50,
             "strategy": "maps",
             "mode": "discovery",
             "search_queries": [batch_info["query"]],
         }),
         f"{BASE_URL}/api/batch/run"],
        capture_output=True, text=True,
    )
    resp = json.loads(result.stdout)
    batch_id = resp.get("batch_id")
    if not batch_id:
        raise RuntimeError(f"Batch launch failed: {result.stdout}")
    return batch_id


def poll_until_done(batch_id: str, timeout: int = 1800) -> dict:
    start = time.time()
    while time.time() - start < timeout:
        result = subprocess.run(
            ["curl", "-s", "-b", "/tmp/stress_cookies.txt",
             f"{BASE_URL}/api/jobs/{batch_id}"],
            capture_output=True, text=True,
        )
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            time.sleep(15)
            continue

        status = data.get("status", "unknown")
        scraped = data.get("companies_scraped", 0)
        query = data.get("current_query", "")
        print(f"  {datetime.now().strftime('%H:%M:%S')} | {status} | {scraped}/50 | {query}")

        if status in ("completed", "failed", "interrupted"):
            return data

        time.sleep(15)

    raise TimeoutError(f"Batch {batch_id} timed out after {timeout}s")


def analyze_batch(batch_id: str, batch_num: int, batch_info: dict) -> str:
    """Deep analysis of a completed batch. Returns markdown report."""
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True  # Avoid holding transactions that could block
    cur = conn.cursor()
    cur.execute("SET lock_timeout = 0")
    cur.execute("SET statement_timeout = 30000")

    # Batch summary
    cur.execute("""
        SELECT status, companies_scraped, companies_qualified, batch_size,
               created_at, updated_at,
               EXTRACT(EPOCH FROM (updated_at - created_at))/60 as duration_min,
               shortfall_reason, queries_json
        FROM batch_data WHERE batch_id = %s
    """, (batch_id,))
    row = cur.fetchone()
    status, scraped, qualified, size = row[0], row[1], row[2], row[3]
    duration = row[6]
    shortfall = row[7]
    queries_json = row[8]

    # Company details
    cur.execute("""
        SELECT DISTINCT bl.siren FROM batch_log bl
        WHERE bl.batch_id = %s AND bl.action = 'maps_lookup'
    """, (batch_id,))
    siren_ids = [r[0] for r in cur.fetchall()]

    # Data quality
    cur.execute("""
        SELECT COUNT(*) FROM contacts WHERE siren = ANY(%s) AND phone IS NOT NULL
    """, (siren_ids,))
    phone_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM contacts WHERE siren = ANY(%s) AND email IS NOT NULL
    """, (siren_ids,))
    email_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM contacts WHERE siren = ANY(%s) AND website IS NOT NULL
    """, (siren_ids,))
    web_count = cur.fetchone()[0]

    # SIREN match rate
    cur.execute("""
        SELECT COUNT(*) FROM companies WHERE siren = ANY(%s) AND linked_siren IS NOT NULL
    """, (siren_ids,))
    siren_matched = cur.fetchone()[0]

    # Match methods breakdown
    cur.execute("""
        SELECT link_method, COUNT(*) FROM companies
        WHERE siren = ANY(%s) AND linked_siren IS NOT NULL
        GROUP BY link_method ORDER BY COUNT(*) DESC
    """, (siren_ids,))
    match_methods = cur.fetchall()

    # Officers found
    cur.execute("""
        SELECT COUNT(DISTINCT bl.siren) FROM batch_log bl
        WHERE bl.batch_id = %s AND bl.action = 'officers_found'
    """, (batch_id,))
    officers_count = cur.fetchone()[0]

    # Total entities in workspace so far
    cur.execute("""
        SELECT COUNT(*) FROM companies WHERE workspace_id = %s AND siren LIKE 'MAPS%%'
    """, (WORKSPACE_ID,))
    total_entities = cur.fetchone()[0]

    # Query memory stats
    cur.execute("""
        SELECT COUNT(*) FROM query_memory WHERE workspace_id = %s
    """, (WORKSPACE_ID,))
    memory_rows = cur.fetchone()[0]

    conn.close()

    # Parse queries_json
    queries_report = ""
    if queries_json:
        if isinstance(queries_json, str):
            queries_json = json.loads(queries_json)
        queries_report = "\n### Per-Query Breakdown\n"
        for q in queries_json:
            exp = " [expansion]" if q.get("is_expansion") else ""
            queries_report += f"- **{q['query']}**: {q['new_companies']} new / {q['cards_found']} cards / {q.get('duration_sec', '?')}s{exp}\n"

    # Build report
    report = f"""## Batch {batch_num}: {batch_info['query']} (dept {batch_info['dept']})

**Batch ID:** {batch_id}
**Status:** {status}
**Duration:** {duration:.1f} min
**Entities:** {scraped}/{size} scraped, {qualified} qualified
{'**Shortfall:** ' + shortfall if shortfall else ''}

### Data Quality ({len(siren_ids)} entities)
| Metric | Count | Rate |
|--------|-------|------|
| Phone | {phone_count} | {phone_count*100//max(len(siren_ids),1)}% |
| Email | {email_count} | {email_count*100//max(len(siren_ids),1)}% |
| Website | {web_count} | {web_count*100//max(len(siren_ids),1)}% |
| SIREN matched | {siren_matched} | {siren_matched*100//max(len(siren_ids),1)}% |
| Officers found | {officers_count} | {officers_count*100//max(len(siren_ids),1)}% |

### SIREN Match Methods
{chr(10).join(f'- {m[0]}: {m[1]}' for m in match_methods) if match_methods else '- None'}
{queries_report}
### Running Totals
- **Total entities in workspace:** {total_entities} / 1000
- **Query memory rows:** {memory_rows}

---
"""
    return report


def commit_and_push(batch_num: int, report: str):
    """Append report to stress test log and push."""
    log_path = "/Users/alancohen/Project Alan copy/fortress/STRESS_TEST_LOG.md"

    # Append to log
    header = ""
    if not os.path.exists(log_path):
        header = f"# Stress Test Log — {datetime.now().strftime('%Y-%m-%d')}\n\n"

    with open(log_path, "a") as f:
        if header:
            f.write(header)
        f.write(report)
        f.write("\n")

    # Git commit + push
    subprocess.run(
        ["git", "add", "STRESS_TEST_LOG.md"],
        cwd="/Users/alancohen/Project Alan copy/fortress",
    )
    subprocess.run(
        ["git", "commit", "-m",
         f"Stress test batch {batch_num} results\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"],
        cwd="/Users/alancohen/Project Alan copy/fortress",
    )
    subprocess.run(
        ["git", "push", "origin", "main"],
        cwd="/Users/alancohen/Project Alan copy/fortress",
    )
    print(f"  Pushed batch {batch_num} results to main")


def run_batch(batch_num: int, batch_info: dict):
    print(f"\n{'='*60}")
    print(f"BATCH {batch_num}: {batch_info['query']} (dept {batch_info['dept']})")
    print(f"{'='*60}")
    print(f"  Started: {datetime.now().strftime('%H:%M:%S')}")

    login()
    batch_id = launch_batch(batch_info)
    print(f"  Batch ID: {batch_id}")

    result = poll_until_done(batch_id)
    print(f"  Completed: {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Status: {result.get('status')} | Scraped: {result.get('companies_scraped')}")

    # Analyze
    report = analyze_batch(batch_id, batch_num, batch_info)
    print(report)

    # Commit + push
    commit_and_push(batch_num, report)

    return result


def main():
    parser = argparse.ArgumentParser(description="Fortress Stress Test")
    parser.add_argument("--start", type=int, default=1, help="Start from batch N")
    parser.add_argument("--batch", type=int, help="Run only batch N")
    args = parser.parse_args()

    # Verify server is up
    try:
        resp = requests.get(f"{BASE_URL}/api/health", timeout=5)
        if resp.status_code != 200:
            print("ERROR: Server not responding. Start it with:")
            print("  cd fortress && python3 -m uvicorn fortress.api.main:app --port 8080 --reload")
            sys.exit(1)
    except Exception:
        print("ERROR: Cannot reach server at localhost:8080")
        sys.exit(1)

    if args.batch:
        # Run single batch
        idx = args.batch - 1
        if idx >= len(BATCH_SCHEDULE):
            print(f"ERROR: Batch {args.batch} not in schedule (max {len(BATCH_SCHEDULE)})")
            sys.exit(1)
        run_batch(args.batch, BATCH_SCHEDULE[idx])
        return

    # Run from start
    for i in range(args.start - 1, len(BATCH_SCHEDULE)):
        batch_num = i + 1
        batch_info = BATCH_SCHEDULE[i]

        # Check if we've reached 1000
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM companies WHERE workspace_id = %s AND siren LIKE 'MAPS%%'", (WORKSPACE_ID,))
        total = cur.fetchone()[0]
        conn.close()

        if total >= 1000:
            print(f"\n{'='*60}")
            print(f"TARGET REACHED: {total} entities in workspace {WORKSPACE_ID}")
            print(f"{'='*60}")
            break

        run_batch(batch_num, batch_info)

        # Planned stops
        if batch_num in PLANNED_STOPS:
            print(f"\n{'='*60}")
            print(f"PLANNED STOP after batch {batch_num}")
            print(f"Total entities so far: check STRESS_TEST_LOG.md")
            print(f"Review results, fix code if needed, then resume with:")
            print(f"  python3 stress_test.py --start {batch_num + 1}")
            print(f"{'='*60}")
            break


if __name__ == "__main__":
    main()
