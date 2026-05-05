#!/usr/bin/env python3
"""QA helper — list every route in a Python file and curl the GETs.

POST/DELETE/PATCH/PUT routes are listed but NOT auto-curled (they mutate state).
Why: the May 4 export.py incident — 4 endpoints in one file had been silently
returning HTTP 500 on prod for an unknown duration because routine prod use only
exercised some of them. Brief 2 v3 caught it by sweeping ALL endpoints in a file.

Usage:
    python3 scripts/qa_endpoint_sweep.py fortress/api/routes/jobs.py
    python3 scripts/qa_endpoint_sweep.py fortress/api/routes/export.py --cookie '<session>'
    python3 scripts/qa_endpoint_sweep.py file.py --base https://fortress-4o6r.onrender.com --prefix /api/export

Exits 1 if any GET returned a non-2xx/401/403 (401/403 = auth-protected, expected).
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

ROUTE_RE = re.compile(r'@(?:router|app)\.(get|post|delete|patch|put)\(\s*["\']([^"\']+)["\']')
PREFIX_RE = re.compile(r'APIRouter\([^)]*prefix\s*=\s*["\']([^"\']*)["\']')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file", help="path to a Python route file")
    ap.add_argument("--base", default="http://localhost:8080", help="base URL (default: localhost:8080)")
    ap.add_argument("--prefix", default=None, help="route prefix (auto-detected from APIRouter(prefix=...) if unset)")
    ap.add_argument("--cookie", default="", help="Cookie header value, e.g. fortress_session=...")
    args = ap.parse_args()

    src_path = Path(args.file)
    if not src_path.exists():
        print(f"FAIL: {args.file} not found")
        sys.exit(2)
    src = src_path.read_text()

    if args.prefix is not None:
        prefix = args.prefix
    else:
        m = PREFIX_RE.search(src)
        prefix = m.group(1) if m else ""

    routes = ROUTE_RE.findall(src)
    print(f"# {args.file}: {len(routes)} routes (prefix={prefix!r}, base={args.base!r})")

    fail = 0
    for method, path in routes:
        full = f"{args.base}{prefix}{path}"
        m_upper = method.upper()
        if m_upper != "GET":
            print(f"  [SKIP-MUTATING] {m_upper} {full}")
            continue

        cmd = ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", full]
        if args.cookie:
            cmd.extend(["-H", f"Cookie: {args.cookie}"])

        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            code = r.stdout.strip()
            ok = code.startswith("2") or code in ("401", "403")
            tag = "OK" if ok else "FAIL"
            if not ok:
                fail += 1
            print(f"  [{tag}] GET {full} -> HTTP {code}")
        except subprocess.TimeoutExpired:
            fail += 1
            print(f"  [TIMEOUT] GET {full}")
        except Exception as exc:
            fail += 1
            print(f"  [ERROR]   GET {full} -> {exc}")

    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
