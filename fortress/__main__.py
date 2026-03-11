"""CLI entry point for Fortress.

Usage:
    python -m fortress "AGRICULTURE 66"
    python -m fortress "RESTAURANT PARIS"
    python -m fortress "62.01Z 75"
    python -m fortress "BOULANGERIE FRANCE"
"""

import asyncio
import sys

import structlog

from fortress.module_a.query_interpreter import (
    AmbiguousQueryError,
    UnresolvableQueryError,
    interpret_query,
)

log = structlog.get_logger()


async def run(query: str) -> None:
    """Execute a Fortress query and print results to stdout."""
    log.info("fortress_query_start", query=query)

    try:
        result = await interpret_query(query)
    except AmbiguousQueryError as exc:
        print(f"\n[AMBIGUOUS] '{query}' matches multiple industries.")
        print("Please be more specific. Possible matches:")
        for option in exc.options:
            print(f"  - {option}")
        print()
        sys.exit(1)
    except UnresolvableQueryError as exc:
        print(f"\n[ERROR] {exc}")
        print()
        sys.exit(1)

    log.info(
        "fortress_query_result",
        query=query,
        naf_codes=result.naf_codes,
        naf_pattern=result.naf_pattern,
        department=result.department,
        is_france_wide=result.is_france_wide,
        company_count=result.company_count,
    )

    # ── Print formatted output ──────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  FORTRESS — Query: {query}")
    print(f"{'=' * 60}")
    print(f"  Industry:    {result.industry_name}")
    print(f"  NAF codes:   {', '.join(result.naf_codes)} ({result.naf_pattern})")

    if result.is_france_wide:
        print("  Location:    France (nationwide)")
    else:
        print(f"  Department:  {result.department} — {result.department_name}")

    print(f"  Companies:   {result.company_count:,} found in local DB")
    print(f"{'=' * 60}")

    if result.company_count == 0:
        print("\n  0 results found in local SIRENE database.")
        print("  (Run sirene_ingester to load data if the DB is empty.)")
    elif result.sample:
        print(f"\n  Sample (first {len(result.sample)}):")
        for i, company in enumerate(result.sample, 1):
            ville = company.ville or "—"
            naf = company.naf_code or "—"
            print(f"  {i:3d}. {company.denomination} | {ville} | {naf}")

    print()


def main() -> None:
    """Parse CLI arguments and run the query."""
    if len(sys.argv) < 2:
        print("Usage: fortress <query>")
        print('Example: fortress "AGRICULTURE 66"')
        sys.exit(1)

    query = " ".join(sys.argv[1:])
    asyncio.run(run(query))


if __name__ == "__main__":
    main()
