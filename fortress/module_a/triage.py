"""Triage — classify SIRENE companies into BLACK / BLUE / GREEN / YELLOW / RED before scraping.

BLACK  — SIREN in blacklisted_sirens table.   Skip entirely. No network calls.
BLUE   — SIREN in client_sirens table.        Skip — client already has this company.
GREEN  — exists in master DB with all MVP fields (phone + website + email). Instant card.
YELLOW — exists in master DB but missing ≥ 1 MVP field. Targeted scrape only.
RED    — never seen before (no row in contacts). Full pipeline.

MVP fields: contacts.phone, contacts.website, contacts.email

Triage always runs before any network request fires.
It prints a dry-run preview so the user can confirm before scraping starts.
"""

from __future__ import annotations

from typing import Any

import structlog

from fortress.models import Company, TriageResult

log = structlog.get_logger(__name__)

# MVP contact fields required to classify a company as GREEN
_MVP_FIELDS = ("phone", "website", "email")


async def triage_companies(
    companies: list[Company],
    batch_name: str,
    pool: Any,  # psycopg_pool.AsyncConnectionPool
) -> TriageResult:
    """Classify companies into BLACK / GREEN / YELLOW / RED.

    Args:
        companies:  Full list of Company objects from SIRENE query.
        batch_name: Human-readable query label, e.g. "AGRICULTURE 66".
        pool:       Async psycopg3 connection pool.

    Returns:
        TriageResult with four buckets. GREEN companies are immediately tagged
        in batch_tags. YELLOW companies have a ``missing_fields`` attribute
        set on the Company object (as extra field).
    """
    result = TriageResult()

    if not companies:
        _print_preview(batch_name, result)
        return result

    sirens = [c.siren for c in companies]

    async with pool.connection() as conn:
        # --- 1. Fetch all blacklisted SIRENs in one query ----------------
        blacklisted: set[str] = await _fetch_blacklisted(conn, sirens)

        # --- 2. Fetch client-owned SIRENs (BLUE) in one query -----------
        client_owned: set[str] = await _fetch_client_sirens(conn, sirens)

        # --- 3. Fetch existing contacts for all SIRENs in one query ------
        contacts_by_siren: dict[str, dict[str, str | None]] = await _fetch_contacts(
            conn, sirens
        )

        # --- 4. Classify each company ------------------------------------
        for company in companies:
            # Companies with no usable name cannot be searched on Maps
            denom = (company.denomination or "").strip()
            if not denom or denom == "[ND]":
                result.black.append(company)
                continue

            if company.siren in blacklisted:
                result.black.append(company)
                continue

            # BLUE: client already has this company in their CRM
            if company.siren in client_owned:
                result.blue.append(company)
                continue

            contact = contacts_by_siren.get(company.siren)
            if contact is None:
                # Never seen — full pipeline needed
                result.red.append(company)
                continue

            missing = [f for f in _MVP_FIELDS if not contact.get(f)]
            if not missing:
                # All MVP fields present — instant card
                result.green.append(company)
            else:
                # Partial data — targeted scrape for missing fields only
                # Use model_copy so missing_fields is preserved as a proper field
                company_copy = company.model_copy(update={"missing_fields": missing})
                result.yellow.append(company_copy)

        # --- 4. Tag GREEN companies in batch_tags ------------------------
        if result.green:
            await _tag_green_companies(conn, result.green, batch_name)

    _print_preview(batch_name, result)
    log.info(
        "triage_complete",
        query=batch_name,
        black=result.black_count,
        blue=result.blue_count,
        green=result.green_count,
        yellow=result.yellow_count,
        red=result.red_count,
    )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _fetch_blacklisted(conn: Any, sirens: list[str]) -> set[str]:
    """Return the set of SIRENs that are in the blacklist."""
    if not sirens:
        return set()
    cur = await conn.execute(
        "SELECT siren FROM blacklisted_sirens WHERE siren = ANY(%s)",
        (sirens,),
    )
    rows = await cur.fetchall()
    return {row[0] for row in rows}


async def _fetch_client_sirens(conn: Any, sirens: list[str]) -> set[str]:
    """Return the set of SIRENs the client already owns (BLUE classification)."""
    if not sirens:
        return set()
    try:
        cur = await conn.execute(
            "SELECT siren FROM client_sirens WHERE siren = ANY(%s)",
            (sirens,),
        )
        rows = await cur.fetchall()
        return {row[0] for row in rows}
    except Exception:
        # Table may not exist yet — treat as no client SIRENs
        return set()


async def _fetch_contacts(
    conn: Any, sirens: list[str]
) -> dict[str, dict[str, str | None]]:
    """Return a dict {siren: {phone, website, email}} for all known SIRENs.

    Uses one batch SELECT. If a SIREN has multiple contact rows (different
    sources), merges them — a non-null value from any source counts.
    """
    if not sirens:
        return {}

    import psycopg.rows

    async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        await cur.execute(
            """
            SELECT siren,
                   MAX(phone)   AS phone,
                   MAX(website) AS website,
                   MAX(email)   AS email
            FROM contacts
            WHERE siren = ANY(%s)
            GROUP BY siren
            """,
            (sirens,),
        )
        rows = await cur.fetchall()

    return {row["siren"]: dict(row) for row in rows}


async def _tag_green_companies(
    conn: Any,
    companies: list[Company],
    batch_name: str,
) -> None:
    """Upsert batch_tags rows for all GREEN companies (already complete)."""
    if not companies:
        return
    from datetime import datetime, timezone

    now = datetime.now(tz=timezone.utc)
    # psycopg3: executemany lives on the cursor, not the connection.
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO batch_tags (siren, batch_name, tagged_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (siren, batch_name) DO NOTHING
            """,
            [(c.siren, batch_name, now) for c in companies],
        )


def _print_preview(batch_name: str, result: TriageResult) -> None:
    """Print the dry-run triage preview to stdout."""
    total = (
        result.black_count
        + result.blue_count
        + result.green_count
        + result.yellow_count
        + result.red_count
    )
    scrape_needed = result.yellow_count + result.red_count
    pct = f"{100 * scrape_needed // total}%" if total else "0%"

    print(f"\nTRIAGE PREVIEW for {batch_name}:")
    print(f"├── BLACK  (blacklisted):   {result.black_count:>6}  → Skipped entirely")
    print(f"├── BLUE   (client owns):   {result.blue_count:>6}  → Skipped (already in CRM)")
    print(f"├── GREEN  (complete):      {result.green_count:>6}  → Instant cards")
    print(f"├── YELLOW (partial data):  {result.yellow_count:>6}  → Targeted scrape")
    print(f"└── RED    (new companies): {result.red_count:>6}  → Full scrape")
    print(f"\nScraping required: {scrape_needed} / {total} ({pct})\n")
