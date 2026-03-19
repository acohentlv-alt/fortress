"""Deduplicator — PostgreSQL-backed upsert for companies, contacts, officers.

Every record is identified by its SIREN (primary key in companies table).
Contacts can have multiple rows per SIREN (one per source), so upsert
is on (siren, source).

Why PostgreSQL for dedup (not in-memory):
  Loading 50,000+ master records into RAM is impractical at scale.
  PostgreSQL handles the dedup via ON CONFLICT clauses and indexes.
  The JSONL master file is an export — not the working copy.

All functions accept an open psycopg3 connection (async).
Transactions are managed by the caller (batch_processor.py).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

from fortress.models import Company, Contact, Officer

log = structlog.get_logger(__name__)


async def upsert_company(conn: Any, company: Company) -> None:
    """Insert or update a company row in the companies table.

    On conflict (siren already exists), updates the mutable fields
    (address, status, NAF code, etc.) while preserving the original
    created_at and fortress_id.
    """
    await conn.execute(
        """
        INSERT INTO companies (
            siren, siret_siege, denomination, naf_code, naf_libelle,
            forme_juridique, adresse, code_postal, ville, departement, region,
            statut, date_creation, tranche_effectif, latitude, longitude
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (siren) DO UPDATE SET
            siret_siege      = EXCLUDED.siret_siege,
            denomination     = EXCLUDED.denomination,
            naf_code         = EXCLUDED.naf_code,
            naf_libelle      = EXCLUDED.naf_libelle,
            forme_juridique  = EXCLUDED.forme_juridique,
            adresse          = EXCLUDED.adresse,
            code_postal      = EXCLUDED.code_postal,
            ville            = EXCLUDED.ville,
            departement      = EXCLUDED.departement,
            region           = EXCLUDED.region,
            statut           = EXCLUDED.statut,
            date_creation    = EXCLUDED.date_creation,
            tranche_effectif = EXCLUDED.tranche_effectif,
            latitude         = COALESCE(EXCLUDED.latitude, companies.latitude),
            longitude        = COALESCE(EXCLUDED.longitude, companies.longitude),
            updated_at       = NOW()
        """,
        (
            company.siren,
            company.siret_siege,
            company.denomination,
            company.naf_code,
            company.naf_libelle,
            company.forme_juridique,
            company.adresse,
            company.code_postal,
            company.ville,
            company.departement,
            company.region,
            company.statut.value if company.statut else "A",
            company.date_creation,
            company.tranche_effectif,
            company.latitude,
            company.longitude,
        ),
    )


async def upsert_contact(conn: Any, contact: Contact) -> None:
    """Insert or update a contact row.

    The (siren, source) pair is the natural dedup key for contacts.
    A company can have one contact row per source (website_crawl, inpi, etc.).

    On conflict, non-null incoming values overwrite existing nulls.
    Existing non-null values are preserved (COALESCE).

    Skips entirely if the contact has no useful data (all MVP fields
    + address + rating + maps_url are null). This prevents ghost rows
    from polluting triage classification and quality gauges.
    """
    # Guard: skip contacts with no useful data at all
    has_data = any([
        contact.phone, contact.email, contact.website,
        contact.address, contact.rating, contact.maps_url,
    ])
    if not has_data:
        return

    await conn.execute(
        """
        INSERT INTO contacts (
            siren, phone, email, email_type, website, address, source,
            social_linkedin, social_facebook, social_twitter,
            social_instagram, social_tiktok,
            rating, review_count, maps_url, collected_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (siren, source) DO UPDATE SET
            phone          = COALESCE(EXCLUDED.phone,          contacts.phone),
            email          = COALESCE(EXCLUDED.email,          contacts.email),
            email_type     = COALESCE(EXCLUDED.email_type,     contacts.email_type),
            website        = COALESCE(EXCLUDED.website,        contacts.website),
            address        = COALESCE(EXCLUDED.address,        contacts.address),
            social_linkedin  = COALESCE(EXCLUDED.social_linkedin,  contacts.social_linkedin),
            social_facebook  = COALESCE(EXCLUDED.social_facebook,  contacts.social_facebook),
            social_twitter   = COALESCE(EXCLUDED.social_twitter,   contacts.social_twitter),
            social_instagram = COALESCE(EXCLUDED.social_instagram, contacts.social_instagram),
            social_tiktok    = COALESCE(EXCLUDED.social_tiktok,    contacts.social_tiktok),
            rating         = COALESCE(EXCLUDED.rating,         contacts.rating),
            review_count   = COALESCE(EXCLUDED.review_count,   contacts.review_count),
            maps_url       = COALESCE(EXCLUDED.maps_url,       contacts.maps_url),
            collected_at   = EXCLUDED.collected_at
        """,
        (
            contact.siren,
            contact.phone,
            contact.email,
            contact.email_type.value if contact.email_type else None,
            contact.website,
            contact.address,
            contact.source.value,
            contact.social_linkedin,
            contact.social_facebook,
            contact.social_twitter,
            contact.social_instagram,
            contact.social_tiktok,
            contact.rating,
            contact.review_count,
            contact.maps_url,
            contact.collected_at or datetime.now(tz=timezone.utc),
        ),
    )


async def upsert_officer(conn: Any, officer: Officer) -> None:
    """Insert an officer row. Uses (siren, nom, prenom) as the natural key.

    Duplicate officer rows (same name + same company from different sources)
    are silently skipped via DO NOTHING.
    """
    await conn.execute(
        """
        INSERT INTO officers (siren, nom, prenom, role, source, collected_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            officer.siren,
            officer.nom,
            officer.prenom,
            officer.role,
            officer.source.value,
            officer.collected_at or datetime.now(tz=timezone.utc),
        ),
    )


async def tag_query(conn: Any, siren: str, query_name: str) -> None:
    """Associate a company with a query in query_tags.

    Uses DO NOTHING so re-tagging the same company for the same query
    is idempotent.
    """
    await conn.execute(
        """
        INSERT INTO query_tags (siren, query_name, tagged_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (siren, query_name) DO NOTHING
        """,
        (siren, query_name),
    )


async def bulk_tag_query(
    conn: Any,
    sirens: list[str],
    query_name: str,
) -> None:
    """Tag multiple companies for a query in a single batch.

    More efficient than calling tag_query() in a loop for large GREEN lists.
    """
    if not sirens:
        return

    now = datetime.now(tz=timezone.utc)
    # psycopg3: executemany lives on the cursor, not the connection.
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO query_tags (siren, query_name, tagged_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (siren, query_name) DO NOTHING
            """,
            [(siren, query_name, now) for siren in sirens],
        )


async def log_audit(
    conn: Any,
    *,
    query_id: str,
    siren: str,
    action: str,
    result: str,
    source_url: str | None = None,
    duration_ms: int | None = None,
    search_query: str | None = None,
) -> None:
    """Write one row to the scrape_audit table.

    action: 'inpi_lookup' | 'web_search' | 'website_crawl' | 'maps_lookup'
    result: 'success' | 'fail' | 'blocked' | 'skipped'
    search_query: the exact Maps search term that found this entity (optional)
    """
    await conn.execute(
        """
        INSERT INTO scrape_audit
            (query_id, siren, action, result, source_url, duration_ms, search_query)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (query_id, siren, action, result, source_url, duration_ms, search_query),
    )
