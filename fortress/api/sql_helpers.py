"""Shared SQL helpers for contact merging across all API routes.

NOTE: This module merges the same fields as _merge_contacts() in companies.py,
with one deliberate omission: social_instagram and social_tiktok are NOT included
here because none of the list/stats queries currently display them. If a future
query needs them, add them following the same _pick_best() pattern. The full
list of social fields in the contacts table is:
  social_linkedin, social_facebook, social_twitter, social_instagram, social_tiktok
Only the first three are merged here.
"""


def _source_priority_case(col_alias: str = "c") -> str:
    """Return the CASE expression for source priority ordering."""
    return f"""CASE {col_alias}.source
        WHEN 'manual_edit' THEN 0
        WHEN 'upload' THEN 1
        WHEN 'website_crawl' THEN 2
        WHEN 'mentions_legales' THEN 2
        WHEN 'google_maps' THEN 3
        WHEN 'google_cse' THEN 3
        WHEN 'recherche_entreprises' THEN 4
        WHEN 'sirene' THEN 5
        WHEN 'google_search' THEN 5
        WHEN 'directory_search' THEN 5
        WHEN 'pages_jaunes' THEN 5
        WHEN 'inpi' THEN 5
        WHEN 'annuaire_entreprises' THEN 5
        WHEN 'synthesized' THEN 6
        ELSE 99
    END"""


def _phone_priority_case(col_alias: str = "c") -> str:
    """Phone-specific source priority: Google Maps > website_crawl.

    Google Maps phones are the business's publicly listed number.
    Website crawl can pick up wrong numbers (tracking IDs, web agency footers).
    """
    return f"""CASE {col_alias}.source
        WHEN 'manual_edit' THEN 0
        WHEN 'upload' THEN 1
        WHEN 'google_maps' THEN 2
        WHEN 'google_cse' THEN 2
        WHEN 'website_crawl' THEN 3
        WHEN 'mentions_legales' THEN 3
        WHEN 'recherche_entreprises' THEN 4
        WHEN 'sirene' THEN 5
        WHEN 'google_search' THEN 5
        WHEN 'directory_search' THEN 5
        WHEN 'pages_jaunes' THEN 5
        WHEN 'inpi' THEN 5
        WHEN 'annuaire_entreprises' THEN 5
        WHEN 'synthesized' THEN 6
        ELSE 99
    END"""


def _pick_best(field: str, priority_case: str | None = None, col_alias: str = "c") -> str:
    """Generate ARRAY_AGG expression to pick best non-null value for a field."""
    pcase = priority_case or _source_priority_case(col_alias)
    return (
        f"(ARRAY_AGG({col_alias}.{field} ORDER BY {pcase}, "
        f"{col_alias}.collected_at DESC) "
        f"FILTER (WHERE {col_alias}.{field} IS NOT NULL))[1]"
    )


def merged_contacts_cte(siren_subquery: str) -> str:
    """Return a 'merged_contacts' CTE that merges all contact rows per SIREN.

    Args:
        siren_subquery: SQL fragment that produces a column called 'siren'.
            Can reference parameters like %s -- the caller is responsible
            for providing matching params.

    Returns:
        SQL string for use as a CTE, e.g.:
            merged_contacts AS (SELECT ... FROM contacts c WHERE c.siren IN (...) GROUP BY c.siren)
    """
    std = _source_priority_case()
    phone_case = _phone_priority_case()

    addr_case = """CASE c.source
        WHEN 'manual_edit' THEN 0
        WHEN 'google_maps' THEN 1
        ELSE 5
    END"""

    maps_case = "CASE c.source WHEN 'google_maps' THEN 0 ELSE 99 END"

    return f"""merged_contacts AS (
    SELECT
        c.siren,
        {_pick_best('phone', priority_case=phone_case)} AS phone,
        {_pick_best('email')} AS email,
        {_pick_best('email_type')} AS email_type,
        {_pick_best('website')} AS website,
        {_pick_best('social_linkedin')} AS social_linkedin,
        {_pick_best('social_facebook')} AS social_facebook,
        {_pick_best('social_twitter')} AS social_twitter,
        {_pick_best('address', priority_case=addr_case)} AS address,
        {_pick_best('rating', priority_case=maps_case)} AS rating,
        {_pick_best('review_count', priority_case=maps_case)} AS review_count,
        {_pick_best('maps_url', priority_case=maps_case)} AS maps_url,
        (ARRAY_AGG(c.source ORDER BY {std}, c.collected_at DESC))[1] AS contact_source
    FROM contacts c
    WHERE c.siren IN ({siren_subquery})
    GROUP BY c.siren
)"""
