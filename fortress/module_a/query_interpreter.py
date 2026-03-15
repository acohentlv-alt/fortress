"""Query Interpreter — parses plain-language queries into structured search parameters.

Accepts natural language like "AGRICULTURE 66" or "BOULANGERIE PARIS" and resolves
them to NAF codes + department, then executes a SQL query against the local SIRENE DB.

Supported input formats:
    "AGRICULTURE 66"          — industry name + dept number
    "BOULANGERIE PARIS"       — industry name + dept name
    "62.01Z 75"               — NAF code + dept number
    "LOGICIEL FRANCE"         — industry name + all of France
    "RESTAURANT ALL"          — industry name + all of France
    "AGRICULTURE"             — ambiguous dept → AmbiguousQueryError
    "66"                      — ambiguous industry → AmbiguousQueryError
"""

from __future__ import annotations

import re
from typing import Any

import psycopg.rows
import structlog
from rapidfuzz import fuzz, process

from fortress.config.departments import DEPARTMENTS, _normalize_code, get_department_code, postal_code_to_dept
from fortress.config.industry_aliases import INDUSTRY_ALIASES
from fortress.models import Company, CompanyStatus, QueryResult

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# NAF code patterns:
#   Full subclass: 62.01Z, 10.71A           (exact match in DB)
#   Hierarchical prefix: 01, 01.2, 01.24    (LIKE prefix match in DB)
# ---------------------------------------------------------------------------
_NAF_CODE_RE = re.compile(r"^\d{2}\.\d{2}[A-Z]$", re.IGNORECASE)
_NAF_PREFIX_RE = re.compile(r"^\d{2}(?:\.\d{1,2}[A-Z]?)?$", re.IGNORECASE)

# Fuzzy thresholds
_INDUSTRY_FUZZY_THRESHOLD = 75
_INDUSTRY_AMBIGUITY_THRESHOLD = 90  # Below this but above 75 → candidate for ambiguity list
_DEPT_FUZZY_THRESHOLD = 80

# France-wide sentinel tokens
_FRANCE_TOKENS = {"france", "all", "tout", "toute", "national", "nationale"}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class AmbiguousQueryError(Exception):
    """Raised when a query matches multiple industry aliases at similar scores.

    Attributes:
        options: list of candidate industry names for the user to choose from.
        message: human-readable explanation.
    """

    def __init__(self, message: str, options: list[str]) -> None:
        super().__init__(message)
        self.options = options


class UnresolvableQueryError(Exception):
    """Raised when a mandatory part of the query cannot be resolved.

    For example: industry token not found, or department token not found.
    """


# ---------------------------------------------------------------------------
# Department resolution helpers
# ---------------------------------------------------------------------------


def _resolve_department_token(token: str) -> tuple[str, str] | None:
    """Try to resolve a single token as a department.

    Returns (dept_code, dept_name) or None if not a department token.
    """
    # France-wide sentinels
    if token.lower() in _FRANCE_TOKENS:
        return None  # Caller checks separately

    # Direct numeric department code (01–95, 971–976, 2A, 2B)
    normalized = _normalize_code(token)
    if normalized is not None:
        return (normalized, DEPARTMENTS[normalized])

    # 5-digit postal code (e.g. "66000", "75001", "97100")
    dept_from_postal = postal_code_to_dept(token)
    if dept_from_postal is not None:
        return (dept_from_postal, DEPARTMENTS[dept_from_postal])

    # Department name — exact case-insensitive first, then fuzzy (threshold 88%).
    # Only attempted when the token has no digits — numeric tokens are already
    # handled by _normalize_code above and would never reach here.
    if not any(ch.isdigit() for ch in token):
        code = get_department_code(token)
        if code is not None:
            return (code, DEPARTMENTS[code])

    return None


def _is_france_wide_token(token: str) -> bool:
    """Return True if token means 'all of France'."""
    return token.lower() in _FRANCE_TOKENS


# ---------------------------------------------------------------------------
# NAF prefix → SQL pattern helpers
# ---------------------------------------------------------------------------


def _build_naf_conditions(naf_codes_or_prefixes: list[str]) -> tuple[str, list[str]]:
    """Build a SQL WHERE fragment and its bind parameters for NAF matching.

    Each item in the input list can be:
      - A full code like "62.01Z"  → exact  naf_code = %s
      - A prefix like "01"        → LIKE    naf_code LIKE '01.%'
      - A prefix like "01.2"      → LIKE    naf_code LIKE '01.2%'

    Returns (sql_fragment, bind_params).
    """
    clauses: list[str] = []
    params: list[str] = []

    for item in naf_codes_or_prefixes:
        if _NAF_CODE_RE.match(item):
            # Full specific code — exact match
            clauses.append("naf_code = %s")
            params.append(item.upper())
        else:
            # Prefix — build a LIKE pattern.
            # "01"   → "01.%"  (section prefix: add dot before wildcard)
            # "01.2" → "01.2%" (subsection prefix: already has dot, just wildcard)
            stripped = item.rstrip(".")
            pattern = stripped + ("%" if "." in stripped else ".%")
            clauses.append("naf_code LIKE %s")
            params.append(pattern)

    if not clauses:
        return ("1=0", [])

    return (" OR ".join(clauses), params)


def _build_naf_pattern_description(naf_prefixes: list[str]) -> str:
    """Build a human-readable / single-string pattern summary for QueryResult.naf_pattern.

    If there's exactly one full code → return it directly.
    Otherwise return comma-joined patterns e.g. "01.%, 02.%, 03.%"
    """
    if len(naf_prefixes) == 1 and _NAF_CODE_RE.match(naf_prefixes[0]):
        return naf_prefixes[0].upper()

    patterns = []
    for item in naf_prefixes:
        if _NAF_CODE_RE.match(item):
            patterns.append(item.upper())
        else:
            patterns.append(item.rstrip(".") + ".%")
    return ", ".join(patterns)


# ---------------------------------------------------------------------------
# Industry resolution
# ---------------------------------------------------------------------------


def _resolve_industry_token(token: str) -> tuple[str, list[str]] | None:
    """Resolve an industry token to (industry_name, naf_prefixes).

    Tries, in order:
    1. Direct NAF code (e.g. "62.01Z")
    2. Exact alias match
    3. Fuzzy alias match (threshold 75)

    Returns (matched_name, prefixes) or None if no match.
    Raises AmbiguousQueryError if multiple matches score 75-90.
    """
    normalized = token.strip().lower()

    # 1. Direct NAF code or hierarchical prefix (01, 01.2, 01.24, 01.24Z)
    if _NAF_PREFIX_RE.match(token):
        code = token.upper().rstrip(".")
        # Full subclass → exact match label; prefix → LIKE label
        label = code if _NAF_CODE_RE.match(code) else f"{code}*"
        return (label, [code])

    # 2. Exact alias match
    if normalized in INDUSTRY_ALIASES:
        return (normalized, INDUSTRY_ALIASES[normalized])

    # 3. Fuzzy match — collect ALL candidates above threshold
    candidates = list(INDUSTRY_ALIASES.keys())
    all_matches = process.extract(
        normalized,
        candidates,
        scorer=fuzz.WRatio,
        score_cutoff=_INDUSTRY_FUZZY_THRESHOLD,
        limit=10,
    )

    if not all_matches:
        return None

    # Sort by score descending
    all_matches.sort(key=lambda x: x[1], reverse=True)
    best_score = all_matches[0][1]
    best_name = all_matches[0][0]

    # If best is high-confidence (>= 90) → use it directly
    if best_score >= _INDUSTRY_AMBIGUITY_THRESHOLD:
        return (best_name, INDUSTRY_ALIASES[best_name])

    # Multiple candidates in the 75-90 ambiguous zone
    close_matches = [m for m in all_matches if m[1] >= _INDUSTRY_FUZZY_THRESHOLD]
    if len(close_matches) > 1:
        option_names = [m[0] for m in close_matches[:6]]  # cap at 6 options
        raise AmbiguousQueryError(
            f"'{token}' matches multiple industries: {', '.join(option_names)}. "
            "Please be more specific.",
            options=option_names,
        )

    # Single candidate above threshold
    return (best_name, INDUSTRY_ALIASES[best_name])


# ---------------------------------------------------------------------------
# Token parsing
# ---------------------------------------------------------------------------


def _parse_tokens(raw_query: str) -> tuple[list[str], str | None, bool]:
    """Split query into (industry_tokens, dept_code_or_None, is_france_wide).

    Iterates over whitespace-split tokens.  For each token it tries:
      - France-wide sentinel → set is_france_wide
      - Department code/name → capture dept_code
      - Everything else → industry token(s)

    Returns (industry_tokens, dept_code | None, is_france_wide).
    """
    tokens = raw_query.strip().split()
    industry_tokens: list[str] = []
    dept_code: str | None = None
    is_france_wide = False

    for token in tokens:
        if _is_france_wide_token(token):
            is_france_wide = True
            continue

        resolved_dept = _resolve_department_token(token)
        if resolved_dept is not None:
            dept_code = resolved_dept[0]
            continue

        # Not a France token, not a department → treat as industry token
        industry_tokens.append(token)

    return industry_tokens, dept_code, is_france_wide


# ---------------------------------------------------------------------------
# DB query
# ---------------------------------------------------------------------------


async def _query_companies(
    naf_prefixes: list[str],
    dept_code: str | None,
    is_france_wide: bool,
    db_pool: Any,
    limit: int | None = 5,
    offset: int = 0,
    *,
    filters: dict | None = None,
) -> tuple[int, list[Company]]:
    """Execute SQL against the local companies table.

    Returns (total_count, sample_list).
    The sample list contains up to `limit` Company objects starting at `offset`.
    Pass limit=None to fetch ALL matching companies (used by runner.py).

    Args:
        limit:   Max companies to return in sample. None = no limit (all companies).
        offset:  Number of rows to skip (for batch pagination). Default 0.
        filters: Optional dict with keys:
            statut           — "active" (default) or "all"
            tranche_effectif — list[str] of INSEE codes, e.g. ["03", "11"]
            forme_juridique  — list[str] of INSEE codes, e.g. ["5499", "5710"]
            date_from        — datetime.date, inclusive lower bound on date_creation
            date_to          — datetime.date, inclusive upper bound on date_creation
    """
    f = filters or {}
    naf_fragment, naf_params = _build_naf_conditions(naf_prefixes)

    # Build WHERE clause and params dynamically
    # If exact naf_code is provided, use it instead of prefix-based matching
    exact_naf = f.get("naf_code")
    if exact_naf:
        where_parts: list[str] = ["naf_code = %s"]
        params: list = [exact_naf]
    else:
        where_parts = [f"({naf_fragment})"]
        params = list(naf_params)

    # Statut (default: active only; "all" removes the filter)
    if f.get("statut", "active") == "active":
        where_parts.append("statut = %s")
        params.append(CompanyStatus.ACTIVE)

    # Department
    if not is_france_wide and dept_code is not None:
        where_parts.append("departement = %s")
        params.append(dept_code)

    # Tranche effectif
    tranche_list = f.get("tranche_effectif") or []
    if tranche_list:
        where_parts.append("tranche_effectif = ANY(%s)")
        params.append(list(tranche_list))

    # Forme juridique
    forme_list = f.get("forme_juridique") or []
    if forme_list:
        where_parts.append("forme_juridique = ANY(%s)")
        params.append(list(forme_list))

    # Date creation range
    date_from = f.get("date_from")
    date_to   = f.get("date_to")
    if date_from:
        where_parts.append("date_creation >= %s")
        params.append(date_from)
    if date_to:
        where_parts.append("date_creation <= %s")
        params.append(date_to)

    where_sql = " AND ".join(where_parts)

    count_sql = f"SELECT COUNT(*) FROM companies WHERE {where_sql}"
    # Build sample SQL — omit LIMIT clause when limit is None (fetch all)
    _limit_clause = f"LIMIT {limit}" if limit is not None else ""
    _offset_clause = f"OFFSET {offset}" if offset > 0 else ""
    sample_sql = f"""
        SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
               forme_juridique, adresse, code_postal, ville, departement,
               region, statut, date_creation, tranche_effectif,
               latitude, longitude, fortress_id
        FROM companies
        WHERE {where_sql}
        ORDER BY denomination
        {_limit_clause}
        {_offset_clause}
    """

    log.debug(
        "query_interpreter_sql",
        naf_fragment=naf_fragment,
        dept_code=dept_code,
        is_france_wide=is_france_wide,
        filters=f,
    )

    async with db_pool.connection() as conn:
        # Total count — use dict_row factory for column-name access
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(count_sql, params)
            count_row = await cur.fetchone()
            total_count: int = count_row["count"] if count_row else 0

            if total_count == 0:
                return 0, []

            # Sample rows (no extra param when limit is None — it's baked into SQL)
            await cur.execute(sample_sql, params)
            rows = await cur.fetchall()

    companies: list[Company] = []
    for row in rows:
        companies.append(
            Company(
                siren=row["siren"],
                siret_siege=row["siret_siege"],
                denomination=row["denomination"],
                naf_code=row["naf_code"],
                naf_libelle=row["naf_libelle"],
                forme_juridique=row["forme_juridique"],
                adresse=row["adresse"],
                code_postal=row["code_postal"],
                ville=row["ville"],
                departement=row["departement"],
                region=row["region"],
                statut=row["statut"] or CompanyStatus.ACTIVE,
                date_creation=row["date_creation"],
                tranche_effectif=row["tranche_effectif"],
                latitude=row["latitude"],
                longitude=row["longitude"],
                fortress_id=row["fortress_id"],
            )
        )

    return total_count, companies


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def interpret_query(
    raw_query: str,
    db_pool: Any | None = None,
    *,
    filters: dict | None = None,
    limit: int | None = 5,
    offset: int = 0,
) -> QueryResult:
    """Parse a natural-language query and return a QueryResult with company data.

    Args:
        raw_query: Plain-text query like "AGRICULTURE 66" or "RESTAURANT PARIS".
        db_pool:   Async psycopg connection pool.  When None, the module opens
                   its own pool from settings (useful for CLI invocation).
        limit:     Max companies to include in QueryResult.sample.
                   Pass None to fetch ALL matching companies (used by runner.py
                   to triage the full dataset, not just a preview sample).

    Returns:
        QueryResult with resolved NAF codes, department, company count and sample.

    Raises:
        AmbiguousQueryError: When the industry token matches multiple aliases
            at similar confidence scores (75-90%).
        UnresolvableQueryError: When the industry token cannot be resolved at all,
            or when only a department was given with no industry.
    """
    log.info("interpret_query_start", raw_query=raw_query)

    # --- tokenise -------------------------------------------------------
    industry_tokens, dept_code, is_france_wide = _parse_tokens(raw_query)

    # Validate: we need at least an industry token
    if not industry_tokens:
        raise UnresolvableQueryError(
            f"No industry found in query '{raw_query}'. "
            "Please specify an industry, e.g. 'AGRICULTURE 66'."
        )

    # Validate: if there is no location at all, ask for one
    # (only if query is not explicitly France-wide)
    if not is_france_wide and dept_code is None:
        # If the user typed only digits (a bare department number that failed
        # to match), give a more specific error.
        bare_numeric = all(t.isdigit() for t in industry_tokens)
        if bare_numeric:
            raise UnresolvableQueryError(
                f"'{raw_query}' looks like a department number but no industry was provided. "
                "Please add an industry name, e.g. 'AGRICULTURE 66'."
            )
        # Otherwise ask for a location
        raise UnresolvableQueryError(
            f"No department or location found in query '{raw_query}'. "
            "Please add a department (e.g. '66') or use 'FRANCE' for nationwide search."
        )

    # --- resolve industry -----------------------------------------------
    industry_query = " ".join(industry_tokens)
    resolution = _resolve_industry_token(industry_query)

    if resolution is None:
        raise UnresolvableQueryError(
            f"Could not resolve '{industry_query}' to any known industry. "
            "Try a different term or a NAF code like '62.01Z'."
        )

    industry_name, naf_prefixes = resolution

    # --- build dept metadata --------------------------------------------
    dept_name: str | None = None
    if dept_code is not None:
        dept_name = DEPARTMENTS.get(dept_code)

    # --- build NAF pattern description ----------------------------------
    naf_pattern = _build_naf_pattern_description(naf_prefixes)

    log.info(
        "interpret_query_resolved",
        raw_query=raw_query,
        industry_name=industry_name,
        naf_prefixes=naf_prefixes,
        dept_code=dept_code,
        is_france_wide=is_france_wide,
    )

    # --- open pool if not provided -------------------------------------
    if db_pool is None:
        from fortress.database.connection import get_pool

        db_pool = await get_pool()

    # --- query DB -------------------------------------------------------
    try:
        total_count, sample = await _query_companies(
            naf_prefixes=naf_prefixes,
            dept_code=dept_code,
            is_france_wide=is_france_wide,
            db_pool=db_pool,
            limit=limit,
            offset=offset,
            filters=filters,
        )
    except Exception as exc:
        error_str = str(exc).lower()
        # Re-raise timeout and critical errors — runner must handle them
        # as job failures. Only swallow non-critical errors for CLI usage.
        if "timeout" in error_str or "cancel" in error_str:
            log.error("interpret_query_timeout", error=str(exc), raw_query=raw_query)
            raise  # Let runner mark job as 'failed'
        log.error("interpret_query_db_error", error=str(exc), raw_query=raw_query)
        raise  # Don't silently hide DB errors — they mask real failures

    log.info(
        "interpret_query_done",
        raw_query=raw_query,
        company_count=total_count,
    )

    return QueryResult(
        raw_query=raw_query,
        industry_name=industry_name.title(),
        naf_codes=naf_prefixes,
        naf_pattern=naf_pattern,
        department=dept_code,
        department_name=dept_name,
        is_france_wide=is_france_wide,
        company_count=total_count,
        sample=sample,
    )
