"""Recherche Entreprises API — fetch official directors from French government registry.

Free API, no key required, 7 req/s rate limit.
Endpoint: https://recherche-entreprises.api.gouv.fr/search?q={siren}&per_page=1

Returns dirigeant names (nom, prénom, qualité) from the Registre National des
Entreprises (RNE). Does NOT synthesize emails — director names only.

Usage:
    dirigeants = await fetch_dirigeants("123456789")
    for d in dirigeants:
        # d = {"nom": "DUPONT", "prenom": "Jean", "qualite": "Président"}
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog

from fortress.scraping.http import CurlClient, CurlClientError

log = structlog.get_logger(__name__)

# INPI is rate-limited; lock serializes all calls so N workers don't fan out
# concurrent requests → 429s → matcher confirm rate regresses.
# All callers (search_by_name, fetch_dirigeants) acquire this lock for the
# entire API call including the post-call 0.5s sleep, capping throughput at ~2 req/s
# system-wide regardless of how many worker tasks are active.
_INPI_GLOBAL_LOCK: asyncio.Lock = asyncio.Lock()

_API_URL = "https://recherche-entreprises.api.gouv.fr/search"
_TIMEOUT = 3.0  # seconds — best-effort, never block the pipeline
_RATE_DELAY = 0.5  # ~2 req/s — the API claims 7/s but enforces stricter per-IP limits

# A2c retry ladder for rate-limited A2-fallback INPI lookups only.
# Opt-in via search_by_name(retry_on_rate_limit=True) — default False for all
# other call sites (Step 0, Step 5, officer fetch, admin UI).
# Total worst-case additional wait per A2 entity: 2 + 5 + 15 = 22s.
_A2_RETRY_DELAYS: tuple[float, ...] = (2.0, 5.0, 15.0)

# Set to True by search_by_name() when retry_on_rate_limit=True AND all three
# retries returned 429. The A2 caller reads and clears this immediately after
# its await. Single-threaded async, single caller — no race concern. Any other
# call site that opts into retry_on_rate_limit must follow the same read-clear
# pattern or leave it alone (default False never writes to this flag).
_LAST_A2_RATE_LIMIT_EXHAUSTED: bool = False


def parse_company_fields(hit: dict) -> dict[str, Any]:
    """Extract company-level fields from a raw Recherche Entreprises API hit.

    Returns a dict with the same keys that fetch_dirigeants builds into
    company_data, so callers (fetch_dirigeants and Step 0 save sites) share
    a single extraction path.
    """
    result: dict[str, Any] = {}

    finances = hit.get("finances", {})
    if finances:
        latest_year = max(finances.keys(), default=None)
        if latest_year:
            ca = finances[latest_year].get("ca")
            resultat = finances[latest_year].get("resultat_net")
            if ca is not None:
                result["chiffre_affaires"] = ca
                result["annee_ca"] = latest_year
            if resultat is not None:
                result["resultat_net"] = resultat

    effectif = hit.get("tranche_effectif_salarie")
    if effectif:
        result["tranche_effectif"] = effectif

    cat = hit.get("categorie_entreprise")
    if cat:
        result["categorie_entreprise"] = cat

    nature = hit.get("nature_juridique")
    if nature:
        result["nature_juridique"] = nature

    for src_key, dst_key in [
        ("date_creation", "date_creation_inpi"),
        ("date_fermeture", "date_fermeture"),
        ("etat_administratif", "etat_administratif_inpi"),
        ("nombre_etablissements_ouverts", "nombre_etablissements_ouverts"),
    ]:
        v = hit.get(src_key)
        if v is not None and v != "":
            result[dst_key] = v

    siege = hit.get("siege") or {}
    if isinstance(siege, dict):
        siege_data: dict[str, Any] = {}
        if siege.get("adresse"):
            siege_data["adresse"] = siege["adresse"]
        if siege.get("liste_enseignes"):
            siege_data["enseignes"] = siege["liste_enseignes"]
        if siege_data:
            result["_siege"] = siege_data

    return result


async def _fire_inpi_get(url: str) -> tuple[int, dict | None]:
    """Fire one INPI GET request under the global lock with the post-call rate gate.

    Held under _INPI_GLOBAL_LOCK for the request + 0.5s rate gate ONLY. Callers
    that retry on 429 must do the retry sleep OUTSIDE this helper so the lock
    is released between attempts — otherwise a 22s worst-case retry blocks
    every other worker's INPI calls.

    Returns (status_code, json_payload). Payload is None if status != 200 or
    JSON parse fails. Exceptions propagate to the caller for logging.
    """
    import httpx
    async with _INPI_GLOBAL_LOCK:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as hx:
                resp = await hx.get(url)
                status = resp.status_code
                try:
                    payload = resp.json() if status == 200 else None
                except Exception:
                    payload = None
        finally:
            await asyncio.sleep(_RATE_DELAY)
    return status, payload


async def search_by_name(
    query: str,
    dept: str | None = None,
    cp: str | None = None,
    *,
    retry_on_rate_limit: bool = True,
) -> tuple[str, str | None, str | None, str | None, dict] | None:
    """Search for a company by name in Recherche Entreprises API.

    Args:
        query: Normalised company name to search for.
        dept: Department code (e.g. '66') — used if cp is None.
        cp: Postal code (e.g. '66300') — preferred over dept if provided.
        retry_on_rate_limit: If True (default since Bug C fix, Apr 28), retry
            up to 3 times on HTTP 429 using the _A2_RETRY_DELAYS ladder
            (2s / 5s / 15s) with the lock released during sleeps. After all
            retries exhaust, sets the module-level _LAST_A2_RATE_LIMIT_EXHAUSTED
            flag to True so the A2 caller can dual-emit telemetry. Pass False
            from any future call site that explicitly wants fail-fast.

    Returns:
        (siren, naf_code, nom_complet, cp, hit) 5-tuple on hit, or None.
        hit is the raw API result dict for field harvesting via parse_company_fields.
    """
    params: list[str] = [f"q={query}", "per_page=3"]
    if cp:
        params.append(f"code_postal={cp}")
    elif dept:
        params.append(f"departement={dept}")
    url = f"{_API_URL}?" + "&".join(params)

    global _LAST_A2_RATE_LIMIT_EXHAUSTED
    if retry_on_rate_limit:
        _LAST_A2_RATE_LIMIT_EXHAUSTED = False

    try:
        status, data = await _fire_inpi_get(url)
        if status == 429 and retry_on_rate_limit:
            for _attempt_idx, _delay in enumerate(_A2_RETRY_DELAYS, start=1):
                log.warning(
                    "inpi.rate_limited_retry",
                    query=query,
                    attempt=_attempt_idx,
                    delay_s=_delay,
                )
                await asyncio.sleep(_delay)  # OUTSIDE the lock
                status, data = await _fire_inpi_get(url)
                if status != 429:
                    break
            else:
                _LAST_A2_RATE_LIMIT_EXHAUSTED = True
    except Exception as exc:
        log.debug("inpi.search_by_name_error", query=query, error=str(exc))
        return None

    if status == 429:
        log.warning("inpi.rate_limited", query=query)
        return None
    if status != 200 or data is None:
        log.debug("inpi.search_by_name_http_error", query=query, status=status)
        return None

    results = data.get("results", [])
    if not results:
        log.debug("inpi.search_by_name_miss", query=query, dept=dept, cp=cp)
        return None

    hit = results[0]
    siren = hit.get("siren") or ""
    naf_code = hit.get("activite_principale") or None
    nom_complet = hit.get("nom_complet") or hit.get("nom_raison_sociale") or None
    hit_cp: str | None = None
    try:
        matching = hit.get("matching_etablissements") or hit.get("siege") or {}
        if isinstance(matching, list) and matching:
            matching = matching[0]
        hit_cp = (matching.get("code_postal") or "").strip() or None
    except Exception:
        hit_cp = None

    if not siren or len(siren) != 9:
        log.debug("inpi.search_by_name_miss", query=query, reason="invalid_siren")
        return None

    log.info("inpi.search_by_name_hit", query=query, siren=siren, nom=nom_complet, naf=naf_code)
    return (siren, naf_code, nom_complet, hit_cp or cp, hit)


async def fetch_dirigeants(
    siren: str,
    *,
    curl_client: CurlClient | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch official directors for a company from Recherche Entreprises API.

    Args:
        siren: 9-digit SIREN number.
        curl_client: Optional CurlClient instance. Currently unused (httpx is
            used directly) but kept for backward compatibility with the
            CurlClientError except branch.

    Returns:
        (dirigeants_list, company_data_dict). Empty containers if API fails or
        rate-limit retries exhaust.

    Bug C fix (Apr 28): on 429, retries 3× with 2s/5s/15s backoff (was 1× with
    3s). Lock released during retry sleeps.
    """
    own_client = curl_client is None
    url = f"{_API_URL}?q={siren}&per_page=1"

    try:
        status, data = await _fire_inpi_get(url)
        if status == 429:
            for _attempt_idx, _delay in enumerate(_A2_RETRY_DELAYS, start=1):
                log.warning(
                    "inpi.rate_limited_retry",
                    siren=siren,
                    attempt=_attempt_idx,
                    delay_s=_delay,
                )
                await asyncio.sleep(_delay)  # OUTSIDE the lock
                status, data = await _fire_inpi_get(url)
                if status != 429:
                    break

        if status != 200 or data is None:
            log.debug("recherche_entreprises.http_error", siren=siren, status=status)
            return [], {}

        results = data.get("results", [])
        if not results:
            log.debug("recherche_entreprises.no_results", siren=siren)
            return [], {}

        company = results[0]

        company_data: dict[str, Any] = parse_company_fields(company)

        dirigeants_raw = company.get("dirigeants", [])
        dirigeants: list[dict[str, Any]] = []

        for d in dirigeants_raw:
            if d.get("type_dirigeant") == "personne morale":
                continue

            nom = (d.get("nom") or "").strip().upper()
            prenom = (d.get("prenoms") or d.get("prenom") or "").strip().title()
            qualite = (d.get("qualite") or d.get("fonction") or "Dirigeant").strip()

            if not nom:
                continue

            civilite = None
            qualite_lower = qualite.lower()
            if "présidente" in qualite_lower or "directrice" in qualite_lower or "gérante" in qualite_lower:
                civilite = "Mme"
            elif "président" in qualite_lower or "directeur" in qualite_lower or "gérant" in qualite_lower:
                civilite = "M."

            annee_naissance = (d.get("annee_de_naissance") or "").strip() or None
            dirigeants.append({
                "nom": nom,
                "prenom": prenom if prenom else None,
                "qualite": qualite,
                "civilite": civilite,
                "annee_naissance": annee_naissance,
            })

        log.info(
            "recherche_entreprises.found",
            siren=siren,
            count=len(dirigeants),
            names=[f"{d.get('prenom', '')} {d['nom']}" for d in dirigeants[:3]],
            ca=company_data.get("chiffre_affaires"),
            effectif=company_data.get("tranche_effectif"),
        )

        return dirigeants, company_data

    except CurlClientError as exc:
        log.debug(
            "recherche_entreprises.network_error",
            siren=siren,
            error=str(exc),
        )
        return [], {}
    except Exception as exc:
        log.debug(
            "recherche_entreprises.unexpected_error",
            siren=siren,
            error=str(exc),
        )
        return [], {}
    finally:
        if own_client and curl_client is not None:
            await curl_client.close()
