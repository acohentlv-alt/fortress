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

_API_URL = "https://recherche-entreprises.api.gouv.fr/search"
_TIMEOUT = 3.0  # seconds — best-effort, never block the pipeline
_RATE_DELAY = 0.5  # ~2 req/s — the API claims 7/s but enforces stricter per-IP limits


async def search_by_name(
    query: str,
    dept: str | None = None,
    cp: str | None = None,
) -> tuple[str, str | None, str | None, str | None] | None:
    """Search for a company by name in Recherche Entreprises API.

    Args:
        query: Normalised company name to search for.
        dept: Department code (e.g. '66') — used if cp is None.
        cp: Postal code (e.g. '66300') — preferred over dept if provided.

    Returns:
        (siren, naf_code, nom_complet, cp) tuple on hit, or None.
    """
    params: list[str] = [f"q={query}", "per_page=3"]
    if cp:
        params.append(f"code_postal={cp}")
    elif dept:
        params.append(f"departement={dept}")
    url = f"{_API_URL}?" + "&".join(params)

    try:
        import httpx
        async with httpx.AsyncClient(timeout=_TIMEOUT) as hx:
            hx_resp = await hx.get(url)
            if hx_resp.status_code == 429:
                log.warning("inpi.rate_limited", query=query)
                await asyncio.sleep(_RATE_DELAY)
                return None
            if hx_resp.status_code != 200:
                log.debug("inpi.search_by_name_http_error", query=query, status=hx_resp.status_code)
                await asyncio.sleep(_RATE_DELAY)
                return None
            data = hx_resp.json()

        results = data.get("results", [])
        if not results:
            log.debug("inpi.search_by_name_miss", query=query, dept=dept, cp=cp)
            await asyncio.sleep(_RATE_DELAY)
            return None

        hit = results[0]
        siren = hit.get("siren") or ""
        naf_code = hit.get("activite_principale") or None
        nom_complet = hit.get("nom_complet") or hit.get("nom_raison_sociale") or None
        # Try to get a postal code from the hit for local SIRENE lookup
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
            await asyncio.sleep(_RATE_DELAY)
            return None

        log.info("inpi.search_by_name_hit", query=query, siren=siren, nom=nom_complet, naf=naf_code)
        await asyncio.sleep(_RATE_DELAY)
        return (siren, naf_code, nom_complet, hit_cp or cp)

    except Exception as exc:
        log.debug("inpi.search_by_name_error", query=query, error=str(exc))
        return None


async def fetch_dirigeants(
    siren: str,
    *,
    curl_client: CurlClient | None = None,
) -> list[dict[str, Any]]:
    """Fetch official directors for a company from Recherche Entreprises API.

    Args:
        siren: 9-digit SIREN number.
        curl_client: Optional CurlClient instance. If None, creates a temporary one.

    Returns:
        List of dicts with keys: nom, prenom, qualite (role).
        Empty list if API fails or returns no data.
    """
    url = f"{_API_URL}?q={siren}&per_page=1"

    own_client = curl_client is None

    try:
        # This is a public government API — no anti-bot protection needed.
        # Use httpx directly (reliable on all platforms) instead of curl_cffi
        # which fails on macOS and wastes an API call before falling back.
        import httpx
        async with httpx.AsyncClient(timeout=_TIMEOUT) as hx:
            hx_resp = await hx.get(url)
            if hx_resp.status_code == 429:
                # Rate limited — wait longer and retry once
                await asyncio.sleep(3.0)
                hx_resp = await hx.get(url)
            if hx_resp.status_code != 200:
                log.debug("recherche_entreprises.http_error", siren=siren, status=hx_resp.status_code)
                return [], {}
            data = hx_resp.json()
        results = data.get("results", [])
        if not results:
            log.debug("recherche_entreprises.no_results", siren=siren)
            return [], {}

        company = results[0]

        # ── Extract company-level data (revenue, effectif) ─────────
        company_data: dict[str, Any] = {}

        # Chiffre d'affaires (revenue) — latest year available
        finances = company.get("finances", {})
        if finances:
            # Get the most recent year's data
            latest_year = max(finances.keys(), default=None)
            if latest_year:
                ca = finances[latest_year].get("ca")
                resultat = finances[latest_year].get("resultat_net")
                if ca is not None:
                    company_data["chiffre_affaires"] = ca
                    company_data["ca_annee"] = latest_year
                if resultat is not None:
                    company_data["resultat_net"] = resultat

        # Tranche effectif salarié (official employee count code)
        effectif = company.get("tranche_effectif_salarie")
        if effectif:
            company_data["tranche_effectif"] = effectif

        # Catégorie entreprise (PME, ETI, GE)
        cat = company.get("categorie_entreprise")
        if cat:
            company_data["categorie_entreprise"] = cat

        # Nature juridique (legal form code — more precise than SIRENE)
        nature = company.get("nature_juridique")
        if nature:
            company_data["nature_juridique"] = nature

        # ── Extract dirigeants from the response ───────────────────
        dirigeants_raw = company.get("dirigeants", [])
        dirigeants: list[dict[str, Any]] = []

        for d in dirigeants_raw:
            # Skip personne morale entries (auditors etc.)
            if d.get("type_dirigeant") == "personne morale":
                continue

            nom = (d.get("nom") or "").strip().upper()
            prenom = (d.get("prenoms") or d.get("prenom") or "").strip().title()
            qualite = (d.get("qualite") or d.get("fonction") or "Dirigeant").strip()

            if not nom:
                continue

            # Determine civilité from qualité or other hints
            civilite = None
            qualite_lower = qualite.lower()
            if "présidente" in qualite_lower or "directrice" in qualite_lower or "gérante" in qualite_lower:
                civilite = "Mme"
            elif "président" in qualite_lower or "directeur" in qualite_lower or "gérant" in qualite_lower:
                civilite = "M."

            dirigeants.append({
                "nom": nom,
                "prenom": prenom if prenom else None,
                "qualite": qualite,
                "civilite": civilite,
            })

        log.info(
            "recherche_entreprises.found",
            siren=siren,
            count=len(dirigeants),
            names=[f"{d.get('prenom', '')} {d['nom']}" for d in dirigeants[:3]],
            ca=company_data.get("chiffre_affaires"),
            effectif=company_data.get("tranche_effectif"),
        )

        # Rate limit delay
        await asyncio.sleep(_RATE_DELAY)

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
