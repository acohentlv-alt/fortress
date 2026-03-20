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

from fortress.module_c.curl_client import CurlClient, CurlClientError

log = structlog.get_logger(__name__)

_API_URL = "https://recherche-entreprises.api.gouv.fr/search"
_TIMEOUT = 3.0  # seconds — best-effort, never block the pipeline
_RATE_DELAY = 0.15  # ~7 req/s


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
    if own_client:
        curl_client = CurlClient(timeout=_TIMEOUT, max_retries=0, delay_min=0, delay_max=0)

    try:
        resp = await curl_client.get(url)

        if resp.status_code != 200:
            log.debug(
                "recherche_entreprises.http_error",
                siren=siren,
                status=resp.status_code,
            )
            return []

        data = resp.json()
        results = data.get("results", [])
        if not results:
            log.debug("recherche_entreprises.no_results", siren=siren)
            return []

        company = results[0]

        # Extract dirigeants from the response
        dirigeants_raw = company.get("dirigeants", [])
        dirigeants: list[dict[str, Any]] = []

        for d in dirigeants_raw:
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
        )

        # Rate limit delay
        await asyncio.sleep(_RATE_DELAY)

        return dirigeants

    except CurlClientError as exc:
        log.debug(
            "recherche_entreprises.network_error",
            siren=siren,
            error=str(exc),
        )
        return []
    except Exception as exc:
        log.debug(
            "recherche_entreprises.unexpected_error",
            siren=siren,
            error=str(exc),
        )
        return []
    finally:
        if own_client:
            await curl_client.close()
