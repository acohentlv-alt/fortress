"""Company API routes — search, detail, and on-demand enrichment.

Search is scoped to scraped companies (via batch_tags) for performance.
Detail view retrieves full data including enriched fields:
  Forme Juridique, Code NAF, Headcount, Dirigeants, Revenue (nullable).
"""

import subprocess
import sys
from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from fortress.api.db import fetch_all, fetch_one, get_conn

router = APIRouter(prefix="/api/companies", tags=["companies"])

# TTL cooldown for deduplication (seconds): 24 hours
_DEDUP_TTL_HOURS = 24


# ---------------------------------------------------------------------------
# Enrich endpoint models & constants
# ---------------------------------------------------------------------------

_VALID_MODULES = {"contact_web", "contact_phone", "financials"}


class EnrichRequest(BaseModel):
    """JSON body for POST /api/companies/{siren}/enrich."""
    target_modules: list[str] = Field(
        ...,
        min_length=1,
        description="Modules to run: contact_web, contact_phone, financials",
    )


# ---------------------------------------------------------------------------
# Sort helpers (SQL-injection-safe whitelist)
# ---------------------------------------------------------------------------

_SORT_COLUMNS = {
    "denomination": "co.denomination",
    "naf": "co.naf_code",
    "siren": "co.siren",
    "ville": "co.ville",
    "departement": "co.departement",
}


# ---------------------------------------------------------------------------
# Action 0: Inline field editing (manual overrides)
# ---------------------------------------------------------------------------

_COMPANY_FIELDS = {"denomination", "adresse", "code_postal", "ville"}
_CONTACT_FIELDS = {"phone", "email", "website", "social_linkedin", "social_facebook", "social_twitter", "social_instagram", "social_tiktok"}
_EDITABLE_FIELDS = _COMPANY_FIELDS | _CONTACT_FIELDS


@router.patch("/{siren}")
async def update_company_fields(siren: str, body: dict):
    """Update individual fields on a company. Used by inline edit UI.

    Accepts JSON with field→value pairs, e.g. {"phone": "+33 4 68 00 00 00"}.
    Company fields update `companies` table, contact fields upsert into `contacts`.
    All edits are logged to `batch_log` with action='manual_edit'.
    """
    # Validate company exists
    company = await fetch_one(
        "SELECT siren FROM companies WHERE siren = %s", (siren,)
    )
    if not company:
        return JSONResponse(
            status_code=404,
            content={"error": "Company not found", "siren": siren},
        )

    # Filter to allowed fields only
    updates = {k: v for k, v in body.items() if k in _EDITABLE_FIELDS}
    if not updates:
        return JSONResponse(
            status_code=422,
            content={"error": "No valid fields provided", "allowed": sorted(_EDITABLE_FIELDS)},
        )

    co_updates = {k: v for k, v in updates.items() if k in _COMPANY_FIELDS}
    ct_updates = {k: v for k, v in updates.items() if k in _CONTACT_FIELDS}
    saved = []

    async with get_conn() as conn:
        # ── Read current values for before/after audit trail ─────
        before_values = {}
        if ct_updates:
            existing = await (await conn.execute(
                "SELECT phone, email, website, social_linkedin, social_facebook, social_twitter, social_instagram, social_tiktok FROM contacts WHERE siren = %s ORDER BY collected_at DESC LIMIT 1",
                (siren,),
            )).fetchone()
            if existing:
                field_map = ["phone", "email", "website", "social_linkedin", "social_facebook", "social_twitter", "social_instagram", "social_tiktok"]
                for i, f in enumerate(field_map):
                    if f in ct_updates and existing[i]:
                        before_values[f] = existing[i]

        # Update companies table
        if co_updates:
            set_parts = [f"{k} = %s" for k in co_updates]
            vals = list(co_updates.values()) + [siren]
            await conn.execute(
                f"UPDATE companies SET {', '.join(set_parts)}, updated_at = NOW() WHERE siren = %s",
                tuple(vals),
            )
            saved.extend(co_updates.keys())

        # Upsert contacts table (source='manual_edit')
        if ct_updates:
            columns = ["siren", "source"] + list(ct_updates.keys())
            placeholders = ["%s"] * len(columns)
            values = [siren, "manual_edit"] + list(ct_updates.values())
            on_conflict = ", ".join(f"{k} = EXCLUDED.{k}" for k in ct_updates)
            await conn.execute(f"""
                INSERT INTO contacts ({', '.join(columns)}, collected_at)
                VALUES ({', '.join(placeholders)}, NOW())
                ON CONFLICT (siren, source) DO UPDATE SET {on_conflict}, collected_at = NOW()
            """, tuple(values))
            saved.extend(ct_updates.keys())

        # Audit trail with before/after
        for field, value in updates.items():
            old_val = before_values.get(field)
            if old_val:
                log_entry = f"{field}: {old_val} → {value}"
            else:
                log_entry = f"{field}={value}"
            await conn.execute("""
                INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp)
                VALUES ('MANUAL_EDIT', %s, 'manual_edit', 'success', %s, NOW())
            """, (siren, log_entry))

        await conn.commit()

    return {"updated": sorted(saved), "siren": siren}


# ---------------------------------------------------------------------------
# Action 1: On-demand enrichment with deduplication
# ---------------------------------------------------------------------------

@router.post("/{siren}/enrich", status_code=202)
async def enrich_company(siren: str, body: EnrichRequest):
    """Queue targeted enrichment modules for a single company.

    Modules:
      - contact_web:   Web search + Playwright crawler
      - contact_phone: PagesJaunes / Directory scraper
      - financials:    INPI / Recherche Entreprises API

    Deduplication: Checks existing data before queueing.
    Returns 202 Accepted with which modules were queued.
    """
    # Validate company exists (indexed lookup on companies.siren PK)
    company = await fetch_one(
        "SELECT siren, denomination FROM companies WHERE siren = %s", (siren,)
    )
    if not company:
        return JSONResponse(
            status_code=404,
            content={"error": "Company not found", "siren": siren},
        )

    # Partition into valid and invalid modules
    requested = set(body.target_modules)
    valid = requested & _VALID_MODULES
    invalid = sorted(requested - _VALID_MODULES)

    if not valid:
        return JSONResponse(
            status_code=422,
            content={
                "error": "No valid modules provided",
                "valid_modules": sorted(_VALID_MODULES),
                "received": body.target_modules,
            },
        )

    # ── Deduplication: check what's already enriched ──────────────
    already_enriched: list[str] = []

    # ── TTL-based recency check: skip if scraped in last 24h ─────
    recent_scrape = await fetch_one("""
        SELECT action, timestamp FROM batch_log
        WHERE siren = %s AND result = 'success'
          AND timestamp > NOW() - INTERVAL '%s hours'
        ORDER BY timestamp DESC LIMIT 1
    """ % ('%s', _DEDUP_TTL_HOURS), (siren,))

    if recent_scrape:
        # If scraped recently with success, skip all modules
        return {
            "message": f"Already enriched within the last {_DEDUP_TTL_HOURS}h — cached result",
            "queued": [],
            "skipped": sorted(valid),
            "siren": siren,
            "cached": True,
            "last_scrape": recent_scrape.get("timestamp"),
        }

    if "contact_phone" in valid or "contact_web" in valid:
        existing_contact = await fetch_one(
            "SELECT phone, email, website FROM contacts WHERE siren = %s LIMIT 1",
            (siren,),
        )
        if existing_contact:
            if "contact_phone" in valid and existing_contact.get("phone"):
                already_enriched.append("contact_phone")
            if "contact_web" in valid and (
                existing_contact.get("email") or existing_contact.get("website")
            ):
                already_enriched.append("contact_web")

    if "financials" in valid:
        existing_officer = await fetch_one(
            "SELECT 1 FROM officers WHERE siren = %s LIMIT 1", (siren,),
        )
        if existing_officer:
            already_enriched.append("financials")

    # Remove already-enriched modules from the queue
    queued = sorted(valid - set(already_enriched))
    skipped = sorted(set(already_enriched) | set(invalid))

    if not queued:
        return {
            "message": "All requested modules already enriched — nothing to queue",
            "queued": [],
            "skipped": skipped,
            "siren": siren,
        }

    # ── Background dispatch: create micro scrape_job + spawn runner ─
    batch_id = f"ENRICH_{siren}"

    # Create or reset the micro job (batch_id is indexed but not UNIQUE)
    try:
        async with get_conn() as conn:
            existing_job = await conn.execute(
                "SELECT id FROM batch_data WHERE batch_id = %s LIMIT 1",
                (batch_id,),
            )
            row = await existing_job.fetchone()

            if row:
                # Reset existing job to 'queued'
                await conn.execute(
                    "UPDATE batch_data SET status = 'queued', updated_at = NOW() WHERE batch_id = %s",
                    (batch_id,),
                )
            else:
                # Create new micro job
                await conn.execute("""
                    INSERT INTO batch_data
                        (batch_id, batch_name, status, batch_number, batch_offset, total_companies)
                    VALUES (%s, %s, 'queued', 1, 0, 1)
                """, (batch_id, f"enrich {siren}"))

            # Ensure the company is tagged for this micro-job
            tag_exists = await conn.execute(
                "SELECT 1 FROM batch_tags WHERE siren = %s AND batch_name = %s LIMIT 1",
                (siren, f"enrich {siren}"),
            )
            if not await tag_exists.fetchone():
                await conn.execute(
                    "INSERT INTO batch_tags (siren, batch_name, tagged_at) VALUES (%s, %s, NOW())",
                    (siren, f"enrich {siren}"),
                )

            await conn.commit()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to create enrich job: {exc}", "siren": siren},
        )

    # Spawn runner subprocess — redirect output to log file for crash diagnosis
    fortress_root = Path(__file__).resolve().parent.parent.parent
    log_dir = fortress_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{batch_id}.log"
    try:
        log_fh = open(log_path, "a")
        proc = subprocess.Popen(
            [sys.executable, "-m", "fortress.runner", batch_id],
            cwd=str(fortress_root.parent),  # Must be PARENT of fortress/ so `-m fortress.runner` resolves
            stdout=log_fh,
            stderr=log_fh,
            start_new_session=True,
        )
        pid = proc.pid
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to spawn runner: {exc}", "siren": siren},
        )

    return {
        "message": f"{len(queued)} module(s) queued for enrichment",
        "queued": queued,
        "skipped": skipped,
        "siren": siren,
        "denomination": company["denomination"],
        "batch_id": batch_id,
        "pid": pid,
    }


# ---------------------------------------------------------------------------
# Action 1.5: Synchronous, single-company website crawl
# ---------------------------------------------------------------------------

@router.post("/{siren}/crawl-website")
async def crawl_website_sync(siren: str):
    """Synchronously crawl a known website to extract email/phone/socials.
    
    Bypasses the `fortress.runner` batch queue entirely.
    """
    company = await fetch_one(
        "SELECT siren, denomination, departement FROM companies WHERE siren = %s", (siren,)
    )
    if not company:
        return JSONResponse(status_code=404, content={"error": "Company not found", "siren": siren})

    company_name = company.get("denomination") or ""
    departement = company.get("departement") or ""

    contact = await fetch_one(
        "SELECT website FROM contacts WHERE siren = %s AND website IS NOT NULL ORDER BY collected_at DESC LIMIT 1",
        (siren,)
    )
    if not contact or not contact.get("website"):
        return JSONResponse(status_code=400, content={"error": "Company has no known website to crawl"})

    website = contact["website"]

    from fortress.module_c.curl_client import CurlClient, CurlClientError
    from fortress.module_b.contact_parser import (
        extract_emails, extract_phones, extract_social_links, extract_siret
    )
    from urllib.parse import urlparse, urljoin
    import time
    import re as _re

    all_emails = []
    all_phones = []
    all_social = {}
    all_html = []  # Store raw HTML for SIRET extraction
    
    try:
        parsed = urlparse(website)
        root_url = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        root_url = website

    # ── Smart page discovery ────────────────────────────────────
    # Contact-related keywords to look for in <a href> links
    _CONTACT_KEYWORDS = _re.compile(
        r"contact|mention|legal|propos|equipe|coordonn|societe|qui-sommes|nous-contacter|impressum",
        _re.IGNORECASE,
    )

    # Start with homepage + common fallback paths
    seed_pages = [
        root_url,
        f"{root_url}/contact",
        f"{root_url}/mentions-legales",
    ]
    crawled_urls: set[str] = set()
    discovered_pages: list[str] = []

    t0 = time.monotonic()
    WALL_CLOCK_LIMIT = 13.0  # Leave 2s buffer for DB ops within 15s total
    MAX_PAGES = 10

    curl_client = CurlClient(timeout=5.0, max_retries=1, delay_min=0.2, delay_max=0.4, delay_jitter=0.0)
    
    async with curl_client as client:
        # Phase 1: Crawl homepage to discover real nav links
        try:
            resp = await client.get(root_url)
            crawled_urls.add(root_url)
            if resp.status_code == 200 and len(resp.text) > 500:
                all_emails.extend(extract_emails(resp.text))
                all_phones.extend(extract_phones(resp.text))
                all_social.update(extract_social_links(resp.text))
                all_html.append(resp.text)

                # Discover contact-related links from homepage
                for href_match in _re.finditer(r'href=["\']([^"\']+)["\']', resp.text):
                    href = href_match.group(1)
                    if _CONTACT_KEYWORDS.search(href):
                        # Resolve relative URLs
                        abs_url = urljoin(root_url, href)
                        if abs_url.startswith(root_url) and abs_url not in crawled_urls:
                            discovered_pages.append(abs_url)
        except CurlClientError as exc:
            err_str = str(exc).lower()
            if "resolve" in err_str or "ssl" in err_str or "certificate" in err_str:
                # DNS/SSL failure — site is unreachable, skip everything
                discovered_pages.clear()
                seed_pages.clear()

        # Phase 2: Crawl seed paths + discovered links
        pages_to_crawl = []
        for url in seed_pages[1:]:  # Skip homepage (already crawled)
            if url not in crawled_urls:
                pages_to_crawl.append(url)
        # Discovered pages go after seeds (they're more likely to succeed)
        for url in discovered_pages:
            if url not in crawled_urls and url not in pages_to_crawl:
                pages_to_crawl.append(url)

        for page_url in pages_to_crawl[:MAX_PAGES - 1]:  # -1 for homepage
            if time.monotonic() - t0 > WALL_CLOCK_LIMIT:
                break
            if page_url in crawled_urls:
                continue
            crawled_urls.add(page_url)
            try:
                resp = await client.get(page_url)
                if resp.status_code == 200 and len(resp.text) > 500:
                    all_emails.extend(extract_emails(resp.text))
                    all_phones.extend(extract_phones(resp.text))
                    all_social.update(extract_social_links(resp.text))
                    all_html.append(resp.text)
            except CurlClientError:
                continue  # Page doesn't exist or timed out, try next

    # ── SIRET extraction from all crawled HTML ──────────────────
    found_siren = None
    for html_chunk in all_html:
        s = extract_siret(html_chunk)
        if s:
            found_siren = s
            break
    
    # Select best email and phone — now with company context
    from fortress.module_d.enricher import _best_email, _best_phone
    from fortress.module_b.contact_parser import is_agency_email
    best_email = _best_email(list(set(all_emails)), root_url, siren, company_name=company_name)
    
    # ── Fix 1: Agency email rejection (domain mismatch) ─────────
    rejected_email = None
    if best_email and is_agency_email(best_email, website):
        rejected_email = best_email
        best_email = None  # Reject the agency email

    # Geographic phone priority — département-aware
    best_phone = _best_phone(list(set(all_phones)), siren, departement=departement)
    
    # Google Maps URL is extracted for display but NOT saved to contacts (no column)
    found_gmaps_url = all_social.pop("google_maps", None)

    extracted = {
        "email": best_email,
        "phone": best_phone,
        "social_linkedin": all_social.get("linkedin"),
        "social_facebook": all_social.get("facebook"),
        "social_twitter": all_social.get("twitter"),
        "social_instagram": all_social.get("instagram"),
        "social_tiktok": all_social.get("tiktok"),
    }
    # Filter out empty values
    extracted = {k: v for k, v in extracted.items() if v}
    
    if not extracted and not rejected_email:
        return {"siren": siren, "message": "Aucun contact trouvé sur le site", "extracted": {}}

    duration = int((time.monotonic() - t0) * 1000)

    # ── Fix 2: Protect Maps/Upload data from overwrite ──────────
    # Read existing trusted data BEFORE upserting
    siret_merge_info = None
    async with get_conn() as conn:
        existing_rows = await (await conn.execute(
            "SELECT phone, email, source FROM contacts WHERE siren = %s AND source IN ('google_maps', 'upload', 'client_upload', 'manual_edit')",
            (siren,),
        )).fetchall()
        
        existing_phone = None
        existing_email = None
        for row in (existing_rows or []):
            if row[0] and not existing_phone:
                existing_phone = row[0]
            if row[1] and not existing_email:
                existing_email = row[1]

        # Don't overwrite trusted phone/email with crawler data
        protected_fields = []
        if existing_phone and "phone" in extracted:
            protected_fields.append(f"phone kept: {existing_phone} (crawler found: {extracted['phone']})")
            del extracted["phone"]
        if existing_email and "email" in extracted:
            protected_fields.append(f"email kept: {existing_email} (crawler found: {extracted['email']})")
            del extracted["email"]

        # ── SIRET merge: link MAPS entity to real company ────────
        if found_siren and found_siren != siren:
            real_co = await (await conn.execute(
                "SELECT siren, denomination, departement, adresse, code_naf, forme_juridique, effectif FROM companies WHERE siren = %s", (found_siren,)
            )).fetchone()
            if real_co:
                if isinstance(real_co, tuple):
                    real_name = real_co[1]
                else:
                    real_name = real_co.get("denomination")
                siret_merge_info = {
                    "real_siren": found_siren,
                    "real_name": real_name,
                }
                # Copy crawl data to the real SIREN's contacts too
                if extracted:
                    merge_cols = ["siren", "source", "website"] + list(extracted.keys())
                    merge_phs = ["%s"] * len(merge_cols)
                    merge_vals = [found_siren, "website_crawl", website] + list(extracted.values())
                    merge_conflict = ", ".join(f"{k} = EXCLUDED.{k}" for k in extracted)
                    await conn.execute(f"""
                        INSERT INTO contacts ({', '.join(merge_cols)}, collected_at)
                        VALUES ({', '.join(merge_phs)}, NOW())
                        ON CONFLICT (siren, source) DO UPDATE SET {merge_conflict}, collected_at = NOW()
                    """, tuple(merge_vals))

                # If current entity is a MAPS discovery, update it with real company data
                if siren.startswith("MAPS"):
                    # Copy MAPS contacts to the real SIREN
                    maps_contacts = await (await conn.execute(
                        "SELECT phone, email, website, social_linkedin, social_facebook, social_twitter, social_instagram, social_tiktok, source FROM contacts WHERE siren = %s",
                        (siren,)
                    )).fetchall()
                    for mc in (maps_contacts or []):
                        mc_source = mc[8] if isinstance(mc, tuple) else mc.get("source")
                        mc_fields = {}
                        field_names = ["phone", "email", "website", "social_linkedin", "social_facebook", "social_twitter", "social_instagram", "social_tiktok"]
                        for i, fname in enumerate(field_names):
                            val = mc[i] if isinstance(mc, tuple) else mc.get(fname)
                            if val:
                                mc_fields[fname] = val
                        if mc_fields:
                            mc_cols = ["siren", "source"] + list(mc_fields.keys())
                            mc_phs = ["%s"] * len(mc_cols)
                            mc_vals = [found_siren, mc_source] + list(mc_fields.values())
                            mc_conflict = ", ".join(f"{k} = EXCLUDED.{k}" for k in mc_fields)
                            await conn.execute(f"""
                                INSERT INTO contacts ({', '.join(mc_cols)}, collected_at)
                                VALUES ({', '.join(mc_phs)}, NOW())
                                ON CONFLICT (siren, source) DO UPDATE SET {mc_conflict}, collected_at = NOW()
                            """, tuple(mc_vals))
                    siret_merge_info["maps_contacts_merged"] = len(maps_contacts or [])

                # Log the SIRET linkage
                await conn.execute("""
                    INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp)
                    VALUES ('SYNC_CRAWL', %s, 'siret_match', 'success', %s, NOW())
                """, (siren, f"SIRET found on website → real SIREN {found_siren} ({real_name})"))

        # ── Fix 4: Before/after audit trail ──────────────────────
        # Build detailed log with before/after and rejections
        log_parts = []
        if rejected_email:
            log_parts.append(f"REJECTED email: {rejected_email} (domain ≠ {root_url})")
        for note in protected_fields:
            log_parts.append(f"PROTECTED {note}")
        for k, v in extracted.items():
            log_parts.append(f"ADDED {k}={v}")
        if found_siren:
            log_parts.append(f"SIRET: {found_siren}")
        log_parts.append(f"pages: {len(crawled_urls)} crawled, {len(discovered_pages)} discovered")
        
        found_data = ", ".join(log_parts) if log_parts else "no new data"

        if extracted:
            columns = ["siren", "source", "website"] + list(extracted.keys())
            placeholders = ["%s"] * len(columns)
            values = [siren, "website_crawl", website] + list(extracted.values())
            on_conflict = ", ".join(f"{k} = EXCLUDED.{k}" for k in extracted)
            
            await conn.execute(f"""
                INSERT INTO contacts ({', '.join(columns)}, collected_at)
                VALUES ({', '.join(placeholders)}, NOW())
                ON CONFLICT (siren, source) DO UPDATE SET {on_conflict}, collected_at = NOW()
            """, tuple(values))

        # Log to batch_log (always log, even if some data was rejected/protected)
        await conn.execute("""
            INSERT INTO batch_log (batch_id, siren, action, result, source_url, duration_ms, timestamp)
            VALUES ('SYNC_CRAWL', %s, 'website_crawl', %s, %s, %s, NOW())
        """, (siren, 'success' if extracted else 'filtered', found_data, duration))

        await conn.commit()

    # Build user-facing message
    warnings = []
    if rejected_email:
        warnings.append(f"Email {rejected_email} rejeté (domaine du développeur web)")
    if protected_fields:
        warnings.append("Données Maps/import existantes protégées")
    
    message = "Enrichissement terminé"
    if extracted:
        message += f" — {len(extracted)} données ajoutées"
    if siret_merge_info:
        message += f" — SIRET trouvé: lié à {siret_merge_info['real_name']} ({siret_merge_info['real_siren']})"
    if warnings:
        message += f" ⚠️ {'; '.join(warnings)}"
    if not extracted and not warnings and not siret_merge_info:
        message = "Aucun contact trouvé sur le site"

    return {
        "siren": siren,
        "message": message,
        "extracted": extracted,
        "rejected": {"email": rejected_email} if rejected_email else {},
        "protected": protected_fields,
        "siret_match": siret_merge_info,
        "pages_crawled": len(crawled_urls),
        "pages_discovered": len(discovered_pages),
    }


# ---------------------------------------------------------------------------
# Action 2: Search with NAF code + sorting
# ---------------------------------------------------------------------------

@router.get("/search")
async def search_companies(
    q: str = Query(..., min_length=1, description="Search by name, SIREN, or NAF code"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    sort_by: str = Query("denomination", description="Sort by: denomination, naf, siren, ville, departement"),
    order: str = Query("asc", description="Sort order: asc or desc"),
    department: str = Query(None, description="Filter by department code (e.g. 66, 31)"),
    sector: str = Query(None, description="Filter by sector/batch_name (e.g. logistique, agriculture)"),
    min_rating: float = Query(None, ge=0, le=5, description="Minimum Google Maps rating (e.g. 4.0)"),
    min_reviews: int = Query(None, ge=0, description="Minimum number of Google Maps reviews"),
):
    """Search for companies by name, SIREN, or NAF code — scoped to scraped data.

    Supports sorting via sort_by and order parameters.
    Supports filtering by department and sector.
    Search string is normalized with UPPER() for case-insensitive matching.
    NAF code is matched via OR condition alongside denomination.
    """
    # Resolve sort clause (whitelist prevents SQL injection)
    sort_col = _SORT_COLUMNS.get(sort_by, "co.denomination")
    sort_dir = "DESC" if order.lower() == "desc" else "ASC"
    sort_clause = f"{sort_col} {sort_dir}"

    clean_q = q.strip().replace(" ", "")
    if clean_q.isdigit() and len(clean_q) == 9:
        # Exact SIREN search (indexed on companies.siren PK)
        rows = await fetch_all(f"""
            SELECT
                co.siren, co.denomination, co.naf_code, co.naf_libelle,
                co.forme_juridique, co.tranche_effectif,
                co.ville, co.departement, co.statut,
                ct.phone, ct.email, ct.website
            FROM batch_tags qt
            JOIN companies co ON co.siren = qt.siren
            LEFT JOIN LATERAL (
                SELECT * FROM contacts c2
                WHERE c2.siren = co.siren
                ORDER BY (CASE WHEN c2.phone IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN c2.email IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
                LIMIT 1
            ) ct ON true
            WHERE co.siren = %s
            GROUP BY co.siren, co.denomination, co.naf_code, co.naf_libelle,
                     co.forme_juridique, co.tranche_effectif,
                     co.ville, co.departement, co.statut,
                     ct.phone, ct.email, ct.website
            ORDER BY {sort_clause}
        """, (clean_q,))
    else:
        # Name / NAF code search — indexed via batch_tags.siren
        like_param = f"%{q.strip()}%"

        # Build dynamic WHERE filters
        where_parts = [
            "(UPPER(co.denomination) LIKE UPPER(%s) OR co.naf_code ILIKE %s)"
        ]
        params: list = [like_param, like_param]

        if department:
            where_parts.append("co.departement = %s")
            params.append(department.strip())

        if sector:
            where_parts.append("UPPER(qt.batch_name) LIKE UPPER(%s)")
            params.append(f"%{sector.strip()}%")

        if min_rating is not None:
            where_parts.append("ct.rating >= %s")
            params.append(min_rating)

        if min_reviews is not None:
            where_parts.append("ct.review_count >= %s")
            params.append(min_reviews)

        where_clause = " AND ".join(where_parts)
        params.append(limit)
        params.append(offset)

        rows = await fetch_all(f"""
            SELECT DISTINCT ON (co.siren)
                co.siren, co.denomination, co.naf_code, co.naf_libelle,
                co.forme_juridique, co.tranche_effectif,
                co.ville, co.departement, co.statut,
                ct.phone, ct.email, ct.website
            FROM batch_tags qt
            JOIN companies co ON co.siren = qt.siren
            LEFT JOIN LATERAL (
                SELECT * FROM contacts c2
                WHERE c2.siren = co.siren
                ORDER BY (CASE WHEN c2.phone IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN c2.email IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
                LIMIT 1
            ) ct ON true
            WHERE {where_clause}
            ORDER BY co.siren, co.denomination
            LIMIT %s OFFSET %s
        """, tuple(params))

    # Re-sort after DISTINCT ON (which forces its own ORDER BY)
    if sort_by != "denomination":
        reverse = order.lower() == "desc"
        rows.sort(key=lambda r: (r.get(sort_by) or ""), reverse=reverse)

    return {"results": rows, "count": len(rows), "offset": offset, "limit": limit}

# ---------------------------------------------------------------------------
# Enrich History — dedicated GET endpoint for the timeline UI
# Must be defined BEFORE the catch-all GET /{siren} route
# ---------------------------------------------------------------------------

@router.get("/{siren}/enrich-history")
async def get_enrich_history(siren: str):
    """Return enrichment audit trail for a single company.

    Queries batch_log table (indexed on siren).
    Used by the frontend's Smart Enrichment Panel timeline.
    """
    rows = await fetch_all("""
        SELECT
            action, result, source_url, duration_ms, timestamp
        FROM batch_log
        WHERE siren = %s
        ORDER BY timestamp DESC
    """, (siren,))
    return {"siren": siren, "history": rows, "count": len(rows)}


@router.delete("/{siren}/tags/{batch_name}")
async def untag_company(siren: str, batch_name: str):
    """Remove a company from a batch's results (untag only — never deletes data)."""
    async with get_conn() as conn:
        result = await conn.execute(
            "DELETE FROM batch_tags WHERE siren = %s AND batch_name = %s RETURNING siren",
            (siren, batch_name),
        )
        row = await result.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Tag introuvable"})
        await conn.commit()
    return {"untagged": True, "siren": siren, "batch_name": batch_name}


@router.delete("/{siren}/tags/")
async def untag_company_all(siren: str):
    """Remove a company from ALL query results (all tags). Never deletes data."""
    async with get_conn() as conn:
        result = await conn.execute(
            "DELETE FROM batch_tags WHERE siren = %s RETURNING siren",
            (siren,),
        )
        rows = await result.fetchall()
        count = len(rows)
        if count == 0:
            return JSONResponse(status_code=404, content={"error": "No tags found for this SIREN"})
        await conn.commit()
    return {"untagged": True, "siren": siren, "removed_count": count}


@router.get("/{siren}")
async def get_company(siren: str):
    """Full company detail with enriched data, contacts, and officers."""
    company = await fetch_one("""
        SELECT
            co.siren, co.siret_siege, co.denomination, co.enseigne,
            co.naf_code, co.naf_libelle, co.forme_juridique,
            co.adresse, co.code_postal, co.ville,
            co.departement, co.region, co.statut,
            co.date_creation, co.tranche_effectif, co.effectif_exact,
            co.latitude, co.longitude, co.fortress_id,
            co.chiffre_affaires, co.annee_ca, co.tranche_ca,
            co.date_fondation, co.type_etablissement,
            co.extra_data,
            co.created_at, co.updated_at
        FROM companies co
        WHERE co.siren = %s
    """, (siren,))

    if not company:
        return JSONResponse(status_code=404, content={"error": "Company not found", "siren": siren})

    # All contacts from different sources
    contacts = await fetch_all("""
        SELECT
            phone, email, email_type, website, source,
            social_linkedin, social_facebook, social_twitter,
            social_instagram, social_tiktok,
            rating, review_count, maps_url, address, collected_at
        FROM contacts
        WHERE siren = %s
        ORDER BY collected_at DESC
    """, (siren,))

    # Officers / Dirigeants
    officers = await fetch_all("""
        SELECT nom, prenom, role, source,
               civilite, email_direct, ligne_directe,
               code_fonction, type_fonction
        FROM officers
        WHERE siren = %s
        ORDER BY nom
    """, (siren,))

    # Merge best contacts
    merged = _merge_contacts(contacts)

    # Query tags (which jobs found this company)
    tags = await fetch_all("""
        SELECT batch_name, tagged_at
        FROM batch_tags
        WHERE siren = %s
        ORDER BY tagged_at DESC
    """, (siren,))

    # Enrichment audit trail (which agents enriched this company)
    enrichment_history = await fetch_all("""
        SELECT
            action, result, source_url, duration_ms, timestamp
        FROM batch_log
        WHERE siren = %s
        ORDER BY timestamp DESC
    """, (siren,))

    # CRM Notes
    notes = await fetch_all("""
        SELECT id, user_id, username, text, created_at
        FROM company_notes
        WHERE siren = %s
        ORDER BY created_at DESC
    """, (siren,))

    return {
        "company": company,
        "contacts": contacts,
        "merged_contact": merged,
        "officers": officers,
        "batch_tags": tags,
        "enrichment_history": enrichment_history,
        "notes": notes or [],
    }


def _merge_contacts(contacts: list[dict]) -> dict:
    """Merge all contact rows into a single best-of dict with per-field provenance."""
    merged = {
        "phone": None, "phone_source": None,
        "email": None, "email_source": None,
        "email_type": None,
        "website": None, "website_source": None,
        "social_linkedin": None, "social_linkedin_source": None,
        "social_facebook": None, "social_facebook_source": None,
        "social_twitter": None, "social_twitter_source": None,
        "social_instagram": None, "social_instagram_source": None,
        "social_tiktok": None, "social_tiktok_source": None,
        "rating": None, "rating_source": None,
        "review_count": None,
        "maps_url": None, "maps_url_source": None,
        "address": None, "address_source": None,
        "sources": [],
    }
    for c in contacts:
        src = c.get("source")
        if src:
            merged["sources"].append(src)
        for key in ("phone", "email", "website", "social_linkedin",
                     "social_facebook", "social_twitter", "social_instagram",
                     "social_tiktok", "maps_url", "address"):
            if merged[key] is None and c.get(key):
                merged[key] = c[key]
                merged[f"{key}_source"] = src
        if merged["email_type"] is None and c.get("email_type"):
            merged["email_type"] = c["email_type"]
        if merged["rating"] is None and c.get("rating"):
            merged["rating"] = c["rating"]
            merged["review_count"] = c.get("review_count")
            merged["rating_source"] = src

    merged["sources"] = list(set(merged["sources"]))
    return merged
