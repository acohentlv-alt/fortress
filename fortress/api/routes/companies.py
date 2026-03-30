"""Company API routes — search, detail, and on-demand enrichment.

Search is scoped to scraped companies (via batch_tags) for performance.
Detail view retrieves full data including enriched fields:
  Forme Juridique, Code NAF, Headcount, Dirigeants, Revenue (nullable).
"""

import subprocess
import sys
from pathlib import Path

from fastapi import APIRouter, Query, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uuid
import datetime

from fortress.api.db import fetch_all, fetch_one, get_conn
from fortress.api.routes.activity import log_activity

router = APIRouter(prefix="/api/companies", tags=["companies"])

# TTL cooldown for deduplication (seconds): 24 hours
_DEDUP_TTL_HOURS = 24


def _get_workspace_filter(request):
    """Return (is_admin, workspace_id) from the request user."""
    user = getattr(request.state, "user", None)
    if not user:
        return False, None
    return user.is_admin, getattr(user, "workspace_id", None)


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

class DeepEnrichRequest(BaseModel):
    sirens: list[str] = Field(
        ...,
        min_length=1,
        max_length=20,
        description="List of exactly 1 to 20 SIRENs to deeply enrich",
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
_CONTACT_FIELDS = {"phone", "email", "website", "address", "social_linkedin", "social_facebook", "social_twitter", "social_instagram", "social_tiktok", "social_whatsapp", "social_youtube"}
_EDITABLE_FIELDS = _COMPANY_FIELDS | _CONTACT_FIELDS


@router.patch("/{siren}")
async def update_company_fields(siren: str, body: dict, request: Request):
    """Update individual fields on a company. Used by inline edit UI.

    Accepts JSON with field→value pairs, e.g. {"phone": "+33 4 68 00 00 00"}.
    Company fields update `companies` table, contact fields upsert into `contacts`.
    All edits are logged to `batch_log` with action='manual_edit'.

    Conflict resolution metadata (optional):
        _conflict_action: "accept" or "dismiss"
        _conflict_field: which field the conflict is on
        _conflict_rejected_value: the value that was NOT chosen
        _conflict_rejected_source: the source of the rejected value
        _conflict_chosen_source: the source of the chosen value
    """
    # MAPS workspace gate
    if siren.startswith("MAPS"):
        is_admin, ws_id = _get_workspace_filter(request)
        if not is_admin:
            owner = await fetch_one(
                "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
            )
            if owner and owner.get("workspace_id") != ws_id:
                return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

    # Validate company exists
    company = await fetch_one(
        "SELECT siren, denomination FROM companies WHERE siren = %s", (siren,)
    )
    if not company:
        return JSONResponse(
            status_code=404,
            content={"error": "Company not found", "siren": siren},
        )

    # Extract conflict metadata before filtering
    conflict_action = body.pop("_conflict_action", None)
    conflict_field = body.pop("_conflict_field", None)
    conflict_rejected_value = body.pop("_conflict_rejected_value", None)
    conflict_rejected_source = body.pop("_conflict_rejected_source", None)
    conflict_chosen_source = body.pop("_conflict_chosen_source", None)

    # Strip alert type prefix if present (e.g., "data_conflict:phone" → "phone")
    if conflict_field and ":" in conflict_field:
        conflict_field = conflict_field.split(":")[-1]
    # Validate the cleaned field name is a known contact field before use in SQL
    if conflict_field and conflict_field not in _EDITABLE_FIELDS:
        conflict_field = None

    # Filter to allowed fields only
    updates = {k: v for k, v in body.items() if k in _EDITABLE_FIELDS}

    # For dismiss-only actions, no field update needed
    if conflict_action == "dismiss" and not updates:
        # Log the dismissal and return
        user = getattr(request.state, "user", None)
        username = getattr(user, "username", "?") if user else "?"
        user_id = getattr(user, "id", None) if user else None
        denomination = company.get("denomination", siren)
        details = f"Conflit ignoré sur {conflict_field} pour {denomination} ({siren}) — valeur rejetée: {conflict_rejected_value} ({conflict_rejected_source})"
        await log_activity(user_id, username, "conflict_dismissed", "company", siren, details)
        return {"dismissed": True, "siren": siren}

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
                "SELECT phone, email, website, address, social_linkedin, social_facebook, social_twitter, social_instagram, social_tiktok, social_whatsapp, social_youtube FROM contacts WHERE siren = %s ORDER BY collected_at DESC LIMIT 1",
                (siren,),
            )).fetchone()
            if existing:
                field_map = ["phone", "email", "website", "address", "social_linkedin", "social_facebook", "social_twitter", "social_instagram", "social_tiktok", "social_whatsapp", "social_youtube"]
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

        # Upsert contacts table
        if ct_updates:
            if conflict_action == "accept" and conflict_rejected_source and conflict_field:
                # ── Conflict resolution: null out the REJECTED source's field ──
                # This prevents the circular loop where writing a 'manual_edit' row
                # immediately creates a new conflict against the original source row.
                # Instead we silence the loser by NULLing its field in-place.
                await conn.execute(f"""
                    UPDATE contacts SET {conflict_field} = NULL
                    WHERE siren = %s AND source = %s
                """, (siren, conflict_rejected_source))
                saved.extend(ct_updates.keys())
            else:
                # Normal manual edit — upsert into 'manual_edit' source row
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

                # ── Deletion propagation ─────────────────────────────
                # When user CLEARS a field (sets to None/empty), also NULL
                # that field in ALL other source rows so _merge_contacts()
                # can't pull the deleted value back from website_crawl etc.
                deleted_fields = [k for k, v in ct_updates.items() if not v]
                if deleted_fields:
                    for field in deleted_fields:
                        await conn.execute(f"""
                            UPDATE contacts SET {field} = NULL
                            WHERE siren = %s AND source != 'manual_edit'
                        """, (siren,))

        # Audit trail with before/after
        for field, value in updates.items():
            old_val = before_values.get(field)
            if old_val:
                log_entry = f"{field}: {old_val} → {value}"
            else:
                log_entry = f"{field}={value}"
            action_label = "conflict_resolved" if conflict_action == "accept" else "manual_edit"
            await conn.execute("""
                INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp, detail)
                VALUES (%s, %s, %s, 'success', NULL, NOW(), %s)
            """, (
                "CONFLICT_RESOLVE" if conflict_action == "accept" else "MANUAL_EDIT",
                siren, action_label, log_entry,
            ))

        await conn.commit()

    # ── Log to activity journal ──────────────────────────────
    user = getattr(request.state, "user", None)
    username = getattr(user, "username", "?") if user else "?"
    user_id = getattr(user, "id", None) if user else None
    denomination = company.get("denomination", siren)

    if conflict_action == "accept":
        chosen_val = list(updates.values())[0] if updates else "?"
        details = (
            f"Conflit résolu sur {conflict_field} pour {denomination} ({siren}) "
            f"— choisi: {chosen_val} ({conflict_chosen_source}), "
            f"rejeté: {conflict_rejected_value} ({conflict_rejected_source})"
        )
        await log_activity(user_id, username, "conflict_resolved", "company", siren, details)
    elif saved:
        # Single consolidated journal entry covering all changed fields
        parts = []
        for field, value in updates.items():
            old_val = before_values.get(field)
            if old_val:
                parts.append(f"{field}: {old_val} → {value or '(vide)'}")
            else:
                parts.append(f"{field} = {value or '(vide)'}")
        detail = f"{denomination} ({siren}) — " + ", ".join(parts)
        await log_activity(user_id, username, "manual_edit", "company", siren, detail)

    return {"updated": sorted(saved), "siren": siren}


# ---------------------------------------------------------------------------
# Action 1: On-demand enrichment with deduplication
# ---------------------------------------------------------------------------

@router.post("/deep-enrich", status_code=202)
async def start_deep_enrich(body: DeepEnrichRequest, background_tasks: BackgroundTasks, request: Request):
    sirens = list(set(body.sirens))
    if len(sirens) > 20:
        return JSONResponse(status_code=400, content={"error": "Maximum 20 SIRENS autorisés."})

    is_admin, ws_id = _get_workspace_filter(request)
    if not is_admin and ws_id is not None:
        maps_sirens = [s for s in sirens if s.startswith("MAPS")]
        if maps_sirens:
            placeholders = ",".join(["%s"] * len(maps_sirens))
            foreign = await fetch_all(
                f"SELECT siren FROM companies WHERE siren IN ({placeholders}) AND (workspace_id IS NULL OR workspace_id != %s)",
                tuple(maps_sirens) + (ws_id,),
            )
            if foreign:
                foreign_list = [r["siren"] for r in foreign]
                return JSONResponse(status_code=403, content={
                    "error": "Certaines entreprises n'appartiennent pas à votre espace.",
                    "forbidden_sirens": foreign_list,
                })
        
    batch_id = f"MANUAL_ENRICH_{uuid.uuid4().hex[:8].upper()}"
    
    async with get_conn() as conn:
        # Create standard batch_data entry for Analyze dashboard tracking
        await conn.execute("""
            INSERT INTO batch_data (batch_id, batch_name, status, total_companies)
            VALUES (%s, %s, 'running', %s)
        """, (batch_id, f"Enrichissement Manuel ({len(sirens)})", len(sirens)))
        
        # Link SIRENS via batch_tags
        now = datetime.datetime.now()
        for s in sirens:
            await conn.execute(
                "INSERT INTO batch_tags (siren, batch_id, batch_name, tagged_at) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (s, batch_id, "Enrichissement Manuel", now)
            )
        await conn.commit()

    background_tasks.add_task(async_deep_enrich_worker, batch_id, sirens)
    return {"batch_id": batch_id, "sirens": sirens}

async def async_deep_enrich_worker(batch_id: str, sirens: list[str]):
    """Re-crawl the known website for each SIREN to pick up fresh contact data.

    This is the backend for the 'Enrichir' button on the company card.
    It does NOT call Maps or INPI — those run during Discovery batches.

    Flow per SIREN:
      1. Fetch the company's known website from contacts table
      2. Re-crawl that website (email, phones, socials)
      3. Insert/update a contacts row with source='website_crawl'
      4. Log to batch_log (action='website_crawl')
      5. _merge_contacts() dedup + conflict UI handles the rest on next card load
    """
    from fortress.api.routes.websocket import manager
    from fortress.scraping.http import CurlClient
    from fortress.scraping.crawl import crawl_website as _crawl_website
    from fortress.processing.dedup import log_audit

    async with get_conn() as conn:
        for siren in sirens:
            await manager.broadcast(batch_id, {"siren": siren, "status": "started"})

            try:
                # 1. Fetch company's known website and company info
                contact_row = await conn.execute(
                    "SELECT website FROM contacts WHERE siren = %s AND website IS NOT NULL ORDER BY collected_at DESC LIMIT 1",
                    (siren,),
                )
                row = await contact_row.fetchone()
                if not row or not row[0]:
                    await manager.broadcast(batch_id, {"siren": siren, "step": "crawl", "status": "skipped", "detail": "Aucun site web connu"})
                    await log_audit(conn, batch_id=batch_id, siren=siren, action="website_crawl", result="skipped", detail="Aucun site web connu pour ce SIREN.")
                    await conn.commit()
                    continue

                website = row[0]
                await manager.broadcast(batch_id, {"siren": siren, "step": "crawl", "status": "running", "detail": website})

                # Fetch company name and department for crawl context
                co_row = await (await conn.execute(
                    "SELECT denomination, departement FROM companies WHERE siren = %s",
                    (siren,),
                )).fetchone()
                co_name = (co_row[0] if co_row else None) or ""
                co_dept = (co_row[1] if co_row else None) or ""

                # 2. Re-crawl the website
                crawl_result = None
                curl_client = CurlClient(timeout=5.0, max_retries=1, delay_min=0.2, delay_max=0.4, delay_jitter=0.0)
                async with curl_client as client:
                    crawl_result = await _crawl_website(
                        url=website,
                        client=client,
                        company_name=co_name,
                        department=co_dept,
                        siren=siren,
                    )

                best_phone = crawl_result.best_phone
                best_email = crawl_result.best_email
                all_social = crawl_result.all_socials

                # 3. Persist as a website_crawl contact row — _merge_contacts() detects conflicts
                cols = ["siren", "source", "website"]
                vals: list = [siren, "website_crawl", website]

                if best_phone:
                    cols.append("phone")
                    vals.append(best_phone)
                if best_email:
                    cols.append("email")
                    vals.append(best_email)

                # crawl_result.all_socials keys: "linkedin", "facebook", etc. (no "social_" prefix)
                social_fields = {
                    "linkedin": "social_linkedin",
                    "facebook": "social_facebook",
                    "twitter": "social_twitter",
                    "instagram": "social_instagram",
                    "tiktok": "social_tiktok",
                    "whatsapp": "social_whatsapp",
                    "youtube": "social_youtube",
                }
                for key, col in social_fields.items():
                    v = all_social.get(key)
                    if v:
                        cols.append(col)
                        vals.append(v)

                phs = ["%s"] * len(cols)
                conflict_clause = ", ".join(
                    f"{c} = EXCLUDED.{c}" for c in cols if c not in ("siren", "source")
                )

                await conn.execute(
                    f"""INSERT INTO contacts ({', '.join(cols)}, collected_at)
                        VALUES ({', '.join(phs)}, NOW())
                        ON CONFLICT (siren, source) DO UPDATE SET {conflict_clause}, collected_at = NOW()
                    """,
                    tuple(vals),
                )

                # 5. Log to batch_log
                found_items = []
                if best_phone:
                    found_items.append(f"📞 {best_phone}")
                if best_email:
                    found_items.append(f"✉️ {best_email}")
                social_count = sum(1 for v in all_social.values() if v)
                if social_count:
                    found_items.append(f"{social_count} réseaux sociaux")

                if found_items:
                    detail = f"Re-crawl de {website} : {', '.join(found_items)}."
                    result_status = "success"
                else:
                    detail = f"Re-crawl de {website} : aucune nouvelle donnée trouvée."
                    result_status = "skipped"

                await log_audit(conn, batch_id=batch_id, siren=siren, action="website_crawl", result=result_status, detail=detail)
                await conn.commit()

                await manager.broadcast(batch_id, {
                    "siren": siren, "step": "crawl",
                    "status": "success" if result_status == "success" else "skipped",
                    "detail": detail,
                })

            except Exception as e:
                await manager.broadcast(batch_id, {"siren": siren, "step": "all", "status": "error", "detail": str(e)})

        # Finalize Batch
        await conn.execute("UPDATE batch_data SET status = 'completed' WHERE batch_id = %s", (batch_id,))
        await conn.commit()
        await manager.broadcast(batch_id, {"status": "completed"})


@router.post("/{siren}/enrich", status_code=202)
async def enrich_company(siren: str, body: EnrichRequest, request: Request):
    """Queue targeted enrichment modules for a single company.

    Modules:
      - contact_web:   Web search + Playwright crawler
      - contact_phone: PagesJaunes / Directory scraper
      - financials:    INPI / Recherche Entreprises API

    Deduplication: Checks existing data before queueing.
    Returns 202 Accepted with which modules were queued.
    """
    # MAPS workspace gate
    if siren.startswith("MAPS"):
        is_admin, ws_id = _get_workspace_filter(request)
        if not is_admin:
            owner = await fetch_one(
                "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
            )
            if owner and owner.get("workspace_id") != ws_id:
                return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

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
          AND timestamp > NOW() - make_interval(hours => %s)
        ORDER BY timestamp DESC LIMIT 1
    """, (siren, _DEDUP_TTL_HOURS))

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
            close_fds=True,
            start_new_session=True,
        )
        log_fh.close()  # Parent releases fd; subprocess keeps its own copy
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
async def crawl_website_sync(siren: str, request: Request):
    """Synchronously crawl a known website to extract email/phone/socials.

    Bypasses the `fortress.runner` batch queue entirely.
    """
    # MAPS workspace gate
    if siren.startswith("MAPS"):
        is_admin, ws_id = _get_workspace_filter(request)
        if not is_admin:
            owner = await fetch_one(
                "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
            )
            if owner and owner.get("workspace_id") != ws_id:
                return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

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

    from fortress.scraping.http import CurlClient
    from fortress.scraping.crawl import crawl_website as _crawl_website
    from fortress.matching.contacts import is_agency_email
    import time
    from urllib.parse import urlparse

    try:
        parsed = urlparse(website)
        root_url = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        root_url = website

    t0 = time.monotonic()

    crawl_result = None
    curl_client = CurlClient(timeout=5.0, max_retries=1, delay_min=0.2, delay_max=0.4, delay_jitter=0.0)
    async with curl_client as client:
        crawl_result = await _crawl_website(
            url=website,
            client=client,
            company_name=company_name,
            department=departement,
            siren=siren,
        )

    all_social = crawl_result.all_socials
    found_siren = crawl_result.siren_from_website
    best_email = crawl_result.best_email
    best_phone = crawl_result.best_phone

    # ── Fix 1: Agency email rejection (domain mismatch) ─────────
    rejected_email = None
    if best_email and is_agency_email(best_email, website):
        rejected_email = best_email
        best_email = None  # Reject the agency email

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
        "social_whatsapp": all_social.get("whatsapp"),
        "social_youtube": all_social.get("youtube"),
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

        # ── SIRET handling: website SIREN differs from current entity ────
        if found_siren and found_siren != siren:
          try:
            real_co = await (await conn.execute(
                "SELECT siren, denomination FROM companies WHERE siren = %s", (found_siren,)
            )).fetchone()

            if siren.startswith("MAPS"):
                # --- MAPS entity: link/merge to real company ---
                if real_co:
                    real_name = real_co[1] if isinstance(real_co, tuple) else real_co.get("denomination")
                    siret_merge_info = {
                        "real_siren": found_siren,
                        "real_name": real_name,
                    }
                    # Copy crawl data to the real SIREN's contacts
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

                    # Copy all MAPS contacts to the real SIREN
                    maps_contacts = await (await conn.execute(
                        "SELECT phone, email, website, social_linkedin, social_facebook, social_twitter, social_instagram, social_tiktok, social_whatsapp, social_youtube, source FROM contacts WHERE siren = %s",
                        (siren,)
                    )).fetchall()
                    for mc in (maps_contacts or []):
                        mc_source = mc[10] if isinstance(mc, tuple) else mc.get("source")
                        mc_fields = {}
                        field_names = ["phone", "email", "website", "social_linkedin", "social_facebook", "social_twitter", "social_instagram", "social_tiktok", "social_whatsapp", "social_youtube"]
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
            else:
                # --- Real SIREN entity: website belongs to a DIFFERENT company ---
                # Do NOT copy data to the other SIREN — the match was likely wrong.
                # Flag siren_match=false so the company card shows a warning.
                real_name = None
                if real_co:
                    real_name = real_co[1] if isinstance(real_co, tuple) else real_co.get("denomination")
                siret_merge_info = {
                    "real_siren": found_siren,
                    "real_name": real_name or found_siren,
                    "mismatch": True,
                }
                # Mark this contact row with siren_match=false
                await conn.execute("""
                    UPDATE contacts SET siren_match = FALSE
                    WHERE siren = %s AND source = 'website_crawl'
                """, (siren,))
                # Log the mismatch
                await conn.execute("""
                    INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp)
                    VALUES ('SYNC_CRAWL', %s, 'siren_mismatch', 'warning', %s, NOW())
                """, (siren, f"SIRET du site web ({found_siren} — {real_name or '?'}) ne correspond pas à cette entreprise ({siren})"))
          except Exception as merge_err:
                import logging
                logging.getLogger("fortress").warning("SIRET merge failed for %s: %s", siren, merge_err)

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
        log_parts.append(f"pages: {crawl_result.pages_crawled if crawl_result else 0} crawled")
        
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
            INSERT INTO batch_log (batch_id, siren, action, result, source_url, duration_ms, timestamp, detail)
            VALUES ('SYNC_CRAWL', %s, 'website_crawl', %s, %s, %s, NOW(), %s)
        """, (siren, 'success' if extracted else 'filtered', website, duration, found_data))

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
        if siret_merge_info.get("mismatch"):
            message += f" ⚠️ SIRET du site ({siret_merge_info['real_siren']}) ≠ cette entreprise — possible mauvaise correspondance"
        else:
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
        "pages_crawled": crawl_result.pages_crawled if crawl_result else 0,
        "pages_discovered": 0,
    }


# ---------------------------------------------------------------------------
# Action 2: Search with NAF code + sorting
# ---------------------------------------------------------------------------

@router.get("/search")
async def search_companies(
    request: Request,
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
    # Workspace scoping
    _user = getattr(request.state, "user", None)
    if _user and not _user.is_admin:
        ws_part = "qt.workspace_id = %s"
        ws_params_base: list = [_user.workspace_id]
    else:
        ws_part = "1=1"
        ws_params_base = []

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
            WHERE co.siren = %s AND {ws_part}
            GROUP BY co.siren, co.denomination, co.naf_code, co.naf_libelle,
                     co.forme_juridique, co.tranche_effectif,
                     co.ville, co.departement, co.statut,
                     ct.phone, ct.email, ct.website
            ORDER BY {sort_clause}
        """, tuple([clean_q] + ws_params_base))
    else:
        # Name / NAF code search — indexed via batch_tags.siren
        like_param = f"%{q.strip()}%"

        # Build dynamic WHERE filters
        where_parts = [
            "(UPPER(co.denomination) LIKE UPPER(%s) OR co.naf_code ILIKE %s)",
            ws_part,
        ]
        params: list = [like_param, like_param] + ws_params_base

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
async def get_enrich_history(siren: str, request: Request):
    """Return enrichment audit trail for a single company.

    Queries batch_log table (indexed on siren).
    Used by the frontend's Smart Enrichment Panel timeline.
    """
    # MAPS workspace gate
    if siren.startswith("MAPS"):
        is_admin, ws_id = _get_workspace_filter(request)
        if not is_admin:
            owner = await fetch_one(
                "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
            )
            if owner and owner.get("workspace_id") != ws_id:
                return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

    rows = await fetch_all("""
        SELECT
            action, result, source_url, detail, search_query, duration_ms, timestamp
        FROM batch_log
        WHERE siren = %s
        ORDER BY timestamp DESC
    """, (siren,))
    return {"siren": siren, "history": rows, "count": len(rows)}


@router.delete("/{siren}/tags/{batch_name}")
async def untag_company(siren: str, batch_name: str, request: Request):
    """Remove a company from a batch's results (untag only — never deletes data).
    Admin: only deletes tags where workspace_id IS NULL.
    Head: only deletes tags for their workspace.
    """
    is_admin, ws_id = _get_workspace_filter(request)

    if is_admin:
        ws_scope = "AND workspace_id IS NULL"
        ws_params: tuple = ()
    elif ws_id is not None:
        ws_scope = "AND workspace_id = %s"
        ws_params = (ws_id,)
    else:
        return JSONResponse(status_code=403, content={"error": "Accès refusé."})

    async with get_conn() as conn:
        result = await conn.execute(
            f"DELETE FROM batch_tags WHERE siren = %s AND batch_name = %s {ws_scope} RETURNING siren",
            (siren, batch_name) + ws_params,
        )
        row = await result.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Tag introuvable"})
        await conn.commit()
    return {"untagged": True, "siren": siren, "batch_name": batch_name}


@router.delete("/{siren}/tags/")
async def untag_company_all(siren: str, request: Request):
    """Remove a company from ALL query results (all tags). Never deletes data.
    Admin: only deletes tags where workspace_id IS NULL.
    Head: only deletes tags for their workspace.
    """
    is_admin, ws_id = _get_workspace_filter(request)

    if is_admin:
        ws_scope = "AND workspace_id IS NULL"
        ws_params: tuple = ()
    elif ws_id is not None:
        ws_scope = "AND workspace_id = %s"
        ws_params = (ws_id,)
    else:
        return JSONResponse(status_code=403, content={"error": "Accès refusé."})

    async with get_conn() as conn:
        result = await conn.execute(
            f"DELETE FROM batch_tags WHERE siren = %s {ws_scope} RETURNING siren",
            (siren,) + ws_params,
        )
        rows = await result.fetchall()
        count = len(rows)
        if count == 0:
            return JSONResponse(status_code=404, content={"error": "Aucun tag trouvé pour ce SIREN"})
        await conn.commit()
    return {"untagged": True, "siren": siren, "removed_count": count}


@router.get("/{siren}")
async def get_company(siren: str, request: Request):
    """Full company detail with enriched data, contacts, and officers."""
    try:
        return await _get_company_impl(siren, request)
    except Exception as e:
        import traceback, logging
        tb = traceback.format_exc()
        logging.getLogger("fortress").error("get_company(%s) failed: %s\n%s", siren, e, tb)
        # Show traceback only to admin users for debugging
        user = getattr(request.state, "user", None)
        if user and getattr(user, "is_admin", False):
            return JSONResponse(status_code=500, content={"error": str(e), "traceback": tb})
        return JSONResponse(status_code=500, content={"error": "Internal server error"})

async def _get_company_impl(siren: str, request=None):
    company = await fetch_one("""
        SELECT
            co.siren, co.siret_siege, co.denomination, co.enseigne,
            co.naf_code, co.naf_libelle, co.forme_juridique,
            co.adresse, co.code_postal, co.ville,
            co.departement, co.region, co.statut,
            co.date_creation, co.tranche_effectif, co.effectif_exact,
            co.latitude, co.longitude, co.fortress_id,
            -- ⚠️ DO NOT REMOVE co.resultat_net — column created by schema.sql on startup
            co.chiffre_affaires, co.resultat_net, co.annee_ca, co.tranche_ca,
            co.date_fondation, co.type_etablissement,
            co.extra_data,
            co.created_at, co.updated_at,
            co.linked_siren, co.link_confidence, co.link_method,
            co.workspace_id
        FROM companies co
        WHERE co.siren = %s
    """, (siren,))

    if not company:
        return JSONResponse(status_code=404, content={"error": "Company not found", "siren": siren})

    # MAPS workspace gate
    if siren.startswith("MAPS") and request:
        user = getattr(request.state, "user", None)
        if user and not user.is_admin:
            company_ws = company.get("workspace_id")
            if company_ws is not None and company_ws != user.workspace_id:
                return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

    # All contacts from different sources
    contacts = await fetch_all("""
        SELECT
            phone, email, email_type, website, source,
            social_linkedin, social_facebook, social_twitter,
            social_instagram, social_tiktok, social_whatsapp, social_youtube,
            siren_match,
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

    # For MAPS entities with a confirmed linked SIREN, also pull officers stored
    # under the real SIREN (enricher writes them there, not on the MAPS id).
    if not officers and company.get("linked_siren") and company.get("link_confidence") == "confirmed":
        officers = await fetch_all("""
            SELECT nom, prenom, role, source,
                   civilite, email_direct, ligne_directe,
                   code_fonction, type_fonction
            FROM officers
            WHERE siren = %s
            ORDER BY nom
        """, (company["linked_siren"],))

    # Merge best contacts
    merged = _merge_contacts(contacts)

    # Query tags (which jobs found this company)
    tags = await fetch_all("""
        SELECT bt.batch_id, batch_name, tagged_at
        FROM batch_tags bt
        WHERE siren = %s
        ORDER BY tagged_at DESC
    """, (siren,))

    enrichment_history = await fetch_all("""
        SELECT
            action, result, source_url, detail, search_query, duration_ms, timestamp
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

    # ── Entity linking: MAPS → SIREN matching ─────────────────
    linked_company = None
    suggested_matches = []

    if siren.startswith("MAPS"):
        link_confidence = company.get("link_confidence")
        linked_siren = company.get("linked_siren")

        if link_confidence == "confirmed" and linked_siren:
            # User confirmed this link — fetch the real company data
            linked_company = await fetch_one("""
                SELECT siren, denomination, naf_code, naf_libelle,
                       forme_juridique, tranche_effectif, effectif_exact,
                       adresse, code_postal, ville, departement
                FROM companies WHERE siren = %s
            """, (linked_siren,))

        elif link_confidence == "rejected":
            # User rejected — no banner, entity stays independent
            pass

        elif link_confidence == "pending" and linked_siren:
            # Runner found a candidate but user hasn't decided yet
            # Show as suggested match for user to confirm/reject
            candidate = await fetch_one("""
                SELECT siren, denomination, naf_code, naf_libelle,
                       forme_juridique, tranche_effectif, effectif_exact,
                       adresse, code_postal, ville, departement
                FROM companies WHERE siren = %s
            """, (linked_siren,))
            if candidate:
                method = company.get("link_method") or "fuzzy_name"
                reason_map = {
                    "address": "Même adresse détectée",
                    "fuzzy_name": "Nom similaire",
                }
                suggested_matches = [{
                    "siren": candidate["siren"],
                    "denomination": candidate.get("denomination"),
                    "confidence": "pending",
                    "method": method,
                    "reason": reason_map.get(method, method),
                    "address": candidate.get("adresse"),
                    "ville": candidate.get("ville"),
                    "naf_code": candidate.get("naf_code"),
                    "tranche_effectif": candidate.get("tranche_effectif"),
                }]
        else:
            # No candidate from the pipeline — don't block page load.
            # Frontend will call /suggest-matches asynchronously.
            pass

    # ── Unified History ───────────────────────────────────────
    # Merge enrichment_history (batch_log) + notes (company_notes) + activity_log
    unified_history = []
    
    for h in enrichment_history:
        unified_history.append({
            "type": "enrichment",
            "action": h["action"],
            "result": h["result"],
            "source_url": h["source_url"],
            "detail": h["detail"],
            "search_query": h["search_query"],
            "duration": h["duration_ms"],
            "timestamp": str(h["timestamp"]),
        })
        
    for n in (notes or []):
        unified_history.append({
            "type": "note",
            "id": n["id"],
            "username": n["username"],
            "text": n["text"],
            "timestamp": str(n["created_at"]),
        })

    # Activity log events for this company (manual edits, links, merges)
    activity_events = await fetch_all("""
        SELECT action, username, details, created_at
        FROM activity_log
        WHERE target_type = 'company' AND target_id = %s
          AND action NOT IN ('note_added', 'note_deleted')
        ORDER BY created_at DESC
        LIMIT 50
    """, (siren,))

    for a in (activity_events or []):
        unified_history.append({
            "type": "activity",
            "action": a["action"],
            "username": a["username"],
            "detail": a["details"],
            "timestamp": str(a["created_at"]),
        })
        
    # Sort by timestamp DESC
    unified_history.sort(key=lambda x: x["timestamp"], reverse=True)

    # ── Build unified alerts list ─────────────────────────────
    alerts = _build_alerts(company, merged, contacts)

    return {
        "company": company,
        "contacts": contacts,
        "merged_contact": merged,
        "officers": officers,
        "batch_tags": tags,
        "history": unified_history,
        "linked_company": linked_company,
        "sirene_denomination": linked_company.get("denomination") if linked_company else None,
        "link_method": company.get("link_method"),
        "link_confidence": company.get("link_confidence"),
        "suggested_matches": suggested_matches,
        "matching_available": siren.startswith("MAPS") and not company.get("linked_siren") and company.get("link_confidence") is None,
        "data_conflicts": merged.get("data_conflicts", []),
        "siren_match": merged.get("siren_match"),
        "alerts": alerts,
    }


def _normalize_phone_for_comparison(phone: str) -> str:
    """Normalize French phone to 0XXXXXXXXX for comparison only."""
    if not phone:
        return ""
    digits = ''.join(c for c in phone if c.isdigit() or c == '+')
    digits = digits.replace(" ", "").replace(".", "").replace("-", "")
    if digits.startswith("+33") and len(digits) >= 12:
        digits = "0" + digits[3:]
    if digits.startswith("0033") and len(digits) >= 13:
        digits = "0" + digits[4:]
    return digits


def _merge_contacts(contacts: list[dict]) -> dict:
    """Merge all contact rows into a single best-of dict with per-field provenance.

    Source priority (highest to lowest):
        Tier 1: manual_edit, upload          (human-verified)
        Tier 2: website_crawl, mentions_legales  (found on company's own site)
        Tier 3: google_maps, google_cse      (external discovery)
        Tier 4: recherche_entreprises        (government API)
        Tier 5: sirene, google_search, etc.  (baseline/legacy)

    Special rules:
        - address: Maps always wins unless manual_edit overrides (SIRENE address often differs)
        - rating/maps_url: Only from google_maps (no other source provides these)

    Returns merged dict with *_source provenance AND *_alt alternate values for UI.
    """
    _SOURCE_PRIORITY: dict[str, int] = {
        "manual_edit": 0,
        "upload": 1,
        "website_crawl": 2,
        "mentions_legales": 2,
        "google_maps": 3,
        "google_cse": 3,
        "recherche_entreprises": 4,
        "sirene": 5,
        "google_search": 5,
        "directory_search": 5,
        "pages_jaunes": 5,
        "inpi": 5,
        "synthesized": 6,
        "annuaire_entreprises": 5,
    }

    # Sort contacts by source priority (best first)
    sorted_contacts = sorted(
        contacts,
        key=lambda c: _SOURCE_PRIORITY.get(c.get("source", ""), 99),
    )

    # Fields to merge with standard priority logic
    MERGE_FIELDS = (
        "phone", "email", "website",
        "social_linkedin", "social_facebook", "social_twitter",
        "social_instagram", "social_tiktok", "social_whatsapp", "social_youtube",
        "maps_url",
    )

    merged: dict = {
        "phone": None, "phone_source": None, "phone_alt": None,
        "email": None, "email_source": None, "email_alt": None,
        "email_type": None,
        "website": None, "website_source": None, "website_alt": None,
        "social_linkedin": None, "social_linkedin_source": None,
        "social_facebook": None, "social_facebook_source": None,
        "social_twitter": None, "social_twitter_source": None,
        "social_instagram": None, "social_instagram_source": None,
        "social_tiktok": None, "social_tiktok_source": None,
        "social_whatsapp": None, "social_whatsapp_source": None,
        "social_youtube": None, "social_youtube_source": None,
        "rating": None, "rating_source": None,
        "review_count": None,
        "maps_url": None, "maps_url_source": None,
        "address": None, "address_source": None, "address_alt": None,
        "sources": [],
    }

    for c in sorted_contacts:
        src = c.get("source")
        if src:
            merged["sources"].append(src)
        for key in MERGE_FIELDS:
            if c.get(key):
                if merged[key] is None:
                    # First (highest-priority) non-null wins
                    merged[key] = c[key]
                    merged[f"{key}_source"] = src
                else:
                    # Store alternate value for UI display (if different)
                    alt_key = f"{key}_alt"
                    if alt_key in merged and merged[alt_key] is None and c[key] != merged[key]:
                        cur_src = merged[f"{key}_source"] or "?"
                        merged[alt_key] = {"value": c[key], "source": src, "current_source": cur_src}

        if merged["email_type"] is None and c.get("email_type"):
            merged["email_type"] = c["email_type"]
        if merged["rating"] is None and c.get("rating"):
            merged["rating"] = c["rating"]
            merged["review_count"] = c.get("review_count")
            merged["rating_source"] = src

    # ── Special rule: address — Maps always wins (unless manual_edit) ──
    # SIRENE address often differs from real business location.
    # Maps address is verified by Google and represents actual location.
    maps_addr = None
    manual_addr = None
    for c in sorted_contacts:
        src = c.get("source")
        if c.get("address"):
            if src == "manual_edit" and not manual_addr:
                manual_addr = c["address"]
            elif src == "google_maps" and not maps_addr:
                maps_addr = c["address"]

    if manual_addr:
        merged["address"] = manual_addr
        merged["address_source"] = "manual_edit"
        if maps_addr and maps_addr != manual_addr:
            merged["address_alt"] = {"value": maps_addr, "source": "google_maps"}
    elif maps_addr:
        merged["address"] = maps_addr
        merged["address_source"] = "google_maps"

    merged["sources"] = list(dict.fromkeys(merged["sources"]))  # dedupe, preserve order

    # ── Build data_conflicts list for interactive merge UI ─────────────
    # Each conflict = a field where two sources disagree.
    _SOURCE_LABELS = {
        "google_maps": "Google Maps",
        "website_crawl": "Site web",
        "mentions_legales": "Mentions légales",
        "upload": "Import CSV",
        "client_upload": "Import client",
        "manual_edit": "Modification manuelle",
        "recherche_entreprises": "API gouvernement",
        "sirene": "Registre SIRENE",
    }
    data_conflicts = []
    CONFLICT_FIELDS = ("phone", "email", "website", "address")
    for field in CONFLICT_FIELDS:
        alt = merged.get(f"{field}_alt")
        if alt and alt.get("value"):
            # For phone: normalize before comparing to avoid false conflicts
            if field == "phone":
                cur_norm = _normalize_phone_for_comparison(merged.get(field) or "")
                alt_norm = _normalize_phone_for_comparison(alt["value"] or "")
                if cur_norm == alt_norm:
                    continue  # Same number, different format — not a conflict
            cur_src = merged.get(f"{field}_source") or "?"
            alt_src = alt["source"] or "?"
            cur_label = _SOURCE_LABELS.get(cur_src, cur_src)
            alt_label = _SOURCE_LABELS.get(alt_src, alt_src)
            reason = f"{cur_label} et {alt_label} ont trouvé des valeurs différentes pour ce champ"
            data_conflicts.append({
                "field": field,
                "current": {"value": merged[field], "source": merged.get(f"{field}_source")},
                "alternative": {"value": alt["value"], "source": alt["source"]},
                "reason": reason,
            })
    merged["data_conflicts"] = data_conflicts

    # ── Extract siren_match status ────────────────────────────────────
    # If any contact source has siren_match = False, flag it.
    siren_match_status = None
    for c in sorted_contacts:
        sm = c.get("siren_match")
        if sm is not None:
            siren_match_status = sm
            break  # First non-null wins (highest priority source)
    merged["siren_match"] = siren_match_status

    # ── Extract match_confidence status ────────────────────────────────
    # Find the highest-priority match_confidence from contact sources.
    match_conf = None
    for c in sorted_contacts:
        mc = c.get("match_confidence")
        if mc is not None:
            match_conf = mc
            break  # First non-null wins (highest priority source)
    merged["match_confidence"] = match_conf

    return merged


def _build_alerts(company: dict, merged: dict, contacts: list[dict]) -> list[dict]:
    """Build a unified alerts list for the company card.

    Alert types:
      - siren_mismatch: website crawl found a SIRET belonging to a different company
      - address_mismatch: Google Maps address differs from SIRENE registry address
      - data_conflict: two sources disagree on phone/email/website (from _merge_contacts)

    Each alert has: type, severity (critical/warning/info), title, description,
    field, current_value, current_source, alt_value, alt_source.
    """
    alerts: list[dict] = []

    # ── 1. SIRET mismatch (critical) ──────────────────────────────
    siren_match = merged.get("siren_match")
    if siren_match is False:
        # Find which source flagged it
        flagging_source = None
        for c in contacts:
            if c.get("siren_match") is False:
                flagging_source = c.get("source", "?")
                break
        alerts.append({
            "type": "siren_mismatch",
            "severity": "critical",
            "title": "SIRET du site web ne correspond pas",
            "description": (
                f"Le site web de cette entreprise contient un SIRET appartenant "
                f"à une autre société. Les données du site pourraient être celles d'une entreprise différente."
            ),
            "field": "siren",
            "current_value": company.get("siren"),
            "current_source": "sirene",
            "alt_value": None,
            "alt_source": flagging_source,
        })

    # ── 2. Address mismatch (Maps vs SIRENE) ──────────────────────
    sirene_addr = company.get("adresse") or ""
    sirene_ville = company.get("ville") or ""
    maps_addr = merged.get("address") or ""
    maps_source = merged.get("address_source")

    if maps_source == "google_maps" and sirene_addr and maps_addr:
        # Compare cities: extract city from Maps address and compare with SIRENE ville
        sirene_city_norm = sirene_ville.strip().upper()
        maps_addr_upper = maps_addr.upper()
        # Maps address typically ends with "VILLE, France" or "CODE VILLE, France"
        city_mismatch = sirene_city_norm and sirene_city_norm not in maps_addr_upper
        if city_mismatch:
            alerts.append({
                "type": "address_mismatch",
                "severity": "warning",
                "title": "Adresse Google Maps différente du registre SIRENE",
                "description": (
                    f"L'adresse trouvée sur Google Maps ne correspond pas "
                    f"à l'adresse enregistrée au registre SIRENE."
                ),
                "field": "address",
                "current_value": f"{sirene_addr}, {sirene_ville}".strip(", "),
                "current_source": "sirene",
                "alt_value": maps_addr,
                "alt_source": "google_maps",
            })

    # ── 3. Data conflicts (phone/email/website) ──────────────────
    for conflict in merged.get("data_conflicts", []):
        alerts.append({
            "type": "data_conflict",
            "severity": "warning",
            "title": f"Conflit sur {conflict['field']}",
            "description": conflict["reason"],
            "field": conflict["field"],
            "current_value": conflict["current"]["value"],
            "current_source": conflict["current"]["source"],
            "alt_value": conflict["alternative"]["value"],
            "alt_source": conflict["alternative"]["source"],
        })

    return alerts


# ── Entity linking endpoints ──────────────────────────────────────────

class LinkBody(BaseModel):
    target_siren: str = Field(..., description="The real SIREN to link to")


@router.post("/{siren}/link")
async def link_entity(siren: str, body: LinkBody, background_tasks: BackgroundTasks, request: Request):
    """Confirm a MAPS entity is linked to a real SIREN.

    After confirmation:
      - Sets link_confidence = 'confirmed'
      - Triggers INPI enrichment (directors + financials) in background
    """
    if not siren.startswith("MAPS"):
        return JSONResponse(status_code=400, content={"error": "Only MAPS entities can be linked"})

    # MAPS workspace gate
    is_admin, ws_id = _get_workspace_filter(request)
    if not is_admin:
        owner = await fetch_one(
            "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
        )
        if owner and owner.get("workspace_id") != ws_id:
            return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

    # Verify target exists
    target = await fetch_one(
        "SELECT siren, denomination FROM companies WHERE siren = %s", (body.target_siren,)
    )
    if not target:
        return JSONResponse(status_code=404, content={"error": f"Target SIREN {body.target_siren} not found"})

    user = getattr(request.state, "user", None)
    username = getattr(user, "username", "?") if user else "?"
    user_id = getattr(user, "id", None) if user else None

    async with get_conn() as conn:
        await conn.execute("""
            UPDATE companies
            SET linked_siren = %s, link_confidence = 'confirmed', link_method = COALESCE(link_method, 'manual')
            WHERE siren = %s
        """, (body.target_siren, siren))

        await conn.execute("""
            INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp, detail)
            VALUES ('ENTITY_LINK', %s, 'link', 'success', NULL, NOW(), %s)
        """, (siren, f"Linked to {body.target_siren} ({target.get('denomination')})"))

        await conn.commit()

    await log_activity(user_id, username, "link", "company", siren,
        f"Lié à {body.target_siren} ({target.get('denomination')})")

    # Trigger INPI enrichment for the confirmed target SIREN (background)
    background_tasks.add_task(_post_link_inpi_enrich, body.target_siren, siren)

    return {
        "linked": True,
        "maps_siren": siren,
        "target_siren": body.target_siren,
        "target_name": target.get("denomination"),
    }


async def _post_link_inpi_enrich(target_siren: str, maps_siren: str):
    """After user confirms a link, fetch directors + financials from INPI for the target SIREN."""
    try:
        from fortress.matching.inpi import fetch_dirigeants
        from fortress.models import Officer, ContactSource
        from fortress.processing.dedup import upsert_officer, log_audit

        dirigeants, company_data = await fetch_dirigeants(target_siren)

        async with get_conn() as conn:
            for d in dirigeants:
                officer = Officer(
                    siren=target_siren,
                    nom=d["nom"],
                    prenom=d.get("prenom"),
                    role=d.get("qualite"),
                    civilite=d.get("civilite"),
                    source=ContactSource.RECHERCHE_ENTREPRISES,
                )
                await upsert_officer(conn, officer)

            if dirigeants:
                await log_audit(
                    conn, batch_id="ENTITY_LINK", siren=maps_siren,
                    action="officers_found", result="success",
                    detail=f"{len(dirigeants)} dirigeant(s) via INPI après lien confirmé",
                )

            # Update financial data on the target company row
            if company_data:
                parts, vals = [], []
                if "chiffre_affaires" in company_data:
                    parts.append("chiffre_affaires = %s")
                    vals.append(company_data["chiffre_affaires"])
                if "resultat_net" in company_data:
                    parts.append("resultat_net = %s")
                    vals.append(company_data["resultat_net"])
                if "tranche_effectif" in company_data:
                    parts.append("tranche_effectif = COALESCE(tranche_effectif, %s)")
                    vals.append(company_data["tranche_effectif"])
                if parts:
                    vals.append(target_siren)
                    await conn.execute(
                        f"UPDATE companies SET {', '.join(parts)} WHERE siren = %s",
                        tuple(vals),
                    )

            await conn.commit()

    except Exception as exc:
        import structlog
        structlog.get_logger().warning(
            "post_link_inpi_enrich.failed",
            target_siren=target_siren,
            maps_siren=maps_siren,
            error=str(exc),
        )


@router.post("/{siren}/reject-link")
async def reject_link(siren: str, request: Request):
    """User rejects the suggested SIRENE match — entity stays independent."""
    if not siren.startswith("MAPS"):
        return JSONResponse(status_code=400, content={"error": "Only MAPS entities can reject links"})

    # MAPS workspace gate
    is_admin_rl, ws_id_rl = _get_workspace_filter(request)
    if not is_admin_rl:
        owner_rl = await fetch_one(
            "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
        )
        if owner_rl and owner_rl.get("workspace_id") != ws_id_rl:
            return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

    user = getattr(request.state, "user", None)
    username = getattr(user, "username", "?") if user else "?"
    user_id = getattr(user, "id", None) if user else None

    async with get_conn() as conn:
        await conn.execute("""
            UPDATE companies
            SET link_confidence = 'rejected'
            WHERE siren = %s
        """, (siren,))

        await conn.execute("""
            INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp, detail)
            VALUES ('ENTITY_LINK', %s, 'reject_link', 'success', NULL, NOW(), 'Correspondance rejetée par l''utilisateur')
        """, (siren,))

        await conn.commit()

    await log_activity(user_id, username, "reject_link", "company", siren,
        "Correspondance rejetée")

    return {"rejected": True, "siren": siren}


@router.post("/{siren}/unlink")
async def unlink_entity(siren: str, request: Request):
    """Remove an existing confirmed link — entity goes back to independent."""
    if not siren.startswith("MAPS"):
        return JSONResponse(status_code=400, content={"error": "Only MAPS entities can be unlinked"})

    # MAPS workspace gate
    is_admin_ul, ws_id_ul = _get_workspace_filter(request)
    if not is_admin_ul:
        owner_ul = await fetch_one(
            "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
        )
        if owner_ul and owner_ul.get("workspace_id") != ws_id_ul:
            return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

    user = getattr(request.state, "user", None)
    username = getattr(user, "username", "?") if user else "?"
    user_id = getattr(user, "id", None) if user else None

    async with get_conn() as conn:
        await conn.execute("""
            UPDATE companies
            SET linked_siren = NULL, link_confidence = NULL, link_method = NULL
            WHERE siren = %s
        """, (siren,))

        await conn.execute("""
            INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp, detail)
            VALUES ('ENTITY_LINK', %s, 'unlink', 'success', NULL, NOW(), 'Lien supprimé par l''utilisateur')
        """, (siren,))

        await conn.commit()

    await log_activity(user_id, username, "unlink", "company", siren,
        "Lien supprimé")

    return {"unlinked": True, "siren": siren}


@router.post("/{siren}/merge")
async def merge_entity(siren: str, body: LinkBody, request: Request):
    """Merge a MAPS entity into a real SIREN — moves all data, then deletes MAPS row."""
    if not siren.startswith("MAPS"):
        return JSONResponse(status_code=400, content={"error": "Only MAPS entities can be merged"})

    # MAPS workspace gate
    is_admin_mg, ws_id_mg = _get_workspace_filter(request)
    if not is_admin_mg:
        owner_mg = await fetch_one(
            "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
        )
        if owner_mg and owner_mg.get("workspace_id") != ws_id_mg:
            return JSONResponse(status_code=403, content={"error": "Accès refusé — entreprise hors de votre espace."})

    user = getattr(request.state, "user", None)
    username = getattr(user, "username", "?") if user else "?"
    user_id = getattr(user, "id", None) if user else None

    target_siren = body.target_siren

    # Verify target exists
    target = await fetch_one(
        "SELECT siren, denomination FROM companies WHERE siren = %s", (target_siren,)
    )
    if not target:
        return JSONResponse(status_code=404, content={"error": f"Target SIREN {target_siren} not found"})

    async with get_conn() as conn:
        # 1. Contacts: move to real SIREN (handle conflicts by updating)
        await conn.execute("""
            UPDATE contacts SET siren = %s
            WHERE siren = %s
            AND source NOT IN (
                SELECT source FROM contacts WHERE siren = %s
            )
        """, (target_siren, siren, target_siren))
        # Delete remaining MAPS contacts (conflicts)
        await conn.execute("DELETE FROM contacts WHERE siren = %s", (siren,))

        # 2. Company notes
        await conn.execute(
            "UPDATE company_notes SET siren = %s WHERE siren = %s",
            (target_siren, siren),
        )

        # 3. Batch tags (ignore conflicts)
        try:
            await conn.execute(
                "UPDATE batch_tags SET siren = %s WHERE siren = %s",
                (target_siren, siren),
            )
        except Exception:
            await conn.execute("DELETE FROM batch_tags WHERE siren = %s", (siren,))

        # 4. Batch log
        await conn.execute(
            "UPDATE batch_log SET siren = %s WHERE siren = %s",
            (target_siren, siren),
        )

        # 5. Enrichment log
        await conn.execute(
            "UPDATE enrichment_log SET siren = %s WHERE siren = %s",
            (target_siren, siren),
        )

        # 6. Officers (ignore conflicts)
        try:
            await conn.execute(
                "UPDATE officers SET siren = %s WHERE siren = %s",
                (target_siren, siren),
            )
        except Exception:
            await conn.execute("DELETE FROM officers WHERE siren = %s", (siren,))

        # 7. Log the merge event
        await conn.execute("""
            INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp, detail)
            VALUES ('ENTITY_MERGE', %s, 'merge', 'success', NULL, NOW(), %s)
        """, (target_siren, f"Merged from {siren}"))

        # 8. Delete the MAPS entity
        await conn.execute("DELETE FROM companies WHERE siren = %s", (siren,))

        await conn.commit()

    await log_activity(user_id, username, "merge", "company", target_siren,
        f"Fusion depuis {siren}")

    return {
        "merged": True,
        "deleted_maps": siren,
        "redirect_to": target_siren,
        "target_name": target.get("denomination"),
    }


# ---------------------------------------------------------------------------
# Manual entity creation — POST /api/companies/create
# ---------------------------------------------------------------------------

class OfficerInput(BaseModel):
    nom: str = ""
    prenom: str = ""
    role: str = ""


class CreateEntityRequest(BaseModel):
    siren: str | None = None
    denomination: str | None = None
    enseigne: str | None = None
    phone: str | None = None
    email: str | None = None
    website: str | None = None
    adresse: str | None = None
    code_postal: str | None = None
    ville: str | None = None
    departement: str | None = None
    social_linkedin: str | None = None
    social_facebook: str | None = None
    social_instagram: str | None = None
    social_tiktok: str | None = None
    social_twitter: str | None = None
    social_whatsapp: str | None = None
    social_youtube: str | None = None
    officers: list[OfficerInput] = []
    notes: str | None = None


@router.post("/create", status_code=201)
async def create_entity(body: CreateEntityRequest, request: Request):
    """Manually create a new MAPS entity with optional SIRENE link.

    If a 9-digit SIREN is provided and found in the DB, creates the MAPS entity
    with linked_siren = siren, link_confidence = 'confirmed', link_method = 'manual'.
    Otherwise creates a standalone MAPS entity.

    Always creates:
    - A row in companies (MAPS entity)
    - A row in contacts (source='manual') with provided contact data
    - Officer rows if provided (source='manual')
    - A batch_tags entry (batch_name='Ajout manuel')
    - An activity log entry
    - A company_notes entry if notes provided
    """
    user = getattr(request.state, "user", None)
    username = getattr(user, "username", "?") if user else "?"
    user_id = getattr(user, "id", None) if user else None
    workspace_id = getattr(user, "workspace_id", None) if user else None

    # Validate: need denomination or a valid SIREN
    provided_siren = (body.siren or "").strip().replace(" ", "")

    if provided_siren.upper().startswith("MAPS"):
        return JSONResponse(
            status_code=422,
            content={"error": "Utilisez un SIREN à 9 chiffres ou laissez vide"}
        )

    if not body.denomination and not (provided_siren and len(provided_siren) == 9):
        return JSONResponse(
            status_code=422,
            content={"error": "Nom d'entreprise requis si aucun SIREN n'est fourni"}
        )

    # Attempt SIRENE lookup if 9-digit SIREN provided
    sirene_record = None
    if provided_siren and len(provided_siren) == 9 and provided_siren.isdigit():
        sirene_record = await fetch_one(
            "SELECT siren, denomination, enseigne, naf_code, forme_juridique, adresse, code_postal, ville, departement, statut FROM companies WHERE siren = %s AND siren NOT LIKE 'MAPS%%'",
            (provided_siren,)
        )

    async with get_conn() as conn:
        # Generate MAPS ID (sequence is race-condition-safe)
        cur = await conn.execute("SELECT nextval('maps_id_seq')")
        next_id = (await cur.fetchone())[0]
        maps_siren = f"MAPS{next_id:05d}"

        # Determine entity fields
        denomination = body.denomination
        enseigne = body.enseigne
        adresse = body.adresse
        code_postal = body.code_postal
        ville = body.ville
        departement = body.departement
        # Validate department: must be a valid 2-3 digit code or Corsica (2A/2B), not "FR"
        if departement and departement.upper() not in ("2A", "2B") and not (departement.isdigit() and 2 <= len(departement) <= 3):
            departement = None
        linked_siren = None
        link_confidence = None
        link_method = None

        if sirene_record:
            # Copy SIRENE reference data (only if not user-provided)
            if not denomination:
                denomination = sirene_record.get("denomination")
            if not enseigne:
                enseigne = sirene_record.get("enseigne")
            if not adresse:
                adresse = sirene_record.get("adresse")
            if not code_postal:
                code_postal = sirene_record.get("code_postal")
            if not ville:
                ville = sirene_record.get("ville")
            if not departement:
                departement = sirene_record.get("departement")
            linked_siren = provided_siren
            link_confidence = "confirmed"
            link_method = "manual"

        # Insert company row
        await conn.execute("""
            INSERT INTO companies (
                siren, denomination, enseigne, adresse, code_postal, ville, departement,
                statut, linked_siren, link_confidence, link_method, workspace_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'A', %s, %s, %s, %s)
        """, (
            maps_siren, denomination, enseigne, adresse, code_postal, ville, departement,
            linked_siren, link_confidence, link_method, workspace_id
        ))

        # Insert contacts row if any contact data provided
        has_contact = any([
            body.phone, body.email, body.website,
            body.social_linkedin, body.social_facebook, body.social_instagram,
            body.social_tiktok, body.social_twitter, body.social_whatsapp, body.social_youtube,
        ])
        if has_contact:
            await conn.execute("""
                INSERT INTO contacts (
                    siren, source,
                    phone, email, website,
                    social_linkedin, social_facebook, social_instagram,
                    social_tiktok, social_twitter, social_whatsapp, social_youtube,
                    collected_at
                ) VALUES (%s, 'manual', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (siren, source) DO UPDATE SET
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email,
                    website = EXCLUDED.website,
                    social_linkedin = EXCLUDED.social_linkedin,
                    social_facebook = EXCLUDED.social_facebook,
                    social_instagram = EXCLUDED.social_instagram,
                    social_tiktok = EXCLUDED.social_tiktok,
                    social_twitter = EXCLUDED.social_twitter,
                    social_whatsapp = EXCLUDED.social_whatsapp,
                    social_youtube = EXCLUDED.social_youtube,
                    collected_at = NOW()
            """, (
                maps_siren,
                body.phone, body.email, body.website,
                body.social_linkedin, body.social_facebook, body.social_instagram,
                body.social_tiktok, body.social_twitter, body.social_whatsapp, body.social_youtube,
            ))

        # Insert officers
        for officer in (body.officers or []):
            nom = (officer.nom or "").strip()
            prenom = (officer.prenom or "").strip()
            if not nom and not prenom:
                continue
            await conn.execute("""
                INSERT INTO officers (siren, nom, prenom, role, source)
                VALUES (%s, %s, %s, %s, 'manual')
                ON CONFLICT (siren, nom, COALESCE(prenom, '')) DO NOTHING
            """, (maps_siren, nom, prenom or None, officer.role or None))

        # Create batch_tags entry so entity appears in dashboard
        batch_name = "Ajout manuel"
        batch_id = f"MANUAL_{maps_siren}"
        await conn.execute("""
            INSERT INTO batch_data (batch_id, batch_name, status, total_companies, workspace_id)
            VALUES (%s, %s, 'completed', 1, %s)
        """, (batch_id, batch_name, workspace_id))

        await conn.execute("""
            INSERT INTO batch_tags (siren, batch_id, batch_name, workspace_id, tagged_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, (maps_siren, batch_id, batch_name, workspace_id))

        # Notes
        if body.notes and body.notes.strip():
            await conn.execute("""
                INSERT INTO company_notes (siren, user_id, username, text)
                VALUES (%s, %s, %s, %s)
            """, (maps_siren, user_id, username, body.notes.strip()))

        # Batch log
        await conn.execute("""
            INSERT INTO batch_log (batch_id, siren, action, result, source_url, timestamp, detail)
            VALUES (%s, %s, 'entity_created', 'success', NULL, NOW(), %s)
        """, (batch_id, maps_siren, f"Création manuelle par {username}"))

        await conn.commit()

    await log_activity(
        user_id, username, "entity_created", "company", maps_siren,
        f"Entreprise créée manuellement : {denomination} ({maps_siren})"
    )

    return {
        "status": "ok",
        "siren": maps_siren,
        "denomination": denomination,
        "linked_siren": linked_siren,
        "link_confidence": link_confidence,
        "link_method": link_method,
    }


# ---------------------------------------------------------------------------
# Async SIRENE match suggestions — GET /api/companies/{siren}/suggest-matches
# ---------------------------------------------------------------------------

@router.get("/{siren}/suggest-matches")
async def suggest_matches(siren: str, request: Request):
    """Run live SIRENE matching for an unmatched MAPS entity.

    Called asynchronously by the frontend after the company page loads.
    Returns suggested matches without blocking the main company detail.
    """
    if not siren.startswith("MAPS"):
        return {"matches": []}

    # MAPS workspace gate — return empty matches (not 403) to avoid breaking frontend
    is_admin, ws_id = _get_workspace_filter(request)
    if not is_admin:
        owner = await fetch_one(
            "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
        )
        if owner and owner.get("workspace_id") != ws_id:
            return {"matches": []}

    company = await fetch_one(
        "SELECT denomination, adresse, departement, link_confidence FROM companies WHERE siren = %s",
        (siren,)
    )
    if not company or company.get("link_confidence") in ("confirmed", "rejected"):
        return {"matches": []}

    try:
        from fortress.matching.entities import find_matches
        async with get_conn() as conn:
            matches = await find_matches(
                maps_siren=siren,
                maps_addr=company.get("adresse"),
                maps_name=company.get("denomination"),
                departement=company.get("departement"),
                conn=conn,
            )
            return {"matches": [
                {
                    "siren": m.siren,
                    "denomination": m.denomination,
                    "confidence": m.confidence,
                    "method": m.method,
                    "reason": m.reason,
                    "address": m.address,
                    "ville": m.ville,
                }
                for m in matches
            ]}
    except Exception:
        return {"matches": []}
