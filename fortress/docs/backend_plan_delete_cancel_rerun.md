# Backend Agent Plan — Delete / Cancel / Rerun API Endpoints

## Problem

**Plain English:** The user needs to be able to delete batches, cancel running pipelines, and rerun failed batches from the UI. Currently, none of these API endpoints exist. The frontend agent needs them before implementing the delete/cancel/rerun buttons.

---

## Proposed Endpoints

### 1. Delete a Batch

```
DELETE /api/jobs/{query_id}
```

**Business rules:**
- Set `scrape_jobs.status = 'deleted'` (soft delete — **never** hard-delete rows)
- Remove `query_tags` entries for this `query_id` (unlinks companies from this batch in dashboard views)
- Do **NOT** delete companies or contacts — other batches may reference them
- Return `200 OK` with `{ "deleted": true, "query_id": "..." }`
- Return `404` if `query_id` not found
- Return `409 Conflict` if job is `in_progress` (must cancel first)

#### [MODIFY] [jobs.py](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/api/routes/jobs.py)

```python
@router.delete("/jobs/{query_id}")
async def delete_job(query_id: str):
    """Soft-delete a batch. Removes query_tags but keeps company/contact data."""
    async with pool.connection() as conn:
        job = await conn.execute("SELECT status FROM scrape_jobs WHERE query_id = %s", (query_id,))
        row = await job.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Job introuvable"})
        if row[0] == 'in_progress':
            return JSONResponse(status_code=409, content={"error": "Arrêtez le batch d'abord"})

        await conn.execute("UPDATE scrape_jobs SET status = 'deleted' WHERE query_id = %s", (query_id,))
        await conn.execute("DELETE FROM query_tags WHERE query_id = %s", (query_id,))
        await conn.commit()
    return {"deleted": True, "query_id": query_id}
```

---

### 2. Cancel a Running Batch

```
POST /api/jobs/{query_id}/cancel
```

**Business rules:**
- Set `scrape_jobs.status = 'cancelled'`
- Kill the subprocess running `runner.py` for this batch (if PID is tracked)
- Data already collected is preserved (checkpoint system handles this)
- Return `200 OK` with `{ "cancelled": true, "query_id": "..." }`
- Return `404` if not found
- Return `409` if job is not `in_progress` or `queued`

> [!IMPORTANT]
> **Subprocess management.** Currently `batch.py` spawns runner.py via `subprocess.Popen`. To cancel, we need to either:
> - Store the PID in `scrape_jobs` when spawning → send SIGTERM on cancel
> - Or set a flag in the DB that `runner.py` checks between waves (graceful shutdown)
>
> **Recommended:** The flag approach is safer. Add a `cancel_requested` boolean column to `scrape_jobs`. The runner checks this before each wave. Cancel endpoint sets the flag.

#### [MODIFY] [jobs.py](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/api/routes/jobs.py)

```python
@router.post("/jobs/{query_id}/cancel")
async def cancel_job(query_id: str):
    """Request cancellation of a running batch."""
    async with pool.connection() as conn:
        job = await conn.execute("SELECT status FROM scrape_jobs WHERE query_id = %s", (query_id,))
        row = await job.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Job introuvable"})
        if row[0] not in ('in_progress', 'queued', 'triage'):
            return JSONResponse(status_code=409, content={"error": "Le batch n'est pas en cours"})

        await conn.execute(
            "UPDATE scrape_jobs SET cancel_requested = TRUE WHERE query_id = %s",
            (query_id,),
        )
        await conn.commit()
    return {"cancelled": True, "query_id": query_id}
```

#### [MODIFY] [schema.sql](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/database/schema.sql)

Add column to `scrape_jobs`:

```sql
ALTER TABLE scrape_jobs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN DEFAULT FALSE;
```

#### [MODIFY] [batch_processor.py](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/module_d/batch_processor.py)

Before each wave, check the flag:

```python
# At the start of each wave loop iteration
async with pool.connection() as conn:
    row = await conn.execute(
        "SELECT cancel_requested FROM scrape_jobs WHERE query_id = %s",
        (query_id,)
    )
    result = await row.fetchone()
    if result and result[0]:
        log.info("batch_processor.cancellation_requested", query_id=query_id)
        await conn.execute(
            "UPDATE scrape_jobs SET status = 'cancelled' WHERE query_id = %s",
            (query_id,),
        )
        await conn.commit()
        break
```

---

### 3. Remove Company from Batch Results (Untag)

```
DELETE /api/companies/{siren}/tags/{query_id}
```

**Business rules:**
- Remove the `query_tags` row for this SIREN + query_id
- Do NOT delete the company or contact data
- Return `200 OK` with `{ "untagged": true }`
- Return `404` if the tag doesn't exist

#### [MODIFY] [companies.py](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/api/routes/companies.py)

```python
@router.delete("/companies/{siren}/tags/{query_id}")
async def untag_company(siren: str, query_id: str):
    """Remove a company from a batch's results (untag only)."""
    async with pool.connection() as conn:
        result = await conn.execute(
            "DELETE FROM query_tags WHERE siren = %s AND query_id = %s RETURNING siren",
            (siren, query_id),
        )
        row = await result.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Tag introuvable"})
        await conn.commit()
    return {"untagged": True, "siren": siren, "query_id": query_id}
```

---

### 4. Expose Batch Parameters for Rerun

The frontend needs the original batch parameters to resubmit. Check if `GET /api/jobs/{query_id}` already returns `sector`, `departement`, `batch_size`, `naf_code`, `city`, `mode`.

If not, add these fields to the job detail response — they should already be stored in `scrape_jobs` from when the batch was created.

---

## Verification

- `EXPLAIN ANALYZE` on the DELETE FROM query_tags query (should use index on `(siren, query_id)` or `(query_id)`)
- Test cancel flow: start a batch → request cancel → verify runner stops after current wave
- Test delete flow: delete a completed batch → verify companies still exist in other batches
- Test untag flow: untag a company → verify it disappears from that batch's results but still exists globally

---

## 5. Expose Original Batch Parameters for Rerun / Refresh

The frontend "Relancer" and "Rafraîchir" buttons both need to re-submit a batch with the same parameters. The `GET /api/jobs/{query_id}` response must include:

```json
{
    "query_id": "...",
    "query_name": "TRANSPORT 66",
    "sector": "TRANSPORT",
    "departement": "66",
    "batch_size": 50,
    "naf_code": "49.41A",
    "city": null,
    "mode": "discovery"
}
```

Check if `scrape_jobs` already stores these fields. If not, add them to the table.

**Refresh flow (frontend handles orchestration):**
1. Frontend calls `POST /api/jobs/{query_id}/cancel`
2. Frontend waits for status to become `cancelled`
3. Frontend calls `POST /api/batch/run` with same parameters
4. Frontend redirects to `#/monitor/{new_query_id}`

---

## 6. Dashboard "Par Job" — Sector Grouping Data

The dashboard needs to group batches by **sector** (not by query string). Either:

- Include `sector` field in `GET /api/jobs` response (preferred — no new endpoint)
- Or create `GET /api/dashboard/stats-by-sector` that returns pre-grouped data

The `sector` value comes from the batch creation form and should already be in `scrape_jobs.query_name` or could be extracted from the NAF code prefix.
