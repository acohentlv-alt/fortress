# Frontend Agent Plan — UX Review Feedback (v1)

Based on the user's page-by-page review of the current UI. These changes are all frontend-only except where noted as requiring backend API endpoints.

---

## 1. Dashboard — Add "Par Recherche" Tab

**Currently:** 2 tabs — "Par Localisation" and "Par Job"
**Requested:** 3 tabs — add "Par Recherche"

### What "Par Recherche" Means

A third view that shows batches grouped by **search query string** (the raw user input like "TRANSPORT 66"). This is subtly different from "Par Job" which groups by `query_name` (the display name). "Par Recherche" should show:

- The original search text
- All batches spawned from that search
- Batch size, scrape progress, status per batch
- Clickable → navigates to job detail

### Files to Modify

#### [MODIFY] [dashboard.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/dashboard.js)

- Add third toggle button: `📋 Par Recherche` (line 108-109)
- Add click handler: calls existing `getJobs()` and groups by `query_id` prefix or search text
- Add `renderBySearch(jobs)` function rendering cards similar to `_renderGroupCard`

---

## 2. Delete / Cancel Capabilities

**Currently:** No delete or cancel actions anywhere in the UI
**Requested:** User must be able to:
- **Delete a completed batch** (from job detail or dashboard)
- **Cancel a running pipeline** (from Pipeline Live monitor view)
- **Delete a single entity** from results (from company card in job view)

> [!WARNING]
> **Backend dependency.** No delete/cancel API endpoints exist today. These must be created first:
> - `DELETE /api/jobs/{query_id}` — delete a batch + its query_tags (soft delete: set `status = 'deleted'`)
> - `POST /api/jobs/{query_id}/cancel` — mark a running job as `cancelled`, kill subprocess
> - `DELETE /api/companies/{siren}/tags/{query_id}` — remove a company from a batch result (untag only, never delete company data)

### Frontend Changes

#### [MODIFY] [job.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/job.js)

- Add red "🗑️ Supprimer ce batch" button in header next to export buttons
- Confirmation modal: "Êtes-vous sûr ? Les données des entreprises seront conservées."
- On confirm: `DELETE /api/jobs/{query_id}` → redirect to dashboard

#### [MODIFY] [monitor.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/monitor.js)

- Add "⏹ Arrêter le batch" button (visible only when `status === 'in_progress'`)
- Confirmation: "Arrêter le batch en cours ? Les données déjà collectées seront conservées."
- On confirm: `POST /api/jobs/{query_id}/cancel` → update status display

#### [MODIFY] [api.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/api.js)

- Add `deleteJob(queryId)` — `DELETE /api/jobs/{query_id}`
- Add `cancelJob(queryId)` — `POST /api/jobs/{query_id}/cancel`
- Add `untagCompany(siren, queryId)` — `DELETE /api/companies/{siren}/tags/{query_id}`

---

## 3. Refresh / Rerun Batch

**Currently:** No way to rerun a failed or suspicious batch
**Requested:** User can click "Relancer" to replay a batch with the same parameters

### Frontend Changes

#### [MODIFY] [job.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/job.js)

- Add "🔄 Relancer" button in header (visible for completed/failed batches)
- On click: calls `POST /api/batch/run` with the same parameters as the original batch
- Show toast: "Nouveau batch lancé — redirection vers le suivi..."
- Redirect to `#/monitor/{new_query_id}`

> [!IMPORTANT]
> **Backend may need to expose the original batch parameters.** The job detail API response should include `sector`, `departement`, `batch_size`, `naf_code`, `city`, `mode` so the frontend can re-submit them. Check if `GET /api/jobs/{query_id}` already returns these fields.

---

## 4. Company Page — Reorder Contact Section

**Currently:** Right column order is:
1. 🏛️ Identité juridique
2. 📊 Activité
3. 💰 Données financières
4. 📍 Localisation
5. 📞 Contact ← too far down
6. ⭐ Avis Google
7. 👤 Dirigeants
8. 📜 Historique

**Requested:** Contact card immediately after identity.

### New Order

1. 🏛️ Identité juridique (SIREN, SIRET, forme, statut, date)
2. 📞 Contact (phone, email, website, Maps, socials) ← moved up
3. ⭐ Avis Google (if exists)
4. 📍 Localisation
5. 📊 Activité
6. 💰 Données financières
7. 👤 Dirigeants
8. 📜 Historique

#### [MODIFY] [company.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/company.js)

Move lines 223-239 (Contact section) to immediately after lines 182-189 (Identité section). Merge Avis Google (lines 241-251) into the Contact section.

---

## 5. Data Provenance Tooltips

**Currently:** Data fields show values but no source attribution
**Requested:** Hover over any data value to see where it came from (e.g., "Source: Google Maps", "Source: SIRENE", "Source: Website Crawl")

### How It Works

Every field already has a known source:
- **SIREN, SIRET, Forme Juridique, NAF, Effectif, Ville, Code Postal** → Source: `SIRENE`
- **Phone, Address, Rating, Maps URL** → Source: `Google Maps`
- **Email, LinkedIn, Facebook** → Source: `Website Crawl`
- **Website URL** → Source: `Google Maps` (discovered via Maps, verified via crawl)
- **CA, Résultat Net** → Source: `Enrichissement` (future)

### Implementation

#### [MODIFY] [company.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/company.js)

Replace `detailRow(label, value)` with `detailRow(label, value, source)`:

```javascript
function detailRow(label, value, source = null) {
    const tooltip = source
        ? `<span class="provenance-badge" title="Source : ${source}">ℹ️</span>`
        : '';
    return `
        <div class="detail-row">
            <span class="detail-label">${label} ${tooltip}</span>
            <span class="detail-value">${value}</span>
        </div>
    `;
}
```

Then update every `detailRow()` call to pass the source:

```javascript
detailRow('SIREN', formatSiren(co.siren), 'SIRENE')
detailRow('Téléphone', mc.phone ? ... : ..., 'Google Maps')
detailRow('Email', mc.email ? ... : ..., 'Site web de l\'entreprise')
```

#### [NEW] CSS for `.provenance-badge` in `components.css`

Small info icon that shows a tooltip on hover with the data source name.

```css
.provenance-badge {
    cursor: help;
    font-size: 12px;
    opacity: 0.5;
    margin-left: 4px;
    transition: opacity 0.2s;
}
.provenance-badge:hover { opacity: 1; }
```

---

## 6. Update Enrichment Panel to Match Pipeline

**Currently:** Three checkboxes that don't match the real pipeline:
- 🌐 Website & Contacts → "Email, site web, réseaux sociaux, avis Google"
- 📞 Phone Numbers → "Numéro de téléphone principal via PagesJaunes" ← **WRONG: PagesJaunes is not used anymore**
- 💰 Financials → "SIRET, CA, résultat net, effectif via Pappers" ← **WRONG: Pappers is not integrated**

### Updated Panel (matching real pipeline)

The pipeline is fixed: **Maps → Crawl**. The enrichment panel should reflect this:

#### [MODIFY] [company.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/company.js)

Replace `enrichmentPanelHTML()` (lines 45-81):

```javascript
function enrichmentPanelHTML() {
    return `
        <div class="enrich-panel" id="enrich-panel">
            <div class="enrich-panel-title">⚡ Enrichissement</div>
            <div class="enrich-pipeline-preview">
                <div class="enrich-step">
                    <span class="enrich-step-icon">🗺️</span>
                    <div>
                        <div class="enrich-step-label">Google Maps</div>
                        <div class="enrich-step-desc">Téléphone, adresse, site web, avis, note</div>
                    </div>
                </div>
                <div class="enrich-step-arrow">→</div>
                <div class="enrich-step">
                    <span class="enrich-step-icon">🌐</span>
                    <div>
                        <div class="enrich-step-label">Site Web</div>
                        <div class="enrich-step-desc">Email, LinkedIn, Facebook, réseaux sociaux</div>
                    </div>
                </div>
            </div>
            <button class="enrich-submit" id="enrich-submit-btn">
                <span class="enrich-spinner"></span>
                <span class="enrich-submit-text">🚀 Lancer l'enrichissement</span>
            </button>
        </div>
    `;
}
```

**No more checkboxes** — the pipeline is fixed (Maps → Crawl), user just clicks "Lancer". The panel shows the 2 pipeline steps clearly so the user knows what will happen.

Update `_initEnrichmentPanel()` — remove checkbox logic, submit always sends `["contact_web", "contact_phone"]` together.

---

## Summary of Backend Dependencies

These items **require backend agent work before the frontend can implement them**:

| Feature | Backend Endpoint Needed |
|---------|------------------------|
| Delete batch | `DELETE /api/jobs/{query_id}` |
| Cancel running batch | `POST /api/jobs/{query_id}/cancel` |
| Remove company from batch | `DELETE /api/companies/{siren}/tags/{query_id}` |
| Rerun batch | Expose original params in `GET /api/jobs/{query_id}` response |

Everything else (reorder, tooltips, enrichment panel) is **frontend-only**.

## Implementation Order

1. **Company page reorder + enrichment panel update** (frontend-only, no risk)
2. **Data provenance tooltips** (frontend-only, no risk)
3. **Dashboard "Par Recherche" tab** (frontend-only, uses existing API)
4. **Delete/Cancel/Rerun** (blocked on backend endpoints)
