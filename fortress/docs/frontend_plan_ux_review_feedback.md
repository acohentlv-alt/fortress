# Frontend Agent Plan — UX Review Feedback (v2)

Based on the user's page-by-page review. All changes are frontend-only except where noted as requiring backend endpoints.

---

## 1. Dashboard — Fix Tab Naming + Add 3rd Tab

**Current problem:** The "Par Job" tab actually groups batches by `query_name` (the raw search string like "TRANSPORT 66"). That's really a **search history** view, not a job view.

### Corrected Tab Structure

#### 📍 Tab 1: Par Localisation
Department folders containing enriched company cards.
- Each department is a collapsible folder card showing: `66 — Pyrénées-Orientales (127 entreprises)`
- Inside each folder: company cards with contact indicators, completude bar
- Quality gauges per department (📞 Tél. / ✉️ Email / 🌐 Web)

#### 📋 Tab 2: Par Job
Groups all batches by **sector/activity type** — the business category, not the raw search string.
- Example: all "TRANSPORT" batches across departments 66, 34, 11 grouped under one "TRANSPORT" card
- Shows total companies found across all related batches
- Shows combined quality gauges
- Clickable → opens a filtered view of all companies in that sector

> [!IMPORTANT]
> **Backend dependency:** Needs `sector` field in jobs response, or grouping by NAF code prefix.

#### 🔍 Tab 3: Par Recherche
A **search history timeline** — every batch the user has launched, organized by the exact search query.

Each search card shows:
```
┌────────────────────────────────────────────────────────┐
│  🔍 TRANSPORT 66                                       │
│  ────────────────────────────────────────────────────── │
│  📅 Dernière recherche: 12/03/2026 à 14:30             │
│  📊 3 batchs lancés · 127 entreprises collectées       │
│  ████████████████████░░░░  84% complétude              │
│                                                        │
│  📞 86% tél. · ✉️ 72% email · 🌐 91% web              │
│                                                        │
│  ▼ Historique des batchs                               │
│  ├─ ● Batch #3 — 50/50 ✅ Terminé (12/03)             │
│  ├─ ● Batch #2 — 50/50 ✅ Terminé (10/03)             │
│  └─ ● Batch #1 — 27/30 ✅ Terminé (08/03)             │
│                                                        │
│  [📥 Exporter tout]  [🔄 Relancer]  [🗑️ Supprimer]    │
└────────────────────────────────────────────────────────┘
```

Key features:
- **Grouped by `query_name`** (case-insensitive) — this is the current `renderByJob` logic
- **Aggregate stats** across all batches within the same search: total companies, quality %
- **Batch timeline** expandable — shows each batch with status, date, progress
- **Action buttons per search:** export all results, relaunch, delete
- **Sorted** by most recent batch date (newest first)
- **Filtering:** search bar at the top to filter searches by name

### Files to Modify

#### [MODIFY] [dashboard.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/dashboard.js)

- Rename current "Par Job" tab → "Par Recherche" and enhance its cards with aggregate stats + action buttons
- Add new "Par Job" tab grouped by sector/NAF
- Add 3 toggle buttons: `📍 Par Localisation` / `📋 Par Job` / `🔍 Par Recherche`
- For "Par Localisation": department folders must contain company cards inside (not just summary counts)
- Add search filter bar for "Par Recherche" tab

---

## 2. Delete / Cancel / Refresh Capabilities

**Requested actions:**
- **Delete a completed batch** (from job detail or dashboard)
- **Cancel a running pipeline** (from Pipeline Live monitor view)
- **Refresh (restart) a currently running pipeline** (from monitor view)
- **Delete a single entity** from results (from company card in job view)
- **Rerun a completed/failed batch** (from job detail)

> [!WARNING]
> **Backend dependency.** These endpoints must exist:
> - `DELETE /api/jobs/{query_id}` — soft-delete batch
> - `POST /api/jobs/{query_id}/cancel` — cancel running batch
> - `DELETE /api/companies/{siren}/tags/{query_id}` — untag company from batch
> - `GET /api/jobs/{query_id}` must return original batch params (`sector`, `departement`, `batch_size`, `naf_code`, `city`, `mode`) for rerun

### Confirmation Modal — Show Exactly What Gets Deleted

The generic "Êtes-vous sûr ?" is not enough. The modal must list exactly what will be affected:

**For deleting a batch:**
```
🗑️ Supprimer ce batch ?

Batch: TRANSPORT 66
Créé le: 12/03/2026 à 14:30
Entreprises collectées: 47
Données de contact liées: 43 téléphones, 38 emails

⚠️ Les tags de recherche seront supprimés.
✅ Les fiches entreprises et contacts resteront dans la base.

[Annuler]  [Supprimer]
```

**For cancelling a pipeline:**
```
⏹ Arrêter ce batch ?

Batch: TRANSPORT 66
Progression: 23/50 entreprises (46%)
Vague: 2/4

✅ Les 23 entreprises déjà collectées seront conservées.
⚠️ Les 27 restantes ne seront pas traitées.

[Annuler]  [Arrêter le batch]
```

**For removing a company:**
```
🗑️ Retirer cette entreprise ?

BAILLOEUIL (SIREN 400 643 128)

⚠️ L'entreprise sera retirée de ce batch.
✅ Sa fiche et ses contacts resteront dans la base.

[Annuler]  [Retirer]
```

### Frontend Changes

#### [MODIFY] [job.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/job.js)

- Add "🗑️ Supprimer" button in header (next to export buttons)
- Add "🔄 Relancer" button (for completed/failed batches)
- Both trigger detailed confirmation modals showing affected data

#### [MODIFY] [monitor.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/monitor.js)

- Add "⏹ Arrêter" button (visible when `status === 'in_progress'`)
- Add "🔄 Rafraîchir" button — restarts the same batch (cancel current + relaunch with same params)
- Confirmation modal with progress data

#### [NEW] [modal.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/components/modal.js) (or add to components.js)

Reusable confirmation modal component:
```javascript
export function showConfirmModal({ title, body, confirmLabel, onConfirm, danger = false })
```

#### [MODIFY] [api.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/api.js)

- Add `deleteJob(queryId)`
- Add `cancelJob(queryId)`
- Add `untagCompany(siren, queryId)`

---

## 3. Company Page — Reorder Contact Section

**Current order** (right column):
1. 🏛️ Identité juridique
2. 📊 Activité
3. 💰 Données financières
4. 📍 Localisation
5. 📞 Contact ← too far down
6. ⭐ Avis Google
7. 👤 Dirigeants
8. 📜 Historique

**New order:**
1. 🏛️ Identité juridique
2. 📞 Contact (+ Avis Google merged in) ← **moved to #2**
3. 📍 Localisation
4. 📊 Activité
5. 💰 Données financières
6. 👤 Dirigeants
7. 📜 Historique

#### [MODIFY] [company.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/company.js)

Move Contact section (lines 223-251) to immediately after Identité section (after line 189).

---

## 4. Data Provenance Tooltips — Including Social Media Sources

**Rule:** Every single data field shows its source on hover. No exceptions — including social media links.

### Source Mapping

| Field | Source | Tooltip text |
|-------|--------|-------------|
| SIREN, SIRET, Forme, NAF, Effectif, Ville, CP | SIRENE | "Source : Registre SIRENE (INSEE)" |
| Phone | Google Maps | "Source : Google Maps" |
| Address (enriched) | Google Maps | "Source : Google Maps" |
| Website URL | Google Maps | "Source : Google Maps" |
| Email | Website crawl | "Source : Site web ({domain})" |
| Rating, Reviews | Google Maps | "Source : Google Maps" |
| LinkedIn | Website crawl | "Source : Trouvé sur {website_url}" |
| Facebook | Website crawl | "Source : Trouvé sur {website_url}" |
| Twitter | Website crawl | "Source : Trouvé sur {website_url}" |
| Maps URL | Google Maps | "Source : Google Maps" |

> [!IMPORTANT]
> **Backend dependency for social source detail.** Currently the `contacts` table stores `social_linkedin`, `social_facebook` etc. but doesn't track *which page* they were found on. To show "Trouvé sur www.bailloeuil.com/contact", the backend would need to store page-level provenance. 
>
> **Short-term workaround:** If the contact has a `website` field, use that as the social media source: `"Source : Trouvé sur {mc.website}"`. This is accurate enough — social links are only extracted during website crawl of that company's website.

### Implementation

#### [MODIFY] [company.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/company.js)

Update `detailRow()` to accept a source parameter:

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

Update all calls:
```javascript
detailRow('SIREN', ..., 'Registre SIRENE')
detailRow('Téléphone', ..., 'Google Maps')
detailRow('Email', ..., mc.website ? `Site web (${mc.website})` : 'Site web')
detailRow('LinkedIn', ..., mc.website ? `Trouvé sur ${mc.website}` : 'Site web')
detailRow('Facebook', ..., mc.website ? `Trouvé sur ${mc.website}` : 'Site web')
```

#### [MODIFY] [components.css](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/css/components.css)

```css
.provenance-badge {
    cursor: help;
    font-size: 12px;
    opacity: 0.4;
    margin-left: 4px;
    transition: opacity var(--duration-fast);
}
.provenance-badge:hover { opacity: 1; }
```

---

## 5. Update Enrichment Panel to Match Pipeline

**Problem:** Current panel shows 3 checkboxes (Website, PagesJaunes, Pappers) — none of these match the real pipeline.

**Fix:** Replace with a visual pipeline diagram showing the 2 real steps, plus timing expectations.

#### [MODIFY] [company.js](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/frontend/js/pages/company.js)

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
                        <div class="enrich-step-time">~5 secondes</div>
                    </div>
                </div>
                <div class="enrich-step-arrow">→</div>
                <div class="enrich-step">
                    <span class="enrich-step-icon">🌐</span>
                    <div>
                        <div class="enrich-step-label">Site Web</div>
                        <div class="enrich-step-desc">Email, LinkedIn, Facebook, réseaux sociaux</div>
                        <div class="enrich-step-time">~20 secondes (recherche sur le site)</div>
                    </div>
                </div>
            </div>
            <div class="enrich-time-notice">
                ⏱️ Durée estimée : ~25 secondes par entreprise
            </div>
            <button class="enrich-submit" id="enrich-submit-btn">
                <span class="enrich-spinner"></span>
                <span class="enrich-submit-text">🚀 Lancer l'enrichissement</span>
            </button>
        </div>
    `;
}
```

**No more checkboxes** — the pipeline is fixed (Maps → Crawl), user just clicks "Lancer".

---

## Backend Dependencies Summary (Updated)

| Feature | Backend Endpoint | Status |
|---------|-----------------|--------|
| Delete batch | `DELETE /api/jobs/{query_id}` | ❌ Doesn't exist |
| Cancel running batch | `POST /api/jobs/{query_id}/cancel` + `cancel_requested` column | ❌ Doesn't exist |
| Remove company from batch | `DELETE /api/companies/{siren}/tags/{query_id}` | ❌ Doesn't exist |
| Rerun batch | `GET /api/jobs/{query_id}` must return `sector`, `departement`, `batch_size`, `naf_code`, `city`, `mode` | ⚠️ Check if fields exist |
| Refresh running pipeline | Cancel current + relaunch → uses cancel + `POST /api/batch/run` (both must work) | ❌ Cancel doesn't exist |
| "Par Job" grouping by sector | `GET /api/dashboard/stats-by-sector` or include `sector` in jobs response | ⚠️ Check if data available |
| Social provenance detail | Store which page social links were found on | ✅ Workaround: use `mc.website` as source |

> [!IMPORTANT]
> **Blocking dependencies:** Items 1-5 in the table above are blocked on backend work. The frontend agent should implement items 3, 4, 5 first (company reorder, provenance tooltips, enrichment panel) since those are frontend-only.

## Implementation Order

1. **Company page reorder + enrichment panel** (frontend-only, zero risk)
2. **Data provenance tooltips** (frontend-only, zero risk)
3. **Dashboard "Par Recherche" rename** (frontend-only — just rename the current tab)
4. **Confirmation modal component** (frontend-only, preparation)
5. **Delete / Cancel / Refresh / Rerun** (blocked on backend endpoints)
6. **Dashboard "Par Job" true sector grouping** (needs backend data)
