# BRIEF — E4 (Per-Query Drill-Down + Parent/Child Count Fix)

> **EXECUTOR — MUST RUN WITH `isolation: "worktree"`.**
> If worktree creation fails, STOP and `cd "/Users/alancohen/Project Alan copy/fortress"` to fix the orchestrator's cwd, then retry the launch with the SAME `isolation: "worktree"` parameter. **No "no-isolation" fallback ever.** See `feedback_no_isolation_fallback.md`.
>
> **Worktree base branch:** `worktree-agent-a4541cf1509610660` (E1+E2+E3 already shipped here). Do NOT branch from `origin/main` — the E1 done/running/queued queries panel is required for E4 to make sense.
>
> **Out of scope:** discovery.py, schema migrations, commits/pushes, social-field GREEN gating, anything not listed below.

---

## 1. Plain-English summary (for Alan)

Two improvements to the post-batch job page (`#/job/:id`) and the live monitor (`#/monitor/:id`):

**E4.A — Drill-down by query.** Today the "Recherches effectuées" panel shows each search line ("camping perpignan", "transport 33", etc.) with the count of new entities. The companies grid below shows ALL entities found by ALL queries mixed together. **After E4.A:** clicking a query line filters the grid to ONLY entities found by that specific query. A removable chip "Filtré par : <query> (×)" appears above the grid, the per-batch CSV button switches to download only the filtered rows, and clicking the same row again (or the chip's ×) clears the filter.

**E4.B — Fix the count contradiction.** Today a primary query like "transport 33" with widening expansions can show "2 nouvelles entités" on the parent line while the child sub-bucket shows "11 nouvelles entités" — confusing. After E4.B the parent reads `"13 total (dont 11 par élargissement)"` — headline cumulative number first, breakdown in parens. One number, no contradiction.

---

## 2. Architecture

E4.A piggybacks on the existing `state_filter` query-string pattern. We add a NEW optional `search_query` querystring to two endpoints (`GET /api/jobs/{id}/companies` and `GET /api/export/{id}/csv`), wired through `batch_log.search_query` (column exists per `database/schema.sql:182`). Frontend: clickable rows in `queries_panel.js` post a custom `qp:filter` DOM event that `job.js` and `monitor.js` listen for, then call `loadCompanies(..., filter)` and update the chip + CSV button. State is stored in a module-level `_currentSearchQuery` variable mirroring the existing `_currentFilter` pattern at `job.js:539`.

E4.B is a pure presentation fix in `queries_panel.js` lines 58-65 — recompute `expansionTotal` and reformat `summaryParts[0]`.

---

## 3. What gets built — file-by-file

### 3.1 `fortress/api/routes/jobs.py` — add `search_query` filter to companies endpoint

**Existing signature** (`jobs.py:853-862`):
```python
@router.get("/{batch_id}/companies")
async def get_job_companies(
    batch_id: str,
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: str = Query("", description="Filter by name, city, or SIREN"),
    sort: str = Query("completude", description="Sort by: completude | name | date"),
    state_filter: str = Query("", description="Filter: naf_confirmed | naf_sibling | pending | unlinked"),
):
```

**Add a new parameter** between `state_filter` and the function body (line 862):
```python
    search_query: str = Query("", description="Filter to entities found by exactly this Maps query"),
```

**Right after the existing `filter_clause` block** (around `jobs.py:907-910`), insert:
```python
    # E4.A drill-down — filter to SIRENs whose batch_log row recorded this exact search_query.
    # batch_log.search_query is populated by log_audit() at processing/dedup.py:282 from
    # _current_search_query in discovery.py.
    sq_clause = ""
    sq_params: list = []
    if search_query:
        sq_clause = """
            AND co.siren IN (
                SELECT DISTINCT siren FROM batch_log
                WHERE batch_id = %s AND search_query = %s
            )
        """
        sq_params = [qid, search_query]

    where_extra = where_extra + filter_clause + sq_clause
```

**Then update both SQL `tuple(...)` builds** that follow `where_extra`:

- Count query (currently `tuple([qid] + search_params)` at `jobs.py:926`) → `tuple([qid] + search_params + sq_params)`
- Fetch query (currently `tuple([qid] + params_fetch)` at `jobs.py:957`) → must become `tuple([qid] + search_params + sq_params + [page_size, offset])`. Restructure the existing `params_fetch = search_params + [page_size, offset]` to interleave `sq_params` correctly. Concretely replace:

```python
    params_fetch = search_params + [page_size, offset]
    rows = await fetch_all(f"""
        ...
    """, tuple([qid] + params_fetch))
```

with:

```python
    rows = await fetch_all(f"""
        ...
    """, tuple([qid] + search_params + sq_params + [page_size, offset]))
```

**Note on placeholder order** — `search_params` placeholders fire first (in `where_extra`), then `sq_params` (in `sq_clause`), then `LIMIT/OFFSET`. The query already starts with `WITH batch_sirens AS (SELECT ... WHERE batch_id = %s)` consuming the first `qid`. Verify the count of `%s` matches the params tuple length when you grep for `%s` in the final query string.

### 3.2 `fortress/api/routes/export.py` — add `search_query` filter to per-batch CSV export

The per-batch endpoint is `export_csv` at `export.py:370-402`. It calls `_fetch_export_data(batch_id)` (defined at `export.py:138-169`). We have two options:

**Option chosen: extend `_fetch_export_data` with optional `search_query` param** (cleanest — keeps JSONL/XLSX consistent if drill-down ever needed there).

Replace `_fetch_export_data` signature and body. Currently:
```python
async def _fetch_export_data(batch_id: str) -> list[dict]:
    ...
    return await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags WHERE batch_id = %s')}
        SELECT {_EXPORT_SELECT}
        FROM (SELECT DISTINCT siren FROM batch_tags WHERE batch_id = %s) sa
        JOIN companies co ON co.siren = sa.siren
        {_EXPORT_JOINS}
        WHERE (co.siren NOT LIKE 'MAPS%%' OR co.link_confidence IN ('confirmed', 'pending'))
          AND (co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge', 'siret_address_naf'))
        ORDER BY (CASE WHEN co.link_confidence = 'pending' THEN 1 ELSE 0 END),
                 co.denomination
    """, (batch_id, batch_id))
```

Change to:
```python
async def _fetch_export_data(batch_id: str, search_query: str | None = None) -> list[dict]:
    ...
    job = await fetch_one(
        "SELECT batch_id FROM batch_data WHERE batch_id = %s", (batch_id,)
    )
    if not job:
        return []

    sq_clause = ""
    sq_params: tuple = ()
    if search_query:
        # E4.A — only entities whose batch_log row recorded this exact search_query.
        sq_clause = """
          AND co.siren IN (
              SELECT DISTINCT siren FROM batch_log
              WHERE batch_id = %s AND search_query = %s
          )
        """
        sq_params = (batch_id, search_query)

    return await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags WHERE batch_id = %s')}
        SELECT {_EXPORT_SELECT}
        FROM (SELECT DISTINCT siren FROM batch_tags WHERE batch_id = %s) sa
        JOIN companies co ON co.siren = sa.siren
        {_EXPORT_JOINS}
        WHERE (co.siren NOT LIKE 'MAPS%%' OR co.link_confidence IN ('confirmed', 'pending'))
          AND (co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge', 'siret_address_naf'))
          {sq_clause}
        ORDER BY (CASE WHEN co.link_confidence = 'pending' THEN 1 ELSE 0 END),
                 co.denomination
    """, (batch_id, batch_id) + sq_params)
```

Then update `export_csv` at `export.py:370-402`. Add `search_query` query param and pass through:
```python
@router.get("/{batch_id}/csv")
async def export_csv(batch_id: str, request: Request, search_query: str = Query("", description="E4.A drill-down filter")):
    ...
    rows = await _fetch_export_data(batch_id, search_query=search_query or None)
    ...
```

**Add `Query` import (verified missing).** `export.py:15` currently reads `from fastapi import APIRouter, Request`. As an explicit Edit step: replace that line with `from fastapi import APIRouter, Query, Request`. Without this, the `Query("")` default in the new `search_query` parameter raises `NameError` at import time.

**Filename suffix** — when `search_query` is set, change the Content-Disposition filename to `{batch_id}_{slug(search_query)}.csv` so Cindy can tell drill-down exports apart. Sluggify with: `re.sub(r'[^a-zA-Z0-9]+', '_', search_query).strip('_')[:50]`.

**Do NOT** modify `/master/csv`, `/master/xlsx`, `/contacts/csv`, `/bulk/csv`, `/{batch_id}/jsonl`, `/{batch_id}/xlsx`. Drill-down is per-batch CSV only for now.

### 3.3 `fortress/frontend/js/api.js` — extend two helpers

At `api.js:185-194`, change `getJobCompanies` to accept `searchQuery`:
```js
export async function getJobCompanies(batchId, { page = 1, pageSize = 20, search = '', sort = 'completude', filter = '', searchQuery = '' } = {}) {
    const params = new URLSearchParams({
        page: page.toString(),
        page_size: pageSize.toString(),
        search,
        sort,
    });
    if (filter) params.set('state_filter', filter);
    if (searchQuery) params.set('search_query', searchQuery);
    return await request(`/jobs/${encodeURIComponent(batchId)}/companies?${params}`);
}
```

At `api.js:300-302`, change `getExportUrl` to accept optional `searchQuery`:
```js
export function getExportUrl(batchId, format = 'csv', searchQuery = '') {
    const base = `${API_BASE}/export/${encodeURIComponent(batchId)}/${format}`;
    if (!searchQuery) return base;
    const params = new URLSearchParams({ search_query: searchQuery });
    return `${base}?${params}`;
}
```

### 3.4 `fortress/frontend/js/components/queries_panel.js` — clickable rows + count fix

#### E4.B — Count fix (lines 58-65)

Replace the existing primary-row summary build:
```js
const primaryEntityCount = p.new_companies || 0;
const expansionCount = expansions.length;
const durationStr = p.duration_sec != null ? `${p.duration_sec}s` : '';

const summaryParts = [`${primaryEntityCount} ${t('monitor.queriesNewEntities')}`];
if (expansionCount > 0) summaryParts.push(`${expansionCount} ${t('monitor.queriesElargissements') || 'élargissements'}`);
if (durationStr) summaryParts.push(durationStr);
```

with:
```js
const primaryEntityCount = p.new_companies || 0;
const expansionEntityTotal = expansions.reduce((s, e) => s + (e.new_companies || 0), 0);
const expansionCount = expansions.length;
const totalEntityCount = primaryEntityCount + expansionEntityTotal;
const durationStr = p.duration_sec != null ? `${p.duration_sec}s` : '';

const summaryParts = [];
if (expansionCount > 0) {
    // E4.B — headline cumulative total, expansion breakdown in parens.
    // FR: "13 total (dont 11 par élargissement)"
    summaryParts.push(t('monitor.queriesPrimaryWithExpansions', {
        total: totalEntityCount,
        expansion: expansionEntityTotal,
    }));
} else {
    summaryParts.push(`${primaryEntityCount} ${t('monitor.queriesNewEntities')}`);
}
if (durationStr) summaryParts.push(durationStr);
```

The `élargissements` count chip stays gone (it's now folded into the new compound string). Sub-bucket headers (`cityTotal`, `postalTotal`) at lines 164 and 208 are unchanged — those are the sums Cindy uses to drill into each widening type.

#### E4.A — clickable primary + branch rows

For **primary rows** (the two `lines.push` blocks at queries_panel.js:69-77 [no expansions] and 79-101 [with expansions]):

Add `data-search-query="${escapeHtml(p.query)}"` to the **outer** `<div class="qp-row qp-row--done">` element on both branches. Then wrap the click handler to dispatch a custom event (and preserve the existing chevron toggle for the with-expansions case):

For the **no-expansions** primary row, replace the inner `<div style="display:flex...">` with:
```html
<div
    class="qp-row-clickable"
    data-search-query="${escapeHtml(p.query)}"
    role="button"
    tabindex="0"
    style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); cursor:pointer; border-radius:var(--radius-sm); background:var(--bg-elevated); border:1px solid var(--border-subtle)"
    title="${t('job.queriesClickToFilter')}"
>
    ...existing children...
</div>
```

For the **with-expansions** primary row (lines 80-96), the outer flex div already has `cursor:pointer` and an inline chevron-toggle handler. Add `data-search-query` to that same div AND keep the chevron-toggle, but split clicks: chevron-area click → expand/collapse; rest of row → filter. Cleanest approach: put the chevron toggle on a small `<span class="qp-chevron-btn">` and let the parent click drive filter:

```html
<div
    class="qp-row-clickable"
    data-search-query="${escapeHtml(p.query)}"
    role="button"
    tabindex="0"
    style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); cursor:pointer; border-radius:var(--radius-sm); background:var(--bg-elevated); border:1px solid var(--border-subtle)"
    title="${t('job.queriesClickToFilter')}"
>
    <span
        class="qp-chevron-btn"
        data-toggle-target="${primaryId}"
        style="cursor:pointer; padding:0 4px"
        onclick="event.stopPropagation(); (function(el){
            var body=document.getElementById(el.dataset.toggleTarget);
            if(!body) return;
            var chevron=el.querySelector('.qp-chevron');
            var hidden=body.style.display==='none';
            body.style.display=hidden?'':'none';
            if(chevron) chevron.style.transform=hidden?'rotate(90deg)':'';
        })(this)"
    >
        <span class="qp-chevron" style="display:inline-block; transition:transform 0.2s; color:var(--text-muted); font-size:var(--font-xs)">▸</span>
    </span>
    <span class="qp-state-icon" aria-label="${t('monitor.queriesStateDone')}">✓</span>
    <strong style="font-size:var(--font-sm)">${escapeHtml(p.query)}</strong>
    <span style="color:var(--text-muted); font-size:var(--font-xs); margin-left:auto">→ ${summaryParts.join(' · ')}</span>
</div>
```

The `event.stopPropagation()` on the chevron prevents it from also firing the filter.

For **branch rows** (`_renderBranchRow` at queries_panel.js:257-275): replace the row with a clickable variant. **Verified at `discovery.py:5022`** — expansion entries always carry `e.query` set to `_widened_query` (the full expanded query string). No psql verification needed. Carry the branch's own `e.query` through `_renderBranchRow`. Modify `_renderExpansionBuckets` callers to pass `e.query` into `_renderBranchRow`, then render:

```js
function _renderBranchRow(e) {
    const n = e.new_companies || 0;
    const dur = e.duration_sec;
    const durColor = dur != null && dur > 60 ? 'var(--danger)' : 'var(--text-muted)';
    const durStr = dur != null ? `<span style="color:${durColor}">${dur}s</span>` : '';
    const errorChip = e.error
        ? `<span style="color:var(--danger); cursor:help; margin-left:4px" title="${escapeHtml(e.error)}">❌ ${t('job.queriesBranchError')}</span>`
        : '';
    const branchQuery = e.query || '';

    return `
        <div
            class="qp-row-clickable qp-row-clickable--branch"
            data-search-query="${escapeHtml(branchQuery)}"
            role="button"
            tabindex="0"
            style="display:flex; gap:6px; padding:3px 0; font-size:var(--font-xs); color:var(--text-secondary); cursor:${branchQuery ? 'pointer' : 'default'}"
            title="${branchQuery ? t('job.queriesClickToFilter') : ''}"
        >
            <span style="color:var(--text-muted)">└─</span>
            <span style="flex:1">${escapeHtml(e.value || '')}</span>
            <span>→ ${n} ${t('monitor.queriesNewEntities')}</span>
            ${durStr}
            ${errorChip}
        </div>
    `;
}
```

_(Branch `e.query` shape is already verified — see note above referencing `discovery.py:5022`. No executor TODO here.)_

#### Public click-binding helper

Add at the bottom of `queries_panel.js`, exported, so `job.js` and `monitor.js` can wire it up:

```js
/**
 * Bind click + keyboard handlers on the panel's clickable rows.
 * Dispatches a `qp:filter` CustomEvent with detail.searchQuery on the panel root.
 *
 * @param {HTMLElement} root - the container that wraps renderQueriesPanel output
 */
export function bindQueriesPanelClicks(root) {
    if (!root) return;
    const handler = (e) => {
        const target = e.target.closest('.qp-row-clickable');
        if (!target) return;
        const sq = target.dataset.searchQuery;
        if (!sq) return;
        root.dispatchEvent(new CustomEvent('qp:filter', { detail: { searchQuery: sq }, bubbles: true }));
    };
    root.addEventListener('click', handler);
    root.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        const target = e.target.closest('.qp-row-clickable');
        if (!target) return;
        e.preventDefault();
        const sq = target.dataset.searchQuery;
        if (!sq) return;
        root.dispatchEvent(new CustomEvent('qp:filter', { detail: { searchQuery: sq }, bubbles: true }));
    });
}
```

### 3.5 `fortress/frontend/js/pages/job.js` — wire chip + filter

Add a new module-level state variable next to existing ones (`job.js` declares `_currentFilter` near top — find it and add):
```js
let _currentSearchQuery = '';
```

Update the `loadCompanies` signature and call sites — add `searchQuery` param. Search and replace at lines 542, 559, 570, 585, 691-694, 761, 811, 814, 856, 917, 960:

Old: `loadCompanies(batchId, page, sort, filter)` and the `getJobCompanies(batchId, { page, pageSize: 20, sort, filter })` body (line 698).

New: `loadCompanies(batchId, page, sort, filter = '', searchQuery = '')` and `getJobCompanies(batchId, { page, pageSize: 20, sort, filter, searchQuery })`.

Inside `loadCompanies`, after the existing assignments at lines 692-694, add:
```js
_currentSearchQuery = searchQuery;
```

Render a chip ABOVE the company-grid header. Inside the existing `companiesContainer.innerHTML = …` block (around `job.js:756-758` where `<div id="job-company-grid">` is built), prepend a chip slot:
```js
const chipHtml = _currentSearchQuery ? `
    <div id="qp-filter-chip" style="display:inline-flex; align-items:center; gap:6px; padding:4px 10px; margin-bottom:var(--space-sm); border-radius:var(--radius-md); background:var(--bg-elevated); border:1px solid var(--border-subtle); font-size:var(--font-sm); color:var(--text-secondary)">
        <span>${t('job.queriesFilterChip', { query: escapeHtml(_currentSearchQuery) })}</span>
        <button id="qp-filter-clear" style="background:none; border:none; color:var(--text-muted); cursor:pointer; padding:0 4px; font-size:var(--font-md)" aria-label="${t('job.queriesFilterClear')}">×</button>
    </div>
` : '';
```

Inject `chipHtml` immediately before `listHeaderHtml` inside the `<div id="job-company-grid">` block.

After `companiesContainer.innerHTML = …`, attach the clear handler:
```js
const clearBtn = document.getElementById('qp-filter-clear');
if (clearBtn) {
    clearBtn.addEventListener('click', async () => {
        _currentSearchQuery = '';
        await loadCompanies(_currentBatchId, 1, _currentSort, _currentFilter, '');
    });
}
```

After `qPanel.innerHTML = renderQueriesPanel(...)` at `job.js:525-528`, wire the click bridge:
```js
import { bindQueriesPanelClicks } from '../components/queries_panel.js';
// ...
bindQueriesPanelClicks(qPanel);
qPanel.addEventListener('qp:filter', async (ev) => {
    const sq = ev.detail.searchQuery;
    // Toggle off if same query clicked again
    _currentSearchQuery = (_currentSearchQuery === sq) ? '' : sq;
    _currentPage = 1;
    await loadCompanies(_currentBatchId, 1, _currentSort, _currentFilter, _currentSearchQuery);
    document.getElementById('job-companies-container')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
});
```

**CSV button — verified at `job.js:632-634`** as `<a href="${getExportUrl(batchId, 'csv')}" download>📥 CSV</a>` (NOT a `<button>` with id `btn-export-csv`; that id does not exist). Convert this static link to a click handler that resolves the URL at click time, picking up the current `_currentSearchQuery` filter state.

**Step 1 — replace the static href.** Change line 632 from:
```html
<a href="${getExportUrl(batchId, 'csv')}" download>📥 CSV</a>
```
to:
```html
<a href="#" id="btn-download-csv" download>📥 CSV</a>
```

**Step 2 — add the click handler near the existing dropdown setup (around `job.js:622-639`):**
```js
document.getElementById('btn-download-csv')?.addEventListener('click', (e) => {
    e.preventDefault();
    const url = getExportUrl(batchId, 'csv', _currentSearchQuery || null);
    window.location.href = url;
});
```

**Scope decision: CSV only.** Do NOT mirror this for `xlsx` or `jsonl` in this brief — those export formats are out of scope for E4 drill-down (see export.py edit in §3.2: only `_fetch_export_data` is extended, and we explicitly limit to per-batch CSV). The `xlsx` / `jsonl` `<a href>` lines stay unchanged.

### 3.6 `fortress/frontend/js/pages/monitor.js` — wire chip + filter on live monitor

Mirror the job.js wiring on the live monitor's `$.queriesList` element (`monitor.js:721-739`). The monitor doesn't have the same companies grid as job.js — it has streaming `$.cardsTitle` + cards (`monitor.js:758-`). For live monitor we have two viable scopes:

**Scope A (recommended — minimal):** make rows clickable and on click NAVIGATE to `#/job/<batchId>?q=<encoded>` (or set a query param the job page reads on load). This keeps live monitor simple — drill-down lives on the post-batch page where it's actually useful. Append `bindQueriesPanelClicks($.queriesList)` after the `renderQueriesPanel` call at `monitor.js:726-734`, then handle `qp:filter`:
```js
bindQueriesPanelClicks($.queriesList);
$.queriesList.addEventListener('qp:filter', (ev) => {
    const sq = ev.detail.searchQuery;
    window.location.hash = `#/job/${encodeURIComponent(batchId)}?q=${encodeURIComponent(sq)}`;
});
```

**Scope B (rejected):** filter the live appended cards array. Costs more; live cards stream in via WebSocket and we'd need to filter on every push. Out of scope.

**Job page must read the `?q=` query param.** In `job.js`, where the page initialises (find the route handler — grep for `loadJob` or where `batchId` is parsed from the hash), parse:
```js
function _readQueryParamFromHash() {
    const hash = window.location.hash || '';
    const idx = hash.indexOf('?');
    if (idx < 0) return '';
    const params = new URLSearchParams(hash.slice(idx + 1));
    return params.get('q') || '';
}
```

Use this value as the initial `_currentSearchQuery` and pass to the first `loadCompanies(...)` call at `job.js:542`.

### 3.7 Translation keys — `fortress/frontend/translations/fr.json` and `en.json`

Add to BOTH files (under `job.*` or `monitor.*` namespace as appropriate):

**fr.json**:
```json
"job.queriesClickToFilter": "Cliquer pour filtrer la liste",
"job.queriesFilterChip": "Filtré par : {{query}}",
"job.queriesFilterClear": "Effacer le filtre",
"monitor.queriesPrimaryWithExpansions": "{{total}} total (dont {{expansion}} par élargissement)"
```

**en.json**:
```json
"job.queriesClickToFilter": "Click to filter the list",
"job.queriesFilterChip": "Filtered by: {{query}}",
"job.queriesFilterClear": "Clear filter",
"monitor.queriesPrimaryWithExpansions": "{{total}} total (incl. {{expansion}} via expansion)"
```

**Verify** that `t()` supports `{{...}}` interpolation. From `queries_panel.js:64` `t('monitor.queriesElargissements')` is plain — but at `monitor.js:125` we see `t('monitor.queriesRunningLive', { count: running.live_count != null ? running.live_count : 0 })` — so YES, the project's i18n helper does take an object map. Use that pattern.

### 3.8 `fortress/frontend/css/components.css` — hover/active styling

Append:
```css
/* E4.A — clickable queries-panel rows */
.qp-row-clickable {
    transition: background 0.15s ease;
}
.qp-row-clickable:hover {
    background: var(--bg-hover) !important;
}
.qp-row-clickable:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 1px;
}
.qp-row-clickable--branch:hover {
    background: var(--bg-hover);
    border-radius: var(--radius-sm);
}
```

If `--bg-hover` isn't a defined token, use `color-mix(in srgb, var(--bg-elevated) 88%, var(--text) 12%)` — verify by grepping `components.css` for `bg-hover`.

---

## 4. Removal / replacement checklist

- **REPLACE** the primary-row summary builder at `queries_panel.js:58-65` — do not "add alongside". The `summaryParts.push(\`${expansionCount} ${t('monitor.queriesElargissements') || 'élargissements'}\`)` line goes away; its info is now folded into `monitor.queriesPrimaryWithExpansions`. The `monitor.queriesElargissements` translation key STAYS in fr.json/en.json (still used as the OR fallback for the with-expansions case in v1, AND used by sub-bucket totals — confirm with grep before pruning. Plan: keep it.).
- **REPLACE** the existing `_currentFilter`-only `loadCompanies` calls — every call site listed in 3.5 must include the new 5th arg. No wrapper functions; do the search-and-replace cleanly.
- **REPLACE** `_fetch_export_data(batch_id)` body and signature in `export.py:138`. The 4 other callers (`export_csv`, `export_jsonl`, `export_xlsx`, plus internal — grep first) all need the optional kwarg added or kept as default.
- **NO new files.**
- **NO changes** to `discovery.py`, `dedup.py`, `main.py`, `schema.sql`, or any DB migration.

---

## 5. Risks & failure modes

1. **Branch `query` field shape — verified, not a risk.** `discovery.py:5022` writes `"query": _widened_query` into every expansion entry, so branch rows always have a `search_query` to filter on.
2. **`batch_log.search_query` populated by all enrichment paths?** — `log_audit` at `dedup.py:282` accepts the param but only callers that pass it will populate. Apr 21+ chain detector and Apr 30 Track 2 must also pass `_current_search_query`. **Mitigation:** the executor should run `SELECT COUNT(*) FROM batch_log WHERE batch_id = '<recent ws174 batch>' AND search_query IS NOT NULL` vs `IS NULL` — if NULL ratio > 30%, drill-down will under-count. This is observability, not a blocker — add a note to the QA report.
3. **Workspace isolation** — the existing companies endpoint already gates on `ws_filter` at `jobs.py:864-870`. The `sq_clause` we add only filters DOWN within rows already gated. No new isolation surface.
4. **CSV filename collision** — sluggified `search_query` of two different queries could collide (rare). Acceptable; user can rename.
5. **Performance** — `IN (SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s AND search_query = %s)` benefits from `idx_batch_log_batch_id` (schema.sql:190). For 2000-entity batches the inner select is fast. No new index.
6. **Click target conflict** — chevron click + row click on the same DOM node. The `event.stopPropagation()` on chevron handles it; verify in QA.

---

## 6. Open questions for Alan

1. **Branches clickable?** **DEFAULT IF NO ANSWER: YES, branches are clickable.** Cindy will want to download just "transport 33 Bordeaux" rows separately from "transport 33 Mérignac". Verified at `discovery.py:5022` that expansion entries always carry `e.query` (set to `_widened_query`), so the branch row has a real `search_query` to filter on — no fallback construction needed.
2. **E4.B French phrasing — DECIDED (Alan picked phrasing C).** Final form: `"{{total}} total (dont {{expansion}} par élargissement)"`. Headline number first, breakdown in parens. The `directe(s)` term is dropped — it was redundant.
3. **Live monitor scope.** Plan goes with Scope A (clickable → navigate to job page with `?q=`). Alan confirm? **DEFAULT IF NO ANSWER:** Scope A.

Executor — if Alan hasn't answered these by start time, proceed with the defaults above and note the choice in your final report.

---

## 7. Definition of Done — verbatim grep proofs

Executor MUST paste all of these verbatim into the report. Failure to paste = NOT done.

```bash
cd "/Users/alancohen/Project Alan copy/fortress"

# (1) Backend filter wired in jobs.py
grep -n "search_query: str = Query" fortress/api/routes/jobs.py
# Expected: 1 hit at the get_job_companies signature

grep -n "AND search_query = %s" fortress/api/routes/jobs.py
# Expected: 1 hit inside sq_clause

# (2) Backend filter wired in export.py
grep -n "search_query: str | None = None" fortress/api/routes/export.py
# Expected: 1 hit at _fetch_export_data signature

grep -n "AND search_query = %s" fortress/api/routes/export.py
# Expected: 1 hit inside sq_clause

# (3) API helpers updated
grep -n "searchQuery" fortress/frontend/js/api.js
# Expected: ≥3 hits (getJobCompanies signature, params.set, getExportUrl signature)

# (4) E4.B count fix in queries_panel
grep -n "expansionEntityTotal\|queriesPrimaryWithExpansions" fortress/frontend/js/components/queries_panel.js
# Expected: ≥2 hits (variable, t() key)

# (5) Clickable wiring in queries_panel
grep -n "qp-row-clickable\|bindQueriesPanelClicks" fortress/frontend/js/components/queries_panel.js
# Expected: ≥3 hits (CSS class on primary, branch, exported function)

# (6) Job page wired chip + clear button
grep -n "qp-filter-chip\|_currentSearchQuery\|qp:filter" fortress/frontend/js/pages/job.js
# Expected: ≥4 hits

# (7) Monitor page wired
grep -n "bindQueriesPanelClicks\|qp:filter" fortress/frontend/js/pages/monitor.js
# Expected: ≥2 hits

# (8) Translation keys present
grep -n "queriesClickToFilter\|queriesFilterChip\|queriesPrimaryWithExpansions" fortress/frontend/translations/fr.json
grep -n "queriesClickToFilter\|queriesFilterChip\|queriesPrimaryWithExpansions" fortress/frontend/translations/en.json
# Expected: 3 hits each
```

### Syntax / boot gates

```bash
# Python
python3 -c "from fortress.api.routes import jobs, export; print('IMPORTS OK')"

# JSON validity
python3 -c "import json; json.load(open('fortress/frontend/translations/fr.json')); json.load(open('fortress/frontend/translations/en.json')); print('JSON OK')"

# JS syntax — node --check on each touched file
node --check fortress/frontend/js/components/queries_panel.js
node --check fortress/frontend/js/pages/job.js
node --check fortress/frontend/js/pages/monitor.js
node --check fortress/frontend/js/api.js
echo "JS OK"

# Boot: server must start without 500
python3 -m uvicorn fortress.api.main:app --port 8081 &
sleep 4
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8081/api/health
kill %1
# Expected: 200
```

### Mandatory: `git diff --stat` proof

After every edit, paste verbatim:
```bash
git status
git diff --stat
```
Per `feedback_executor_diff_proof.md` and `feedback_executor_fabricates_diff_stat.md` — `M` markers on each touched file are the ONLY proof edits persisted. Do not paraphrase. Do not "summarize the diff". Paste the literal output of `git diff --stat`.

### Out of scope

- No `git commit` / `git push`
- No edits to `discovery.py`, `processing/dedup.py`, `database/schema.sql`, `api/main.py`
- No new endpoints beyond the two extended params
- No social-field GREEN gating, no triage rename, no Cindy fixtures

---

## 8. QA TEST PLAN — E4

### Section 1 — Automated checks (QA agent runs in terminal)

```bash
# Boot the server with the worktree's branch
cd "/Users/alancohen/Project Alan copy/fortress/.claude/worktrees/agent-a4541cf1509610660"
python3 -m uvicorn fortress.api.main:app --port 8082 &
APP_PID=$!
sleep 5
```

**Test 1.1 — Health & login**
```bash
curl -s -c /tmp/qa_e4.cookies -X POST http://localhost:8082/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"head.test","password":"Test1234"}' | jq .
# Expected: {"ok": true, ...} or similar success shape
```

**Test 1.2 — Pick a recent ws174 batch with multiple queries**
```sql
-- via psql, against $DATABASE_URL
SELECT bd.batch_id, bd.batch_name, bd.created_at,
       jsonb_array_length(bd.queries_json) AS n_queries,
       (SELECT COUNT(DISTINCT search_query) FROM batch_log WHERE batch_id = bd.batch_id AND search_query IS NOT NULL) AS distinct_recorded_queries
FROM batch_data bd
WHERE bd.workspace_id = 174
  AND bd.status = 'completed'
  AND jsonb_array_length(bd.queries_json) > 1
  AND bd.created_at::date >= CURRENT_DATE - INTERVAL '14 days'
ORDER BY bd.created_at DESC
LIMIT 5;
```
Pick one `batch_id` for the next tests. Call it `$B`. Pick one `search_query` value from that batch and call it `$Q`.

**Test 1.3 — Companies endpoint without filter (control)**
```bash
curl -s -b /tmp/qa_e4.cookies "http://localhost:8082/api/jobs/$B/companies?page=1&page_size=20" | jq '.total'
# Record this number as TOTAL_ALL.
```

**Test 1.4 — Companies endpoint WITH search_query filter**
```bash
curl -s -b /tmp/qa_e4.cookies "http://localhost:8082/api/jobs/$B/companies?page=1&page_size=20&search_query=$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))' "$Q")" | jq '.total'
# Record this number as TOTAL_FILTERED.
# Expected: TOTAL_FILTERED > 0 AND TOTAL_FILTERED <= TOTAL_ALL.
```

**Test 1.5 — SQL truth check (HARD pass criterion)**

Run BOTH the API call from Test 1.4 AND the SQL below for the same `$B` / `$Q`. Both must produce comparable `siren` counts.

```bash
# API count (re-run for clarity, save to var)
TOTAL_FILTERED=$(curl -s -b /tmp/qa_e4.cookies "http://localhost:8082/api/jobs/$B/companies?page=1&page_size=20&search_query=$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))' "$Q")" | jq '.total')
echo "TOTAL_FILTERED (API) = $TOTAL_FILTERED"
```

```sql
-- SQL ground truth — paste $B and $Q literals
SELECT COUNT(DISTINCT siren) AS sql_count
FROM batch_log
WHERE batch_id = '<paste $B>'
  AND search_query = '<paste $Q>';
```

**Pass criterion (HARD):** `TOTAL_FILTERED` must equal `sql_count` to within **5%** (i.e., `abs(TOTAL_FILTERED - sql_count) / sql_count <= 0.05`). If the delta exceeds 5%, **FAIL the test and flag for investigation before merge** — do not proceed to commit. Likely causes if it fails: NAF-mismatch / unmatched-MAPS exclusions in the API path that the SQL doesn't apply, OR a JOIN bug in the new `sq_clause`. Either way, planner must investigate before ship.

**Test 1.6 — CSV export with search_query**
```bash
curl -s -b /tmp/qa_e4.cookies "http://localhost:8082/api/export/$B/csv?search_query=$(python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))' "$Q")" \
  -o /tmp/qa_e4_filtered.csv
wc -l /tmp/qa_e4_filtered.csv
# Expected: > 1 (header + at least one data row), <= TOTAL_FILTERED + 1.
head -1 /tmp/qa_e4_filtered.csv
# Expected: the standard CSV header row (Statut SIRENE, Statut lien, etc.)
```

**Test 1.7 — CSV export without search_query (control)**
```bash
curl -s -b /tmp/qa_e4.cookies "http://localhost:8082/api/export/$B/csv" -o /tmp/qa_e4_all.csv
wc -l /tmp/qa_e4_all.csv
# Expected: row count >= filtered row count
```

**Test 1.8 — Empty / nonsense search_query**
```bash
curl -s -b /tmp/qa_e4.cookies "http://localhost:8082/api/jobs/$B/companies?search_query=nonexistent_query_xyz" | jq '.total'
# Expected: 0
```

**Test 1.9 — Workspace isolation still enforced**
```bash
# Login as user.test (also ws174, OK), then try to filter a Workspace 1 batch
# (would need a ws1 batch_id from admin SELECT). Expected: 404 "Batch introuvable"
# regardless of search_query value.
```

### MANDATORY — 99% goal tracking SQL (per CLAUDE.md)

```sql
WITH recent AS (
    SELECT DISTINCT co.siren, co.linked_siren, co.link_confidence
    FROM batch_data bd
    JOIN batch_tags bt ON bt.batch_id = bd.batch_id
    JOIN companies co ON co.siren = bt.siren
    WHERE bd.workspace_id = 174 AND bd.status = 'completed'
      AND bd.created_at::date >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT COUNT(DISTINCT siren) AS total,
       ROUND(100.0 * SUM(CASE WHEN link_confidence = 'confirmed' THEN 1 ELSE 0 END)
             / NULLIF(COUNT(DISTINCT siren), 0), 1) AS confirmed_pct
FROM recent;
```

QA agent must report:
> "99% GOAL TRACKING: ws174 confirm rate now X.X% (previous QA baseline: 52.8%, delta +/-Z.Zpp). Gap to 99%: N.Npp."

This is informational — E4 is pure UX, no expected confirm-rate movement. Reporting it preserves session continuity per the cardinal CLAUDE.md rule.

### Section 2 — Manual-equivalent Playwright steps (QA agent runs these too — no homework for Alan)

QA agent must use `mcp__claude-in-chrome__*` tools to drive a real browser session:

1. Navigate to `http://localhost:8082/` → click Login → submit `head.test / Test1234`.
2. Open `#/job/$B`. Verify `Recherches effectuées` panel is visible with ≥2 query rows.
3. Take a screenshot of the panel — confirm a primary row that has expansion children renders with the new `"{total} total (dont {expansion} par élargissement)"` string (e.g., literally "13 total (dont 11 par élargissement)"). **Pass:** the leading `{total}` value EQUALS `primary.new_companies + sum(expansion.new_companies)` (i.e., the parent total equals direct + sum of children's `→ N nouvelles entités`). The `{expansion}` value EQUALS the sum of children's `new_companies`.
4. Click the primary row text (NOT the chevron). Confirm:
   - The chip "Filtré par : <query> (×)" appears above the company list.
   - The company list count drops to ≤ the original total.
   - Hovering the row shows the title tooltip "Cliquer pour filtrer la liste".
5. Click the same primary row again. Confirm the chip disappears and the count returns to the unfiltered total.
6. Click the chip's `×` button after re-applying the filter. Confirm same clearing behavior.
7. Click a branch row inside the expansion (cities or postals bucket). Confirm filter applies with the FULL expanded query string in the chip.
8. With a filter active, click the per-batch CSV download button. Confirm the downloaded filename contains a slug of the active query (e.g., `MAPS_camping_perpignan.csv`).
9. Open the downloaded CSV in a text editor; confirm the row count matches the on-screen filtered count (header + data rows).
10. Navigate to `#/monitor/<a running batch>` (or open a fresh ws174 batch that's still running). Click a query row in its panel. Confirm browser navigates to `#/job/<batch_id>?q=<query>` and the chip is auto-applied.
11. Take screenshots at steps 3, 4, 7, 8, 10. Attach to QA report.

### Section 3 — Scorecard

| # | Step / Check | Expected | Actual | Pass/Fail |
|---|---|---|---|---|
| 1.1 | Login as head.test | 200 + cookie | | |
| 1.2 | Pick batch with multi-query | ≥1 batch found | | |
| 1.3 | Unfiltered companies total | TOTAL_ALL recorded | | |
| 1.4 | Filtered companies total | 0 < FILTERED ≤ ALL | | |
| 1.5 | SQL ground truth match | API vs SQL within 5% (HARD) | | |
| 1.6 | Filtered CSV row count | matches Test 1.4 | | |
| 1.7 | Unfiltered CSV row count | ≥ filtered | | |
| 1.8 | Nonsense query | total = 0 | | |
| 1.9 | Workspace isolation | 404 on cross-ws | | |
| 99% | Confirm rate vs 52.8% baseline | reported | | |
| 2.3 | Parent reads "{total} total (dont {expansion} par élargissement)"; total = direct + sum(children) | true | | |
| 2.4 | Click primary applies filter | chip + reduced count | | |
| 2.5 | Re-click clears | chip gone | | |
| 2.6 | × clears | chip gone | | |
| 2.7 | Branch row filters | full query in chip | | |
| 2.8 | CSV filename has query slug | yes | | |
| 2.9 | CSV row count matches UI | yes | | |
| 2.10 | Monitor click → job page nav | URL has `?q=` | | |

### Section 4 — Pass/fail criteria

**PASS:** all rows in scorecard are PASS; 99% confirm rate reported (any value); no Python/JS exceptions in browser console; server logs clean.
**FAIL:** any scorecard row FAIL, OR JS console error, OR the parent count contradiction still visible after E4.B, OR drill-down filter returns wrong rows compared to SQL ground truth.

---

## 9. Cost estimate

Pure UX/SQL — no LLM calls, no scraping, no new DB writes. Estimated executor effort: 90-150 minutes including DoD greps and Playwright. QA effort: 30-45 minutes.
