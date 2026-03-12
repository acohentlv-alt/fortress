# Fortress UI/UX Agent — Handshake Briefing

## Your Role

You are the **end-to-end UX/UI designer and frontend engineer** for Fortress. Your job is to make every page modern, beautiful, professional, and user-friendly — to a premium standard.

**Stack:** Vanilla JS SPA, vanilla CSS, NO frameworks. Inter font via Google Fonts.

---

## Your Workflow

You must follow this exact sequence:

### Phase 1: Plan

1. Read the contract documents to understand the system:
   - [Pipeline Contract](pipeline.md) — how the backend works
   - [Database Contract](database.md) — what data exists and how it's structured
2. Review each frontend page's current state
3. Write an **implementation plan** listing:
   - Which pages you will change, in priority order
   - What specific improvements you will make per page
   - What CSS/component changes are needed
   - Estimated scope per page (small/medium/large)

### Phase 2: Approval

4. **Present the plan to the user and WAIT for approval**
5. Do not write any code until the user says "approved" or "go ahead"
6. If the user requests changes to the plan, update it and re-present

### Phase 3: Implement

7. Make changes page by page, in approved priority order
8. After finishing each page, verify it works:
   - Hard-refresh in browser (`Cmd+Shift+R`)
   - Check browser console for errors
   - Test navigation to/from the page (cleanup system)
   - Test empty state and error state

### Phase 4: Verify End-to-End

9. **Run a real query through the website** to test the full flow:
   - Go to `http://localhost:8080` in browser
   - Navigate to "Nouveau Batch" (`#/new-batch`)
   - Submit a small batch: sector "transport", département "34", size 5, mode "discovery"
   - Navigate to Monitor (`#/monitor`) and watch the pipeline run
   - Verify live progress updates, company cards appearing, gauges updating
   - When complete, navigate to the Job Detail (`#/job/{query_id}`)
   - Verify company list, quality gauges, CSV export works
   - Navigate to Dashboard (`#/`) and verify stats reflect the new batch
   - Navigate to Search (`#/search`) and find a company from the batch
   - Open the Company Detail page and verify all enriched data displays

---

# 1. Product Overview

Fortress is a **B2B lead collection platform** for France. It operates on a PostgreSQL database of **14.7 million French companies** from the SIRENE registry.

**User flow:**
1. User searches by industry + location (e.g., "transport 66" or "boulangerie FRANCE")
2. System finds matching companies in the 14.7M database
3. Background pipeline enriches them: Google Maps → website crawl
4. Collected data: phone, email, website, Google Maps rating, social links, addresses
5. User exports enriched data as CSV or browses it in the dashboard

**Target users:** Non-technical French business operators. **All UI text must be in French.**

---

# 2. Frontend Architecture

```
index.html                    ← Single page: sidebar + header + #page-content
  └── js/app.js               ← Hash-based SPA router + cleanup system
       ├── js/api.js           ← Fetch wrapper (injects _status, _ok)
       ├── js/components.js    ← Gauges, cards, badges, pagination, toast
       └── js/pages/
            ├── dashboard.js   ← Stats + dept grid + job timeline
            ├── search.js      ← Company search with sort/filter/pagination
            ├── company.js     ← Company detail page (Pappers-style)
            ├── new-batch.js   ← Batch form (launch new collection job)
            ├── monitor.js     ← Live pipeline monitor (1.5s polling) ⭐
            ├── job.js         ← Job detail (completed batch results)
            ├── upload.js      ← CRM CSV upload (drag & drop)
            ├── department.js  ← Department breakdown
            ├── open-query.js  ← Free-form query builder
            └── login.js       ← API key auth (optional)

  css/
    ├── design-system.css      ← Design tokens, layout, sidebar, header
    └── components.css         ← Cards, gauges, badges, forms, tables
```

### Hard Rules (Non-Negotiable)

| # | Rule | Why |
|---|------|-----|
| 1 | **Vanilla JS + CSS only** — no React, Vue, Svelte, Tailwind, no build step | Architectural constraint |
| 2 | **French UI** — all user-facing text in French | Target market |
| 3 | **Cleanup system** — any `setInterval`/`setTimeout`/`addEventListener` MUST register via `registerCleanup()` in `app.js` | Prevents cross-page interval leaks |
| 4 | **DOM patching on polling pages** — render skeleton ONCE, then patch by `getElementById()`. Never rebuild on each poll | Performance |
| 5 | **API convention** — always inject `_status` and `_ok`. Use `extractApiError()` for errors | Consistency |
| 6 | **Dark mode only** — Pappers-inspired deep navy. No light mode | Design system |
| 7 | **Inter font** — already imported, use `var(--font-family)` | Design system |
| 8 | **No breaking API changes** — backend endpoints are fixed. Frontend adapts to existing responses | Backend contract |
| 9 | **Module exports** — each page exports a `render*` function called by router with `(container, ...params)` | Architecture |
| 10 | **Unique IDs** — all interactive elements need unique descriptive IDs for browser testing | Testability |

### Critical Patterns

| Pattern | Implementation | Location |
|---------|---------------|----------|
| SPA Routing | Hash-based (`#/`, `#/search`, `#/job/{id}`) | `app.js` |
| Cleanup System | `registerCleanup(fn)` — called on every navigation | `app.js` |
| Components | Functions that return HTML strings; `innerHTML` insertion | `components.js` |
| API Wrapper | `fetch` + `_status`/`_ok` injection + error extraction | `api.js` |
| Error States | 503 → "DB offline" + retry button. 404 → "not found". All French. | Each page |
| Toast | `showToast(msg, type)` — auto-dismiss 4s. Types: success/error/info/warning | `components.js` |
| DOM Patching | Monitor skeleton renders once; `update()` patches by ID per poll | `monitor.js` |

---

# 3. API Endpoints (Backend Complete)

The backend is fully operational. These endpoints are your data contract.

### Dashboard & Stats
| Method | Endpoint | Returns |
|--------|----------|---------|
| GET | `/api/dashboard/stats` | `{total_companies, with_phone, with_email, with_website, total_officers}` |
| GET | `/api/dashboard/stats/by-job` | Pre-grouped job stats array |
| GET | `/api/departments` | Department list with phone/email/web percentages |
| GET | `/api/departments/{dept}/jobs` | Jobs for a specific department |

### Jobs & Monitoring
| Method | Endpoint | Returns |
|--------|----------|---------|
| GET | `/api/jobs` | All jobs with status, progress, triage counts |
| GET | `/api/jobs/{query_id}` | Single job: `batch_size`, `companies_scraped`, `replaced_count`, `wave_current`, `wave_total`, `triage_*` |
| GET | `/api/jobs/{query_id}/companies` | Paginated companies: `?page=&page_size=&search=&sort=` |
| GET | `/api/jobs/{query_id}/quality` | Quality gauges: `phone_pct`, `email_pct`, `website_pct` |

### Companies
| Method | Endpoint | Returns |
|--------|----------|---------|
| GET | `/api/companies/search` | Search: `?q=&limit=&offset=&sort_by=&department=&sector=` |
| GET | `/api/companies/{siren}` | Full detail: `{company, merged_contact, officers, query_tags, contacts}` |
| POST | `/api/companies/{siren}/enrich` | Per-company enrichment trigger |

### Batch Operations
| Method | Endpoint | Returns |
|--------|----------|---------|
| POST | `/api/batch/run` | Launch batch: `{sector, department, size, mode, city?, naf_code?}` → 202 + `query_id` |

### Export
| Method | Endpoint | Returns |
|--------|----------|---------|
| GET | `/api/export/{query_id}/csv` | CSV for batch (`;` delimiter, UTF-8 BOM for Excel) |
| GET | `/api/export/master/csv` | CSV of ALL scraped companies |

### CRM Upload
| Method | Endpoint | Returns |
|--------|----------|---------|
| POST | `/api/client/upload` | Upload CSV: `{inserted, already_existed}` |
| GET | `/api/client/stats` | Upload stats: `{total_sirens, uploads}` |
| DELETE | `/api/client/clear` | Clear all client SIRENs |

### System
| Method | Endpoint | Returns |
|--------|----------|---------|
| GET | `/api/health` | `{status: "ok", database: "connected"}` |
| GET | `/api/auth/check` | `{auth_required: bool}` |
| POST | `/api/auth/login` | Login with API key |

---

# 4. Pages — Current State & Required Improvements

## 4.1 Monitor (`#/monitor`) — monitor.js ⭐ HIGHEST PRIORITY

**Current state:** Job list with progress bars. Single job view shows: progress %, progress bar, counts (completed/failed/batch/replaced), wave counter, triage colors, quality gauges, company cards grid.

**Required improvements — "Live Data Factory" feel:**

| # | Improvement | Details |
|---|------------|---------|
| 1 | **Animated progress** | Progress bar/ring that pulses and animates, not static. Counter should count up smoothly (CSS counter animation). |
| 2 | **Live company card feed** | As companies are enriched, cards slide/fade in one by one like a live feed. Currently they appear in bulk on count change. |
| 3 | **Rich company cards** | Each card must show ALL collected data: name, phone, email, website, rating (stars), social links — not just basic info. |
| 4 | **Stage indicator** | Visual pipeline stage: Maps → Crawl → Save. Show which step is active. |
| 5 | **Smooth counter animations** | Numbers count up smoothly — don't jump between poll cycles. Use CSS transitions or requestAnimationFrame. |
| 6 | **Triage visualization** | Colored triage counts as a horizontal stacked bar or small pie. |
| 7 | **Wave progress** | Show current wave number visually distinct from overall progress. |
| 8 | **Factory pull UX** | Page should feel like watching an assembly line. Pipeline stages lit up, cards sliding into view. |

**Technical constraints (must preserve):**
- Polls every 1.5s via `setInterval` — MUST register via `registerCleanup()`
- Skeleton renders ONCE, `update()` patches by ID — no full rebuild per poll
- Guard: check `window.location.hash` before updating (prevent cross-page writes)
- Company cards rebuild only when `companies_scraped` count changes

---

## 4.2 Dashboard (`#/`) — dashboard.js

**Current state:** Stats bar (companies, phones, emails, websites) + toggle views (By Location / By Job). Department cards with gauges. Job timeline view.

**Required improvements:**
- Stats bar: glassmorphism, gradient accents, animated counters
- Department grid: hover effects, better visual hierarchy
- Job timeline: better typography, breathing room
- Add trend indicators or sparklines
- Add "recent activity" feed

---

## 4.3 New Batch (`#/new-batch`) — new-batch.js

**Current state:** Form with sector, department dropdown, city, batch size, NAF code, pipeline display, mode select, live summary preview.

**Required improvements:**
- Department dropdown: **searchable** select/combobox (101 entries too many to scroll)
- **Must add "FRANCE" / "Toute la France"** option (API supports `department: "FRANCE"`)
- Summary preview: animate transitions when values change
- Pipeline section: visual pipeline diagram instead of disabled checkboxes
- Inline form validation (no alert boxes)

---

## 4.4 Company Detail (`#/company/{siren}`) — company.js

**Current state:** Best page. Pappers-inspired 2-column layout. Identity card with gauges. Data sections: identity, activity, financials, location, contact, reviews, directors, enrichment history.

**Required improvements:**
- Better empty state for unenriched fields
- Social links with brand icons (LinkedIn blue, Facebook blue, etc.)
- Enrichment history as visual timeline

---

## 4.5 Search (`#/search`) — search.js

**Current state:** Search input + département/sector filters + sort + company cards grid + pagination.

**Required improvements:**
- Modern search UX (Google-like, instant feel)
- Company cards: rating stars, clearer contact indicators
- Collapsible filter pills or slide-out panel
- Better empty state

---

## 4.6 Job Detail (`#/job/{id}`) — job.js

**Current state:** Completed batch view. Stats, quality gauges, company list with pagination, CSV export.

**Required improvements:**
- Shared premium aesthetic with monitor page
- Export button with format options
- Quality gauge visuals matching monitor improvements

---

## 4.7 Upload (`#/upload`) — upload.js

**Current state:** Drag-and-drop CSV upload. Shows total SIRENs, upload history table, clear button.

**Required improvements:**
- Clear column mapping guidance
- CSV preview before upload (show first 5 rows, highlight SIREN column)
- Upload progress bar
- Success animation
- Upload history as visual cards

---

## 4.8 CSV Export

**Available via:**
- Per-batch: `GET /api/export/{query_id}/csv` (from job detail page)
- Master: `GET /api/export/master/csv` (from dashboard)

**Format:** Semicolon-delimited, UTF-8 BOM (Excel). 23 columns.

**Improvements:**
- Column selection UI (choose which fields to export)
- Export preview (first rows before download)
- Multi-job combined export

---

# 5. Design System (Already Exists)

### Color Tokens
```css
--bg-primary: #06132D;     /* Deep navy */
--bg-secondary: #0C1E3A;
--bg-elevated: #132744;
--bg-hover: #1A3155;
--accent: #2563EB;         /* Blue */
--accent-hover: #3B82F6;
--success: #22C55E;        /* Green */
--warning: #F59E0B;        /* Amber */
--danger: #EF4444;         /* Red */
--info: #06B6D4;           /* Cyan */
```

### Typography
- Font: Inter (Google Fonts, already imported)
- Sizes: `--font-xs` (11px) through `--font-3xl` (40px)

### Existing Components (components.js)

| Component | Function | Notes |
|-----------|----------|-------|
| `renderGauge(pct, label)` | SVG circular gauge | Colors: green ≥80%, amber ≥50%, red <50% |
| `statusBadge(status)` | Status pill | completed/in_progress/queued/failed/triage |
| `companyCard(company)` | Company card | Name/SIREN/location/contacts/completude bar |
| `contactIndicators(contact)` | Phone/email/web indicators | Shows "Oui" or "—" |
| `completudeBar(company)` | Horizontal progress bar | 8-field score |
| `renderPagination(page, total, onChange)` | Pagination | WeakMap-based handler |
| `showToast(msg, type)` | Toast notification | Auto-dismiss 4s |
| `breadcrumb(items)` | Breadcrumb nav | Array of `{label, href}` |
| `escapeHtml(str)` | XSS protection | Always use on user data |

---

# 6. Pipeline Context (What the Backend Does)

You don't need to modify the backend, but you must understand it to build the right UI.

### Pipeline Stages (reflected in monitor page)

```
1. Interpret Query  →  SQL search on 14.7M companies
2. Triage           →  Classify: BLACK/BLUE/GREEN/YELLOW/RED
3. Enrich           →  Maps (Playwright) → Website crawl (curl_cffi)
4. Wave Processing  →  Per-company save + checkpoint
5. Complete         →  Status → completed, all data in DB
```

### Job Progress Model (what `/api/jobs/{query_id}` returns)

| Field | Meaning | UI usage |
|-------|---------|----------|
| `batch_size` | User-requested count | **Progress denominator** |
| `companies_scraped` | Processed so far | **Progress numerator** |
| `replaced_count` | Companies swapped out | Show as secondary stat |
| `wave_current` / `wave_total` | Wave progress | Wave indicator |
| `triage_black` | Blacklisted (skipped) | Stacked bar: BLACK segment |
| `triage_blue` | Client-owned (skipped) | Stacked bar: BLUE segment |
| `triage_green` | Already complete | Stacked bar: GREEN segment |
| `triage_yellow` | Partial data (targeted scrape) | Stacked bar: YELLOW segment |
| `triage_red` | New (full pipeline) | Stacked bar: RED segment |
| `status` | `new/triage/queued/in_progress/completed/failed` | Status badge |

### Data per Company (what enrichment collects)

The monitor's company cards should display ALL of these when available:

| Field | Source | Display suggestion |
|-------|--------|-------------------|
| `denomination` | SIRENE | Company name (header) |
| `siren` | SIRENE | Subtle badge |
| `naf_code` + `naf_libelle` | SIRENE | Industry tag |
| `ville`, `departement` | SIRENE | Location line |
| `phone` | Google Maps | 📞 with click-to-call |
| `email` | Website crawl | ✉️ with mailto link |
| `website` | Google Maps | 🌐 with external link |
| `rating` | Google Maps | ⭐ stars (0–5) |
| `review_count` | Google Maps | "(N avis)" |
| `social_linkedin` | Website crawl | LinkedIn icon |
| `social_facebook` | Website crawl | Facebook icon |
| `maps_url` | Google Maps | "Voir sur Maps" link |

---

# 7. Priority Order

| Priority | Page | Scope | Impact |
|----------|------|-------|--------|
| 🔥 1 | **Monitor** (Pipeline Live) | Large | Core UX — the "wow" page |
| 2 | **Dashboard** | Medium | First page users see |
| 3 | **New Batch** | Medium | Main action entry point |
| 4 | **Company Detail** | Small | Already good — polish |
| 5 | **Search** | Medium | Daily use page |
| 6 | **Job Detail** | Small | Shares monitor aesthetic |
| 7 | **Upload** | Small | Less frequent use |
| 8 | **Export** | Small | Enhancement |

---

# 8. Testing

### Local development

```bash
# Start backend (from project root)
cd "/Users/alancohen/Downloads/Project Alan copy/fortress"
python3 -m fortress.api.main
# Visit http://localhost:8080

# After CSS/JS changes: Cmd+Shift+R (hard refresh, bypasses 304 cache)
```

### Verification checklist per page

- [ ] Hard-refresh — no console errors
- [ ] Navigation to page — no stale intervals from other pages
- [ ] Navigation away — cleanup runs (verify in console)
- [ ] 503 state — stop DB, verify error UI + retry button
- [ ] Empty state — no data, verify graceful display
- [ ] Mobile viewport — verify readable (if applicable)

### End-to-end verification (after all pages done)

1. Navigate to `#/new-batch`
2. Fill: sector "transport", department "34", size 5, mode "discovery"
3. Submit → should redirect to or show monitor
4. Watch monitor: progress bar animates, cards appear live, gauges update
5. Wait for completion → navigate to job detail
6. Verify company list, quality gauges, export button
7. Go to dashboard → verify new batch appears in stats
8. Search for a company from the batch → open company detail
9. Verify all enriched data displays correctly

API server: **http://localhost:8080**
