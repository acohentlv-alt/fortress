# Fortress UI/UX Agent — Full Handshake Briefing

> **Goal:** You are the end-to-end UX/UI designer and frontend engineer for Fortress.  
> Your job: Make every page modern, beautiful, professional, and user-friendly.  
> Stack: **Vanilla JS SPA, vanilla CSS, NO frameworks.** Inter font via Google Fonts.  

---

## 1. What Is Fortress?

Fortress is a **B2B lead collection platform** for the French market. It operates on a PostgreSQL database of **14.7 million French companies** (imported from the national SIRENE registry).

**What it does:**
1. User searches by industry + location (e.g., "transport 66" or "boulangerie FRANCE")
2. System finds matching companies in the 14.7M database
3. Background pipeline enriches them: Google Maps → website crawl
4. Collected data: phone, email, website, Google Maps rating, social links, addresses
5. User exports enriched data as CSV or browses it in the dashboard

**Target users:** Non-technical French business operators. The UI must be in **French**.

---

## 2. Architecture

```
index.html                    ← Single page, sidebar + header + #page-content
  └── js/app.js               ← Hash-based SPA router + cleanup system
       ├── js/api.js           ← Fetch wrapper (injects _status, _ok)
       ├── js/components.js    ← Gauges, cards, badges, pagination, toast
       └── js/pages/
            ├── dashboard.js   ← Stats + dept grid + job timeline
            ├── search.js      ← Company search with sort/filter/pagination  
            ├── company.js     ← Company detail page (Pappers-style)
            ├── new-batch.js   ← Batch form (launch new collection job)
            ├── monitor.js     ← Live pipeline monitor (1.5s polling)
            ├── job.js         ← Job detail (completed batch results)
            ├── upload.js      ← CRM CSV upload (drag & drop)
            ├── department.js  ← Department breakdown
            ├── open-query.js  ← Free-form query builder
            └── login.js       ← API key auth (optional)

  css/
    ├── design-system.css      ← Design tokens, layout, sidebar, header
    └── components.css         ← Cards, gauges, badges, forms, tables
```

### Critical Frontend Patterns

| Pattern | How It Works |
|---------|-------------|
| **SPA Routing** | Hash-based (`#/`, `#/search`, `#/job/{id}`). `app.js` matches routes and calls `render*()` |
| **Cleanup System** | `registerCleanup(fn)` in `app.js`. Pages register interval/listener cleanup. Called on navigation. **NEVER break this.** |
| **Component Pattern** | `components.js` exports functions that return HTML strings. No virtual DOM — just `innerHTML`. |
| **DOM Patching** | `monitor.js` renders skeleton ONCE, then patches by `document.getElementById()`. No rebuild on poll. |
| **API Convention** | Every API response gets `_status` and `_ok` injected. `extractApiError()` for user-facing errors. |
| **Error Handling** | 503 → "DB offline" error state with retry button. 404 → "not found". All in French. |
| **Toast System** | `showToast(message, type)` — auto-dismiss in 4 seconds. Types: success, error, info, warning. |

---

## 3. API Endpoints (Backend Already Built)

| Method | Endpoint | Purpose | Notes |
|--------|----------|---------|-------|
| GET | `/api/dashboard/stats` | Aggregate stats (total companies, phones, emails, websites) | Used by dashboard |
| GET | `/api/dashboard/stats/by-job` | Pre-grouped job stats | Used by "Par Job" view |
| GET | `/api/departments` | Department list with phone/email/web percentages | Used by "Par Location" view |
| GET | `/api/departments/{dept}/jobs` | Jobs for a specific department | Used by department detail |
| GET | `/api/jobs` | All jobs list | Status, progress, triage counts |
| GET | `/api/jobs/{query_id}` | Single job detail | Includes `batch_size`, `companies_scraped`, `replaced_count`, `wave_current`, `wave_total`, `triage_*` |
| GET | `/api/jobs/{query_id}/companies` | Paginated company list for job | Has `?page=&page_size=&search=&sort=` params |
| GET | `/api/jobs/{query_id}/quality` | Quality gauges (phone_pct, email_pct, etc.) | Used by monitor + job detail |
| GET | `/api/companies/search` | Search companies by name/SIREN/NAF | Has `?q=&limit=&offset=&sort_by=&department=&sector=` |
| GET | `/api/companies/{siren}` | Full company detail | Returns `{company, merged_contact, officers, query_tags, contacts}` |
| POST | `/api/companies/{siren}/enrich` | Per-company enrichment | `{target_modules: [...]}` |
| POST | `/api/batch/run` | Launch new batch | `{sector, department, size, mode, city?, naf_code?}` → returns 202 + `query_id` |
| GET | `/api/export/{query_id}/csv` | CSV export for batch | `;` delimiter, UTF-8 BOM for Excel |
| GET | `/api/export/master/csv` | CSV of ALL scraped companies | Global export |
| POST | `/api/client/upload` | Upload CRM CSV (BLUE triage dedup) | Multipart form, returns `{inserted, already_existed}` |
| GET | `/api/client/stats` | Client SIREN upload stats | Returns `{total_sirens, uploads: [...]}` |
| DELETE | `/api/client/clear` | Clear all client SIRENs | Returns `{status: "ok"}` |
| GET | `/api/health` | Health check | Returns `{status: "ok", database: "connected"}` |
| GET | `/api/auth/check` | Auth required? | Returns `{auth_required: bool}` |
| POST | `/api/auth/login` | Login with API key | `{api_key: "..."}` |

---

## 4. Current Pages — Status & Improvement Areas

### 4.1 Dashboard (`#/`) — dashboard.js

**Current:** Stats bar (companies, phones, emails, websites) + two views toggle (By Location / By Job). Department cards have gauges. Job view groups batches with timelines.

**Needs improvement:**
- Stats bar looks functional but not premium — needs glassmorphism, gradient accents, animated counters
- Department grid is basic — needs hover effects, better visual hierarchy
- Job timeline view works but feels dense — needs breathing room, better typography
- No charts or trends — consider adding simple sparklines or trend indicators
- No "recent activity" feed

---

### 4.2 Search (`#/search`) — search.js

**Current:** Search input + département/sector filters + sort dropdown + company cards grid with pagination.

**Needs improvement:**
- Search should feel instant/modern (Google-like autocomplete UX)
- Company cards (`companyCard()` in components.js) need visual polish — rating stars, clearer contact indicators
- Filters could be collapsible pills or a slide-out panel
- Empty state is plain — needs illustration or animation

---

### 4.3 Company Detail (`#/company/{siren}`) — company.js

**Current:** Pappers-inspired 2-column layout. Left: identity card with gauges. Right: data sections (identity, activity, financials, location, contact, Google reviews, directors, enrichment history).

**Strength:** Already the most polished page. Has smart enrichment panel with checkboxes.

**Needs improvement:**
- "Unenriched field" empty states could be more visually inviting
- Map embed potential (show company on actual map)
- Social links section could have brand icons
- Enrichment history timeline could be more visual

---

### 4.4 New Batch (`#/new-batch`) — new-batch.js

**Current:** Form with sector, department dropdown, city, batch size, NAF code, pipeline display (Maps + Crawl checkboxes, disabled), mode select, live summary preview.

**Needs improvement:**
- Department dropdown has 101 entries — needs searchable select or combobox
- **Must add a "FRANCE" / "Toute la France" option** to the department dropdown (API now supports it via `department: "FRANCE"`)
- Summary preview is okay but could animate/transition when values change
- Pipeline section is just checkboxes — could be a visual pipeline diagram
- No estimated time or cost preview
- Form validation feedback should be inline, not alerts

---

### 4.5 Pipeline Live / Monitor (`#/monitor`) — monitor.js ⭐ MOST IMPORTANT

**Current:** Job list with progress bars. Single job view: big progress percentage, progress bar, counts (completed/failed/batch/replaced), wave counter, triage colors, quality gauges, and company cards grid.

**The user SPECIFICALLY wants this page improved to be:**

1. **Modern progress visualization** — animated progress bar/ring that feels alive, not just a static percentage
2. **Live company cards feed** — as companies are enriched, they should appear one by one with a smooth animation (slide-in, fade-in), like a live feed. Currently they update when count changes but there's no animation.
3. **Rich company cards** — each card in the live feed should show: company name, phone, email, website, rating, social links — ALL the collected data visually, not just basic info
4. **Stage visualization** — show which step the pipeline is on (Maps search → Website crawl → Save)
5. **Live counter animations** — numbers should count up smoothly, not jump
6. **Triage visualization** — the colored triage numbers could be a visual bar chart or pie
7. **Wave visualization** — show current wave progress distinctly
8. **"Pull" UX** — the page should feel like watching a live factory/assembly line
9. **Sound/haptic feedback** (optional) — subtle notification when a new company is found

**Technical constraints for monitor.js:**
- Polls every 1.5 seconds via `setInterval`
- **Must use `registerCleanup()`** to clear interval on navigation
- Skeleton renders ONCE, then `update()` patches by ID
- Guard checks `window.location.hash` before updating (prevents cross-page writes)
- Company cards only rebuild when `scraped` count changes

---

### 4.6 Job Detail (`#/job/{id}`) — job.js

**Current:** Similar to monitor but for completed batches. Shows stats, quality gauges, company list with pagination and per-batch CSV export.

**Needs improvement:**
- Shared aesthetics with monitor page
- Export button could have format previews/options
- Quality gauges could have historical comparison

---

### 4.7 Client Upload / Base Client (`#/upload`) — upload.js

**Current:** Drag-and-drop CSV upload zone. Shows total imported SIRENs count, upload history table, clear button.

**Purpose:** Client uploads their existing CRM as CSV ( column with `SIREN`). These SIRENs are marked BLUE in triage → skipped during scraping (dedup).

**Needs improvement:**
- Clear column mapping guidance (what columns to include)
- CSV validation preview before upload (show first 5 rows, highlight SIREN column found)
- Progress bar for large uploads
- Success animation
- Upload history could be more visual (cards instead of basic table)

---

### 4.8 CSV Export Logic

**Available via:**
- Per-batch: `GET /api/export/{query_id}/csv` — clicked from job detail page
- Master export: `GET /api/export/master/csv` — clicked from dashboard
- JSONL variant: `GET /api/export/{query_id}/jsonl`

**CSV format:** Semicolon-delimited, UTF-8 BOM (for Excel). 23 columns including: company identity, contact data, social links, ratings.

**Needs improvement:**
- Column selection UI (let user choose which fields to export)
- Export preview (show first rows before downloading)
- Multi-job export (select several jobs → combined export)

---

## 5. Design System (Already Exists)

### Color Tokens (Dark Mode — Pappers-inspired)
```css
--bg-primary: #06132D;     /* Deep navy */
--bg-secondary: #0C1E3A;
--bg-elevated: #132744;
--accent: #2563EB;         /* Blue accent */
--success: #22C55E;        /* Green */
--warning: #F59E0B;        /* Amber */
--danger: #EF4444;         /* Red */
--info: #06B6D4;           /* Cyan */
```

### Typography
- Font: Inter (Google Fonts, already imported)
- Sizes: `--font-xs` (11px) through `--font-3xl` (40px)

### Component Library (components.js)
| Component | Function | Notes |
|-----------|----------|-------|
| `renderGauge(pct, label)` | SVG circular gauge | Colors: green ≥80%, amber ≥50%, red <50% |
| `statusBadge(status)` | Colored status pill | completed/in_progress/queued/failed/triage |
| `companyCard(company)` | Company card with name/SIREN/location/contacts/completude bar | Main reusable card |
| `contactIndicators(contact)` | Phone/email/web/address/rating indicators | Shows "Oui" or "—" |
| `completudeBar(company)` | Horizontal progress bar | 8-field score |
| `renderPagination(page, total, onChange)` | Pagination with event delegation | WeakMap-based handler |
| `showToast(msg, type)` | Toast notification | Auto-dismiss 4s |
| `breadcrumb(items)` | Breadcrumb nav | Array of `{label, href}` |
| `escapeHtml(str)` | XSS protection | Always use on user data |

---

## 6. Hard Rules

1. **Vanilla JS + CSS only** — NO React, Vue, Svelte, Tailwind. No build step.
2. **French UI** — All user-facing text in French.
3. **Cleanup system** — Any `setInterval`, `setTimeout`, or `addEventListener` that persists across page loads MUST be registered via `registerCleanup()`.
4. **DOM patching** — On polling pages (monitor), render skeleton ONCE, then patch by ID. Never rebuild the whole page on each poll.
5. **API convention** — Always inject `_status` and `_ok`. Use `extractApiError()` for all error displays.
6. **Dark mode only** — The design system is dark. No light mode toggle needed.
7. **Inter font** — Already imported, use `var(--font-family)`.
8. **No breaking changes to API** — Backend endpoints are fixed. Only add new frontend features on top.
9. **Module exports** — Each page exports a `render*` function. `app.js` routes call them with `(container, ...params)`.
10. **ID-based elements** — All interactive elements need unique IDs for browser testing.

---

## 7. File Map

```
fortress/frontend/
├── index.html                     ← Shell: sidebar nav + header + #page-content
├── css/
│   ├── design-system.css          ← Variables, reset, layout, sidebar, header
│   └── components.css             ← Cards, gauges, badges, forms, tables, pagination
├── js/
│   ├── app.js                     ← Router + cleanup system + global search + auth gate
│   ├── api.js                     ← All API calls (fetch wrapper)
│   ├── components.js              ← Reusable UI components
│   ├── constants.js               ← DEPARTMENTS array (101 entries)
│   └── pages/
│       ├── dashboard.js (314 lines) ← Stats bar + dept grid + job timeline
│       ├── search.js (319 lines)    ← Search + filters + sort + pagination
│       ├── company.js (457 lines)   ← Full company detail (most polished)
│       ├── new-batch.js (215 lines) ← Launch batch form
│       ├── monitor.js (307 lines)   ← 🔥 LIVE PIPELINE (priority page)
│       ├── job.js (195 lines)       ← Completed job detail
│       ├── upload.js (219 lines)    ← CRM CSV upload
│       ├── department.js (109 lines)← Department breakdown
│       ├── open-query.js (139 lines)← Free-form query
│       └── login.js (63 lines)     ← API key auth
```

---

## 8. Priority Order

1. **🔥 Monitor page (Pipeline Live)** — Make it feel like a live, premium data factory
2. **Dashboard** — Premium stats, modern cards, wow factor
3. **New Batch** — Searchable department picker, France-wide option, polished form UX
4. **Company Detail** — Already good, refine visual polish  
5. **Search** — Modern search UX, refined cards
6. **Upload** — Better drag-drop, preview, progress
7. **Export** — Column picker, preview, multi-job

---

## 9. How to Test

```bash
# Start the backend (from project root)
cd "/Users/alancohen/Downloads/Project Alan copy/fortress"
python3 -m fortress.api.main
# Visit http://localhost:8080

# Hard refresh (Cmd+Shift+R) — JS files are served with 304 caching
# Check browser console for errors
# Test 503 state by stopping the DB
# Test empty state by clearing query_tags
```

API server runs on **port 8080**. Frontend is served as static files by FastAPI from `fortress/frontend/`.
