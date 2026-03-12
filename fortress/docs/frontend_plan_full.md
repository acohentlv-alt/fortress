# Fortress — Frontend Plan (CTO Reviewed)

> **CTO assessment**: Cross-referenced all 6 tasks against the current codebase.
> Items marked ✅ are already built. Items marked 🔨 need work. Items marked ⏳ are blocked on backend.

---

## Current State Summary

| File | Status |
|------|--------|
| `login.js` | Exists — API key mode. Needs upgrade to username/password |
| `auth.py` | ✅ Fully built — `POST /login`, `POST /logout`, `GET /me` with session cookies |
| `company.js` | ✅ Reordered (Contact #2), per-field provenance, enrichment pipeline |
| `job.js` | Has provenance panel. Missing delete/cancel/rerun buttons |
| `monitor.js` | Has live polling, pipeline stages, triage bar. Missing cancel button + animation polish |
| `dashboard.js` | 2 tabs (Par Localisation + Par Job). "Par Job" is actually Par Recherche. Missing true sector grouping |
| `upload.js` | Works. Missing preview, column guidance, progress bar |
| `api.js` | Has `deleteJob()`, `cancelJob()`, `untagCompany()`. Missing `login()` with username/password |
| `components.js` | No `showConfirmModal()` exists yet |

---

## Task 1: Login Page 🔨

**Backend**: ✅ Already built (`auth.py` — username/password login with bcrypt, session cookie).

**Frontend work needed**:

| Item | Status | Work |
|------|--------|------|
| Login form (username + password) | 🔨 | Current `login.js` uses API key field — replace with username + password |
| Error message | ✅ | Already shows errors |
| Loading state | ✅ | Button shows "Connexion..." |
| After login | 🔨 | Wire `onSuccess` callback to redirect to `#/` |
| Logout button | 🔨 | Add to sidebar — call `POST /api/auth/logout` |
| Auth guard | 🔨 | Check `GET /api/auth/me` on app load; redirect to `#/login` if 401 |
| `api.js` | 🔨 | Replace `loginWithApiKey()` with `login(username, password)` |

#### Files to modify
- `login.js` — replace API key input with username + password form
- `api.js` — add `login(username, password)`, `logout()`, `getMe()`
- `app.js` — add auth guard before routing

---

## Task 2: Delete / Cancel / Rerun Buttons 🔨

**Backend**: ✅ All endpoints already built (B4–B9).
**API client**: ✅ `deleteJob()`, `cancelJob()`, `untagCompany()` already in `api.js`.

| Item | Status | Work |
|------|--------|------|
| 2a. Delete button (job detail) | 🔨 | Add button to `job.js` header, trigger modal |
| 2b. Cancel button (monitor) | 🔨 | Add to `monitor.js`, visible only during `in_progress` |
| 2c. Rerun button (job detail) | 🔨 | Read `filters_json` from job response, navigate to `#/new-batch` with pre-fill |
| 2d. Remove company (× on cards) | 🔨 | Add × button to `companyCard()`, call `untagCompany()`, animate out |
| 2e. Confirmation modal | 🔨 | New reusable component in `components.js` |

#### `showConfirmModal()` spec
```javascript
showConfirmModal({
    title: 'Supprimer ce batch ?',
    body: '<p>47 entreprises collectées</p><p>⚠️ Tags supprimés. ✅ Fiches conservées.</p>',
    confirmLabel: 'Supprimer',
    danger: true,    // red button if true, accent if false
    onConfirm: async () => { ... }
})
```

#### Files to modify
- `components.js` — add `showConfirmModal()` + CSS
- `components.css` — modal overlay, content, animations
- `job.js` — delete + rerun buttons in header
- `monitor.js` — cancel button (visible during `in_progress`)
- `components.js` — add × to `companyCard()` for untag

---

## Task 3: Company Page Improvements ✅ DONE

| Item | Status |
|------|--------|
| 3a. Reorder (Contact #2) | ✅ Done |
| 3b. Data source tooltips | ✅ Done — per-field provenance (`phone_source`, `email_source`, etc.) |
| 3c. Fix enrichment panel | ✅ Done — Maps→Crawl pipeline, no checkboxes |

**No work needed.**

---

## Task 4: Dashboard 3-Tab Structure 🔨

**Current**: 2 tabs — "Par Localisation" + "Par Job" (which is actually search history).

| Tab | Status | Work |
|-----|--------|------|
| 📍 Par Localisation | ✅ Built | Polish department cards |
| 📋 Par Job (sector grouping) | 🔨 NEW | Group batches by industry keyword. **Needs backend**: sector grouping endpoint |
| 🔍 Par Recherche | 🔨 Rename | Rename current "Par Job" tab. Add delete/rerun action buttons on each card |

#### Backend needed for Tab 2
A new endpoint that groups jobs by extracted sector keyword:
```
GET /api/dashboard/by-sector
→ [{ sector: "TRANSPORT", jobs: [...], total_companies: 150, phone_pct: 88 }, ...]
```

> [!IMPORTANT]
> The sector grouping is a string extraction from `query_name` (split on space, first token = sector). This is simple but fragile. Consider adding a `sector` column to `scrape_jobs` instead.

#### Files to modify
- `dashboard.js` — add 3rd tab, rename existing "Par Job" → "Par Recherche"
- `api.js` — add `getDashboardBySector()`
- Backend: new endpoint in `routes/dashboard.py`

---

## Task 5: Monitor Page — "Live Factory" 🔨

**Current state**: Polling works, pipeline stages exist, triage bar exists, company cards render.

| Improvement | Status | Work |
|-------------|--------|------|
| Animated progress bar | ✅ Done | Already has `.animated` shimmer class |
| Smooth number counting | 🔨 | `animateCounter()` for stats instead of instant jumps |
| Live company card feed | 🔨 | New cards slide in with CSS animation instead of full re-render |
| Rich cards with all data | 🔨 | Show phone, email, rating stars, social links on each card |
| Stage indicator | ✅ Done | `renderPipelineStages()` exists |
| Triage bar | ✅ Done | Fixed color (Nouveau = accent blue, not red) |
| Cancel button | 🔨 | Part of Task 2b |

#### Key constraint
Monitor polls every 1.5s. **Do not rebuild DOM on each poll.** Use surgical patching:
- Cache stable element refs after first render
- Use `document.createDocumentFragment()` for new card batches
- `animateCounter()` must not overlap on repeated calls

#### Files to modify
- `monitor.js` — smooth counters, card slide-in animation
- `components.js` — add `animateCounter()`, enhance card rendering
- `components.css` — card slide-in animation (`@keyframes slideIn`)

---

## Task 6: Upload Page Improvements 🔨

**Current**: Drag-and-drop works, upload history as table.

| Improvement | Status | Work |
|-------------|--------|------|
| Column guidance | 🔨 | Show expected CSV format before upload |
| Preview (first 5 rows) | 🔨 | Parse CSV client-side, show table before upload |
| Progress bar | 🔨 | For large files, show processing progress |
| History as cards | 🔨 | Replace table with visual cards |

#### Files to modify
- `upload.js` — column guidance, CSV preview, progress bar, card-style history

---

## Implementation Order

| Phase | Task | Backend ready? | Effort |
|-------|------|----------------|--------|
| **A** | Task 2e: Confirmation modal | ✅ Yes | Small |
| **B** | Task 1: Login upgrade | ✅ Yes | Medium |
| **C** | Task 2a-d: Delete/Cancel/Rerun buttons | ✅ Yes | Medium |
| **D** | Task 5: Monitor polish | ✅ Yes (frontend only) | Medium |
| **E** | Task 4: Dashboard 3rd tab | ⏳ Needs sector endpoint | Medium |
| **F** | Task 6: Upload improvements | ✅ Yes (frontend only) | Small |

> [!NOTE]
> Task 3 is fully complete. Phases A–D can start immediately. Phase E needs one backend endpoint (`/api/dashboard/by-sector`).

---

## Engineering Rules for Frontend Agent

1. **No DOM rebuilds on poll** — patch by ID, cache refs
2. **No duplicate listeners** — bind once, use delegation
3. **Register all intervals/listeners** — use `registerCleanup()` in `app.js`
4. **Truncate long text** — `max-width` + `text-overflow: ellipsis`
5. **Sanitize user data** — always `escapeHtml()` before rendering
6. **French labels** — all UI text in French
7. **Accessible controls** — keyboard-usable, visible focus, labeled buttons
8. **Test with hard refresh** — `Cmd+Shift+R` after JS changes (304 caching)
