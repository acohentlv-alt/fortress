# Fortress — Frontend Plan

Everything the frontend agent needs to build. Backend endpoints are listed where needed (they must be built first — see Backend Plan).

---

# Task 1: Login Page ✅ DONE

## What this does (plain English)

When someone opens Fortress, the first thing they see is a login page — a simple form with username and password. After logging in, they stay logged in until they close the browser or click "Déconnexion" (logout). No signup form — accounts are created by the admin using a command-line script.

### What to build

| Element | Details |
|---------|---------|
| Login form | Username + password fields, "Connexion" button |
| Error message | "Identifiants incorrects" if wrong credentials |
| Loading state | Button shows spinner while checking credentials |
| After login | Redirect to Dashboard (`#/`) |
| Logout button | In the sidebar or header — calls `/api/auth/logout`, redirects to login |
| Auth guard | If not logged in, every page redirects to login |
| Session storage | Browser remembers the session via a cookie (handled by backend) |

### Needs backend first

- `POST /api/auth/login` — check credentials, create session
- `POST /api/auth/logout` — end session
- `GET /api/auth/me` — "am I logged in?" (returns user info or 401)

---

# Task 2: Delete / Cancel / Rerun Buttons ✅ DONE

## What this does (plain English)

Right now you can launch a batch and watch it run, but you can't stop it, delete it, or restart it. These buttons fix that.

### 2a. Delete button (on Job Detail page)

**Where:** Job detail page (`#/job/{id}`), in the header next to the export button.

**What happens when clicked:** A popup appears showing exactly what will happen:
```
🗑️ Supprimer ce batch ?

Batch: TRANSPORT 66
Créé le: 12/03/2026 à 14:30
47 entreprises collectées

⚠️ Les tags de recherche seront supprimés.
✅ Les fiches entreprises et contacts resteront dans la base.

[Annuler]  [Supprimer]
```

If confirmed → calls `DELETE /api/jobs/{query_id}` → shows success toast → redirects to dashboard.

### 2b. Cancel button (on Monitor page)

**Where:** Monitor page (`#/monitor/{id}`), visible only when status is `in_progress`.

**What happens:**
```
⏹ Arrêter ce batch ?

Batch: TRANSPORT 66
Progression: 23/50 entreprises (46%)

✅ Les 23 entreprises déjà collectées seront conservées.
⚠️ Les 27 restantes ne seront pas traitées.

[Annuler]  [Arrêter le batch]
```

If confirmed → calls `POST /api/jobs/{query_id}/cancel` → polls until status changes → shows toast.

### 2c. Rerun button (on Job Detail page)

**Where:** Job detail page, for completed or failed batches.

**What happens:** Reads the original batch settings from the job response, opens the New Batch page with those settings pre-filled, user clicks "Lancer" to confirm.

### 2d. Remove company button (on Job Detail company cards)

**Where:** Small `×` or 🗑️ on each company card in the job results list.

**What happens:** Quick confirmation → calls `DELETE /api/companies/{siren}/tags/{query_id}` → card disappears with animation.

### 2e. Confirmation Modal component

Build a reusable popup component that all the above buttons use:

```javascript
showConfirmModal({
    title: 'Supprimer ce batch ?',
    body: '47 entreprises collectées...',
    confirmLabel: 'Supprimer',
    danger: true,
    onConfirm: () => deleteJob(queryId)
})
```

### Needs backend first

- `DELETE /api/jobs/{query_id}`
- `POST /api/jobs/{query_id}/cancel`
- `DELETE /api/companies/{siren}/tags/{query_id}`
- `GET /api/jobs/{query_id}` must return original batch params for rerun

---

# Task 3: Company Page Improvements

## What this does (plain English)

Three visual improvements to the company detail page. No backend changes needed.

### 3a. Reorder sections — Contact moves up

**Current order** (right column): Identity → Activity → Financials → Location → **Contact** (#5) → Reviews → Directors → History

**New order:** Identity → **Contact + Reviews** (#2) → Location → Activity → Financials → Directors → History

**Why:** When a salesperson opens a company page, the first thing they want is the phone number and email — not the legal form or NAF code.

### 3b. Data source tooltips

Every data field shows where it came from when you hover over a small `ℹ️` icon:

| Field | Tooltip |
|-------|---------|
| SIREN, SIRET, Legal form, NAF | "Source : Registre SIRENE (INSEE)" |
| Phone, Address, Website, Rating | "Source : Google Maps" |
| Email | "Source : Site web (www.company.com)" |
| LinkedIn, Facebook | "Source : Trouvé sur www.company.com" |

**Why:** So the user trusts the data and knows exactly where each piece came from.

### 3c. Fix enrichment panel

The current panel shows checkboxes for "Website", "PagesJaunes", "Pappers" — none of these are real anymore. The actual pipeline is Maps → Website Crawl.

**Replace with:** A visual pipeline diagram showing the 2 real steps:
```
🗺️ Google Maps → 🌐 Site Web
(~5 seconds)     (~20 seconds)
```
One button: "🚀 Lancer l'enrichissement". No checkboxes.

---

# Task 4: Dashboard 3-Tab Structure ✅ DONE

## What this does (plain English)

The dashboard currently has 2 views. You want 3:

### Tab 1: 📍 Par Localisation (current — improve)

Departments as folder cards, each showing how many companies were found and quality percentages. Click a department → see the companies inside.

### Tab 2: 📋 Par Job (NEW)

Groups all batches by **industry** — transport, logistics, bakery, etc. Shows the total across all related batches.

Example: you ran "TRANSPORT 66", "TRANSPORT 34", "TRANSPORT 11" → they all appear under one "TRANSPORT" card showing combined stats.

### Tab 3: 🔍 Par Recherche (rename current "Par Job")

Search history timeline — every batch you've launched, sorted by date. Each card shows the search query, date, number of results, quality percentages, and action buttons (export, rerun, delete).

### Needs backend

- Sector grouping data (Task 4 in Backend Plan)

---

# Task 5: Monitor Page — "Live Factory" Feel 🔨 (Partially Done - Animated Counters)

## What this does (plain English)

The monitor page shows a running pipeline. Right now it works but feels static. The goal is to make it feel alive — like watching a factory assembly line.

| Improvement | What it means |
|------------|--------------|
| Animated progress | The progress bar pulses and glows while running, numbers count up smoothly instead of jumping |
| Live company feed | When a new company is found, its card slides into view with a smooth animation — like a live news feed |
| Rich cards | Each card shows ALL collected data: name, phone, email, website, rating stars, social links |
| Stage indicator | A visual showing which step the pipeline is on: Maps 🗺️ → Crawl 🌐 → Save 💾 |
| Triage visualization | The colored triage counts (BLACK/GREEN/YELLOW/RED) as a visual bar instead of just numbers |

### No backend changes needed — frontend only

---

# Task 6: Upload Page Improvements

## What this does (plain English)

The CRM upload page lets you drag-and-drop a CSV file of companies you already own (your existing clients). These companies are marked "BLUE" — meaning the pipeline skips them during scraping (no point scraping your own clients).

### Improvements

| Feature | What it does |
|---------|-------------|
| Column guidance | Show which columns the CSV needs (at minimum: a SIREN column) |
| Preview | Before uploading, show the first 5 rows so the user can check it looks right |
| Progress bar | For large files, show how much has been processed |
| Better history | Show previous uploads as visual cards instead of a plain table |

---

# Implementation Order

| Phase | Tasks | Blocked on backend? |
|-------|-------|:------------------:|
| **A** | Login page (Task 1) | ✅ Yes — auth endpoints |
| **B** | Company page improvements (Task 3) | ❌ No — start now |
| **C** | Delete/Cancel/Rerun buttons (Task 2) | ✅ Yes — action endpoints |
| **D** | Monitor "live factory" (Task 5) | ❌ No — frontend only |
| **E** | Dashboard tabs (Task 4) | ✅ Partially — sector data |
| **F** | Upload improvements (Task 6) | ❌ No — frontend only |

**Tasks 3, 5, and 6 can start immediately** — they're frontend-only.
**Tasks 1, 2, and 4 need the backend built first.**
