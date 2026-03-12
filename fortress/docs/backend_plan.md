# Fortress — Backend Plan

Everything the backend agent needs to build. No frontend work here — that goes in the Frontend Plan.

---

## Quick Answers to Your Comments

**What is npm?**
npm stands for "Node Package Manager." It's a tool that downloads code libraries other people wrote — in our case, a testing tool called `vitest`. When you run `npm install`, it downloads those libraries into a folder called `node_modules`. You can delete that folder and recreate it anytime with one command.

**What is `__pycache__`?**
When Python runs your code for the first time, it creates a "compiled" version to make it faster next time. These compiled files are stored in folders called `__pycache__`. If you delete them, Python just recreates them automatically the next time you run the code. They're like temporary work files.

**What is `data/`?**
Not test results — these are **runtime files** created when the pipeline runs. It includes: log files from each batch (what happened during scraping), checkpoint files (bookmarks so a crashed batch can resume), and JSONL output files (the collected company cards). They grow over time and can be cleaned up.

**Is `node_modules` on GitHub?**
✅ Checked — your `.gitignore` already excludes `node_modules/`, `__pycache__/`, and `data/`. They are NOT uploaded to GitHub. Your GitHub repo is clean.

---

# Task 1: Authentication System

## What this does (plain English)

Right now anyone with the URL can use Fortress — no password, no login, nothing. Before going live, we need a door with a key. You want 3 people to use it on day one, each with their own login.

**How it will work:**
- When someone opens Fortress, they see a login page
- They type their username and password
- The system checks if they're allowed in
- If yes, they get a session (stays logged in until they close the browser or click "Déconnexion")
- Different people can have different permissions

## User roles (revised per your feedback)

| Action | User (opérateur) | Admin (directeur) |
|--------|:-:|:-:|
| View dashboard, search, browse companies | ✅ | ✅ |
| Launch new batch | ✅ | ✅ |
| Monitor pipeline | ✅ | ✅ |
| Delete a batch | ✅ | ✅ |
| Cancel a running batch | ✅ | ✅ |
| Rerun a batch | ✅ | ✅ |
| Export CSV | ✅ | ✅ |
| Upload CRM/CSV | ✅ | ✅ |
| **Access ALL clients' data** | ❌ own data only | ✅ sees everything |
| System maintenance / diagnostics | ❌ | ✅ |

**Key difference:** A user sees only companies from batches they launched. The admin (you) sees everything across all users.

## Technical approach

**Username + password stored in the database** (not API keys — because you want 3+ real users who can be added/removed).

| What | How |
|------|-----|
| Passwords | Hashed with `bcrypt` (the industry standard — stores a scrambled version, not the real password) |
| Sessions | Server-side session token stored in a cookie. When you log in, the server gives your browser a secret ticket. Each request shows that ticket to prove who you are. |
| User table | New `users` table in PostgreSQL: `id`, `username`, `password_hash`, `role`, `created_at` |
| Initial users | Created via a setup script: `python3 -m fortress.setup_users` — asks for usernames and passwords |
| No external service | Everything runs on your own server. No cloud auth service, no monthly fee for auth. |

## Files to create/modify

| File | What changes |
|------|-------------|
| `database/schema.sql` | Add `users` table |
| `api/auth.py` **[NEW]** | Login/logout logic, password checking, session management |
| `api/routes/auth.py` **[NEW]** | `POST /api/auth/login`, `POST /api/auth/logout`, `GET /api/auth/me` |
| `api/main.py` | Add auth middleware — every request checks for valid session |
| `api/routes/*.py` | Add user context — so we know WHO is making the request |
| `setup_users.py` **[NEW]** | Command-line script to create the first admin + user accounts |
| `config/settings.py` | Add `session_secret` setting (random string for encrypting cookies) |

## Deployment note (your "Director Plan")

For 3 users accessing from different computers, you need the system on a server — not just your laptop. The cheapest path:

| Option | Monthly cost | What you get |
|--------|-------------|-------------|
| **DigitalOcean Droplet** | ~€6/month | Linux server with your own IP address |
| **Domain name** (optional) | ~€10/year | `fortress.yourname.com` instead of an IP address |
| **SSL certificate** | Free (Let's Encrypt) | The padlock icon in the browser — makes passwords safe |

The auth system I'm building works on both your laptop (for testing) and on a cloud server (for production). No changes needed between the two.

---

# Task 2: Delete / Cancel / Rerun Endpoints

## What this does (plain English)

Right now, once you launch a batch, you can only watch it. You can't stop it, delete it, or restart it. These endpoints give the frontend the buttons to do that.

### 2a. Delete a batch

**What happens:** The batch disappears from your dashboard and job list. But the company data and contacts it collected stay in the database — other batches might reference them. It's like removing a folder label, not shredding the documents inside.

| Detail | Value |
|--------|-------|
| Endpoint | `DELETE /api/jobs/{query_id}` |
| Who can do it | User (own batches) + Admin (any batch) |
| What it does | Sets batch status to `deleted`, removes search tags for that batch |
| What it preserves | All company records, all contact records |
| Protection | Can't delete a running batch — must cancel first |

### 2b. Cancel a running batch

**What happens:** The pipeline finishes the company it's currently working on, then stops. Everything collected so far is kept. The remaining companies are left unprocessed.

| Detail | Value |
|--------|-------|
| Endpoint | `POST /api/jobs/{query_id}/cancel` |
| Who can do it | User (own batches) + Admin (any batch) |
| How it stops | Sets a "please stop" flag in the database. The runner checks this flag before starting each new company. |
| Data safety | Per-company saving means nothing is lost — any company already processed is preserved |

### 2c. Remove a single company from results

**What happens:** One company disappears from a batch's results. The company itself stays in the database — it's just unlinked from that specific batch. Like removing a name from a guest list.

| Detail | Value |
|--------|-------|
| Endpoint | `DELETE /api/companies/{siren}/tags/{query_id}` |
| Who can do it | User + Admin |

### 2d. Rerun a batch

**What happens:** The frontend reads the original parameters (sector, department, size) from the completed/failed batch, then submits a new batch with the same settings. No new backend endpoint needed — just make sure the existing job detail response includes the original parameters.

| Detail | Value |
|--------|-------|
| Change needed | `GET /api/jobs/{query_id}` must return `sector`, `departement`, `batch_size`, `naf_code`, `city`, `mode` |
| New endpoint? | No — frontend handles the flow: read old params → call `POST /api/batch/run` with same params |

### 2e. Schema change for cancellation

Add one column to `scrape_jobs`:
```sql
ALTER TABLE scrape_jobs ADD COLUMN cancel_requested BOOLEAN DEFAULT FALSE;
```

And one check in `batch_processor.py`: before each new wave, read this flag. If true, stop the loop and set status to `cancelled`.

---

# Task 3: Maps Name Matching

## What this does (plain English)

Right now, when we search Google Maps for "BAILLOEUIL PERPIGNAN", Maps might return a completely different business that happens to be in Perpignan. Our system says "same city? great — high confidence!" and stores that wrong company's phone number.

**The fix:** After Maps returns a result, also check if the **business name** matches, not just the city. If Maps says "Pizza Hut" but we're looking for "BAILLOEUIL", that's a wrong match — replace the company.

### 3a. Extract the business name from Google Maps

**What changes:** When the Playwright browser scrapes a Maps result, it now also reads the **business name** shown in the panel header (the big text at the top of the Maps sidebar).

| File | Change |
|------|--------|
| `playwright_maps_scraper.py` | Extract the `<h1>` text from the Maps panel → save as `maps_name` |

### 3b. Add name comparison to confidence scoring

**What changes:** The `_assess_match()` function currently only checks geography (postal code, city). Now it also compares the Maps business name to the SIRENE denomination using fuzzy matching (handles "TRANSPORTS BAILLOEUIL" vs "BAILLOEUIL" correctly).

| File | Change |
|------|--------|
| `enricher.py` | Rewrite `_assess_match()` to score name + geography. Name match = high confidence. No name match = low confidence (even if same city). |

### 3c. Log the Maps name for diagnostics

So you can review what Maps actually returned for each company.

| File | Change |
|------|--------|
| `enricher.py` | Include `maps_name` in the `enrichment_log` entry |

---

# Task 4: Dashboard Sector Grouping Data

## What this does (plain English)

The frontend wants to group batches by **industry type** (transport, logistics, bakery) instead of by raw search string. The backend needs to provide this grouping.

| Detail | Value |
|--------|-------|
| Change | Include `sector` field in `GET /api/jobs` and `GET /api/jobs/{query_id}` responses |
| Source | The sector comes from the batch creation form — it's already stored as part of `query_name` |

---

# Implementation Order

| Phase | Tasks | Depends on |
|-------|-------|-----------|
| **A** | Auth system (Task 1) | Nothing — can start now |
| **B** | Delete/Cancel/Rerun (Task 2) | Nothing — can start now |
| **C** | Maps name matching (Task 3) | Nothing — independent |
| **D** | Sector grouping (Task 4) | Nothing — independent |

Phases A and B are **production blockers**. C and D improve quality and UX.
