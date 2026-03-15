# 1st Step: Learning Code Through Fortress

## A Macro-to-Micro Guide for Complete Beginners

---

## Level 1: The Big Picture — What Is Software?

Software is made of **3 layers** that talk to each other:

| Layer | What it does | Built with |
|-------|-------------|------------|
| **Frontend** (what the user sees) | Draws buttons, tables, progress bars on screen | HTML, CSS, JavaScript |
| **Backend** (the brain) | Receives requests, makes decisions, talks to the database | Python (FastAPI) |
| **Database** (the memory) | Permanently stores 14.7 million companies and their contacts | PostgreSQL |

### How they communicate

These layers talk via **API calls** — like a waiter carrying orders between the dining room and the kitchen:

1. User clicks "Search for bakeries in Paris" on the **Frontend**
2. Frontend sends a request to the **Backend** API
3. Backend queries the **Database** for matching companies
4. Backend sends results back to the Frontend
5. Frontend displays the results on screen

**In Fortress:** The user searches for businesses, the backend finds them in a 14.7M company registry, enriches them with phone/email/website data from the internet, and the frontend shows live progress.

---

## Level 2: File & Folder Organization — Why Not One Big File?

Imagine running a company where every department — HR, sales, accounting, security — shares one giant notebook. Chaos. Software works the same way: **you split code into files and folders by responsibility**.

### Fortress Folder Structure

```
fortress/
├── config/          → Manager's Rulebook (settings, passwords, environment rules)
│   └── settings.py
├── api/             → Front Desk (receives web requests, checks login)
│   ├── main.py      → Traffic cop — sets up the web server
│   ├── db.py        → Database operator — manages connections
│   ├── auth.py      → Security guard — passwords, login/logout
│   └── routes/      → Specific web addresses the frontend can call
├── database/        → Blueprint Room (table definitions)
│   └── schema.sql   → The exact structure of every database table
├── module_a/        → Sorting Hat (interprets searches, classifies companies)
│   ├── query_interpreter.py  → Translates "transport 66" into a database query
│   └── triage.py             → Sorts companies into color-coded buckets
├── module_c/        → Field Workers (scraping tools)
│   ├── playwright_maps_scraper.py  → Opens invisible Chrome, searches Google Maps
│   └── curl_client.py              → Visits company websites pretending to be Chrome
├── module_d/        → Assembly Line (enrichment logic, batch processing)
│   ├── enricher.py          → The strict inspector — enforces quality rules
│   ├── batch_processor.py   → Breaks work into waves of 50, saves checkpoints
│   └── deduplicator.py      → Saves data without creating duplicates
├── frontend/        → Storefront (visual dashboard)
│   └── app.js       → Dashboard with buttons, progress bars, export
├── data/            → Storage Room (checkpoints, logs, exports)
└── runner.py        → Factory Manager — orchestrates the entire pipeline
```

### Why split into files?

- **Independence:** Fixing the scraper won't break the login system
- **Teamwork:** Multiple programmers can work on different files simultaneously
- **Readability:** You know exactly where to look when something breaks
- **Reusability:** The `curl_client.py` tool can be used by any file that needs to visit websites

---

## Level 3: Core Coding Concepts — Through Fortress Examples

### 1. Variables (Labeled Boxes)

A variable stores a piece of information so the program can remember it.

```python
batch_size = 50          # How many companies to process
wave_current = 3         # Which batch we're currently on
company_name = "GEODIS"  # The name we're looking up
phone = None             # No phone found yet (empty box)
```

**In Fortress:** `wave_current` starts at 0 and increases by 1 after each batch of 50 companies is processed.

### 2. Conditionals (If/Then Decisions)

The program makes choices based on conditions — exactly like human logic.

```python
if phone_number is not None:
    # Company is qualified! Save it.
    save_to_database(company)
else:
    # No phone = reject. Get a replacement.
    replace_with_next_company()
```

**In Fortress:** This is the "MVP Phone Gate" — the most important decision in the pipeline. No phone number? Company gets rejected immediately, no matter how much other data was found.

### 3. Loops (Repeat Until Done)

A loop tells the computer: "Do this action over and over until a condition is met."

```python
for company in wave_companies:    # For each company in this batch of 50...
    data = scrape_google_maps(company)   # Go find their data
    if data.phone:                       # If phone found...
        save_to_database(data)           # Save it!
    else:
        skip_and_replace(company)        # Otherwise, replace
```

**In Fortress:** The `batch_processor.py` loops through companies in "waves" of 50. After each wave, it saves a checkpoint file — a bookmark so it can resume if the system crashes.

### 4. Functions (Reusable Mini-Programs)

Instead of writing the same code over and over, you write it once in a function and call it by name.

```python
def _assess_match(maps_name, registry_name, maps_city, registry_city):
    """Compare Google Maps result to official registry data."""
    if names_match and cities_match:
        return "high"      # Trusted data
    elif names_match:
        return "low"       # Might be a branch office
    else:
        return "none"      # Wrong business entirely
```

**In Fortress:** `_assess_match()` is called every time the scraper finds a business on Google Maps. It returns a confidence grade that determines what data to keep vs. discard.

### 5. Classes (Blueprints for Complex Tools)

A function is a verb (an action). A class is a noun (a machine with its own memory and rules).

```python
class CurlClient:
    timeout = 8          # Wait max 8 seconds per page
    delay = 0.4          # Wait 0.4s between page visits
    impersonate = "chrome"  # Pretend to be Chrome browser

    def get(self, url):
        # Visit a webpage using the rules above
        ...
```

**In Fortress:** `CurlClient` is a virtual machine that impersonates Chrome. Because it's a class, it remembers its own settings (timeout, delay) across every page it visits.

### 6. Import Statements (Files Talking to Each Other)

An import borrows code from another file — like one department calling another.

```python
# Inside runner.py (the factory manager):
from fortress.module_a.query_interpreter import interpret_query
from fortress.module_a.triage import classify_companies
from fortress.module_d.enricher import enrich_company
```

**In Fortress:** `runner.py` doesn't know how to scrape — it imports specialized workers. `enricher.py` imports scraping tools from `module_c`. Each file focuses on one job.

### 7. Async/Await (Multitasking While Waiting)

Normal code runs line by line. If line 3 waits for a website to respond, everything freezes. Async says: "While waiting, go do something else useful."

```python
async def enrich_company(company):
    data = await scrape_google_maps(company)  # Start scraping, don't freeze
    # While Maps loads, the server can still update the user's dashboard
    await save_to_database(data)              # Save when ready
```

**In Fortress:** While waiting 3 seconds for Google Maps to load, the server can still answer requests from the user's dashboard to update the progress bar. Without async, the entire system would freeze on every web request.

---

## Level 4: Database Design — How Data Is Stored Permanently

### What is a database?

A database is like a massive Excel spreadsheet with strict rules:

- **Table** = one spreadsheet tab (holds one type of thing)
- **Column** = a category header (Name, Phone, City)
- **Row** = one entry (one specific company)
- **Primary Key** = a unique ID that guarantees no two rows are confused
- **Foreign Key** = a link that connects rows in different tables

### Fortress Tables

```
┌─────────────────────────────────────────────────────┐
│                 companies (14.7M rows)              │
│  THE MASTER REGISTRY — Protected, read-only         │
│─────────────────────────────────────────────────────│
│  siren (PK)  │ denomination │ code_postal │ ville   │
│  123456789   │ GEODIS       │ 31000       │ TOULOUSE│
│  987654321   │ BOULANGERIE  │ 75001       │ PARIS   │
└──────────┬──────────────────────────────────────────┘
           │ siren (Foreign Key links everything back)
     ┌─────┼──────────────┐
     ▼     ▼              ▼
┌──────────┐ ┌───────────┐ ┌──────────────┐
│ contacts │ │ officers  │ │ query_tags   │
│──────────│ │───────────│ │──────────────│
│ siren    │ │ siren     │ │ siren        │
│ phone    │ │ nom       │ │ query_name   │
│ email    │ │ prenom    │ │ ("transport  │
│ website  │ │ role      │ │   66")       │
│ linkedin │ │           │ │              │
└──────────┘ └───────────┘ └──────────────┘

┌──────────────────────────────────────┐
│           scrape_jobs                │
│  TRACKS PIPELINE PROGRESS            │
│──────────────────────────────────────│
│ query_id │ status      │ scraped    │
│ BATCH_001│ in_progress │ 23/50      │
│ BATCH_002│ completed   │ 50/50      │
└──────────────────────────────────────┘
```

**Key rule:** The `companies` table is PROTECTED. The scraper never modifies it — it only reads from it. All scraped data goes into `contacts` (linked by `siren`).

---

## Level 5: Architectural Decisions — Why These Tools?

Every tool choice solves a specific problem:

| Decision | The Problem | The Solution | Why |
|----------|-------------|-------------|-----|
| **PostgreSQL** | 14.7M rows to search | Advanced indexing | SQLite can't handle this scale — need indexes to avoid scanning every row |
| **FastAPI** | Server freezes while waiting | Async web framework | Built for I/O-heavy work — serves users while scraping runs in background |
| **curl_cffi** | Websites block bots | TLS impersonation | Mimics Chrome's cryptographic fingerprint — websites think it's a real browser |
| **Playwright** | Google Maps needs JavaScript | Headless Chrome | Maps loads data dynamically — a simple HTTP request gets an empty page |
| **Waves of 50** | Crash = lose all progress | Checkpoint after each wave | System resumes exactly where it left off |
| **Connection pool (max 5)** | Opening DB connections is slow | Keep 5 "warm" connections | Workers borrow and return connections instead of creating new ones |
| **Module separation** | Changes break unrelated code | a/c/d folders | Sorting, scraping, and business logic are independent concerns |
| **asyncio.Lock()** | Maps bans rapid-fire searches | One search at a time | Forces sequential Maps queries with human-like delays |

---

## Level 6: Design Patterns — Real-World Problem Solving

### 1. Checkpoint / Resume (Crash Recovery)

**Problem:** Server crashes at company 499 out of 500 — lose everything?

**Solution:** After every wave of 50, save a checkpoint file to disk.

```
data/checkpoints/TRANSPORT_66_BATCH_001/
  └── checkpoint.json  →  {"wave_current": 7, "seen_set": [siren1, siren2, ...]}
```

On restart: read the file, resume at wave 8. Zero lost work.

### 2. Seen Set (Deduplication on Resume)

**Problem:** After a crash, how do you avoid re-processing companies already done?

**Solution:** The checkpoint file contains a `seen_set` — a list of every SIREN already processed. On resume, skip anything in the set.

### 3. Data Bank (Smart Reuse)

**Problem:** User A scraped "GEODIS" last week. User B searches for it today. Scrape again?

**Solution:** No. The Triage stage checks if a company is already in the Data Bank (has been successfully scraped before). If yes, mark it GREEN — instant reuse, no scraping needed.

### 4. Waterfall + Fast-Fail (Resource Efficiency)

**Problem:** Website crawling is expensive (5+ pages per company). What if the company has no phone?

**Solution:** Search Maps first. No phone? Reject immediately. Never waste time crawling the website of a company that already failed the minimum requirement.

### 5. Rate Limiting (Act Human, Don't Get Banned)

**Problem:** Google blocks IPs that make too many requests too fast.

**Solution:**
- `asyncio.Lock()` forces one Maps search at a time
- Random 2-3 second delay between searches
- Random 5-15 second cooldown between waves
- `curl_cffi` impersonates Chrome's TLS fingerprint

---

## Summary: The Learning Path

```
Level 1: What is software?           → 3 layers: Frontend, Backend, Database
Level 2: How is code organized?      → Files & folders by responsibility
Level 3: How does code work?         → Variables, conditionals, loops, functions, classes, imports, async
Level 4: How is data stored?         → Tables, keys, relationships
Level 5: Why these specific tools?   → Each tool solves a real problem
Level 6: How do you handle failure?  → Checkpoints, deduplication, reuse, fast-fail, rate limiting
```

Each level builds on the one before. Every concept is grounded in something Fortress actually does — not abstract theory, but real decisions that make the system work.
