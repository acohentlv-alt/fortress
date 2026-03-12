# Backend Agent Plan — Maps Match Validation

## Problem

**Plain English:** When we search Google Maps for a company, we check if the *address* Maps returns is in the same city as the SIRENE address. But we never check if the *business name* matches. So if Maps returns a completely different company in the same city, we accept it as "high confidence." That's wrong — we could be storing someone else's phone number and website.

**Real case:** BAILLOEUIL (SIREN in Perpignan at 38 Avenue Julien Panchot). Their actual website shows a different address (122 Avenue Victor Dalbiez). The SIRENE address may be the owner's home or old registration address. The Maps result may have picked up a different business entirely.

## Root Cause Analysis

### Current search query (line 537-539 of `enricher.py`)
```python
search_location = company.ville or company.code_postal or company.departement or ""
maps_result = await maps_scraper.search(denomination, search_location, siren=siren)
```
The search query is **`denomination + ville`** — this is correct. No street address leaks in.

### Current validation (`_assess_match`, lines 61-102 of `enricher.py`)
```python
# Only checks geography — never checks business name
if company.code_postal and company.code_postal in maps_address: return "high"
if company.ville and company.ville.lower() in maps_address: return "high"
```

**Gap:** A result in the same city but for a **completely different business** gets `"high"` confidence.

### Missing data from Maps scraper
`playwright_maps_scraper.py` extracts phone, website, address, rating, reviews — but **never extracts the business name** displayed on the Maps panel. Without this, we can't compare.

---

## Proposed Changes

### Phase 1: Extract Business Name from Maps Panel

#### [MODIFY] [playwright_maps_scraper.py](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/module_c/playwright_maps_scraper.py)

In `_extract_from_page()`, add extraction of the business name from the Maps panel header:

```python
# After existing extractions, before maps_url capture:
# ── Business name: h1 or aria-label from panel header
try:
    name_el = await page.query_selector('h1.DUwDvf, h1[class*="header"], div.lMbq3e h1')
    if name_el:
        result["maps_name"] = (await name_el.inner_text()).strip()
except Exception:
    pass
```

The Maps business panel always has an `<h1>` with class `DUwDvf` containing the business name. This is a stable selector used across Google Maps layouts.

**Return value change:** `maps_result` dict will now optionally include `"maps_name"`.

---

### Phase 2: Add Name Matching to `_assess_match`

#### [MODIFY] [enricher.py](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/module_d/enricher.py)

Rewrite `_assess_match()` to score **both name AND geography**:

```python
def _assess_match(maps_result: dict, company: Any) -> str:
    """Assess match confidence using name + geography.

    Scoring matrix:
        Name match + same city/postal   → "high"
        Name match + no address          → "high" (Maps confirmed the name)
        No name match + same city/postal → "low"  (probably wrong business)
        No name match + no address       → "low"
        No data at all                   → "none"
    """
    if not maps_result:
        return "none"

    # ── Name match ─────────────────────────────────
    maps_name = (maps_result.get("maps_name") or "").strip()
    denomination = (company.denomination or "").strip()
    name_matches = _names_match(maps_name, denomination)

    # ── Geographic match (existing logic) ──────────
    maps_address = (maps_result.get("address") or "").lower()
    geo_matches = _geo_matches(maps_address, company)

    # ── Decision matrix ────────────────────────────
    if name_matches:
        return "high"  # Name confirmed — geography is secondary
    if not maps_name:
        # Couldn't extract name (panel issue) — fall back to geo-only
        return "high" if geo_matches else ("low" if maps_result.get("phone") else "none")
    # Name didn't match but we got a name from Maps
    return "low"  # Wrong business, even if same city
```

Add a new helper `_names_match()`:

```python
def _names_match(maps_name: str, denomination: str) -> bool:
    """Fuzzy compare Maps business name with SIRENE denomination.

    Strategy:
        1. Normalize: lowercase, strip legal forms (SARL, SAS, EURL, etc.)
        2. Token overlap: if ≥50% of denomination tokens appear in maps_name → match
        3. Containment: if one is a substring of the other → match

    Returns True if the names plausibly refer to the same business.
    """
    if not maps_name or not denomination:
        return False

    # Strip common French legal form suffixes
    _LEGAL_FORMS = {"sarl", "sas", "sasu", "eurl", "sa", "sci", "snc",
                     "scs", "sca", "ei", "eirl", "asso", "association"}

    def _normalize(name: str) -> list[str]:
        tokens = re.sub(r'[^a-z0-9àâäéèêëïîôùûüÿçœæ\s]', '', name.lower()).split()
        return [t for t in tokens if t not in _LEGAL_FORMS and len(t) > 1]

    maps_tokens = _normalize(maps_name)
    denom_tokens = _normalize(denomination)

    if not maps_tokens or not denom_tokens:
        return False

    # Containment check (handles "BAILLOEUIL" vs "Bailloeuil Perpignan")
    maps_joined = " ".join(maps_tokens)
    denom_joined = " ".join(denom_tokens)
    if maps_joined in denom_joined or denom_joined in maps_joined:
        return True

    # Token overlap: ≥50% of denomination tokens found in maps name
    overlap = sum(1 for t in denom_tokens if t in maps_tokens)
    threshold = max(1, len(denom_tokens) * 0.5)
    return overlap >= threshold
```

Extract existing geo logic to `_geo_matches()` (keep the same code, just refactored):

```python
def _geo_matches(maps_address: str, company: Any) -> bool:
    """Check if Maps address is in the same city/postal/dept as SIRENE."""
    if not maps_address:
        return False
    if company.code_postal and company.code_postal in maps_address:
        return True
    if company.ville and company.ville.lower() in maps_address:
        return True
    if company.departement:
        postal_matches = re.findall(r"\b(\d{5})\b", maps_address)
        for postal in postal_matches:
            if postal[:2] == company.departement or (
                len(company.departement) == 3 and postal[:3] == company.departement
            ):
                return True
    return False
```

---

### Phase 3: Log Name Match Data for Diagnostics

#### [MODIFY] [enricher.py](file:///Users/alancohen/Downloads/Project%20Alan%20copy/fortress/fortress/module_d/enricher.py)

In the existing `enricher.maps_result` log line (line 556-564), add `maps_name`:

```python
log.info(
    "enricher.maps_result",
    siren=siren,
    match_confidence=match_confidence,
    maps_name=maps_result.get("maps_name"),    # NEW
    maps_phone=maps_result.get("phone"),
    maps_website=maps_result.get("website"),
    maps_address=maps_result.get("address"),
)
```

Also update `_log_enrichment()` to include `maps_name` in the `enrichment_log` table (add column via schema migration).

---

## What This Does NOT Change

- **Search query** stays `denomination + ville` — no change needed
- **Website crawl logic** — no change (separate concern)
- **Qualification flow** — same `qualified = contact is not None and match_confidence != "none"` logic
- **Low-confidence phone filtering** — still discards phones on `"low"` confidence (but now "low" means "wrong business name", which is even more correct)

## Risks

> [!WARNING]
> **Potential increase in replacement rate.** Companies where Maps returns a result with a slightly different name (e.g., "TRANSPORTS BAILLOEUIL" vs "BAILLOEUIL") would be marked `"low"` if `_names_match()` is too strict. The token-overlap threshold (50%) mitigates this — "BAILLOEUIL" appears in both, so overlap = 1/1 = 100%.

> [!IMPORTANT]
> **Maps panel `h1` selector stability.** `h1.DUwDvf` is the current Google Maps class name for the business panel header. Google can change this. The scraper should fall back to `h1` if the specific class isn't found.

## Verification Plan

### Automated
- `python3 -c "import ast; ast.parse(open('enricher.py').read())"` for syntax
- `python3 -c "import ast; ast.parse(open('playwright_maps_scraper.py').read())"` for syntax

### Functional Test
- Run a 5-company batch against département 66
- Check `enrichment_log` for `maps_name` values
- Verify: companies where Maps name matches denomination → `"high"` confidence
- Verify: companies where Maps name doesn't match → `"low"` → replaced

### Manual Verification
- Search "BAILLOEUIL PERPIGNAN" on Google Maps in a browser
- Check what the `<h1>` panel shows
- Compare with SIRENE denomination
- Confirm the new `_names_match()` logic would produce the correct result
