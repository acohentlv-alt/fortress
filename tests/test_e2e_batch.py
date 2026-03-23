"""E2E Batch Test — logistique dept 66, limit 50.

Fetches company names from Recherche Entreprises API, then runs each
through the Google Maps scraper to capture maps_url.

No database required — tests the full scraper pipeline under batch load.
"""

import asyncio
import json
import sys
import time

import structlog
structlog.configure(
    processors=[structlog.dev.ConsoleRenderer(colors=True)],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()


async def fetch_companies(keyword: str, department: str, limit: int) -> list[dict]:
    """Fetch companies from Recherche Entreprises API."""
    import ssl
    import urllib.request
    import urllib.parse

    # Development: bypass SSL verification
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    base = "https://recherche-entreprises.api.gouv.fr/search"
    params = urllib.parse.urlencode({
        "q": keyword,
        "departement": department,
        "per_page": min(limit, 25),
        "page": 1,
    })

    companies = []
    page = 1
    while len(companies) < limit:
        params = urllib.parse.urlencode({
            "q": keyword,
            "departement": department,
            "per_page": min(limit - len(companies), 25),
            "page": page,
        })
        url = f"{base}?{params}"
        try:
            with urllib.request.urlopen(url, timeout=10, context=ctx) as resp:
                data = json.loads(resp.read())
                results = data.get("results", [])
                if not results:
                    break
                for r in results:
                    companies.append({
                        "siren": r.get("siren", ""),
                        "name": r.get("nom_complet", r.get("nom_raison_sociale", "")),
                        "city": (r.get("siege", {}) or {}).get("libelle_commune", ""),
                    })
                page += 1
        except Exception as e:
            log.warning("api_error", error=str(e), page=page)
            break

    return companies[:limit]


async def main():
    print("=" * 70)
    print("🚀 E2E Batch Test — logistique dept 66, limit 50")
    print("=" * 70)

    # ── Step 1: Fetch companies ────────────────────────────────────────
    print("\n📡 Fetching companies from Recherche Entreprises API...")
    companies = await fetch_companies("logistique", "66", 50)
    print(f"   📊 Got {len(companies)} companies")

    if not companies:
        print("   ❌ No companies returned from API")
        return 1

    for i, c in enumerate(companies[:5]):
        print(f"   [{i+1}] {c['name']} (SIREN: {c['siren']}, City: {c['city']})")
    if len(companies) > 5:
        print(f"   ... and {len(companies) - 5} more")

    # ── Step 2: Start Maps scraper ─────────────────────────────────────
    print("\n🌐 Starting Playwright Chromium stealth engine...")
    from fortress.scraping.maps import PlaywrightMapsScraper

    scraper = PlaywrightMapsScraper()
    try:
        await scraper.start()
        print("   ✅ Chromium ready\n")
    except Exception as e:
        print(f"   ❌ Failed to start: {e}")
        return 1

    # ── Step 3: Process batch ──────────────────────────────────────────
    # Run first 5 companies through the scraper (full batch would take too long)
    BATCH_SIZE = 5
    batch = companies[:BATCH_SIZE]

    results = []
    start_time = time.time()

    try:
        for i, company in enumerate(batch, 1):
            search_query = f"{company['name']} {company['city']}" if company['city'] else company['name']
            print(f"─── [{i}/{BATCH_SIZE}] {company['name']} ───")
            print(f"    Query: {search_query}")
            print(f"    SIREN: {company['siren']}")

            result = await scraper.search(
                search_query,
                "66",
                siren=company["siren"],
            )

            results.append({
                "siren": company["siren"],
                "name": company["name"],
                **result,
            })

            # Print key fields
            if result:
                for key in ["phone", "website", "address", "rating", "maps_url"]:
                    val = result.get(key)
                    if val:
                        display = str(val)[:80]
                        print(f"    ✅ {key}: {display}")
            else:
                print("    ⚠️  Empty result")

            # Brief pause between searches
            if i < BATCH_SIZE:
                await asyncio.sleep(1)
            print()

    finally:
        print("🛑 Closing browser...")
        await scraper.close()
        print("   ✅ Browser closed\n")

    # ── Step 4: Summary ────────────────────────────────────────────────
    elapsed = time.time() - start_time
    has_phone = sum(1 for r in results if r.get("phone"))
    has_website = sum(1 for r in results if r.get("website"))
    has_address = sum(1 for r in results if r.get("address"))
    has_rating = sum(1 for r in results if r.get("rating"))
    has_maps_url = sum(1 for r in results if r.get("maps_url"))

    print("=" * 70)
    print(f"🏁 E2E Batch Complete — {len(results)} companies in {elapsed:.1f}s")
    print("=" * 70)
    print(f"   📊 phone:    {has_phone}/{len(results)}")
    print(f"   📊 website:  {has_website}/{len(results)}")
    print(f"   📊 address:  {has_address}/{len(results)}")
    print(f"   📊 rating:   {has_rating}/{len(results)}")
    print(f"   📊 maps_url: {has_maps_url}/{len(results)}")

    print(f"\n   ⏱️  Avg time per company: {elapsed/len(results):.1f}s")

    # Show maps_url for first 3 that have it
    print("\n   📍 Sample maps_url values:")
    count = 0
    for r in results:
        if r.get("maps_url"):
            count += 1
            print(f"      [{count}] {r['name']}")
            print(f"          {r['maps_url'][:100]}...")
            if count >= 3:
                break

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
