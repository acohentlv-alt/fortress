"""Test script — verify Playwright Maps scraper launches and extracts data.

Feeds a known local business directly into the scraper and prints results.
No database needed — tests the browser engine + extraction logic only.
"""

import asyncio
import sys

# Suppress noisy structlog JSON during test
import structlog
structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)


async def main():
    print("=" * 70)
    print("🧪 Maps Scraper Integration Test")
    print("=" * 70)

    # ── Step 1: Import and instantiate ─────────────────────────────────
    print("\n📦 Importing PlaywrightMapsScraper...")
    try:
        from fortress.scraping.maps import PlaywrightMapsScraper
        print("   ✅ Import successful")
    except ImportError as e:
        print(f"   ❌ Import failed: {e}")
        sys.exit(1)

    scraper = PlaywrightMapsScraper()

    # ── Step 2: Start Playwright Chromium ──────────────────────────────
    print("\n🚀 Starting Playwright Chromium...")
    try:
        await scraper.start()
        print("   ✅ Chromium launched (no binary-missing errors)")
    except Exception as e:
        print(f"   ❌ Failed to start: {e}")
        sys.exit(1)

    # ── Step 3: Search for a known Paris bakery ───────────────────────
    # Using a well-known business that definitely exists on Google Maps.
    test_cases = [
        {
            "name": "Café de Flore Paris",
            "dept": "75",
            "siren": "TEST_WITH_SITE",
            "desc": "Famous café — HAS a website",
        },
        {
            "name": "Boulangerie Julien 33 Rue de Turin Paris",
            "dept": "75",
            "siren": "TEST_NO_SITE",
            "desc": "Local bakery — NO website on Maps",
        },
    ]

    try:
        for tc in test_cases:
            print(f"\n🔍 Searching: {tc['name']} ({tc['desc']})")
            print(f"   Department: {tc['dept']}, SIREN: {tc['siren']}")

            result = await scraper.search(
                tc["name"],
                tc["dept"],
                siren=tc["siren"],
            )

            print(f"\n   📊 Results:")
            if not result:
                print("   ⚠️  Empty result — captcha or not found")
            else:
                for key, value in result.items():
                    emoji = "✅" if value else "❌"
                    print(f"   {emoji} {key}: {value}")

            # Summary
            print(f"\n   📈 Extraction summary:")
            fields = ["phone", "website", "address", "rating", "review_count"]
            for f in fields:
                status = "✅ Found" if result.get(f) else "❌ Missing"
                print(f"      {f}: {status}")

    finally:
        # ── Step 4: Close browser ─────────────────────────────────────
        print("\n🛑 Closing browser...")
        await scraper.close()
        print("   ✅ Browser closed cleanly")

    print("\n" + "=" * 70)
    print("🏁 Test complete")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
