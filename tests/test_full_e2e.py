"""Phase 4 E2E — Full system verification with live PostgreSQL.

Tests:
  1. DB connection (psycopg3 async)
  2. Scraper → extract data
  3. Upsert into contacts table (address, maps_url columns)
  4. Read back from DB to verify persistence
  5. Master export to data/outputs/
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import structlog
structlog.configure(
    processors=[structlog.dev.ConsoleRenderer(colors=True)],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()


async def main():
    print("=" * 70)
    print("🔬 Phase 4 — Full System Verification (Live PostgreSQL)")
    print("=" * 70)

    # ── Step 1: Verify DB connection ──────────────────────────────────
    print("\n📡 Step 1: Database Connection")
    import psycopg

    from fortress.config.settings import settings
    db_url = settings.db_url
    print(f"   URL: {db_url.split('@')[-1]}")

    try:
        conn = await psycopg.AsyncConnection.connect(db_url, autocommit=True)
        result = await conn.execute("SELECT version()")
        row = await result.fetchone()
        print(f"   ✅ Connected: {row[0][:60]}...")

        # Verify tables exist
        result = await conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        tables = [r[0] for r in await result.fetchall()]
        print(f"   📊 Tables: {', '.join(tables)}")

        # Verify contacts columns include address, maps_url
        result = await conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'contacts' ORDER BY ordinal_position"
        )
        columns = [r[0] for r in await result.fetchall()]
        print(f"   📊 contacts columns: {', '.join(columns)}")
        assert "address" in columns, "❌ 'address' column missing!"
        assert "maps_url" in columns, "❌ 'maps_url' column missing!"
        print("   ✅ address + maps_url columns confirmed")

    except Exception as e:
        print(f"   ❌ DB connection failed: {e}")
        return 1

    # ── Step 2: Scrape test companies ─────────────────────────────────
    print("\n🌐 Step 2: Scrape Test Companies")
    from fortress.scraping.maps import PlaywrightMapsScraper

    scraper = PlaywrightMapsScraper()
    await scraper.start()
    print("   ✅ Chromium started")

    test_companies = [
        {"name": "Saint Charles Logistique Perpignan", "dept": "66", "siren": "501854970"},
        {"name": "Café de Flore Paris", "dept": "75", "siren": "582069390"},
    ]

    scraped = []
    for tc in test_companies:
        result = await scraper.search(tc["name"], tc["dept"], siren=tc["siren"])
        result["siren"] = tc["siren"]
        result["denomination"] = tc["name"]
        scraped.append(result)
        fields = sum(1 for k in ["phone", "website", "address", "rating", "maps_url"] if result.get(k))
        print(f"   📊 {tc['name']}: {fields}/5 fields")

    await scraper.close()
    print("   ✅ Chromium closed")

    # ── Step 3: Upsert into PostgreSQL ────────────────────────────────
    print("\n💾 Step 3: Database Upserts")
    from fortress.models import Contact, ContactSource

    # First, insert parent company rows (FK constraint)
    for data in scraped:
        await conn.execute(
            "INSERT INTO companies (siren, denomination) VALUES (%s, %s) "
            "ON CONFLICT (siren) DO NOTHING",
            (data["siren"], data["denomination"]),
        )
        print(f"   ✅ Company row ensured: {data['denomination']} ({data['siren']})")

    for data in scraped:
        rating_raw = data.get("rating")
        contact = Contact(
            siren=data["siren"],
            phone=data.get("phone"),
            website=data.get("website"),
            address=data.get("address"),
            source=ContactSource.GOOGLE_MAPS,
            rating=Decimal(str(rating_raw)) if rating_raw is not None else None,
            review_count=data.get("review_count"),
            maps_url=data.get("maps_url"),
            collected_at=datetime.now(tz=timezone.utc),
        )

        # Manual upsert (same SQL as deduplicator.py)
        try:
            await conn.execute(
                """
                INSERT INTO contacts (
                    siren, phone, email, email_type, website, address, source,
                    social_linkedin, social_facebook, social_twitter,
                    rating, review_count, maps_url, collected_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (siren, source) DO UPDATE SET
                    phone          = COALESCE(EXCLUDED.phone,          contacts.phone),
                    email          = COALESCE(EXCLUDED.email,          contacts.email),
                    email_type     = COALESCE(EXCLUDED.email_type,     contacts.email_type),
                    website        = COALESCE(EXCLUDED.website,        contacts.website),
                    address        = COALESCE(EXCLUDED.address,        contacts.address),
                    social_linkedin = COALESCE(EXCLUDED.social_linkedin, contacts.social_linkedin),
                    social_facebook = COALESCE(EXCLUDED.social_facebook, contacts.social_facebook),
                    social_twitter  = COALESCE(EXCLUDED.social_twitter,  contacts.social_twitter),
                    rating         = COALESCE(EXCLUDED.rating,         contacts.rating),
                    review_count   = COALESCE(EXCLUDED.review_count,   contacts.review_count),
                    maps_url       = COALESCE(EXCLUDED.maps_url,       contacts.maps_url),
                    collected_at   = EXCLUDED.collected_at
                """,
                (
                    contact.siren,
                    contact.phone,
                    contact.email,
                    contact.email_type.value if contact.email_type else None,
                    contact.website,
                    contact.address,
                    contact.source.value,
                    contact.social_linkedin,
                    contact.social_facebook,
                    contact.social_twitter,
                    contact.rating,
                    contact.review_count,
                    contact.maps_url,
                    contact.collected_at,
                ),
            )
            print(f"   ✅ UPSERTED {data['denomination']} (siren={data['siren']})")
        except Exception as e:
            print(f"   ❌ UPSERT FAILED for {data['denomination']}: {e}")
            return 1

    # ── Step 4: Read back from DB ─────────────────────────────────────
    print("\n🔍 Step 4: Verify Persistence (Read Back)")
    for data in scraped:
        result = await conn.execute(
            "SELECT siren, phone, website, address, rating, review_count, maps_url, source "
            "FROM contacts WHERE siren = %s AND source = 'google_maps'",
            (data["siren"],),
        )
        row = await result.fetchone()
        if row:
            print(f"   📊 {data['denomination']}:")
            print(f"      siren:        {row[0]}")
            print(f"      phone:        {row[1] or '—'}")
            print(f"      website:      {(row[2] or '—')[:50]}")
            print(f"      address:      {row[3] or '—'}")
            print(f"      rating:       {row[4] or '—'}")
            print(f"      review_count: {row[5] or '—'}")
            print(f"      maps_url:     {(row[6] or '—')[:70]}")
            print(f"      source:       {row[7]}")
        else:
            print(f"   ❌ No row found for siren={data['siren']}")

    # ── Step 5: Master export ─────────────────────────────────────────
    print("\n📦 Step 5: Master Export")
    from fortress.export import master_file as mf

    cards = []
    for data in scraped:
        cards.append({
            "siren": data["siren"],
            "denomination": data["denomination"],
            "phone": data.get("phone"),
            "website": data.get("website"),
            "address": data.get("address"),
            "rating": data.get("rating"),
            "review_count": data.get("review_count"),
            "maps_url": data.get("maps_url"),
            "source": "google_maps",
            "verified_at": datetime.now(tz=timezone.utc).isoformat(),
        })
    mf.append_records(cards)

    master_path = Path("data/outputs/fortress_master.jsonl")
    print(f"   📁 {master_path} exists={master_path.exists()}")
    if master_path.exists():
        size = master_path.stat().st_size
        # Count lines
        with master_path.open() as f:
            lines = sum(1 for _ in f)
        print(f"   📊 Size: {size:,} bytes, {lines} records")

        # Show last 2 records (our new ones)
        with master_path.open() as f:
            all_lines = f.readlines()
        for line in all_lines[-2:]:
            rec = json.loads(line)
            print(f"   ✅ {rec.get('denomination', '?')} | maps_url={bool(rec.get('maps_url'))}")

    await conn.close()

    print("\n" + "=" * 70)
    print("🏁 Phase 4 — Full System Verification COMPLETE")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
