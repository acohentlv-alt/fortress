"""Run CASCADE FK migration on the Fortress database.

Usage:
    python run_migration.py "postgresql://neondb_owner:PASSWORD@ep-noisy-tree-agzjuw4w-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"
    
Or set DATABASE_URL environment variable and just run:
    DATABASE_URL="..." python run_migration.py
"""

import sys
import os
import psycopg


def main():
    db_url = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATABASE_URL")
    if not db_url:
        print("Usage: python run_migration.py <DATABASE_URL>")
        print("   or: DATABASE_URL=... python run_migration.py")
        sys.exit(1)

    print(f"Connecting to: ...{db_url[-40:]}")
    conn = psycopg.connect(db_url, autocommit=False)
    cur = conn.cursor()

    statements = [
        # Drop existing non-cascade siren FKs
        "ALTER TABLE batch_tags DROP CONSTRAINT IF EXISTS query_tags_siren_fkey",
        "ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_siren_fkey",
        "ALTER TABLE officers DROP CONSTRAINT IF EXISTS officers_siren_fkey",
        "ALTER TABLE company_notes DROP CONSTRAINT IF EXISTS company_notes_siren_fkey",
        # Recreate with CASCADE
        "ALTER TABLE batch_tags ADD CONSTRAINT batch_tags_siren_fkey FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE",
        "ALTER TABLE contacts ADD CONSTRAINT contacts_siren_fkey FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE",
        "ALTER TABLE officers ADD CONSTRAINT officers_siren_fkey FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE",
        "ALTER TABLE company_notes ADD CONSTRAINT company_notes_siren_fkey FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE",
        # Clean orphans and add NEW FKs
        "DELETE FROM batch_log bl WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.siren = bl.siren)",
        "DELETE FROM enrichment_log el WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.siren = el.siren)",
        "ALTER TABLE batch_log ADD CONSTRAINT batch_log_siren_fkey FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE",
        "ALTER TABLE enrichment_log ADD CONSTRAINT enrichment_log_siren_fkey FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE",
        # User FKs — SET NULL on delete
        "ALTER TABLE activity_log DROP CONSTRAINT IF EXISTS activity_log_user_id_fkey",
        "ALTER TABLE activity_log ADD CONSTRAINT activity_log_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL",
        "ALTER TABLE batch_data DROP CONSTRAINT IF EXISTS scrape_jobs_user_id_fkey",
        "ALTER TABLE batch_data ADD CONSTRAINT batch_data_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL",
        "ALTER TABLE company_notes DROP CONSTRAINT IF EXISTS company_notes_user_id_fkey",
        "ALTER TABLE company_notes ADD CONSTRAINT company_notes_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL",
    ]

    for i, sql in enumerate(statements):
        try:
            cur.execute(sql)
            print(f"  ✅ [{i+1:2d}/{len(statements)}] {sql[:80]}")
        except Exception as e:
            print(f"  ❌ [{i+1:2d}/{len(statements)}] ERROR: {e}")
            conn.rollback()
            raise

    conn.commit()
    print()
    print("🎉 Migration committed! Verifying constraints...")
    print()

    cur.execute("""
        SELECT conname, conrelid::regclass, confrelid::regclass,
               CASE confdeltype WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'a' THEN 'NO ACTION' END
        FROM pg_constraint
        WHERE contype = 'f' AND connamespace = 'public'::regnamespace
        ORDER BY conrelid::regclass::text, conname
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:42s} {str(row[1]):20s} → {str(row[2]):10s} {row[3]}")

    print()
    print("✅ All Foreign Keys verified. Migration complete.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
