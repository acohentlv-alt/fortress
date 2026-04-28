"""One-off backfill: reset MAPS entities mis-linked to _HOSTING_SIRENS.

Resets the link state AND the SIRENE-copied fields on all MAPS rows where
linked_siren in _HOSTING_SIRENS. After backfill, these rows are pure 'maps_only'
and become candidates for the eventual A1.2 retrofit sweep.

Affected rows verified Apr 28: ws174=23, ws1=9, ws417=9, wsNULL=3 (40 unique).
Plus 6 MS VACANCES (384598421) mis-links (added to blacklist this brief).
Total ~46 rows.

Usage:
  python3 -m scripts.backfill_hosting_relinks --dry-run
  python3 -m scripts.backfill_hosting_relinks
  python3 -m scripts.backfill_hosting_relinks --workspace 174
"""
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from psycopg import AsyncConnection

# Bootstrap (matches scripts/cleanup_orphan_maps.py:1-30 pattern)
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
from dotenv import load_dotenv
load_dotenv(_project_root / ".env")
from fortress.matching.contacts import _HOSTING_SIRENS


async def main(dry_run: bool, workspace: int | None) -> None:
    db_url = os.environ["DATABASE_URL"]
    async with await AsyncConnection.connect(db_url, autocommit=False) as conn:
        ws_clause = "AND workspace_id = %s" if workspace else ""
        ws_args = (
            (list(_HOSTING_SIRENS), workspace)
            if workspace else (list(_HOSTING_SIRENS),)
        )
        cur = await conn.execute(f"""
            SELECT siren, denomination, code_postal, linked_siren, link_method,
                   link_confidence, naf_status, workspace_id
              FROM companies
             WHERE siren LIKE 'MAPS%%'
               AND linked_siren = ANY(%s) {ws_clause}
        """, ws_args)
        affected = await cur.fetchall()
        print(f"Found {len(affected)} affected rows")

        if dry_run:
            for r in affected:
                print(f"  WOULD reset: {r[0]} (linked={r[3]} via {r[4]}/{r[5]}) ws={r[7]}")
            return

        for row in affected:
            maps_siren = row[0]
            await conn.execute("""
                UPDATE companies
                   SET linked_siren = NULL,
                       link_confidence = NULL,
                       link_method = NULL,
                       link_signals = NULL,
                       naf_status = NULL,
                       siret_siege = NULL,
                       naf_code = NULL,
                       naf_libelle = NULL,
                       forme_juridique = NULL,
                       date_creation = NULL,
                       tranche_effectif = NULL
                 WHERE siren = %s AND siren LIKE 'MAPS%%'
            """, (maps_siren,))
            await conn.execute("""
                INSERT INTO batch_log (batch_id, siren, action, result, detail,
                                       workspace_id, timestamp)
                VALUES ('BACKFILL_HOSTING', %s, 'backfill_hosting_unlink', 'success',
                        %s, %s, NOW())
            """, (
                maps_siren,
                json.dumps({
                    "original_linked_siren": row[3],
                    "original_method": row[4],
                    "original_confidence": row[5],
                }, ensure_ascii=False),
                row[7],
            ))
        await conn.commit()
        print(f"Reset {len(affected)} rows. Audit log written with batch_id='BACKFILL_HOSTING'.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--workspace", type=int, default=None)
    args = p.parse_args()
    asyncio.run(main(args.dry_run, args.workspace))
