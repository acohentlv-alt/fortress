#!/usr/bin/env python3
"""
cleanup_orphan_maps.py — Supprime les entités MAPS orphelines.

Une entité MAPS est orpheline si elle n'est liée à aucun batch_tag ni batch_log.
Par défaut : mode dry-run (aucune suppression). Passer --no-dry-run pour supprimer.

Usage:
    python scripts/cleanup_orphan_maps.py
    python scripts/cleanup_orphan_maps.py --workspace-id 174
    python scripts/cleanup_orphan_maps.py --no-dry-run
    python scripts/cleanup_orphan_maps.py --workspace-id 174 --no-dry-run
"""

import argparse
import os
import sys

import psycopg
from dotenv import load_dotenv

# Load .env from project root (two levels up from this script)
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_script_dir)
load_dotenv(os.path.join(_project_root, ".env"))

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERREUR : DATABASE_URL introuvable dans l'environnement ou le fichier .env")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Nettoie les entités MAPS orphelines (sans batch_tag ni batch_log)."
    )
    parser.add_argument(
        "--workspace-id",
        type=int,
        default=None,
        help="Restreindre au workspace indiqué (optionnel, défaut : tous les workspaces).",
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        default=False,
        help="Effectuer la suppression réelle (défaut : dry-run).",
    )
    return parser.parse_args()


def find_orphans(conn, workspace_id):
    """Retourne la liste des entités MAPS orphelines."""
    base_query = """
        SELECT c.siren, c.denomination, c.workspace_id
        FROM companies c
        WHERE c.siren LIKE 'MAPS%%'
          AND NOT EXISTS (
              SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren
          )
          AND NOT EXISTS (
              SELECT 1 FROM batch_log bl WHERE bl.siren = c.siren
          )
    """
    if workspace_id is not None:
        query = base_query + " AND c.workspace_id = %s ORDER BY c.siren"
        cur = conn.execute(query, (workspace_id,))
    else:
        query = base_query + " ORDER BY c.siren"
        cur = conn.execute(query)

    return cur.fetchall()


def delete_orphan(conn, siren):
    """Supprime une entité MAPS et toutes ses données liées, dans l'ordre FK."""
    conn.execute("DELETE FROM officers WHERE siren = %s", (siren,))
    conn.execute("DELETE FROM contacts WHERE siren = %s", (siren,))
    conn.execute("DELETE FROM company_notes WHERE siren = %s", (siren,))
    conn.execute("DELETE FROM enrichment_log WHERE siren = %s", (siren,))
    # Safety: batch_tags and batch_log should already be empty (that's why it's orphan),
    # but clean up anyway to avoid FK violations.
    conn.execute("DELETE FROM batch_tags WHERE siren = %s", (siren,))
    conn.execute("DELETE FROM batch_log WHERE siren = %s", (siren,))
    conn.execute("DELETE FROM companies WHERE siren = %s", (siren,))


def main():
    args = parse_args()
    dry_run = not args.no_dry_run
    workspace_id = args.workspace_id

    mode_label = "DRY-RUN (aucune suppression)" if dry_run else "SUPPRESSION REELLE"
    print(f"\n=== Nettoyage des entités MAPS orphelines — {mode_label} ===")
    if workspace_id is not None:
        print(f"  Workspace ciblé : {workspace_id}")
    else:
        print("  Workspace ciblé : tous")
    print()

    with psycopg.connect(DATABASE_URL) as conn:
        orphans = find_orphans(conn, workspace_id)

        if not orphans:
            print("Aucune entité MAPS orpheline trouvée.")
            return

        print(f"{len(orphans)} entité(s) orpheline(s) trouvée(s) :\n")
        print(f"  {'SIREN':<15} {'DENOMINATION':<50} {'WORKSPACE'}")
        print("  " + "-" * 75)
        for siren, denomination, ws_id in orphans:
            denom_display = (denomination or "(sans nom)")[:48]
            ws_display = str(ws_id) if ws_id is not None else "admin (NULL)"
            print(f"  {siren:<15} {denom_display:<50} {ws_display}")

        print()

        if dry_run:
            print("Mode dry-run : aucune suppression effectuée.")
            print("Relancez avec --no-dry-run pour supprimer ces entités.")
        else:
            print(f"Suppression de {len(orphans)} entité(s)...")
            deleted = 0
            for siren, denomination, ws_id in orphans:
                try:
                    delete_orphan(conn, siren)
                    deleted += 1
                    print(f"  Supprimé : {siren} — {denomination or '(sans nom)'}")
                except Exception as exc:
                    print(f"  ERREUR lors de la suppression de {siren} : {exc}")
                    conn.rollback()
                    continue
                conn.commit()

            print(f"\n{deleted}/{len(orphans)} entité(s) supprimée(s).")


if __name__ == "__main__":
    main()
