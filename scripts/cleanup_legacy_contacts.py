#!/usr/bin/env python3
"""
cleanup_legacy_contacts.py — Nettoie les contacts hérités du pipeline legacy.

Partie 1 : Supprime les contacts avec source='google_maps' et un vrai SIREN
           (SIREN NOT LIKE 'MAPS%'). Ces ~100 lignes datent de l'ancien pipeline
           et polluent le tri GREEN et la détection de doublons.

Partie 2 : Délie les entités MAPS dont le lien téléphonique est un faux positif —
           quand le nom MAPS et le nom SIRENE ne se ressemblent pas assez.

Par défaut : mode dry-run (aucune modification). Passer --no-dry-run pour agir.

Usage:
    python scripts/cleanup_legacy_contacts.py
    python scripts/cleanup_legacy_contacts.py --no-dry-run
"""

import argparse
import os
import re
import sys
import unicodedata

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


# ---------------------------------------------------------------------------
# Name matching logic — copied from fortress/discovery.py (self-contained)
# ---------------------------------------------------------------------------

_LEGAL_SUFFIXES = {
    "sarl", "sas", "sasu", "eurl", "sa", "sci", "scp", "snc",
    "earl", "gaec", "gie", "sem", "selarl", "selas", "scm", "sccv", "eirl",
}

_ARTICLES = {"le", "la", "les", "du", "de", "des", "l", "d", "au", "aux", "en", "et"}

_INDUSTRY_WORDS = {
    "transport", "transports", "logistique", "logistiq", "camping",
    "hotel", "hotels", "restaurant", "boulangerie", "pharmacie",
    "garage", "plomberie", "electricite", "menuiserie", "pressing",
    "coiffure", "beaute", "auto", "taxi", "ambulance", "demenagement",
    "nettoyage", "securite", "formation", "conseil", "immobilier",
    "assurance", "agence", "bureau", "services", "solutions",
    "clinique", "laboratoire", "cabinet", "boutique", "atelier",
    "studio", "institut", "societe", "entreprise", "groupe",
    "espace", "comptabilite", "expertise", "renovation",
    "construction", "batiment", "travaux", "distribution",
    "location", "maintenance", "depannage", "livraison",
    "commerce", "import", "export", "editions", "production",
    "communication", "informatique", "digital", "consulting",
    "ingenierie", "technique", "technologies", "systemes",
    "medical", "dentaire", "optique", "veterinaire",
    "village", "domaine", "chateau", "parc", "residence",
    "vacances", "loisirs", "tourisme", "club",
    "ferme", "auberge", "gite", "relais",
    "hotellerie", "restauration",
}


def _normalize_name(name: str) -> list[str]:
    """Lowercase, strip accents, remove legal suffixes, split on spaces/apostrophes/hyphens."""
    nfkd = unicodedata.normalize("NFD", name.lower())
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Split on apostrophes and hyphens before cleaning punctuation
    ascii_name = re.sub(r"['\u2019\-]", " ", ascii_name)
    cleaned = re.sub(r"[^a-z0-9\s]", "", ascii_name)
    tokens = [t for t in cleaned.split() if t and t not in _LEGAL_SUFFIXES]
    return tokens


def _name_match_score(name_a: str, name_b: str) -> float:
    """Compute similarity between two names (0.0 to 1.0)."""
    if not name_a or not name_b:
        return 0.0
    ta = _normalize_name(name_a)
    tb = _normalize_name(name_b)
    if not ta or not tb:
        return 0.0
    ja = " ".join(ta)
    jb = " ".join(tb)
    if ja in jb or jb in ja:
        return 1.0
    overlap = sum(1 for t in ta if t in tb)
    return overlap / max(len(ta), len(tb))


def _is_industry_generic(name: str) -> bool:
    """Check if all significant tokens (ignoring articles) are generic industry words."""
    tokens = _normalize_name(name)
    significant = [t for t in tokens if t not in _ARTICLES]
    if not significant:
        return False
    return all(t in _INDUSTRY_WORDS for t in significant)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Nettoie les contacts legacy (source=google_maps + vrai SIREN) "
            "et délie les faux liens téléphoniques sur les entités MAPS."
        )
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        default=False,
        help="Effectuer les modifications réelles (défaut : dry-run).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Part 1 — Legacy contact rows
# ---------------------------------------------------------------------------

def find_legacy_contacts(conn):
    """Retourne les contacts hérités du pipeline legacy."""
    cur = conn.execute(
        """
        SELECT siren, phone, email, website, collected_at
        FROM contacts
        WHERE source = 'google_maps'
          AND siren NOT LIKE 'MAPS%%'
        ORDER BY siren
        """
    )
    return cur.fetchall()


def delete_legacy_contacts(conn):
    """Supprime les contacts hérités du pipeline legacy. Retourne le nombre supprimé."""
    cur = conn.execute(
        """
        DELETE FROM contacts
        WHERE source = 'google_maps'
          AND siren NOT LIKE 'MAPS%%'
        """
    )
    return cur.rowcount


# ---------------------------------------------------------------------------
# Part 2 — False phone matches
# ---------------------------------------------------------------------------

def find_phone_linked_maps(conn):
    """Retourne les entités MAPS liées par téléphone avec un SIREN."""
    cur = conn.execute(
        """
        SELECT siren, denomination, linked_siren, link_method
        FROM companies
        WHERE siren LIKE 'MAPS%%'
          AND link_method = 'phone'
          AND linked_siren IS NOT NULL
        ORDER BY siren
        """
    )
    return cur.fetchall()


def fetch_sirene_denomination(conn, siren):
    """Retourne (denomination, enseigne) du SIREN SIRENE correspondant."""
    cur = conn.execute(
        "SELECT denomination, enseigne FROM companies WHERE siren = %s",
        (siren,),
    )
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def unlink_maps_entity(conn, siren):
    """Efface le lien SIREN d'une entité MAPS."""
    conn.execute(
        """
        UPDATE companies
        SET linked_siren = NULL,
            link_confidence = NULL,
            link_method = NULL
        WHERE siren = %s
        """,
        (siren,),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    dry_run = not args.no_dry_run

    mode_label = "DRY-RUN (aucune modification)" if dry_run else "MODIFICATION REELLE"
    print(f"\n=== Nettoyage des contacts legacy et faux liens téléphoniques — {mode_label} ===\n")

    legacy_rows = []
    false_matches = []

    with psycopg.connect(DATABASE_URL) as conn:

        # ------------------------------------------------------------------
        # Partie 1 — Contacts legacy
        # ------------------------------------------------------------------
        print("--- Partie 1 : Contacts legacy (source=google_maps, vrai SIREN) ---\n")

        legacy_rows = find_legacy_contacts(conn)

        if not legacy_rows:
            print("  Aucun contact legacy trouvé.\n")
        else:
            print(f"  {len(legacy_rows)} contact(s) legacy trouvé(s) :\n")
            print(f"  {'SIREN':<12} {'PHONE':<18} {'EMAIL':<35} {'WEBSITE':<35} {'COLLECTED_AT'}")
            print("  " + "-" * 115)
            for siren, phone, email, website, collected_at in legacy_rows:
                phone_d = (phone or "")[:16]
                email_d = (email or "")[:33]
                website_d = (website or "")[:33]
                collected_d = str(collected_at)[:19] if collected_at else ""
                print(f"  {siren:<12} {phone_d:<18} {email_d:<35} {website_d:<35} {collected_d}")
            print()

            if dry_run:
                print(f"  Mode dry-run : {len(legacy_rows)} contact(s) seraient supprimés.")
            else:
                count = delete_legacy_contacts(conn)
                conn.commit()
                print(f"  {count} contact(s) supprimé(s).")

        print()

        # ------------------------------------------------------------------
        # Partie 2 — Faux liens téléphoniques
        # ------------------------------------------------------------------
        print("--- Partie 2 : Faux liens téléphoniques sur entités MAPS ---\n")

        phone_linked = find_phone_linked_maps(conn)

        if not phone_linked:
            print("  Aucune entité MAPS liée par téléphone trouvée.\n")
        else:
            print(f"  {len(phone_linked)} entité(s) MAPS liée(s) par téléphone — vérification des noms...\n")

            for maps_siren, maps_denom, linked_siren, link_method in phone_linked:
                sirene_denom, sirene_enseigne = fetch_sirene_denomination(conn, linked_siren)

                # Use enseigne if available, fall back to denomination for scoring
                sirene_name_for_score = sirene_enseigne or sirene_denom or ""
                maps_name_for_score = maps_denom or ""

                score = _name_match_score(maps_name_for_score, sirene_name_for_score)

                # Determine threshold
                maps_generic = _is_industry_generic(maps_name_for_score)
                sirene_generic = _is_industry_generic(sirene_name_for_score)
                threshold = 0.80 if (maps_generic or sirene_generic) else 0.30

                if score < threshold:
                    false_matches.append((
                        maps_siren, maps_denom, linked_siren,
                        sirene_denom, sirene_enseigne, score, threshold,
                    ))

            if not false_matches:
                print("  Aucun faux lien détecté.\n")
            else:
                print(f"  {len(false_matches)} faux lien(s) détecté(s) :\n")
                header = (
                    f"  {'MAPS SIREN':<12} {'MAPS NOM':<35} {'SIREN':<12} "
                    f"{'SIRENE NOM':<35} {'ENSEIGNE':<25} {'SCORE':>6} {'SEUIL':>6}"
                )
                print(header)
                print("  " + "-" * 135)
                for (maps_siren, maps_denom, linked_siren,
                     sirene_denom, sirene_enseigne, score, threshold) in false_matches:
                    maps_d = (maps_denom or "")[:33]
                    sirene_d = (sirene_denom or "")[:33]
                    enseigne_d = (sirene_enseigne or "")[:23]
                    print(
                        f"  {maps_siren:<12} {maps_d:<35} {linked_siren:<12} "
                        f"{sirene_d:<35} {enseigne_d:<25} {score:>6.2f} {threshold:>6.2f}"
                    )
                print()

                if dry_run:
                    print(f"  Mode dry-run : {len(false_matches)} lien(s) seraient effacés.")
                else:
                    unlinked = 0
                    for (maps_siren, *_rest) in false_matches:
                        try:
                            unlink_maps_entity(conn, maps_siren)
                            unlinked += 1
                        except Exception as exc:
                            print(f"  ERREUR pour {maps_siren} : {exc}")
                            conn.rollback()
                            continue
                    conn.commit()
                    print(f"  {unlinked}/{len(false_matches)} lien(s) effacé(s).")

        print()

        # ------------------------------------------------------------------
        # Résumé final
        # ------------------------------------------------------------------
        print("=== Résumé ===")
        if dry_run:
            legacy_count = len(legacy_rows) if legacy_rows else 0
            false_count = len(false_matches)
            print(f"  Contacts legacy à supprimer : {legacy_count}")
            print(f"  Faux liens à effacer        : {false_count}")
            print()
            print("  Relancez avec --no-dry-run pour appliquer ces modifications.")
        else:
            print("  Modifications appliquées. Relancez en dry-run pour vérifier.")
        print()


if __name__ == "__main__":
    main()
