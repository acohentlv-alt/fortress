"""French departments reference data (101 departments, 2024 regions)."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Complete mapping: department code -> department name
# 01-19, 2A, 2B, 21-95, 971-976  (101 departments)
# ---------------------------------------------------------------------------

DEPARTMENTS: dict[str, str] = {
    "01": "Ain",
    "02": "Aisne",
    "03": "Allier",
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "07": "Ardeche",
    "08": "Ardennes",
    "09": "Ariege",
    "10": "Aube",
    "11": "Aude",
    "12": "Aveyron",
    "13": "Bouches-du-Rhone",
    "14": "Calvados",
    "15": "Cantal",
    "16": "Charente",
    "17": "Charente-Maritime",
    "18": "Cher",
    "19": "Correze",
    "2A": "Corse-du-Sud",
    "2B": "Haute-Corse",
    "21": "Cote-d'Or",
    "22": "Cotes-d'Armor",
    "23": "Creuse",
    "24": "Dordogne",
    "25": "Doubs",
    "26": "Drome",
    "27": "Eure",
    "28": "Eure-et-Loir",
    "29": "Finistere",
    "30": "Gard",
    "31": "Haute-Garonne",
    "32": "Gers",
    "33": "Gironde",
    "34": "Herault",
    "35": "Ille-et-Vilaine",
    "36": "Indre",
    "37": "Indre-et-Loire",
    "38": "Isere",
    "39": "Jura",
    "40": "Landes",
    "41": "Loir-et-Cher",
    "42": "Loire",
    "43": "Haute-Loire",
    "44": "Loire-Atlantique",
    "45": "Loiret",
    "46": "Lot",
    "47": "Lot-et-Garonne",
    "48": "Lozere",
    "49": "Maine-et-Loire",
    "50": "Manche",
    "51": "Marne",
    "52": "Haute-Marne",
    "53": "Mayenne",
    "54": "Meurthe-et-Moselle",
    "55": "Meuse",
    "56": "Morbihan",
    "57": "Moselle",
    "58": "Nievre",
    "59": "Nord",
    "60": "Oise",
    "61": "Orne",
    "62": "Pas-de-Calais",
    "63": "Puy-de-Dome",
    "64": "Pyrenees-Atlantiques",
    "65": "Hautes-Pyrenees",
    "66": "Pyrenees-Orientales",
    "67": "Bas-Rhin",
    "68": "Haut-Rhin",
    "69": "Rhone",
    "70": "Haute-Saone",
    "71": "Saone-et-Loire",
    "72": "Sarthe",
    "73": "Savoie",
    "74": "Haute-Savoie",
    "75": "Paris",
    "76": "Seine-Maritime",
    "77": "Seine-et-Marne",
    "78": "Yvelines",
    "79": "Deux-Sevres",
    "80": "Somme",
    "81": "Tarn",
    "82": "Tarn-et-Garonne",
    "83": "Var",
    "84": "Vaucluse",
    "85": "Vendee",
    "86": "Vienne",
    "87": "Haute-Vienne",
    "88": "Vosges",
    "89": "Yonne",
    "90": "Territoire de Belfort",
    "91": "Essonne",
    "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne",
    "95": "Val-d'Oise",
    # DOM-TOM
    "971": "Guadeloupe",
    "972": "Martinique",
    "973": "Guyane",
    "974": "La Reunion",
    "975": "Saint-Pierre-et-Miquelon",
    "976": "Mayotte",
}

# ---------------------------------------------------------------------------
# Department -> Region mapping (new regions as of 2016 reform, current 2024)
# ---------------------------------------------------------------------------

DEPARTMENT_REGIONS: dict[str, str] = {
    # Auvergne-Rhone-Alpes
    "01": "Auvergne-Rhone-Alpes",
    "03": "Auvergne-Rhone-Alpes",
    "07": "Auvergne-Rhone-Alpes",
    "15": "Auvergne-Rhone-Alpes",
    "26": "Auvergne-Rhone-Alpes",
    "38": "Auvergne-Rhone-Alpes",
    "42": "Auvergne-Rhone-Alpes",
    "43": "Auvergne-Rhone-Alpes",
    "63": "Auvergne-Rhone-Alpes",
    "69": "Auvergne-Rhone-Alpes",
    "73": "Auvergne-Rhone-Alpes",
    "74": "Auvergne-Rhone-Alpes",
    # Bourgogne-Franche-Comte
    "21": "Bourgogne-Franche-Comte",
    "25": "Bourgogne-Franche-Comte",
    "39": "Bourgogne-Franche-Comte",
    "58": "Bourgogne-Franche-Comte",
    "70": "Bourgogne-Franche-Comte",
    "71": "Bourgogne-Franche-Comte",
    "89": "Bourgogne-Franche-Comte",
    "90": "Bourgogne-Franche-Comte",
    # Bretagne
    "22": "Bretagne",
    "29": "Bretagne",
    "35": "Bretagne",
    "56": "Bretagne",
    # Centre-Val de Loire
    "18": "Centre-Val de Loire",
    "28": "Centre-Val de Loire",
    "36": "Centre-Val de Loire",
    "37": "Centre-Val de Loire",
    "41": "Centre-Val de Loire",
    "45": "Centre-Val de Loire",
    # Corse
    "2A": "Corse",
    "2B": "Corse",
    # Grand Est
    "08": "Grand Est",
    "10": "Grand Est",
    "51": "Grand Est",
    "52": "Grand Est",
    "54": "Grand Est",
    "55": "Grand Est",
    "57": "Grand Est",
    "67": "Grand Est",
    "68": "Grand Est",
    "88": "Grand Est",
    # Hauts-de-France
    "02": "Hauts-de-France",
    "59": "Hauts-de-France",
    "60": "Hauts-de-France",
    "62": "Hauts-de-France",
    "80": "Hauts-de-France",
    # Ile-de-France
    "75": "Ile-de-France",
    "77": "Ile-de-France",
    "78": "Ile-de-France",
    "91": "Ile-de-France",
    "92": "Ile-de-France",
    "93": "Ile-de-France",
    "94": "Ile-de-France",
    "95": "Ile-de-France",
    # Normandie
    "14": "Normandie",
    "27": "Normandie",
    "50": "Normandie",
    "61": "Normandie",
    "76": "Normandie",
    # Nouvelle-Aquitaine
    "16": "Nouvelle-Aquitaine",
    "17": "Nouvelle-Aquitaine",
    "19": "Nouvelle-Aquitaine",
    "23": "Nouvelle-Aquitaine",
    "24": "Nouvelle-Aquitaine",
    "33": "Nouvelle-Aquitaine",
    "40": "Nouvelle-Aquitaine",
    "47": "Nouvelle-Aquitaine",
    "64": "Nouvelle-Aquitaine",
    "79": "Nouvelle-Aquitaine",
    "86": "Nouvelle-Aquitaine",
    "87": "Nouvelle-Aquitaine",
    # Occitanie
    "09": "Occitanie",
    "11": "Occitanie",
    "12": "Occitanie",
    "30": "Occitanie",
    "31": "Occitanie",
    "32": "Occitanie",
    "34": "Occitanie",
    "46": "Occitanie",
    "48": "Occitanie",
    "65": "Occitanie",
    "66": "Occitanie",
    "81": "Occitanie",
    "82": "Occitanie",
    # Pays de la Loire
    "44": "Pays de la Loire",
    "49": "Pays de la Loire",
    "53": "Pays de la Loire",
    "72": "Pays de la Loire",
    "85": "Pays de la Loire",
    # Provence-Alpes-Cote d'Azur
    "04": "Provence-Alpes-Cote d'Azur",
    "05": "Provence-Alpes-Cote d'Azur",
    "06": "Provence-Alpes-Cote d'Azur",
    "13": "Provence-Alpes-Cote d'Azur",
    "83": "Provence-Alpes-Cote d'Azur",
    "84": "Provence-Alpes-Cote d'Azur",
    # DOM-TOM
    "971": "Guadeloupe",
    "972": "Martinique",
    "973": "Guyane",
    "974": "La Reunion",
    "975": "Saint-Pierre-et-Miquelon",
    "976": "Mayotte",
}

# ---------------------------------------------------------------------------
# Reverse lookup: lowercase name -> code  (built once at import time)
# ---------------------------------------------------------------------------

_NAME_TO_CODE: dict[str, str] = {
    name.lower(): code for code, name in DEPARTMENTS.items()
}


def _normalize_code(code: str) -> str | None:
    """Normalize a department code string (strip, uppercase, zero-pad)."""
    code = code.strip().upper()
    # Corsica special cases
    if code in ("2A", "2B"):
        return code
    # DOM-TOM 3-digit codes
    if code.isdigit() and len(code) == 3 and code in DEPARTMENTS:
        return code
    # Metropolitan 1- or 2-digit codes
    if code.isdigit():
        padded = code.zfill(2)
        if padded in DEPARTMENTS:
            return padded
    return None


def get_department_name(code: str) -> str | None:
    """Return the department name for a given code.

    Accepts codes with or without leading zeros (e.g. ``"1"`` or ``"01"``).
    Also accepts a department *name* (case-insensitive, fuzzy-matched via
    ``rapidfuzz`` when available, exact otherwise) and returns the canonical
    name if found.

    Returns ``None`` when no match is found.
    """
    # Try as a code first
    normalized = _normalize_code(code)
    if normalized is not None:
        return DEPARTMENTS[normalized]

    # Try as a name (exact case-insensitive)
    query = code.strip().lower()
    if query in _NAME_TO_CODE:
        return DEPARTMENTS[_NAME_TO_CODE[query]]

    # Fuzzy match via rapidfuzz — threshold 88% to avoid false positives
    # (e.g. "agriculture" scores 77% against "eure" at 75%, which is wrong)
    try:
        from rapidfuzz import fuzz, process  # type: ignore[import-untyped]

        match = process.extractOne(
            query,
            _NAME_TO_CODE.keys(),
            scorer=fuzz.WRatio,
            score_cutoff=88,
        )
        if match is not None:
            matched_name = match[0]
            return DEPARTMENTS[_NAME_TO_CODE[matched_name]]
    except ImportError:
        pass

    return None


def postal_code_to_dept(postal_code: str) -> str | None:
    """Convert a 5-digit French postal code to a department code.

    Handles metropolitan departments, Corsica (2A/2B), and DOM-TOM (971-976).

    Returns the department code string (e.g. "66", "2A", "971") or None if
    the input is not a valid 5-digit postal code.
    """
    code = postal_code.strip()
    if len(code) != 5 or not code.isdigit():
        return None

    # DOM-TOM: postal codes starting with 97X → department 97X
    if code.startswith("97"):
        prefix3 = code[:3]
        if prefix3 in DEPARTMENTS:
            return prefix3

    # Corsica: postal codes starting with 20
    # Approximate split: 20000-20190 → 2A (Corse-du-Sud), 20200+ → 2B (Haute-Corse)
    if code[:2] == "20":
        return "2A" if int(code) <= 20190 else "2B"

    # Metropolitan: first 2 digits → department code
    prefix2 = code[:2].zfill(2)
    if prefix2 in DEPARTMENTS:
        return prefix2

    return None


def get_department_code(name: str) -> str | None:
    """Return the department code for a given name.

    Performs a case-insensitive lookup.  When ``rapidfuzz`` is installed a
    fuzzy match (score >= 88%) is attempted as a fallback.

    The threshold is intentionally strict (88%) to prevent industry keywords
    like "agriculture" (77% against "eure") from being misclassified as
    department names.

    Returns ``None`` when no match is found.
    """
    query = name.strip().lower()

    # Exact case-insensitive match
    if query in _NAME_TO_CODE:
        return _NAME_TO_CODE[query]

    # Fuzzy match via rapidfuzz — threshold 88% (not 75%) to avoid false positives
    try:
        from rapidfuzz import fuzz, process  # type: ignore[import-untyped]

        match = process.extractOne(
            query,
            _NAME_TO_CODE.keys(),
            scorer=fuzz.WRatio,
            score_cutoff=88,
        )
        if match is not None:
            matched_name = match[0]
            return _NAME_TO_CODE[matched_name]
    except ImportError:
        pass

    return None
