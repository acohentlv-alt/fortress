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


# ---------------------------------------------------------------------------
# Major cities -> department code  (used as backend safety net for query parsing)
# ---------------------------------------------------------------------------

CITY_TO_DEPT: dict[str, str] = {
    "perpignan": "66", "montpellier": "34", "toulouse": "31", "marseille": "13",
    "lyon": "69", "nice": "06", "bordeaux": "33", "nantes": "44", "strasbourg": "67",
    "lille": "59", "rennes": "35", "grenoble": "38", "toulon": "83", "narbonne": "11",
    "carcassonne": "11", "beziers": "34", "nimes": "30", "avignon": "84", "cannes": "06",
    "antibes": "06", "pau": "64", "bayonne": "64", "biarritz": "64", "lourdes": "65",
    "tarbes": "65", "paris": "75", "rouen": "76", "le havre": "76", "caen": "14",
    "dijon": "21", "besancon": "25", "orleans": "45", "tours": "37", "limoges": "87",
    "clermont ferrand": "63", "saint etienne": "42", "annecy": "74", "chambery": "73",
    "valence": "26", "metz": "57", "nancy": "54", "reims": "51", "troyes": "10",
    "amiens": "80", "poitiers": "86", "la rochelle": "17", "angouleme": "16",
    "brest": "29", "quimper": "29", "lorient": "56", "vannes": "56", "saint malo": "35",
    "ajaccio": "2A", "bastia": "2B", "angers": "49", "le mans": "72",
}


# ---------------------------------------------------------------------------
# Notable cities per department — used by query expansion in discovery.py
# Curated list: prefectures, tourist towns, commercial centers.
# ---------------------------------------------------------------------------

DEPT_CITIES: dict[str, list[str]] = {
    "01": ["Bourg-en-Bresse", "Oyonnax", "Belley", "Ambérieu-en-Bugey"],
    "02": ["Laon", "Saint-Quentin", "Soissons", "Château-Thierry"],
    "03": ["Moulins", "Vichy", "Montluçon", "Gannat"],
    "04": ["Digne-les-Bains", "Manosque", "Sisteron", "Forcalquier"],
    "05": ["Gap", "Briançon", "Embrun", "Laragne-Montéglin"],
    "06": ["Nice", "Cannes", "Antibes", "Grasse", "Menton"],
    "07": ["Privas", "Aubenas", "Annonay", "Tournon-sur-Rhône"],
    "08": ["Charleville-Mézières", "Sedan", "Rethel", "Fumay"],
    "09": ["Foix", "Pamiers", "Saint-Girons", "Lavelanet"],
    "10": ["Troyes", "Romilly-sur-Seine", "Bar-sur-Aube", "Nogent-sur-Seine"],
    "11": ["Carcassonne", "Narbonne", "Limoux", "Castelnaudary"],
    "12": ["Rodez", "Millau", "Villefranche-de-Rouergue", "Decazeville"],
    "13": ["Marseille", "Aix-en-Provence", "Arles", "Martigues", "Salon-de-Provence"],
    "14": ["Caen", "Deauville", "Lisieux", "Bayeux", "Honfleur"],
    "15": ["Aurillac", "Saint-Flour", "Mauriac", "Maurs"],
    "16": ["Angoulême", "Cognac", "Ruffec", "Confolens"],
    "17": ["La Rochelle", "Rochefort", "Saintes", "Royan", "Saint-Jean-d'Angély"],
    "18": ["Bourges", "Vierzon", "Saint-Amand-Montrond", "Mehun-sur-Yèvre"],
    "19": ["Tulle", "Brive-la-Gaillarde", "Ussel", "Égletons"],
    "2A": ["Ajaccio", "Sartène", "Porto-Vecchio", "Propriano"],
    "2B": ["Bastia", "Corte", "Calvi", "Ghisonaccia"],
    "21": ["Dijon", "Beaune", "Chenôve", "Talant"],
    "22": ["Saint-Brieuc", "Lannion", "Dinan", "Guingamp"],
    "23": ["Guéret", "Aubusson", "La Souterraine", "Bourganeuf"],
    "24": ["Périgueux", "Bergerac", "Sarlat-la-Canéda", "Nontron"],
    "25": ["Besançon", "Pontarlier", "Montbéliard", "Morteau"],
    "26": ["Valence", "Montélimar", "Romans-sur-Isère", "Die"],
    "27": ["Évreux", "Vernon", "Louviers", "Bernay"],
    "28": ["Chartres", "Dreux", "Châteaudun", "Nogent-le-Rotrou"],
    "29": ["Brest", "Quimper", "Morlaix", "Concarneau", "Douarnenez"],
    "30": ["Nîmes", "Alès", "Beaucaire", "Bagnols-sur-Cèze", "Le Grau-du-Roi"],
    "31": ["Toulouse", "Colomiers", "Tournefeuille", "Muret", "Blagnac"],
    "32": ["Auch", "Condom", "Fleurance", "Lectoure"],
    "33": ["Bordeaux", "Mérignac", "Pessac", "Libourne", "Arcachon"],
    "34": ["Montpellier", "Béziers", "Sète", "Agde", "Lunel"],
    "35": ["Rennes", "Saint-Malo", "Fougères", "Vitré", "Dinard"],
    "36": ["Châteauroux", "Issoudun", "Le Blanc", "La Châtre"],
    "37": ["Tours", "Amboise", "Chinon", "Loches"],
    "38": ["Grenoble", "Vienne", "Bourgoin-Jallieu", "Voiron", "Échirolles"],
    "39": ["Lons-le-Saunier", "Dole", "Saint-Claude", "Poligny"],
    "40": ["Mont-de-Marsan", "Dax", "Biscarrosse", "Mimizan", "Capbreton"],
    "41": ["Blois", "Vendôme", "Romorantin-Lanthenay", "Salbris"],
    "42": ["Saint-Étienne", "Roanne", "Montbrison", "Firminy"],
    "43": ["Le Puy-en-Velay", "Brioude", "Yssingeaux", "Monistrol-sur-Loire"],
    "44": ["Nantes", "Saint-Nazaire", "Rezé", "Saint-Herblain", "Pornic"],
    "45": ["Orléans", "Montargis", "Pithiviers", "Gien"],
    "46": ["Cahors", "Figeac", "Gourdon", "Souillac"],
    "47": ["Agen", "Villeneuve-sur-Lot", "Marmande", "Tonneins"],
    "48": ["Mende", "Florac", "Marvejols", "Langogne"],
    "49": ["Angers", "Cholet", "Saumur", "Segré"],
    "50": ["Saint-Lô", "Cherbourg-en-Cotentin", "Granville", "Coutances"],
    "51": ["Châlons-en-Champagne", "Reims", "Épernay", "Vitry-le-François"],
    "52": ["Chaumont", "Saint-Dizier", "Langres", "Joinville"],
    "53": ["Laval", "Château-Gontier", "Mayenne", "Évron"],
    "54": ["Nancy", "Lunéville", "Toul", "Pont-à-Mousson"],
    "55": ["Bar-le-Duc", "Verdun", "Commercy", "Saint-Mihiel"],
    "56": ["Vannes", "Lorient", "Auray", "Pontivy", "Carnac"],
    "57": ["Metz", "Thionville", "Forbach", "Sarreguemines"],
    "58": ["Nevers", "Cosne-Cours-sur-Loire", "Clamecy", "Decize"],
    "59": ["Lille", "Roubaix", "Tourcoing", "Dunkerque", "Valenciennes"],
    "60": ["Beauvais", "Compiègne", "Senlis", "Creil"],
    "61": ["Alençon", "Flers", "Argentan", "L'Aigle"],
    "62": ["Calais", "Boulogne-sur-Mer", "Arras", "Lens", "Béthune"],
    "63": ["Clermont-Ferrand", "Issoire", "Riom", "Thiers"],
    "64": ["Pau", "Bayonne", "Biarritz", "Anglet", "Saint-Jean-de-Luz"],
    "65": ["Tarbes", "Lourdes", "Bagnères-de-Bigorre", "Argelès-Gazost"],
    "66": ["Perpignan", "Argelès-sur-Mer", "Canet-en-Roussillon", "Saint-Cyprien", "Collioure"],
    "67": ["Strasbourg", "Haguenau", "Schiltigheim", "Illkirch-Graffenstaden", "Sélestat"],
    "68": ["Mulhouse", "Colmar", "Saint-Louis", "Guebwiller", "Thann"],
    "69": ["Lyon", "Villeurbanne", "Vénissieux", "Vaulx-en-Velin", "Bron"],
    "70": ["Vesoul", "Lure", "Héricourt", "Gray"],
    "71": ["Mâcon", "Chalon-sur-Saône", "Le Creusot", "Autun"],
    "72": ["Le Mans", "La Flèche", "Sablé-sur-Sarthe", "Mamers"],
    "73": ["Chambéry", "Aix-les-Bains", "Albertville", "Saint-Jean-de-Maurienne"],
    "74": ["Annecy", "Thonon-les-Bains", "Annemasse", "Chamonix"],
    "75": ["Paris"],
    "76": ["Rouen", "Le Havre", "Dieppe", "Fécamp", "Étretat"],
    "77": ["Meaux", "Melun", "Chelles", "Fontainebleau", "Provins"],
    "78": ["Versailles", "Saint-Germain-en-Laye", "Mantes-la-Jolie", "Rambouillet"],
    "79": ["Niort", "Bressuire", "Parthenay", "Thouars"],
    "80": ["Amiens", "Abbeville", "Péronne", "Albert"],
    "81": ["Albi", "Castres", "Gaillac", "Mazamet"],
    "82": ["Montauban", "Castelsarrasin", "Moissac", "Caussade"],
    "83": ["Toulon", "Fréjus", "Saint-Raphaël", "Hyères", "Draguignan"],
    "84": ["Avignon", "Orange", "Carpentras", "Cavaillon", "Apt"],
    "85": ["La Roche-sur-Yon", "Les Sables-d'Olonne", "Challans", "Fontenay-le-Comte", "Saint-Gilles-Croix-de-Vie"],
    "86": ["Poitiers", "Châtellerault", "Loudun", "Montmorillon"],
    "87": ["Limoges", "Saint-Junien", "Bellac", "Rochechouart"],
    "88": ["Épinal", "Saint-Dié-des-Vosges", "Remiremont", "Gérardmer"],
    "89": ["Auxerre", "Sens", "Joigny", "Avallon"],
    "90": ["Belfort", "Delle", "Giromagny", "Beaucourt"],
    "91": ["Évry-Courcouronnes", "Corbeil-Essonnes", "Massy", "Palaiseau"],
    "92": ["Boulogne-Billancourt", "Nanterre", "Courbevoie", "Colombes", "Issy-les-Moulineaux"],
    "93": ["Saint-Denis", "Montreuil", "Bobigny", "Aubervilliers", "Pantin"],
    "94": ["Créteil", "Vitry-sur-Seine", "Champigny-sur-Marne", "Ivry-sur-Seine", "Vincennes"],
    "95": ["Cergy", "Argenteuil", "Sarcelles", "Enghien-les-Bains", "Pontoise"],
    "971": ["Pointe-à-Pitre", "Les Abymes", "Basse-Terre", "Sainte-Anne"],
    "972": ["Fort-de-France", "Le Lamentin", "Le Robert", "Sainte-Luce"],
    "973": ["Cayenne", "Kourou", "Saint-Laurent-du-Maroni", "Matoury"],
    "974": ["Saint-Denis", "Saint-Pierre", "Saint-Paul", "Le Tampon"],
    "975": ["Saint-Pierre"],
    "976": ["Mamoudzou", "Koungou", "Dzaoudzi", "Dembéni"],
}


# Adjacent departments — used by Smart Expansion to suggest nearby searches.
# Symmetric: if A lists B, then B lists A.  DOM-TOM get empty lists.
ADJACENT_DEPTS: dict[str, list[str]] = {
    "01": ["38", "39", "69", "71", "73", "74"],
    "02": ["08", "51", "59", "60", "77", "80"],
    "03": ["15", "18", "23", "42", "58", "63"],
    "04": ["05", "06", "26", "83", "84"],
    "05": ["04", "26", "38", "73"],
    "06": ["04", "83"],
    "07": ["26", "30", "38", "42", "43", "48", "69", "84"],
    "08": ["02", "51", "55"],
    "09": ["11", "31", "66"],
    "10": ["21", "51", "52", "77", "89"],
    "11": ["09", "31", "34", "66", "81"],
    "12": ["15", "30", "34", "46", "48", "81", "82"],
    "13": ["30", "83", "84"],
    "14": ["27", "50", "61", "76"],
    "15": ["03", "12", "19", "43", "46", "48", "63"],
    "16": ["17", "24", "79", "86", "87"],
    "17": ["16", "33", "79", "85"],
    "18": ["03", "23", "36", "41", "45", "58"],
    "19": ["15", "23", "24", "46", "63", "87"],
    "2A": ["2B"],
    "2B": ["2A"],
    "21": ["10", "39", "52", "58", "70", "71", "89"],
    "22": ["29", "35", "56"],
    "23": ["03", "18", "19", "36", "63", "87"],
    "24": ["16", "19", "33", "46", "47", "87"],
    "25": ["39", "70", "90"],
    "26": ["04", "05", "07", "38", "84"],
    "27": ["14", "28", "60", "61", "76", "78", "95"],
    "28": ["27", "41", "45", "61", "72", "78", "91"],
    "29": ["22", "56"],
    "30": ["07", "12", "13", "34", "48", "84"],
    "31": ["09", "11", "32", "65", "81", "82"],
    "32": ["31", "40", "47", "64", "65", "82"],
    "33": ["17", "24", "40", "47"],
    "34": ["11", "12", "30", "81"],
    "35": ["22", "44", "49", "50", "53", "56"],
    "36": ["18", "23", "37", "41", "86", "87"],
    "37": ["36", "41", "49", "72", "86"],
    "38": ["01", "05", "07", "26", "42", "69", "73"],
    "39": ["01", "21", "25", "70", "71"],
    "40": ["32", "33", "47", "64"],
    "41": ["18", "28", "36", "37", "45", "72"],
    "42": ["03", "07", "38", "43", "63", "69", "71"],
    "43": ["07", "15", "42", "48", "63"],
    "44": ["35", "49", "56", "85"],
    "45": ["18", "28", "41", "58", "77", "89", "91"],
    "46": ["12", "15", "19", "24", "47", "82"],
    "47": ["24", "32", "33", "40", "46", "82"],
    "48": ["07", "12", "15", "30", "43"],
    "49": ["35", "37", "44", "53", "72", "79", "85", "86"],
    "50": ["14", "35", "53", "61"],
    "51": ["02", "08", "10", "52", "55", "77"],
    "52": ["10", "21", "51", "55", "70", "88"],
    "53": ["35", "49", "50", "61", "72"],
    "54": ["55", "57", "67", "88"],
    "55": ["08", "51", "52", "54", "88"],
    "56": ["22", "29", "35", "44"],
    "57": ["54", "67"],
    "58": ["03", "18", "21", "45", "71", "89"],
    "59": ["02", "62", "80"],
    "60": ["02", "27", "76", "77", "80", "95"],
    "61": ["14", "27", "28", "50", "53", "72", "76"],
    "62": ["59", "80"],
    "63": ["03", "15", "19", "23", "42", "43"],
    "64": ["32", "40", "65"],
    "65": ["31", "32", "64"],
    "66": ["09", "11"],
    "67": ["54", "57", "68", "88"],
    "68": ["67", "88", "90"],
    "69": ["01", "07", "38", "42", "71"],
    "70": ["21", "25", "39", "52", "88", "90"],
    "71": ["01", "21", "39", "42", "58", "69"],
    "72": ["28", "37", "41", "49", "53", "61"],
    "73": ["01", "05", "38", "74"],
    "74": ["01", "73"],
    "75": ["92", "93", "94"],
    "76": ["14", "27", "60", "61", "80"],
    "77": ["02", "10", "45", "51", "60", "89", "91", "93", "94"],
    "78": ["27", "28", "91", "92", "95"],
    "79": ["16", "17", "49", "85", "86"],
    "80": ["02", "59", "60", "62", "76"],
    "81": ["11", "12", "31", "34", "82"],
    "82": ["12", "31", "32", "46", "47", "81"],
    "83": ["04", "06", "13", "84"],
    "84": ["04", "07", "13", "26", "30", "83"],
    "85": ["17", "44", "49", "79"],
    "86": ["16", "36", "37", "49", "79", "87"],
    "87": ["16", "19", "23", "24", "36", "86"],
    "88": ["52", "54", "55", "67", "68", "70"],
    "89": ["10", "21", "45", "58", "77"],
    "90": ["25", "68", "70"],
    "91": ["28", "45", "77", "78", "92", "94"],
    "92": ["75", "78", "91", "93", "94", "95"],
    "93": ["75", "77", "92", "94", "95"],
    "94": ["75", "77", "91", "92", "93"],
    "95": ["27", "60", "78", "92", "93"],
    # DOM-TOM: isolated, no adjacent departments
    "971": [], "972": [], "973": [], "974": [], "975": [], "976": [],
}

# Startup assertion: adjacency must be symmetric
for _dept, _neighbors in ADJACENT_DEPTS.items():
    for _n in _neighbors:
        assert _dept in ADJACENT_DEPTS.get(_n, []), (
            f"ADJACENT_DEPTS asymmetry: {_dept} lists {_n} but {_n} does not list {_dept}"
        )


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
