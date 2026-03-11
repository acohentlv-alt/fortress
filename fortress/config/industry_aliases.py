"""Human-readable industry names → NAF code patterns.

Maps common French industry terms to NAF code prefixes.
Supports wildcard matching: "01" matches all codes starting with "01" (01.11Z, 01.12Z, etc.)
"""

from rapidfuzz import fuzz, process

# Industry name → list of NAF code prefixes
# A prefix like "01" matches all codes starting with "01."
# A specific code like "10.71C" matches exactly that code.
INDUSTRY_ALIASES: dict[str, list[str]] = {
    # ── Agriculture & Alimentaire ──
    "agriculture": ["01"],
    "culture": ["01.1"],
    "elevage": ["01.4"],
    "viticulture": ["01.21Z"],
    "vigne": ["01.21Z"],
    "vin": ["01.21Z", "11.02"],
    "cave a vin": ["01.21Z", "11.02", "47.25Z"],
    "sylviculture": ["02"],
    "foret": ["02"],
    "peche": ["03"],
    "aquaculture": ["03.2"],
    "boulangerie": ["10.71A", "10.71B", "10.71C", "10.71D"],
    "patisserie": ["10.71D", "10.72Z"],
    "boucherie": ["10.11Z", "10.12Z", "10.13A", "10.13B", "47.22Z"],
    "charcuterie": ["10.13B"],
    "poissonnerie": ["10.20Z", "47.23Z"],
    "fromagerie": ["10.51C"],
    "laiterie": ["10.51"],
    "chocolaterie": ["10.82Z"],
    "confiserie": ["10.82Z"],
    "agroalimentaire": ["10", "11"],
    "boissons": ["11"],
    "brasserie": ["11.05Z", "56.10A"],
    "biere": ["11.05Z"],
    # ── Construction & BTP ──
    "construction": ["41", "42", "43"],
    "batiment": ["41", "43"],
    "btp": ["41", "42", "43"],
    "travaux publics": ["42"],
    "gros oeuvre": ["43.99C"],
    "maconnerie": ["43.99C"],
    "electricite": ["43.21"],
    "electricien": ["43.21A"],
    "plomberie": ["43.22A"],
    "plombier": ["43.22A"],
    "chauffage": ["43.22B"],
    "climatisation": ["43.22B"],
    "peinture batiment": ["43.34Z"],
    "menuiserie": ["43.32A", "43.32B"],
    "couverture": ["43.91B"],
    "charpente": ["43.91A"],
    "carrelage": ["43.33Z"],
    "isolation": ["43.29A"],
    "demolition": ["43.11Z"],
    "terrassement": ["43.12"],
    "promotion immobiliere": ["41.10"],
    # ── Commerce ──
    "commerce": ["45", "46", "47"],
    "commerce de gros": ["46"],
    "commerce de detail": ["47"],
    "supermarche": ["47.11D"],
    "hypermarche": ["47.11F"],
    "epicerie": ["47.11B"],
    "automobile": ["45"],
    "garage": ["45.20"],
    "concessionnaire": ["45.11Z"],
    "fleuriste": ["47.76Z"],
    "librairie": ["47.61Z"],
    "pharmacie": ["47.73Z"],
    "optique": ["47.78C"],
    "habillement": ["47.71Z"],
    "e-commerce": ["47.91"],
    "vente en ligne": ["47.91"],
    # ── Transport & Logistique ──
    "transport": ["49", "50", "51", "52", "53"],
    "transport routier": ["49.41"],
    "taxi": ["49.32Z"],
    "vtc": ["49.32Z"],
    "demenagement": ["49.42Z"],
    "logistique": ["52"],
    "entreposage": ["52.10"],
    "messagerie": ["52.29A"],
    "fret": ["49.41", "50.20Z", "52.29B"],
    "livraison": ["53.20Z"],
    "transport maritime": ["50"],
    "transport aerien": ["51"],
    # ── Hebergement & Restauration ──
    "hotel": ["55.10Z"],
    "hotellerie": ["55"],
    "hebergement": ["55"],
    "camping": ["55.30Z"],
    "restaurant": ["56.10A", "56.10B", "56.10C"],
    "restauration": ["56"],
    "restauration rapide": ["56.10C"],
    "fast food": ["56.10C"],
    "traiteur": ["56.21Z"],
    "bar": ["56.30Z"],
    "cafe": ["56.30Z"],
    "restauration collective": ["56.29A"],
    # ── IT & Numerique ──
    "informatique": ["62", "63"],
    "logiciel": ["62.01Z", "58.29"],
    "software": ["62.01Z", "58.29"],
    "programmation": ["62.01Z"],
    "developpement": ["62.01Z"],
    "conseil informatique": ["62.02A"],
    "ssii": ["62.02A", "62.01Z"],
    "esn": ["62.02A", "62.01Z"],
    "hebergement web": ["63.11Z"],
    "data center": ["63.11Z"],
    "telecoms": ["61"],
    "telecommunication": ["61"],
    "edition logiciel": ["58.29"],
    "jeux video": ["58.21Z"],
    # ── Finance & Assurance ──
    "banque": ["64.19Z"],
    "finance": ["64", "66"],
    "assurance": ["65"],
    "courtage": ["66.12Z", "66.22Z"],
    "gestion de fonds": ["66.30Z"],
    "holding": ["64.20Z"],
    "credit": ["64.92Z"],
    # ── Immobilier ──
    "immobilier": ["68"],
    "agence immobiliere": ["68.31Z"],
    "location immobiliere": ["68.20"],
    "gestion immobiliere": ["68.32A"],
    "marchand de biens": ["68.10Z"],
    # ── Services aux entreprises ──
    "conseil": ["70.22Z"],
    "consulting": ["70.22Z"],
    "cabinet conseil": ["70.22Z"],
    "communication": ["70.21Z"],
    "relations publiques": ["70.21Z"],
    "publicite": ["73.11Z", "73.12Z"],
    "marketing": ["73.11Z", "73.20Z"],
    "etude de marche": ["73.20Z"],
    "design": ["74.10Z"],
    "graphisme": ["74.10Z"],
    "photographie": ["74.20Z"],
    "traduction": ["74.30Z"],
    "comptabilite": ["69.20Z"],
    "expert comptable": ["69.20Z"],
    "avocat": ["69.10Z"],
    "juridique": ["69.10Z"],
    "notaire": ["69.10Z"],
    "architecture": ["71.11Z"],
    "architecte": ["71.11Z"],
    "ingenierie": ["71.12B"],
    "bureau d'etudes": ["71.12B"],
    "geometre": ["71.12A"],
    "controle technique": ["71.20A", "71.20B"],
    "recherche": ["72"],
    "interim": ["78.20Z"],
    "travail temporaire": ["78.20Z"],
    "recrutement": ["78.10Z"],
    "securite": ["80.10Z"],
    "nettoyage": ["81.21Z", "81.22Z", "81.29"],
    "paysagiste": ["81.30Z"],
    "evenementiel": ["82.30Z"],
    "centre d'appels": ["82.20Z"],
    "recouvrement": ["82.91Z"],
    # ── Enseignement & Formation ──
    "enseignement": ["85"],
    "formation": ["85.59A"],
    "auto ecole": ["85.53Z"],
    "ecole": ["85"],
    "universite": ["85.42Z"],
    # ── Sante ──
    "sante": ["86"],
    "hopital": ["86.10Z"],
    "medecin": ["86.21Z", "86.22"],
    "dentiste": ["86.23Z"],
    "infirmier": ["86.90D"],
    "kine": ["86.90E"],
    "kinesitherapeute": ["86.90E"],
    "laboratoire": ["86.90B"],
    "ambulance": ["86.90A"],
    "opthalmo": ["86.22C"],
    "ophtalmologie": ["86.22C"],
    "veterinaire": ["75.00Z"],
    "ehpad": ["87.10A", "87.30A"],
    "aide a domicile": ["88.10A"],
    "creche": ["88.91A"],
    # ── Arts, Loisirs, Sport ──
    "spectacle": ["90"],
    "cinema": ["59.14Z"],
    "musee": ["91.02Z"],
    "sport": ["93.1"],
    "salle de sport": ["93.13Z"],
    "club sportif": ["93.12Z"],
    "loisirs": ["93.2"],
    "parc d'attractions": ["93.21Z"],
    # ── Services aux particuliers ──
    "coiffure": ["96.02A"],
    "coiffeur": ["96.02A"],
    "beaute": ["96.02B"],
    "esthetique": ["96.02B"],
    "pressing": ["96.01B"],
    "pompes funebres": ["96.03Z"],
    "reparation": ["95"],
    "reparation informatique": ["95.11Z"],
    "cordonnerie": ["95.23Z"],
    # ── Location ──
    "location voiture": ["77.11"],
    "agence de voyage": ["79.11Z"],
    "tourisme": ["79"],
}

# Threshold for fuzzy matching (0-100)
FUZZY_THRESHOLD = 75


def resolve_industry(query: str) -> list[str] | None:
    """Resolve a human industry name to NAF code prefixes.

    Returns list of NAF prefixes or None if no match.
    Tries exact match first, then fuzzy match.
    """
    normalized = query.strip().lower()

    # Exact match
    if normalized in INDUSTRY_ALIASES:
        return INDUSTRY_ALIASES[normalized]

    # Fuzzy match
    candidates = list(INDUSTRY_ALIASES.keys())
    result = process.extractOne(normalized, candidates, scorer=fuzz.WRatio)
    if result and result[1] >= FUZZY_THRESHOLD:
        matched_key = result[0]
        return INDUSTRY_ALIASES[matched_key]

    return None


def resolve_industry_with_name(query: str) -> tuple[str, list[str]] | None:
    """Like resolve_industry but also returns the matched name.

    Returns (matched_name, naf_prefixes) or None.
    """
    normalized = query.strip().lower()

    if normalized in INDUSTRY_ALIASES:
        return (normalized, INDUSTRY_ALIASES[normalized])

    candidates = list(INDUSTRY_ALIASES.keys())
    result = process.extractOne(normalized, candidates, scorer=fuzz.WRatio)
    if result and result[1] >= FUZZY_THRESHOLD:
        matched_key = result[0]
        return (matched_key, INDUSTRY_ALIASES[matched_key])

    return None
