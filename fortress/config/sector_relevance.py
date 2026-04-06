"""Sector → Google Maps category relevance rules.

Each sector key maps to:
  - relevant: frozenset of category substrings that ARE relevant
  - irrelevant: frozenset of category substrings that are definitely WRONG

If a business's Maps category contains any "irrelevant" substring for the
active sector, it is filtered out BEFORE expensive enrichment.

Unknown categories (not matching either list) ALWAYS pass through.
We only reject what we are certain is wrong.
"""

SECTOR_RULES: dict[str, dict[str, frozenset[str]]] = {
    "transport": {
        "relevant": frozenset({
            "transport", "logistique", "demenagement", "fret",
            "messagerie", "livraison", "entreposage", "routier",
            "camion", "affrètement", "coursier",
            # English (JSON-LD @type)
            "transportation", "logistics", "moving", "freight",
            "delivery", "trucking",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur", "coiffure",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "médecin", "opticien", "bijouterie",
            "camping", "supermarché", "hypermarché",
            "bakery", "campground", "hair",
        }),
    },
    "transport routier": {
        "relevant": frozenset({
            "transport", "routier", "camion", "logistique",
            "demenagement", "fret", "messagerie", "livraison",
            "affrètement", "coursier",
            "transportation", "logistics", "moving", "freight",
            "delivery", "trucking",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur", "coiffure",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "médecin", "opticien", "bijouterie",
            "camping", "supermarché", "hypermarché",
            "bakery", "campground", "hair",
        }),
    },
    "camping": {
        "relevant": frozenset({
            "camping", "caravaning", "mobil-home", "glamping",
            "parc résidentiel", "hébergement", "vacances",
            # English (JSON-LD @type)
            "campground", "rv park", "mobile home park",
        }),
        "irrelevant": frozenset({
            "restaurant", "transport", "garage", "pharmacie",
            "boulangerie", "pressing", "coiffeur", "dentiste",
            "parking", "supermarché", "hypermarché",
            "location de voiture", "magasin",
            "bakery", "hair", "plumber", "electrician",
            "auto repair", "real estate",
        }),
    },
    "restaurant": {
        "relevant": frozenset({
            "restaurant", "brasserie", "traiteur", "pizzeria",
            "crêperie", "bistrot", "cuisine", "gastronomie",
            "restauration", "snack", "fast food", "kebab",
            "cafe", "diner", "food",
        }),
        "irrelevant": frozenset({
            "transport", "garage", "pharmacie", "pressing",
            "dentiste", "camping", "hôtel",
            "campground", "plumber", "electrician",
        }),
    },
    "hotel": {
        "relevant": frozenset({
            "hôtel", "hotel", "hébergement", "résidence",
            "chambre d'hôtes", "auberge", "lodge", "appart",
            "lodging", "inn", "hostel", "motel", "resort",
        }),
        "irrelevant": frozenset({
            "transport", "garage", "pharmacie", "pressing",
            "dentiste", "boulangerie", "coiffeur",
            "bakery", "plumber", "electrician",
        }),
    },
    "boulangerie": {
        "relevant": frozenset({
            "boulangerie", "pâtisserie", "patisserie", "viennoiserie",
            "pain", "fournil",
            "bakery", "pastry",
        }),
        "irrelevant": frozenset({
            "transport", "garage", "pharmacie", "pressing",
            "dentiste", "camping", "hôtel", "coiffeur",
            "campground", "plumber", "electrician",
        }),
    },
    "plomberie": {
        "relevant": frozenset({
            "plomb", "chauffage", "sanitaire", "plombier",
            "salle de bain", "robinetterie",
            "plumber", "plumbing", "heating",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "camping",
            "bakery", "campground", "hair",
        }),
    },
    "electricite": {
        "relevant": frozenset({
            "electri", "électri", "domotique", "photovoltaïque",
            "solaire", "énergie", "energie",
            "electrical", "solar", "energy",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "camping",
            "bakery", "campground", "hair",
        }),
    },
    "garage": {
        "relevant": frozenset({
            "garage", "automobile", "mécanique", "carrosserie",
            "réparation auto", "contrôle technique", "pneu",
            "vidange", "auto", "voiture",
            "auto repair", "car repair", "mechanic", "tire",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "camping",
            "bakery", "campground", "hair",
        }),
    },
    "immobilier": {
        "relevant": frozenset({
            "immobili", "agence", "location", "gestion",
            "syndic", "foncier", "logement",
            "real estate", "property",
        }),
        "irrelevant": frozenset({
            "restaurant", "transport", "garage", "pharmacie",
            "boulangerie", "pressing", "dentiste", "camping",
            "bakery", "campground", "plumber",
        }),
    },
}


def is_irrelevant_category(
    sector_word: str, maps_category: str
) -> bool:
    """Check if a Maps category is irrelevant for the given sector.

    Lookup strategy (cascading):
        1. Try the full sector_word phrase
        2. Try progressively shorter prefixes
        3. Try each individual word
        4. If no sector rules found → return False (pass through)
    """
    cat_lower = maps_category.lower()
    sector_lower = sector_word.lower().strip()

    rules = None

    # 1. Exact full phrase
    if sector_lower in SECTOR_RULES:
        rules = SECTOR_RULES[sector_lower]
    else:
        words = sector_lower.split()
        # 2. Progressively shorter prefixes
        if len(words) > 1:
            for end in range(len(words) - 1, 0, -1):
                prefix = " ".join(words[:end])
                if prefix in SECTOR_RULES:
                    rules = SECTOR_RULES[prefix]
                    break

        # 3. Individual words (first match wins)
        if rules is None:
            for w in words:
                if w in SECTOR_RULES:
                    rules = SECTOR_RULES[w]
                    break

    if rules is None:
        return False

    for irr in rules["irrelevant"]:
        if irr in cat_lower:
            return True

    return False


# ── Name-based pre-filter (checked BEFORE navigating to detail page) ──
# Conservative: only words with near-zero false-positive risk.
# NOTE for camping: "hotel"/"hostel"/"auberge" are deliberately ABSENT.
# "Hotellerie de plein air" is the official French industry term for campings.

SECTOR_NAME_BLACKLIST: dict[str, frozenset[str]] = {
    "camping": frozenset({
        "parking", "stationnement", "aire de stationnement",
        "restaurant", "pizzeria", "brasserie", "kebab",
        "pharmacie", "garage", "carrosserie",
        "coiffeur", "coiffure", "pressing",
        "dentiste", "medecin", "opticien",
        "supermarche", "hypermarche",
        "notaire", "avocat", "banque",
        "office de tourisme", "tourist office",
        "location camping-car", "location de van", "location van",
    }),
    "transport": frozenset({
        "restaurant", "pizzeria", "brasserie", "kebab",
        "pharmacie", "coiffeur", "coiffure", "pressing",
        "dentiste", "medecin", "opticien",
        "camping", "campground",
        "supermarche", "hypermarche",
    }),
    "transport routier": frozenset({
        "restaurant", "pizzeria", "brasserie", "kebab",
        "pharmacie", "coiffeur", "coiffure", "pressing",
        "dentiste", "medecin", "opticien",
        "camping", "campground",
        "supermarche", "hypermarche",
    }),
    "restaurant": frozenset({
        "pharmacie", "garage", "carrosserie",
        "coiffeur", "coiffure", "pressing",
        "dentiste", "medecin", "opticien",
        "camping", "campground",
    }),
    "hotel": frozenset({
        "restaurant", "pizzeria", "brasserie",
        "pharmacie", "garage", "carrosserie",
        "coiffeur", "coiffure", "pressing",
        "dentiste", "medecin", "opticien",
        "camping", "campground",
    }),
    "boulangerie": frozenset({
        "pharmacie", "garage", "carrosserie",
        "coiffeur", "coiffure", "pressing",
        "dentiste", "medecin", "opticien",
        "camping", "campground",
    }),
}

GLOBAL_BRAND_BLACKLIST: frozenset[str] = frozenset({
    "mcdonalds", "mcdonald's", "burger king", "kfc",
    "starbucks", "subway", "dominos", "domino's",
    "carrefour", "leclerc", "auchan", "lidl", "aldi",
    "intermarche", "super u", "monoprix", "franprix",
    "decathlon", "intersport", "leroy merlin", "castorama", "ikea",
    "hertz", "europcar", "avis", "sixt", "cabesto",
    "bouygues", "sfr", "orange", "free",
    "la poste", "credit agricole", "bnp paribas",
    "societe generale", "caisse d'epargne",
    "total", "totalenergies", "shell",
})


def is_irrelevant_name(sector_word: str, business_name: str) -> bool:
    """Fast pre-filter: check if a business NAME is obviously wrong for this sector.

    Uses the same cascading sector lookup as is_irrelevant_category.
    Returns True if the name contains a blacklisted word OR matches a global brand.
    """
    import unicodedata

    # Normalize: remove accents, lowercase
    nfkd = unicodedata.normalize("NFKD", business_name.lower())
    name_lower = "".join(c for c in nfkd if not unicodedata.combining(c))

    # Global brand check first
    for brand in GLOBAL_BRAND_BLACKLIST:
        brand_nfkd = unicodedata.normalize("NFKD", brand.lower())
        brand_clean = "".join(c for c in brand_nfkd if not unicodedata.combining(c))
        if brand_clean in name_lower:
            return True

    # Sector-specific lookup (same cascade as is_irrelevant_category)
    sector_lower = sector_word.lower().strip()
    blacklist = None

    if sector_lower in SECTOR_NAME_BLACKLIST:
        blacklist = SECTOR_NAME_BLACKLIST[sector_lower]
    else:
        words = sector_lower.split()
        if len(words) > 1:
            for end in range(len(words) - 1, 0, -1):
                prefix = " ".join(words[:end])
                if prefix in SECTOR_NAME_BLACKLIST:
                    blacklist = SECTOR_NAME_BLACKLIST[prefix]
                    break
        if blacklist is None:
            for w in words:
                if w in SECTOR_NAME_BLACKLIST:
                    blacklist = SECTOR_NAME_BLACKLIST[w]
                    break

    if blacklist is None:
        return False

    for banned in blacklist:
        banned_nfkd = unicodedata.normalize("NFKD", banned.lower())
        banned_clean = "".join(c for c in banned_nfkd if not unicodedata.combining(c))
        if banned_clean in name_lower:
            return True

    return False
