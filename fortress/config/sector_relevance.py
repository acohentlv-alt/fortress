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
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur", "coiffure",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "médecin", "opticien", "bijouterie",
            "camping", "supermarché", "hypermarché",
        }),
    },
    "transport routier": {
        "relevant": frozenset({
            "transport", "routier", "camion", "logistique",
            "demenagement", "fret", "messagerie", "livraison",
            "affrètement", "coursier",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur", "coiffure",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "médecin", "opticien", "bijouterie",
            "camping", "supermarché", "hypermarché",
        }),
    },
    "camping": {
        "relevant": frozenset({
            "camping", "caravaning", "mobil-home", "glamping",
            "parc résidentiel", "hébergement", "vacances",
        }),
        "irrelevant": frozenset({
            "restaurant", "transport", "garage", "pharmacie",
            "boulangerie", "pressing", "coiffeur", "dentiste",
            "parking", "supermarché", "hypermarché",
            "location de voiture", "magasin",
        }),
    },
    "restaurant": {
        "relevant": frozenset({
            "restaurant", "brasserie", "traiteur", "pizzeria",
            "crêperie", "bistrot", "cuisine", "gastronomie",
            "restauration", "snack", "fast food", "kebab",
        }),
        "irrelevant": frozenset({
            "transport", "garage", "pharmacie", "pressing",
            "dentiste", "camping", "hôtel",
        }),
    },
    "hotel": {
        "relevant": frozenset({
            "hôtel", "hotel", "hébergement", "résidence",
            "chambre d'hôtes", "auberge", "lodge", "appart",
        }),
        "irrelevant": frozenset({
            "transport", "garage", "pharmacie", "pressing",
            "dentiste", "boulangerie", "coiffeur",
        }),
    },
    "boulangerie": {
        "relevant": frozenset({
            "boulangerie", "pâtisserie", "patisserie", "viennoiserie",
            "pain", "fournil",
        }),
        "irrelevant": frozenset({
            "transport", "garage", "pharmacie", "pressing",
            "dentiste", "camping", "hôtel", "coiffeur",
        }),
    },
    "plomberie": {
        "relevant": frozenset({
            "plomb", "chauffage", "sanitaire", "plombier",
            "salle de bain", "robinetterie",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "camping",
        }),
    },
    "electricite": {
        "relevant": frozenset({
            "electri", "électri", "domotique", "photovoltaïque",
            "solaire", "énergie", "energie",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "camping",
        }),
    },
    "garage": {
        "relevant": frozenset({
            "garage", "automobile", "mécanique", "carrosserie",
            "réparation auto", "contrôle technique", "pneu",
            "vidange", "auto", "voiture",
        }),
        "irrelevant": frozenset({
            "restaurant", "hôtel", "hotel", "coiffeur",
            "boulangerie", "pharmacie", "pressing", "fleuriste",
            "dentiste", "camping",
        }),
    },
    "immobilier": {
        "relevant": frozenset({
            "immobili", "agence", "location", "gestion",
            "syndic", "foncier", "logement",
        }),
        "irrelevant": frozenset({
            "restaurant", "transport", "garage", "pharmacie",
            "boulangerie", "pressing", "dentiste", "camping",
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
