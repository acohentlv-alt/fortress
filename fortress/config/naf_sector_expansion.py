"""Per-sector NAF expansion map for pipeline auto-confirm (Option C, Apr 18).

When the user's picker is a leaf NAF code, the pipeline treats any matched
SIRENE NAF in SECTOR_EXPANSIONS[picker] as "verified" for auto-confirmation.

Used by `_compute_naf_status` in fortress/discovery.py.

Design rules:
  - Keys are leaf NAF codes exactly as they appear in config/naf_codes.py
    (format "XX.YYL" e.g. "10.71C").
  - Values are frozensets of sibling codes. Include the key itself for
    symmetry (strict-prefix path already catches it, but explicit is clearer).
  - Never include codes from an unrelated sector. Justify each inclusion
    with an inline French comment; justify each excluded neighbour too.
  - When in doubt, exclude. Missing expansion falls back to strict
    prefix-match (safe default).
  - If picker is not a key in this dict, expansion path is a no-op and
    _compute_naf_status returns "mismatch" — no fuzzy fallback.
"""

SECTOR_EXPANSIONS: dict[str, frozenset[str]] = {
    # ═══ BTP gros œuvre ═══
    # Maison individuelle — constructeur de pavillon. Pas de sibling légitime
    # (41.20B autres bâtiments = échelle différente, 43.99C maçonnerie = sous-traitant).
    "41.20A": frozenset({"41.20A"}),

    # Autres bâtiments (immeubles, locaux industriels).
    "41.20B": frozenset({"41.20B"}),

    # Maçonnerie générale / gros œuvre — artisan maçon. Métier isolé.
    "43.99C": frozenset({"43.99C"}),
    # Excluded: 43.99A (étanchéité — métier distinct), 43.99B (structures métalliques — métier distinct)

    # ═══ BTP second œuvre ═══
    # Électricité — 43.21A bâtiment + 43.21B voie publique (même corps d'état).
    "43.21A": frozenset({"43.21A", "43.21B"}),
    "43.21B": frozenset({"43.21B", "43.21A"}),  # Symétrie : voie publique ↔ bâtiment

    # Plomberie-chauffage — 43.22A eau/gaz + 43.22B thermique/clim (même artisan souvent).
    "43.22A": frozenset({"43.22A", "43.22B"}),
    "43.22B": frozenset({"43.22B", "43.22A"}),
    # Excluded: 43.21X électricité (métier distinct)

    # Plâtrerie — métier isolé.
    "43.31Z": frozenset({"43.31Z"}),
    # Excluded: 43.29A isolation (métier proche mais distinct, risque faux positif)

    # Menuiserie bois/PVC — 43.32A pose + 43.32C agencement de lieux de vente (même menuisier).
    "43.32A": frozenset({"43.32A", "43.32C"}),
    "43.32C": frozenset({"43.32C", "43.32A"}),  # Symétrie : agencement ↔ pose menuiserie
    # Excluded: 43.32B menuiserie métallique / serrurerie (métier distinct)

    # Menuiserie métallique / serrurerie — métier à part entière.
    "43.32B": frozenset({"43.32B"}),

    # Carrelage / revêtement — métier isolé.
    "43.33Z": frozenset({"43.33Z"}),

    # Peinture / vitrerie — métier isolé.
    "43.34Z": frozenset({"43.34Z"}),
    # Excluded: 43.39Z autres finitions (fourre-tout imprécis)

    # Charpente + couverture — souvent la même entreprise.
    "43.91A": frozenset({"43.91A", "43.91B"}),
    "43.91B": frozenset({"43.91B", "43.91A"}),

    # ═══ Nettoyage / Propreté ═══
    # Nettoyage courant + industriel — mêmes prestataires selon le contrat dominant.
    "81.21Z": frozenset({"81.21Z", "81.22Z"}),
    "81.22Z": frozenset({"81.22Z", "81.21Z"}),
    # Excluded: 81.29A désinfection/3D (métier spécialisé), 81.29B autres (fourre-tout), 81.30Z espaces verts (paysagisme)

    # ═══ Sécurité privée ═══
    "80.10Z": frozenset({"80.10Z"}),

    # ═══ Coiffure / beauté ═══
    # Coiffure — métier isolé.
    "96.02A": frozenset({"96.02A"}),
    # Excluded: 96.02B soins de beauté (métier distinct), 96.04Z entretien corporel (spa, clientèle différente)

    # Soins de beauté / esthétique — métier à part.
    "96.02B": frozenset({"96.02B"}),

    # ═══ Commerce alimentaire de proximité ═══
    # Supérettes + alimentation générale — même échelle/clientèle quartier.
    "47.11C": frozenset({"47.11C", "47.11B"}),
    "47.11B": frozenset({"47.11B", "47.11C"}),
    # Excluded: 47.11A surgelés (spécialité), 47.11D supermarchés, 47.11E multi-commerces, 47.11F hypermarchés (échelle différente)

    # ═══ Boucherie / charcuterie ═══
    # Boucherie de détail — métier isolé côté retail.
    "47.22Z": frozenset({"47.22Z"}),
    # Excluded: 10.13B charcuterie fabrication (industrie), 46.32Z gros viandes (wholesale)

    # Charcuterie fabrication — 10.13A/B frontière floue selon taille atelier.
    "10.13B": frozenset({"10.13B", "10.13A"}),
    "10.13A": frozenset({"10.13A", "10.13B"}),  # Symétrie : transformation viande ↔ charcuterie
    # Excluded: 47.22Z retail (clientèle différente)

    # ═══ Boulangerie ═══
    # Alan a explicitement approuvé 47.24Z (commerce de détail pain) dans l'expansion —
    # une boulangerie avec comptoir retail enregistrée en 47.24Z reste la cible Cindy.
    # Seul saut inter-section (C fabrication → G commerce) du map.
    "10.71C": frozenset({"10.71C", "10.71D", "47.24Z"}),
    "10.71D": frozenset({"10.71D", "10.71C", "47.24Z"}),  # Symétrie : cuisson pain ↔ boulangerie traditionnelle (+ retail pain)
    # Régression ANTONE Artisan Boulanger (boulangerie 33000, 18 avr.) — picker 10.71D, SIREN 10.71C.
    # 47.24Z reste intentionnellement absent comme clé (décision Alan : singleton one-way).
    # Excluded: 10.71A industriel (échelle différente), 10.71B cuisson surgelés (métier distinct),
    #           47.11X épicerie (sector distinct), 56.XX restauration

    # ═══ Garage auto ═══
    # Entretien VL — métier isolé côté grand public.
    "45.20A": frozenset({"45.20A"}),
    # Excluded: 45.20B poids lourds/bus (clientèle B2B)

    # Carrosserie / PL — périmètre distinct.
    "45.20B": frozenset({"45.20B"}),

    # ═══ Transport voyageurs ═══
    # Taxi — régime de licence propre.
    "49.32Z": frozenset({"49.32Z"}),
    # Excluded: 49.39B VTC (licence distincte), 49.39A lignes régulières (autocariste)

    # VTC / autres voyageurs — 49.39B. Régime distinct du taxi.
    "49.39B": frozenset({"49.39B"}),

    # ═══ Transport fret ═══
    # Fret routier — cluster étendu par Alan: 49.41A/B/C + 49.42Z déménagement + 52.29A/B messagerie/affrètement.
    # Justification: même fleet routière, mêmes logisticiens, classification variable selon trafic dominant.
    "49.41A": frozenset({"49.41A", "49.41B", "49.41C", "49.42Z", "52.29A", "52.29B"}),
    "49.41B": frozenset({"49.41B", "49.41A", "49.41C", "49.42Z", "52.29A", "52.29B"}),
    "49.41C": frozenset({"49.41C", "49.41A", "49.41B", "49.42Z", "52.29A", "52.29B"}),  # Symétrie fret
    "49.42Z": frozenset({"49.42Z", "49.41A", "49.41B", "49.41C", "52.29A", "52.29B"}),  # Symétrie déménagement
    "52.29A": frozenset({"52.29A", "49.41A", "49.41B", "49.41C", "49.42Z", "52.29B"}),  # Symétrie messagerie
    "52.29B": frozenset({"52.29B", "49.41A", "49.41B", "49.41C", "49.42Z", "52.29A"}),  # Symétrie affrètement
    # Excluded: 49.32Z taxi, 49.39X voyageurs, 77.11X location sans chauffeur

    # ═══ Aide à domicile ═══
    "88.10A": frozenset({"88.10A"}),
    # Excluded: 88.10B accueil sans hébergement (structure, pas domicile), 88.10C ESAT (métier distinct)

    # ═══ Pressing / laverie ═══
    # Blanchisserie de gros — clientèle B2B.
    "96.01A": frozenset({"96.01A"}),
    # Excluded: 96.01B détail (clientèle différente)

    # Pressing / laverie de détail — clientèle grand public.
    "96.01B": frozenset({"96.01B"}),

    # ═══ Restauration rapide / traiteur ═══
    # Restauration rapide — kebab/snack/fast-food. Modèle distinct.
    "56.10C": frozenset({"56.10C"}),
    # Excluded: 56.10A traditionnelle (service à table), 56.10B cafétéria (libre-service),
    #           56.21Z traiteurs (événementiel), 56.30Z débits de boissons

    # Traiteurs — événementiel/B2B.
    "56.21Z": frozenset({"56.21Z"}),
    # Excluded: 56.29A collective sous contrat (échelle différente), 56.10C rapide (grand public)

    # ═══ Hôtellerie ═══
    "55.10Z": frozenset({"55.10Z"}),
    # Excluded: 55.20Z courte durée (Airbnb/gîtes), 55.30Z camping, 55.90Z autres (foyers, internats)

    # ═══ Camping ═══
    "55.30Z": frozenset({"55.30Z"}),

    # ═══ Restaurant traditionnel ═══
    # Service à table. Distinct de la restauration rapide, des débits de boisson, des traiteurs.
    "56.10A": frozenset({"56.10A"}),
    # Excluded: 56.10B cafétérias (libre-service), 56.10C rapide, 56.21Z traiteurs, 56.30Z débits
}
