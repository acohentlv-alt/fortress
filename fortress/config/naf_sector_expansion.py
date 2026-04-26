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
    # Investigated Apr 19 for clique with 43.34Z (peinture) and 43.33Z (carrelage).
    # Decision: keep singleton. Map's design rule (line 14) is "when in doubt, exclude";
    # plâtriers typically register strictly under 43.31Z and bundling with peinture/
    # carrelage risks false positives. Revisit if regressions appear in the 43.3X range.
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

    # ═══ Hôtellerie / Hébergement touristique ═══
    # Une recherche Maps "hôtel Bordeaux" remonte indistinctement hôtels (55.10Z),
    # appart-hôtels et résidences (55.20Z), campings haut de gamme (55.30Z) et
    # autres formes (55.90Z — foyers de jeunes travailleurs ouverts au public, etc.).
    # Cindy traite ces 4 codes comme un même secteur hébergement touristique.
    # Expansion en clique mutuelle.
    "55.10Z": frozenset({"55.10Z", "55.20Z", "55.30Z", "55.90Z"}),
    "55.20Z": frozenset({"55.20Z", "55.10Z", "55.30Z", "55.90Z"}),  # Symétrie : courte durée ↔ hôtels/camping/autres
    "55.30Z": frozenset({"55.30Z", "55.10Z", "55.20Z", "55.90Z"}),  # Symétrie : camping ↔ hôtels/courte durée/autres
    "55.90Z": frozenset({"55.90Z", "55.10Z", "55.20Z", "55.30Z"}),  # Symétrie : autres hébergements ↔ hôtels/courte durée/camping

    # ═══ Restauration grand public ═══
    # Une recherche Maps "restaurant Bordeaux" remonte traditionnel (56.10A — service à
    # table), rapide (56.10C — kebab/burger/sushi à emporter ou sur place), et certains
    # traiteurs (56.21Z) qui ouvrent une boutique au comptoir. Du point de vue Cindy
    # (cible commerce de bouche grand public), ces 3 codes sont un même secteur.
    # Expansion en clique mutuelle. Restent exclus : 56.10B cafétérias (libre-service
    # institutionnel), 56.29A restauration collective sous contrat, 56.30Z débits de
    # boissons (modèle bar/café distinct).
    "56.10A": frozenset({"56.10A", "56.10C", "56.21Z"}),
    "56.10C": frozenset({"56.10C", "56.10A", "56.21Z"}),  # Symétrie : rapide ↔ traditionnelle/traiteur
    "56.21Z": frozenset({"56.21Z", "56.10A", "56.10C"}),  # Symétrie : traiteur ↔ traditionnelle/rapide

    # ═══ Horticulture / pépinières / fleuristes ═══
    # Une pépinière physique peut être enregistrée côté production (01.30Z reproduction
    # de plantes), côté autres cultures permanentes (01.19Z — fleurs coupées, plantes
    # ornementales en plein champ), côté commerce de gros (46.22Z — vente aux fleuristes
    # et paysagistes), ou côté commerce de détail (47.76Z — boutique avec comptoir).
    # Du point de vue Cindy (recherche "pépinière 49"), ces 4 codes sont la même filière
    # horticole. Régression Pépinières de Vair Sur Loire (44150) — 26 avril : picker 01.30Z,
    # SIREN 821086378 enregistré 46.22Z (vente en gros), match enseigne+adresse confirmé,
    # quarantaine Gemini D1b par mismatch de section. Clique mutuelle pour rattraper.
    # Excluded: 01.13Z légumes / maraîchage (filière distincte — chaîne d'approvisionnement
    #           différente, pas de pépinière ornementale), 46.21Z céréales/semences/aliments
    #           bétail (recouvrement faible — semences agricoles ≠ semences ornementales),
    #           81.30Z paysagisme (métier de service, pas de production/négoce de plantes).
    "01.30Z": frozenset({"01.30Z", "01.19Z", "46.22Z", "47.76Z"}),
    "01.19Z": frozenset({"01.19Z", "01.30Z", "46.22Z", "47.76Z"}),  # Symétrie : autres cultures ↔ pépinière/gros/détail
    "46.22Z": frozenset({"46.22Z", "01.30Z", "01.19Z", "47.76Z"}),  # Symétrie : gros plantes ↔ production/détail
    "47.76Z": frozenset({"47.76Z", "01.30Z", "01.19Z", "46.22Z"}),  # Symétrie : détail plantes ↔ production/gros
}


def same_sector_group(code_a: str, code_b: str) -> bool:
    """True if two NAF leaf codes belong to the same curated sector group.

    Rule:
      - Identity: same code always shares its group with itself.
      - Mutual: code_b is in SECTOR_EXPANSIONS[code_a], OR code_a is in SECTOR_EXPANSIONS[code_b].
      - Isolated codes (keys with single-element set, e.g. 41.20A) share only with themselves.
      - Codes NOT in SECTOR_EXPANSIONS as a key (e.g. 47.24Z) can only match via the
        other side being a key whose expansion contains them. Picking two non-key codes
        together is always False (they have no anchor).

    Note: Section letters and 2-digit divisions are NEVER keys in SECTOR_EXPANSIONS.
    A user who picks section letter 'I' cannot combine it with anything else
    (same_sector_group returns False for any pair with a section letter — intentional,
    section-letter picks must stand alone).
    """
    if code_a == code_b:
        return True
    exp_a = SECTOR_EXPANSIONS.get(code_a)
    if exp_a is not None and code_b in exp_a:
        return True
    exp_b = SECTOR_EXPANSIONS.get(code_b)
    if exp_b is not None and code_a in exp_b:
        return True
    return False


def all_same_sector_group(codes: list[str]) -> bool:
    """True if every pair of codes in the list shares a sector group.

    Empty list and single-element list are trivially True. Used by backend validator.
    """
    if len(codes) <= 1:
        return True
    anchor = codes[0]
    return all(same_sector_group(anchor, c) for c in codes[1:])
