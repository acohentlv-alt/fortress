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

    # ═══ Boulangerie artisanale ═══
    # Alan a explicitement approuvé 47.24Z (commerce de détail pain) dans l'expansion —
    # une boulangerie avec comptoir retail enregistrée en 47.24Z reste la cible Cindy.
    # 10.71B (cuisson de produits de boulangerie) est ajouté au clique 26 avr. :
    # le code désigne en pratique les artisans qui cuisent sur place mais dont la mise
    # à jour SIRENE n'est pas alignée sur 10.71C. Régressions : "Le fournil auvergnat"
    # (SIREN 795194810, 15000) et "Le Pain de Mon Moulin" (SIREN 752972141, 66000) —
    # picker 10.71C, SIRENE 10.71B, enseigne+adresse confirment le même artisan.
    # Seul saut inter-section (C fabrication → G commerce) du map.
    "10.71B": frozenset({"10.71B", "10.71C", "10.71D", "47.24Z"}),  # Symétrie : cuisson ↔ boulangerie traditionnelle
    "10.71C": frozenset({"10.71C", "10.71B", "10.71D", "47.24Z"}),
    "10.71D": frozenset({"10.71D", "10.71B", "10.71C", "47.24Z"}),  # Symétrie : pâtisserie ↔ boulangerie/cuisson (+ retail pain)
    # Régression ANTONE Artisan Boulanger (boulangerie 33000, 18 avr.) — picker 10.71D, SIREN 10.71C.
    # 47.24Z reste intentionnellement absent comme clé (décision Alan : singleton one-way).
    # Excluded: 10.71A industriel — fabrication en chambre froide à grande échelle, pas un artisan de quartier.
    #           10.72Z biscuits/biscottes — industriel, conservation longue durée.
    #           47.11X épicerie (secteur distinct), 56.XX restauration.

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

    # ═══ Viticulture / domaines viticoles ═══
    # Un domaine viticole physique peut être enregistré côté production (01.21Z culture
    # de la vigne), côté transformation (11.02B vinification ou 11.02A vins effervescents
    # pour les bulles/crémants), ou côté négoce (46.34Z commerce de gros de boissons —
    # cas des maisons de négoce en vin ou des caves coopératives). Du point de vue Cindy
    # (recherche "domaine viticole 66" ou "exploitation vigne 11"), un même domaine peut
    # être sous l'un quelconque de ces 4 codes selon l'activité dominante déclarée.
    # Régressions ws174 : Domaine Boudau (SIREN 394702583, 66) — picker 01 (section A),
    # SIRENE 46.34Z (négoce boissons), match enseigne. Les Clos de Paulilles (SIREN
    # 317809093, 66) — picker section A, SIRENE 46.34Z, match siren_website.
    # Excluded: 56.30Z débits de boissons (bar/cave de consommation sur place — métier
    #           distinct, pas de production), 47.25Z commerce de détail boissons
    #           (caviste grand public ≠ producteur), 11.05Z bière (filière distincte),
    #           11.07B sodas (filière distincte).
    "01.21Z": frozenset({"01.21Z", "11.02A", "11.02B", "46.34Z"}),
    "11.02A": frozenset({"11.02A", "01.21Z", "11.02B", "46.34Z"}),  # Symétrie : effervescents ↔ vigne/vinif/négoce
    "11.02B": frozenset({"11.02B", "01.21Z", "11.02A", "46.34Z"}),  # Symétrie : vinification ↔ vigne/effervescents/négoce
    "46.34Z": frozenset({"46.34Z", "01.21Z", "11.02A", "11.02B"}),  # Symétrie : négoce boissons ↔ vigne/vinif/effervescents

    # ═══ Arboriculture / exploitation fruitière ═══
    # Une exploitation arboricole (pommiers, pruniers, noyers, amandiers, agrumes,
    # avocatiers, olives) peut être enregistrée sous plusieurs codes selon la variété
    # dominante. En pratique, Cindy cible l'ensemble du spectre "fruit" dans les
    # départements 47, 66, 83, 84. Un même verger peut basculer de 01.24Z (pépins/noyau)
    # à 01.25Z (noix/noisettes) voire 01.29Z (autres permanentes : figues, kiwis, etc.)
    # selon la déclaration SIRENE. La coopérative ou le négoce acquéreur est souvent
    # 10.39B (transformation et conservation de fruits — ex. France Prune, Rougeline).
    # Régressions ws174 : RIVIERE Exploitation Agricole (MAPS02893, 47) — picker 01.25Z,
    # SIRENE 941661613 (ROUGELINE ACHATS) NAF 10.39B. Également SCEA de Guyenne
    # (SIREN 450850383, 47) — picker 01.24Z, SIRENE 01.13Z (légumes) — cas EXCLU
    # car maraîchage ≠ arboriculture.
    # Excluded: 01.13Z légumes/maraîchage (chaîne d'approvisionnement distincte,
    #           cultures saisonnières annuelles vs arbres pérennes), 01.21Z vigne
    #           (déjà son propre clique viticulture), 01.11Z céréales (filière distincte),
    #           46.31Z commerce gros fruits&légumes (trop large — couvre aussi légumes).
    "01.22Z": frozenset({"01.22Z", "01.23Z", "01.24Z", "01.25Z", "01.26Z", "01.29Z", "10.39B"}),  # Symétrie : fruits tropicaux
    "01.23Z": frozenset({"01.23Z", "01.22Z", "01.24Z", "01.25Z", "01.26Z", "01.29Z", "10.39B"}),  # Symétrie : agrumes
    "01.24Z": frozenset({"01.24Z", "01.22Z", "01.23Z", "01.25Z", "01.26Z", "01.29Z", "10.39B"}),  # Symétrie : pépins/noyau ↔ autres fruits
    "01.25Z": frozenset({"01.25Z", "01.22Z", "01.23Z", "01.24Z", "01.26Z", "01.29Z", "10.39B"}),  # Symétrie : noix/châtaignes
    "01.26Z": frozenset({"01.26Z", "01.22Z", "01.23Z", "01.24Z", "01.25Z", "01.29Z", "10.39B"}),  # Symétrie : oléagineux
    "01.29Z": frozenset({"01.29Z", "01.22Z", "01.23Z", "01.24Z", "01.25Z", "01.26Z", "10.39B"}),  # Symétrie : autres cultures permanentes
    "10.39B": frozenset({"10.39B", "01.22Z", "01.23Z", "01.24Z", "01.25Z", "01.26Z", "01.29Z"}),  # Symétrie : transformation fruits ↔ vergers

    # ═══ EHPAD / hébergement médicalisé personnes âgées ═══
    # Un EHPAD peut être enregistré sous 87.10A (hébergement médicalisé personnes
    # âgées — EHPAD "full médical") ou 87.30A (hébergement social personnes âgées —
    # résidence autonomie, anciennement "foyer-logement"). Du point de vue Cindy
    # (recherche "EHPAD 46"), les deux codes correspondent au même type d'établissement
    # accueillant des seniors. Régression ws174 : "EHPAD du Centre Hospitalier"
    # (SIREN 264600172, 46) — picker 87.10A, match inpi. La distinction médicalisé vs
    # non-médicalisé dépend du niveau de soins dispensés mais l'établissement reste
    # "une maison de retraite" pour Cindy.
    # Excluded: 87.10B hébergement handicapés enfants (clientèle entièrement distincte),
    #           87.10C adultes handicapés (idem), 87.20Z maladies mentales/addictions
    #           (filière psychiatrique, pas personnes âgées), 86.10Z activités hospitalières
    #           (hôpital aigu ≠ résidence, même si un EHPAD peut être rattaché à un CH).
    "87.10A": frozenset({"87.10A", "87.30A"}),
    "87.30A": frozenset({"87.30A", "87.10A"}),  # Symétrie : résidence autonomie ↔ EHPAD médicalisé

    # ═══ Maraîchage / cultures légumières ═══
    # Une exploitation maraîchère (légumes plein champ ou serre, melons, salades,
    # racines) peut être enregistrée côté production (01.13Z), côté transformation
    # (10.39A — légumes transformés/conserves), côté gros (46.31Z — commerce de
    # gros fruits & légumes), ou côté détail (47.21Z — primeurs).
    # Régressions ws174 14j : "Au Jardin des Sables" + "SCEA de Guyenne" (47, 01.13Z),
    # "DOMAINE LOS PENEDES" + "Ferme des 3 soleils" (46.31Z) — pickers divers,
    # match strong-method bloqué par mismatch NAF.
    #
    # CROSS-CLIQUE NOTE — 46.31Z asymétrie : le code 46.31Z est aussi pertinent côté
    # arboriculture (commerce gros fruits ET légumes), mais le clique arboriculture
    # l'exclut explicitement comme "trop large". On accepte 46.31Z ici côté
    # maraîchage uniquement — picker 01.13Z + match 46.31Z = verified, mais picker
    # 01.24Z + match 46.31Z reste mismatch (asymétrie intentionnelle). Si 46.31Z est
    # un jour ajouté au clique arboriculture, auditer les deux blocs.
    #
    # Excluded: 01.11Z céréales (filière distincte), 01.16Z sucre (industriel),
    #           01.30Z pépinière (horticulture clique), 10.39B fruits (arbo clique),
    #           46.21Z céréales/semences/aliments bétail, 46.39Z gros alim générale
    #           (trop large), 81.30Z paysagisme (service ≠ production).
    "01.13Z": frozenset({"01.13Z", "10.39A", "46.31Z", "47.21Z"}),
    "10.39A": frozenset({"10.39A", "01.13Z", "46.31Z", "47.21Z"}),
    "46.31Z": frozenset({"46.31Z", "01.13Z", "10.39A", "47.21Z"}),
    "47.21Z": frozenset({"47.21Z", "01.13Z", "10.39A", "46.31Z"}),
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
