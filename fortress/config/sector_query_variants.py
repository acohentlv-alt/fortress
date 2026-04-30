"""Per-sector Maps query phrasing variants — Lever D (variant pills, Apr 30).

When the user picks a NAF code in the New Batch picker, the frontend renders a
row of clickable variant pills below the picker. Clicking a pill spawns a new
query input prefilled with `<phrasing> <dept>` so the user can broaden the same
sector across multiple Maps phrasings in one batch.

Why this exists:
  Cindy's Apr 9 "vignobles 33000" batch and Apr 30 "CULTURE DE LA VIGNE 33000"
  batch had ZERO entity overlap (50 vs 22 results, no SIREN intersection).
  Same dept, same wine sector — Maps ranks by literal text. Auto-expansion
  (discovery.py) only widens GEOGRAPHY, not PHRASING. This dict lets the user
  widen phrasing manually with one click.

Design rules (mirror naf_sector_expansion.py):
  - Keys are leaf NAF codes from fortress/config/naf_codes.py (format "XX.YYL").
  - Values are lists of {"phrasing": "...", "label": "..."} dicts.
  - For sector cliques (vigne, EHPAD, arboriculture, hotels, boulangerie,
    restauration, pépinières), every member NAF maps to the SAME list — symmetry.
  - Phrasings must be sector-discriminating. NEVER use generic tokens that
    bleed into adjacent sectors ("vin" → wine bars; "transport" → taxis/metro).
  - Frontend computes the UNION across all picked NAFs (Apr 30 fix — was
    intersection; broke when sibling NAFs without their own variant lists
    were added (e.g., picking 55.30Z camping then adding
    55.10Z hotels via the sibling chip → empty intersection → variants
    vanished). Same-sector-group rule already prevents truly cross-sector
    picks, so union is safe.
  - Frontend never appends a city/dept itself for the pill phrasing — it
    inlines the same DEPT_NAMES extraction logic used by the launch handler
    and produces "<phrasing> <dept>". Phrasing strings here are sector-only,
    NEVER include departments or postal codes.
"""

# --- Vigne / Domaines viticoles ---------------------------------------
# SIRENE 01.21Z (33,34,11,66,47): "vignobles" n=82, "domaine" n=147 (label).
# SIRENE 46.34Z negoce: "vins" n=212, "vignobles" n=90, "maison" n=104.
# SIRENE 11.02B vinif: "cave" n=25, "vignerons" n=55, "vinicole" n=13.
# Cindy ws1 history: "vignobles 33000", "CULTURE DE LA VIGNE 33000".
# "vin" alone EXCLUDED (matches bars/restaurants/cavistes — too broad).
_VIGNE_VARIANTS = [
    {"phrasing": "vignobles", "label": "vignobles"},
    {"phrasing": "domaine viticole", "label": "domaine viticole"},
    {"phrasing": "château vignoble", "label": "château vignoble"},
    {"phrasing": "cave vigneronne", "label": "cave vigneronne"},
    {"phrasing": "négociant en vin", "label": "négociant"},
    {"phrasing": "exploitation viticole", "label": "exploitation viticole"},
    {"phrasing": "producteur de vin", "label": "producteur de vin"},
    # Apr 30 expansion (Alan request "expand all"):
    {"phrasing": "cave coopérative vinicole", "label": "cave coopérative"},
    {"phrasing": "vigneron indépendant", "label": "vigneron indépendant"},
    {"phrasing": "propriété viticole", "label": "propriété viticole"},
]

# --- Camping / Hôtellerie de plein air -------------------------------
# SIRENE 55.30Z (66,34,17,40,85,24,83,13): "camping" n=914, "caravaning" n=15.
# Cindy ws1 history: "HOTELLERIE DE PLEIN AIR ET CAMPING - 34000" (x5),
# "camping 66000" (x3), "camping 11" (x1).
# 55.30Z singleton — hôtels (55.10Z) split into HOTELS clique below since
# Cindy's screenshot of Apr 30 clicked "55.10Z" sibling chip and lost variants.
_CAMPING_VARIANTS = [
    {"phrasing": "camping", "label": "camping"},
    {"phrasing": "hôtellerie de plein air", "label": "hôtellerie de plein air"},
    {"phrasing": "camping caravaning", "label": "camping caravaning"},
    {"phrasing": "village vacances", "label": "village vacances"},
    {"phrasing": "camping 4 étoiles", "label": "camping 4 étoiles"},
    {"phrasing": "aire de camping-car", "label": "aire camping-car"},
    # Apr 30 expansion:
    {"phrasing": "mobil-home", "label": "mobil-home"},
    {"phrasing": "camping municipal", "label": "camping municipal"},
    {"phrasing": "camping 5 étoiles", "label": "camping 5 étoiles"},
    {"phrasing": "parc résidentiel de loisirs", "label": "PRL"},
    {"phrasing": "glamping", "label": "glamping"},
]

# --- Hotels / hébergement classique (NEW Apr 30 — fixes screenshot bug) -
# SIRENE 55.10Z (75,06,33,13,69): "hôtel" n=very high (>10k), "hotel" n=high,
# "auberge" n=high, "logis" n=mid. Cindy hasn't named a hotels-only batch yet
# but the picker exposes 55.10Z as sibling of 55.30Z — Lever D bug surfaced
# when she clicked 55.10Z and all camping variants vanished. New entry plugs
# the hole and lets her broaden hotel phrasings independently.
_HOTELS_VARIANTS = [
    {"phrasing": "hôtel", "label": "hôtel"},
    {"phrasing": "hôtel restaurant", "label": "hôtel-restaurant"},
    {"phrasing": "hôtel de tourisme", "label": "hôtel tourisme"},
    {"phrasing": "auberge", "label": "auberge"},
    {"phrasing": "chambres d'hôtes", "label": "chambres d'hôtes"},
    {"phrasing": "gîte", "label": "gîte"},
    {"phrasing": "résidence hôtelière", "label": "résidence hôtelière"},
]

# --- EHPAD / Maisons de retraite -------------------------------------
# SIRENE 87.10A (33,34,69,75,13,59,46): "ehpad" n=103, "résidence" n=113,
# "maison de retraite" tokens "maison"+"retraite" n=86+78.
# SIRENE 87.30A: "résidence" n=36, "seniors" n=11, "maison" n=28.
_EHPAD_VARIANTS = [
    {"phrasing": "EHPAD", "label": "EHPAD"},
    {"phrasing": "maison de retraite", "label": "maison de retraite"},
    {"phrasing": "résidence senior", "label": "résidence senior"},
    {"phrasing": "résidence autonomie", "label": "résidence autonomie"},
    {"phrasing": "résidence services seniors", "label": "résidence services"},
    {"phrasing": "foyer-logement seniors", "label": "foyer-logement"},
    # Apr 30 expansion:
    {"phrasing": "hébergement personnes âgées", "label": "hébergement âgés"},
    {"phrasing": "unité Alzheimer", "label": "unité Alzheimer"},
    {"phrasing": "USLD unité soins longue durée", "label": "USLD"},
]

# --- Transport fret routier ------------------------------------------
# SIRENE 49.41A/B (33,69,75,13,59,67,44,31): "transport(s)" n=900+848 / 1341+1000,
# "express" n=390+730, "logistique" n=99+139.
# Cindy ws1 imports: "Fichier KOMPASS Logistique et Transport 31 - 33.xlsx",
# "Base_Transport.xlsx". CLAUDE.md "TRANSPORT 33" canonical example.
# "transport" alone EXCLUDED — matches taxis/VTC/ambulances/metro/bus.
_TRANSPORT_FRET_VARIANTS = [
    {"phrasing": "transport routier", "label": "transport routier"},
    {"phrasing": "transport de marchandises", "label": "transport marchandises"},
    {"phrasing": "transporteur logistique", "label": "transporteur logistique"},
    {"phrasing": "messagerie express", "label": "messagerie express"},
    {"phrasing": "déménagement", "label": "déménagement"},
    {"phrasing": "affrètement routier", "label": "affrètement"},
    {"phrasing": "fret routier", "label": "fret routier"},
    # Apr 30 expansion:
    {"phrasing": "entreposage logistique", "label": "entreposage"},
    {"phrasing": "commission de transport", "label": "commissionnaire"},
    {"phrasing": "groupage routier", "label": "groupage"},
    {"phrasing": "plateforme logistique", "label": "plateforme logistique"},
]

# --- Arboriculture / Vergers -----------------------------------------
# SIRENE 01.22Z–01.29Z (82,17,40,69,32,47,16,46): "vergers" n=110, "verger"
# n=22, "domaine" n=40, "fruits" n=28, "pepinieres" n=20.
# SIRENE 10.39B: "confitures" n=7, "pruneaux/pruneau" n=6+6, "vergers" n=7.
# Cindy ws1 history: "EXPLOITATIONS ARBORICOLES" — 30 batches across 8 depts
# in last 60 days (53% of her workload).
# "fruit" alone EXCLUDED — too broad (juice bars, primeurs/47.21Z maraîchers).
_ARBORICULTURE_VARIANTS = [
    {"phrasing": "verger", "label": "verger"},
    {"phrasing": "vergers", "label": "vergers"},
    {"phrasing": "exploitation arboricole", "label": "exploitation arboricole"},
    {"phrasing": "arboriculteur", "label": "arboriculteur"},
    {"phrasing": "producteur de fruits", "label": "producteur de fruits"},
    {"phrasing": "pépinière fruitière", "label": "pépinière fruitière"},
    {"phrasing": "culture d'arbres fruitiers", "label": "culture arbres fruitiers"},
    # Apr 30 expansion:
    {"phrasing": "producteur de pommes", "label": "producteur pommes"},
    {"phrasing": "producteur de prunes", "label": "producteur prunes"},
    {"phrasing": "conserverie de fruits", "label": "conserverie"},
]

# --- Boulangerie / Pâtisserie (NEW Apr 30) ---------------------------
# SIRENE 10.71C (75,13,69,33,59,06,31): "boulangerie" n=very high, "pâtisserie"
# n=high. ws174 testing batches BOULANGERIE_75 / BOULANGERIE_66 / BOULANGERIE_33.
# Standalone "pâtisserie" included — 47.24Z is the standalone pastry retail
# code, complementary to 10.71C/D production.
_BOULANGERIE_VARIANTS = [
    {"phrasing": "boulangerie", "label": "boulangerie"},
    {"phrasing": "boulangerie pâtisserie", "label": "boulangerie pâtisserie"},
    {"phrasing": "pâtisserie", "label": "pâtisserie"},
    {"phrasing": "viennoiserie", "label": "viennoiserie"},
    {"phrasing": "artisan boulanger", "label": "artisan boulanger"},
    {"phrasing": "boulangerie traditionnelle", "label": "boulangerie traditionnelle"},
    {"phrasing": "boulangerie bio", "label": "boulangerie bio"},
]

# --- Restauration (NEW Apr 30) ---------------------------------------
# SIRENE 56.10A: "restaurant" n=very high, "brasserie" n=high.
# SIRENE 56.10C: "restauration rapide" n=high, "snack" n=mid.
# SIRENE 56.21Z: "traiteur" n=high.
# "restaurant" alone is fine — 56.10A is dominant for the token.
_RESTAURATION_VARIANTS = [
    {"phrasing": "restaurant", "label": "restaurant"},
    {"phrasing": "restaurant gastronomique", "label": "gastronomique"},
    {"phrasing": "brasserie", "label": "brasserie"},
    {"phrasing": "pizzeria", "label": "pizzeria"},
    {"phrasing": "traiteur", "label": "traiteur"},
    {"phrasing": "restauration rapide", "label": "restauration rapide"},
    {"phrasing": "restaurant traditionnel", "label": "traditionnel"},
]

# --- Pépinières / Horticulture (NEW Apr 30) --------------------------
# SIRENE 01.30Z (49,44,30,13,06): "pépinières" n=high, "horticulture" n=mid.
# SIRENE 47.76Z: "jardinerie" n=high, "fleurs" n=high.
# SIRENE 46.22Z: "fleuriste" n=mid (gros plantes).
# Cindy: "pépinière 49" referenced in naf_sector_expansion.py rationale.
# "fleuriste" alone OK — 47.76Z fleuristes are part of this clique by design.
_PEPINIERES_VARIANTS = [
    {"phrasing": "pépinière", "label": "pépinière"},
    {"phrasing": "pépiniériste", "label": "pépiniériste"},
    {"phrasing": "horticulture", "label": "horticulture"},
    {"phrasing": "jardinerie", "label": "jardinerie"},
    {"phrasing": "fleuriste", "label": "fleuriste"},
    {"phrasing": "plantes ornementales", "label": "plantes ornementales"},
    {"phrasing": "végétaux d'extérieur", "label": "végétaux"},
]


SECTOR_QUERY_VARIANTS: dict[str, list[dict]] = {
    # Vigne clique (4 NAFs, same list — symmetry per naf_sector_expansion.py)
    "01.21Z": _VIGNE_VARIANTS,
    "11.02A": _VIGNE_VARIANTS,
    "11.02B": _VIGNE_VARIANTS,
    "46.34Z": _VIGNE_VARIANTS,

    # Camping (55.30Z)
    "55.30Z": _CAMPING_VARIANTS,

    # Hotels clique (55.10Z, 55.20Z, 55.90Z) — NEW Apr 30, fixes screenshot bug
    "55.10Z": _HOTELS_VARIANTS,
    "55.20Z": _HOTELS_VARIANTS,
    "55.90Z": _HOTELS_VARIANTS,

    # EHPAD pair
    "87.10A": _EHPAD_VARIANTS,
    "87.30A": _EHPAD_VARIANTS,

    # Transport fret 6-clique
    "49.41A": _TRANSPORT_FRET_VARIANTS,
    "49.41B": _TRANSPORT_FRET_VARIANTS,
    "49.41C": _TRANSPORT_FRET_VARIANTS,
    "49.42Z": _TRANSPORT_FRET_VARIANTS,
    "52.29A": _TRANSPORT_FRET_VARIANTS,
    "52.29B": _TRANSPORT_FRET_VARIANTS,

    # Arboriculture 7-clique
    "01.22Z": _ARBORICULTURE_VARIANTS,
    "01.23Z": _ARBORICULTURE_VARIANTS,
    "01.24Z": _ARBORICULTURE_VARIANTS,
    "01.25Z": _ARBORICULTURE_VARIANTS,
    "01.26Z": _ARBORICULTURE_VARIANTS,
    "01.29Z": _ARBORICULTURE_VARIANTS,
    "10.39B": _ARBORICULTURE_VARIANTS,

    # Boulangerie pair (NEW Apr 30)
    "10.71C": _BOULANGERIE_VARIANTS,
    "10.71D": _BOULANGERIE_VARIANTS,
    "47.24Z": _BOULANGERIE_VARIANTS,

    # Restauration clique (NEW Apr 30)
    "56.10A": _RESTAURATION_VARIANTS,
    "56.10C": _RESTAURATION_VARIANTS,
    "56.21Z": _RESTAURATION_VARIANTS,

    # Pépinières/horticulture clique (NEW Apr 30)
    "01.30Z": _PEPINIERES_VARIANTS,
    "01.19Z": _PEPINIERES_VARIANTS,
    "46.22Z": _PEPINIERES_VARIANTS,
    "47.76Z": _PEPINIERES_VARIANTS,
}
