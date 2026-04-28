"""Chain/franchise detector for Fortress discovery pipeline.

Recognizes franchise storefronts (Paul, Franck Provost, McDonald's, Marie Blachère, etc.)
whose SIRENE registrations are under operating-company names (e.g. "SARL JMC COIFFURE").
Standard SIRENE matching misses these because the brand name never appears in the legal name.

NAF sampling performed 2026-04-21 against Neon (14.7M companies):
- paul: 68.20/47.99 dominate (false positives on first name); bakery NAFs absent at top
  → single-token brand requires sector_tokens guard
- franck provost: 96.02A=181 dominant → coiffure NAFs validated
- marie blachere: 10.71C=28, 47.24Z=2, 56.10C=1 → boulangerie NAFs validated
  Note: 00.00Z=5 (unclassified), 10.71A=3 (industrial — excluded per Alan's decision)
- krys: 47.78A=262 dominant → optique NAFs validated
- mcdonald: 56.10C=216 dominant → restauration NAFs validated
- v2 (2026-04-21, camping/hotel/fitness sample):
  * siblu: 66.30Z/64.20Z/68.20A/68.20B — holding+real-estate model, ZERO 55.30Z
  * capfun: 55.30Z=6 → camping NAFs validated
  * huttopia: 55.30Z=55 dominant → camping NAFs validated
  * sandaya: 64.20Z=3, 55.30Z=3 → mixed
  * ibis: 68.20B=365, 55.10Z=295 → hotel NAFs need widening
  * fitness park: 93.13Z=67, 93.11Z=16, 93.12Z=11, 85.51Z=4 → spread across 4 fitness NAFs
  * poivre rouge: 56.10A=23 → restauration_trad NAFs validated
  Conclusion for camping/hotel: widened to include 68.20A/B (real-estate model). Picker's
  branded-enseigne guard filters out HQ/holding rows, so widening is safe.

Public API: match_chain, match_ehpad_pseudo_chain, find_chain_siret, ChainHit, CHAIN_MAP.
"""
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ChainEntry:
    canonical: str                    # normalized: "franck provost"
    aliases: tuple[str, ...]          # raw variants for matching
    nafs: frozenset[str]              # SIRENE NAFs (retail/operational only)
    sector: str                       # logging label
    sector_tokens: frozenset[str] = frozenset()  # required for single-token canonicals


@dataclass
class ChainHit:
    chain_name: str
    nafs: frozenset[str]
    sector: str
    confidence: float
    aliases: tuple[str, ...] = ()


# NAF code sets reused across many chains — defined once for readability.
_NAF_BOULANGERIE = frozenset({"10.71C", "10.71D", "47.24Z", "56.10C"})
_NAF_BOULANGERIE_TIGHT = frozenset({"10.71C", "10.71D", "47.24Z"})
_NAF_COIFFURE = frozenset({"96.02A", "96.02B"})
_NAF_RESTAU = frozenset({"56.10A", "56.10C", "56.21Z"})
_NAF_RESTAU_WIDE = frozenset({"56.10A", "56.10B", "56.10C", "56.21Z", "56.30Z"})
_NAF_OPTIQUE = frozenset({"47.78A"})
_NAF_CAMPING = frozenset({"55.20Z", "55.30Z", "55.90Z", "68.20A", "68.20B"})
_NAF_HOTEL = frozenset({"55.10Z", "55.20Z", "68.20A", "68.20B"})
_NAF_FITNESS = frozenset({"93.11Z", "93.12Z", "93.13Z", "85.51Z"})
# EHPAD: medical (87.10A/B) + non-medical elderly housing (87.30A/B) +
# real-estate rental model (68.20A/B — sampling shows Emeis/Emera/Colisée use this heavily)
# + facilities mgmt (81.10Z — Colisée/Jardins d'Arcadie). Picker filters non-branded rows.
_NAF_EHPAD = frozenset({"87.10A", "87.10B", "87.30A", "87.30B", "68.20A", "68.20B", "81.10Z"})
# Caviste: retail wine/spirits (47.25Z main), wholesale wine (46.34Z), wine-bar hybrid (56.30Z)
_NAF_CAVISTE = frozenset({"47.25Z", "46.34Z", "56.30Z"})
# Arboriculture: grower NAFs (01.24Z pommes/poires/pêches, 01.25Z noix/châtaignes,
# 01.13Z légumes, 01.61Z soutien) + processor/wholesale (10.39B, 46.31Z).
# Low chain-detector ceiling: most growers register under individual farm names
# (EARL/GAEC) not the coop brand. Only 5-6 brands actually surface in SIRENE.
_NAF_ARBO = frozenset({"01.24Z", "01.25Z", "01.13Z", "01.61Z", "10.39B", "46.31Z"})

# Narrow EHPAD set for pseudo-chain (no real-estate codes — 68.20A/B/81.10Z were
# verified to flood with SCIs and produce false positives). Healthcare only.
_NAF_EHPAD_PUBLIC = frozenset({"87.10A", "87.10B", "87.30A", "87.30B", "86.10Z"})

_EHPAD_PREFIX_PATTERNS: tuple[str, ...] = (
    "ehpad",
    "maison de retraite",
)


CHAIN_MAP: tuple[ChainEntry, ...] = (
    # ═══ Boulangerie/pâtisserie ═══
    # 10.71A (industrial bread) and 10.71B (frozen reheat) excluded per Alan's decision:
    # franchise storefronts are retail bakers, NOT industrial producers.
    ChainEntry("paul", (), _NAF_BOULANGERIE, "boulangerie",
               frozenset({"boulangerie", "patisserie", "sandwich"})),
    ChainEntry("marie blachere", ("marie blachère",), _NAF_BOULANGERIE, "boulangerie"),
    ChainEntry("la mie caline", ("la mie câline",), _NAF_BOULANGERIE_TIGHT, "boulangerie"),
    ChainEntry("brioche doree", ("brioche dorée",), _NAF_BOULANGERIE, "boulangerie"),
    ChainEntry("ange", (), _NAF_BOULANGERIE_TIGHT, "boulangerie",
               frozenset({"boulangerie", "patisserie"})),
    ChainEntry("banette", (), _NAF_BOULANGERIE_TIGHT, "boulangerie",
               frozenset({"boulangerie", "patisserie"})),
    ChainEntry("feuillette", (), _NAF_BOULANGERIE_TIGHT, "boulangerie"),
    ChainEntry("le fournil de pierre", (), _NAF_BOULANGERIE_TIGHT, "boulangerie"),
    ChainEntry("boulangerie louise", (), _NAF_BOULANGERIE_TIGHT, "boulangerie"),

    # ═══ Coiffure ═══
    ChainEntry("franck provost", (), _NAF_COIFFURE, "coiffure"),
    ChainEntry("jean louis david", (), _NAF_COIFFURE, "coiffure"),
    ChainEntry("saint algue", (), _NAF_COIFFURE, "coiffure"),
    ChainEntry("jacques dessange", (), _NAF_COIFFURE, "coiffure"),  # must precede "dessange"
    ChainEntry("dessange", (), _NAF_COIFFURE, "coiffure",
               frozenset({"coiffure", "salon"})),
    ChainEntry("vog coiffure", ("vog",), _NAF_COIFFURE, "coiffure"),
    ChainEntry("fabio salsa", (), _NAF_COIFFURE, "coiffure"),
    ChainEntry("coiff co", ("coiff & co",), _NAF_COIFFURE, "coiffure"),
    ChainEntry("camille albane", (), _NAF_COIFFURE, "coiffure"),
    ChainEntry("tchip coiffure", ("tchip",), _NAF_COIFFURE, "coiffure"),
    ChainEntry("shampoo", (), _NAF_COIFFURE, "coiffure",
               frozenset({"coiffure", "salon"})),
    ChainEntry("jean marc joubert", ("jmj",), _NAF_COIFFURE, "coiffure"),

    # ═══ Restauration rapide ═══
    ChainEntry("mcdonalds", ("mcdonald s", "mc donald", "mcdo"), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("burger king", (), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("quick", (), _NAF_RESTAU, "restauration_rapide",
               frozenset({"burger", "restauration", "restaurant"})),
    ChainEntry("kfc", (), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("subway", (), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("dominos pizza", ("domino s pizza", "dominos"), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("pizza hut", (), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("o tacos", ("o'tacos",), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("pitaya", (), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("speed burger", (), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("pizza pai", ("pizza paï",), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("columbus cafe", ("columbus café",), _NAF_RESTAU_WIDE, "restauration_rapide"),
    ChainEntry("five guys", (), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("pret a manger", ("prêt à manger",), _NAF_RESTAU, "restauration_rapide"),
    ChainEntry("bagel corner", (), _NAF_RESTAU, "restauration_rapide"),

    # ═══ Restauration traditionnelle ═══
    ChainEntry("hippopotamus", ("hippo",), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("buffalo grill", (), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("courtepaille", (), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("flunch", (), frozenset({"56.10A", "56.10B", "56.10C"}), "restauration_trad"),
    ChainEntry("leon de bruxelles", ("léon de bruxelles",), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("la boucherie", (), frozenset({"56.10A", "56.10C"}), "restauration_trad"),
    ChainEntry("del arte", (), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("bistrot regent", ("bistrot régent",), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("au bureau", (), frozenset({"56.10A", "56.10C", "56.30Z"}), "restauration_trad"),
    ChainEntry("big mamma", (), frozenset({"56.10A", "56.10C"}), "restauration_trad"),
    ChainEntry("poivre rouge", (), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("3 brasseurs", ("les 3 brasseurs",), _NAF_RESTAU_WIDE, "restauration_trad"),
    ChainEntry("la pataterie", (), _NAF_RESTAU, "restauration_trad"),
    ChainEntry("la taverne de maitre kanter", ("taverne de maitre kanter", "taverne kanter"),
               _NAF_RESTAU_WIDE, "restauration_trad"),
    ChainEntry("tablapizza", (), _NAF_RESTAU, "restauration_trad"),

    # ═══ Optique ═══
    ChainEntry("optic 2000", (), _NAF_OPTIQUE, "optique"),
    ChainEntry("krys", (), _NAF_OPTIQUE, "optique"),
    ChainEntry("afflelou", ("alain afflelou",), _NAF_OPTIQUE, "optique"),
    ChainEntry("grand optical", (), _NAF_OPTIQUE, "optique"),
    ChainEntry("atol", ("atol les opticiens",), _NAF_OPTIQUE, "optique",
               frozenset({"optique", "opticien", "lunettes"})),
    ChainEntry("optical center", (), _NAF_OPTIQUE, "optique"),
    ChainEntry("generale d optique", ("générale d'optique",), _NAF_OPTIQUE, "optique"),
    ChainEntry("lissac opticien", ("lissac",), _NAF_OPTIQUE, "optique"),
    ChainEntry("maison du lunetier", (), _NAF_OPTIQUE, "optique"),

    # ═══ Camping (NEW v2 — target of franchise HQ-leak evidence from D1a) ═══
    # NAFs widened to include 68.20A/B because Siblu (and similar) register
    # franchisees under real-estate rental codes, not 55.30Z camping code.
    # Picker's branded-enseigne rule filters HQ/holding rows out of 68.20B noise.
    ChainEntry("siblu", ("siblu villages",), _NAF_CAMPING, "camping",
               frozenset({"camping", "village", "vacances"})),
    ChainEntry("capfun", (), _NAF_CAMPING, "camping"),
    ChainEntry("huttopia", (), _NAF_CAMPING, "camping"),
    ChainEntry("sandaya", (), _NAF_CAMPING, "camping",
               frozenset({"camping", "parc", "village"})),
    ChainEntry("yelloh village", ("yelloh! village", "yelloh"), _NAF_CAMPING, "camping"),
    ChainEntry("homair", ("homair vacances",), _NAF_CAMPING, "camping",
               frozenset({"camping", "vacances", "mobil"})),
    ChainEntry("les castels", (), _NAF_CAMPING, "camping"),
    ChainEntry("flower campings", ("flower",), _NAF_CAMPING, "camping"),
    ChainEntry("sunelia", ("sunêlia",), _NAF_CAMPING, "camping"),
    ChainEntry("campeole", ("campéole",), _NAF_CAMPING, "camping"),
    ChainEntry("tohapi", (), _NAF_CAMPING, "camping"),
    ChainEntry("marvilla parks", ("marvilla",), _NAF_CAMPING, "camping"),
    ChainEntry("vacances directes", (), _NAF_CAMPING, "camping"),
    ChainEntry("camping paradis", (), _NAF_CAMPING, "camping"),
    ChainEntry("ciela village", ("ciela",), _NAF_CAMPING, "camping",
               frozenset({"camping", "village", "vacances"})),
    ChainEntry("onlycamp", ("only camp",), _NAF_CAMPING, "camping"),
    ChainEntry("vacanceselect", ("vacance select",), _NAF_CAMPING, "camping"),
    ChainEntry("les campings bleus", ("campings bleus",), _NAF_CAMPING, "camping"),

    # ═══ Hôtellerie (NEW v2) ═══
    # NAFs widened for same reason as camping: hotel franchisees register under
    # both 55.10Z (hotels) and 68.20B (real-estate rental model).
    ChainEntry("ibis budget", (), _NAF_HOTEL, "hotellerie"),
    ChainEntry("ibis styles", (), _NAF_HOTEL, "hotellerie"),
    ChainEntry("ibis", (), _NAF_HOTEL, "hotellerie",
               frozenset({"hotel", "hôtel"})),  # must come AFTER "ibis budget"/"ibis styles"
    ChainEntry("novotel", (), _NAF_HOTEL, "hotellerie"),
    ChainEntry("mercure", (), _NAF_HOTEL, "hotellerie",
               frozenset({"hotel", "hôtel"})),
    ChainEntry("sofitel", (), _NAF_HOTEL, "hotellerie"),
    ChainEntry("pullman", (), _NAF_HOTEL, "hotellerie",
               frozenset({"hotel", "hôtel"})),
    ChainEntry("mama shelter", (), _NAF_HOTEL, "hotellerie"),
    ChainEntry("b b hotels", ("b&b hotels", "b&b hôtels", "b and b hotels"),
               _NAF_HOTEL, "hotellerie"),
    ChainEntry("premiere classe", ("première classe",), _NAF_HOTEL, "hotellerie"),
    ChainEntry("campanile", (), _NAF_HOTEL, "hotellerie",
               frozenset({"hotel", "hôtel"})),
    ChainEntry("kyriad", (), _NAF_HOTEL, "hotellerie"),
    ChainEntry("logis hotels", ("logis hôtels", "logis de france"), _NAF_HOTEL, "hotellerie"),
    ChainEntry("best western", (), _NAF_HOTEL, "hotellerie"),
    ChainEntry("adagio", (), _NAF_HOTEL, "hotellerie",
               frozenset({"hotel", "hôtel", "aparthotel", "appart", "résidence"})),
    ChainEntry("citadines", (), _NAF_HOTEL, "hotellerie",
               frozenset({"hotel", "hôtel", "résidence", "aparthotel"})),
    ChainEntry("okko hotels", ("okko",), _NAF_HOTEL, "hotellerie"),

    # ═══ Fitness (NEW v2) ═══
    ChainEntry("basic fit", ("basic-fit",), _NAF_FITNESS, "fitness"),
    ChainEntry("fitness park", (), _NAF_FITNESS, "fitness"),
    ChainEntry("l orange bleue", ("l'orange bleue", "orange bleue"), _NAF_FITNESS, "fitness"),
    ChainEntry("keepcool", ("keep cool",), _NAF_FITNESS, "fitness"),
    ChainEntry("magic form", (), _NAF_FITNESS, "fitness"),
    ChainEntry("cmg sports club", ("cmg", "cmg sports"), _NAF_FITNESS, "fitness"),
    ChainEntry("on air fitness", ("on air",), _NAF_FITNESS, "fitness"),
    ChainEntry("neoness", (), _NAF_FITNESS, "fitness",
               frozenset({"fitness", "sport", "salle"})),
    ChainEntry("waou", (), _NAF_FITNESS, "fitness",
               frozenset({"fitness", "sport", "club"})),
    ChainEntry("vita liberte", ("vita liberté",), _NAF_FITNESS, "fitness"),
    ChainEntry("l appart fitness", ("l'appart fitness", "appart fitness"), _NAF_FITNESS, "fitness"),

    # ═══ EHPAD / Maisons de retraite (NEW v2.1 — Cindy runs this sector) ═══
    # NAF sampling 2026-04-21: Korian/Emeis/DomusVi/Emera all heavily use 68.20B
    # (real-estate rental model), not 87.10A. Widened set mirrors camping pattern.
    ChainEntry("korian", (), _NAF_EHPAD, "ehpad"),
    ChainEntry("emeis", (), _NAF_EHPAD, "ehpad"),  # new name for Orpea
    ChainEntry("orpea", (), _NAF_EHPAD, "ehpad"),  # legacy brand still on many signs
    ChainEntry("domusvi", (), _NAF_EHPAD, "ehpad"),
    ChainEntry("colisee", ("colisée",), _NAF_EHPAD, "ehpad",
               frozenset({"ehpad", "retraite", "maison"})),  # generic word (Roman amphitheater)
    ChainEntry("lna sante", ("lna santé", "lna", "groupe noble age"), _NAF_EHPAD, "ehpad"),
    ChainEntry("emera", (), _NAF_EHPAD, "ehpad",
               frozenset({"ehpad", "retraite", "maison", "senior"})),  # generic-ish, guard
    ChainEntry("repotel", (), _NAF_EHPAD, "ehpad"),
    ChainEntry("residalya", (), _NAF_EHPAD, "ehpad"),
    ChainEntry("argian", ("maisons de famille",), _NAF_EHPAD, "ehpad"),
    ChainEntry("les jardins d arcadie", ("jardins d'arcadie", "jardins d arcadie"),
               _NAF_EHPAD, "ehpad"),

    # ═══ Caviste / Wine retail (NEW v2.1 — covers vignobles search overlap) ═══
    # Arboriculture/viticulture EXPLOITANTS aren't chain-addressable (no brands,
    # individual domaines handled by Step 4b surname extractor). But WINE SHOPS
    # on the retail side ARE chains — Nicolas alone has ~400+ storefronts.
    ChainEntry("nicolas", (), _NAF_CAVISTE, "caviste",
               frozenset({"cave", "vin", "vins", "caviste", "spiritueux", "bouteille"})),
    ChainEntry("v and b", ("v&b", "v b", "vins et bieres", "vins & bieres"),
               _NAF_CAVISTE, "caviste"),
    ChainEntry("cavavin", (), _NAF_CAVISTE, "caviste"),
    ChainEntry("le repaire de bacchus", ("repaire de bacchus",), _NAF_CAVISTE, "caviste"),
    ChainEntry("inter caves", (), _NAF_CAVISTE, "caviste"),
    ChainEntry("les grappes", ("grappes",), _NAF_CAVISTE, "caviste",
               frozenset({"cave", "vin", "vins", "caviste"})),

    # ═══ Arboriculture / Fruit coops (NEW v2.1 — Cindy's 53% workload) ═══
    # CAVEAT: chain detector has a low ceiling here. Grower SIRENs register under
    # individual farm names (EARL/GAEC/SCEA), not the coop/brand. Only the 5-6
    # brands that DO surface in SIRENE legal names are listed here. Real
    # arboriculture lift needs different levers (Step 2 enseigne, address match).
    # NAF sampling 2026-04-21 against Neon:
    #   fruits rouges: 30 hits, 01.24Z=7 + 01.25Z=12 (strong fit)
    #   perlim: 23 hits (mixed NAFs, need sector guard)
    #   terrena: 16 hits (mostly holdings 68.20B, use sector guard)
    #   vergers du sud: 5 hits, 01.24Z=1 (phrase match safer)
    #   blue whale: 8 hits (sparse but distinct brand)
    ChainEntry("blue whale", (), _NAF_ARBO, "arboriculture",
               frozenset({"fruit", "fruits", "pomme", "pommes", "verger", "vergers"})),
    ChainEntry("perlim", (), _NAF_ARBO, "arboriculture",
               frozenset({"fruit", "fruits", "pomme", "pommes", "verger", "vergers", "noix"})),
    ChainEntry("fruits rouges", (), _NAF_ARBO, "arboriculture"),  # multi-token, phrase match
    ChainEntry("vergers du sud", (), _NAF_ARBO, "arboriculture"),
    ChainEntry("cofruid oc", ("cofruid'oc", "cofruid",), _NAF_ARBO, "arboriculture"),
    ChainEntry("terrena", (), _NAF_ARBO, "arboriculture",
               frozenset({"fruit", "fruits", "pomme", "verger", "coop", "cooperative"})),
)


def _tokens_contain_phrase(tokens: list[str], phrase: list[str]) -> bool:
    """Return True if phrase appears as consecutive tokens in tokens."""
    if len(phrase) > len(tokens):
        return False
    for i in range(len(tokens) - len(phrase) + 1):
        if tokens[i:i + len(phrase)] == phrase:
            return True
    return False


def match_chain(maps_name: str) -> "ChainHit | None":
    """Return a ChainHit if maps_name resolves to a known chain brand.

    Multi-token brands: require full phrase as consecutive normalized tokens.
    Single-token brands: require token + >=1 sector keyword co-occurring.
    Longest canonical matches first (prefers 'jacques dessange' over 'dessange').
    """
    from fortress.discovery import _normalize_name  # late import to avoid cycle
    normalized = _normalize_name(maps_name)
    if not normalized:
        return None
    tokens = normalized.split()
    if not tokens:
        return None
    token_set = set(tokens)

    sorted_entries = sorted(CHAIN_MAP, key=lambda e: -len(e.canonical))
    for entry in sorted_entries:
        for pattern in (entry.canonical, *entry.aliases):
            pat_norm = _normalize_name(pattern)
            pat_tokens = pat_norm.split()
            if not pat_tokens:
                continue
            if len(pat_tokens) >= 2:
                if _tokens_contain_phrase(tokens, pat_tokens):
                    return ChainHit(entry.canonical, entry.nafs, entry.sector, 0.95, entry.aliases)
            else:
                single = pat_tokens[0]
                if single in token_set:
                    if not entry.sector_tokens:
                        return ChainHit(entry.canonical, entry.nafs, entry.sector, 0.90, entry.aliases)
                    if token_set & entry.sector_tokens:
                        return ChainHit(entry.canonical, entry.nafs, entry.sector, 0.85, entry.aliases)
    return None


def match_ehpad_pseudo_chain(maps_name: str) -> "ChainHit | None":
    """Detect public EHPAD prefix and return ChainHit with residual local name.

    Examples:
      'EHPAD Bel Air' -> ChainHit(chain_name='bel air', nafs=_NAF_EHPAD_PUBLIC, ...)
      'Maison de Retraite Bel Air' -> ChainHit(chain_name='bel air', ...)
        (both match SIRENE 'EHPAD BEL AIR' via picker's % patterns)
      'EHPAD' -> None (no residual)
      'Camping Bel Air' -> None (no EHPAD prefix)
    """
    from fortress.discovery import _normalize_name  # late import to avoid cycle
    normalized = _normalize_name(maps_name)
    if not normalized:
        return None
    norm_tokens = normalized.split()
    for prefix in _EHPAD_PREFIX_PATTERNS:
        prefix_tokens = prefix.split()
        if len(norm_tokens) <= len(prefix_tokens):
            continue
        if norm_tokens[:len(prefix_tokens)] == prefix_tokens:
            residual = " ".join(norm_tokens[len(prefix_tokens):])
            if len(residual) >= 3:
                return ChainHit(
                    chain_name=residual,
                    nafs=_NAF_EHPAD_PUBLIC,
                    sector="ehpad_public",
                    confidence=0.85,
                    aliases=(),
                )
    return None


def _row_to_candidate(row: Any) -> dict:
    return {
        "siren": row[0],
        "denomination": row[1] or "",
        "enseigne": row[2] or "",
        "score": 0.88,
        "method": "chain",
        "adresse": row[3] or "",
        "ville": row[4] or "",
    }


async def find_chain_siret(
    conn: Any,
    chain_hit: ChainHit,
    maps_cp: str,
) -> "dict | None":
    """Query SIRENE for the chain storefront at maps_cp. Return candidate dict or None.

    Strict picker:
      - 0 rows -> None
      - 1 row -> that row
      - N rows -> exactly one whose normalized enseigne/denomination contains
        the chain canonical token set; else None (ambiguous)
    """
    # Build LIKE patterns from canonical + aliases. Replace spaces with '%' so
    # multi-word brands match across punctuation: "yelloh village" -> '%YELLOH%VILLAGE%'
    # matches enseigne 'CAMPING YELLOH ! VILLAGE - SAINT EMILION'.
    patterns = [chain_hit.chain_name] + list(chain_hit.aliases or ())
    like_patterns = [f"%{p.upper().replace(' ', '%')}%" for p in patterns]

    cur = await conn.execute(
        """SELECT siren, denomination, enseigne, adresse, ville, code_postal, naf_code
             FROM companies
            WHERE naf_code = ANY(%s)
              AND code_postal = %s
              AND statut = 'A'
              AND siren NOT LIKE 'MAPS%%'
              AND (
                UPPER(COALESCE(enseigne, '')) ILIKE ANY(%s)
                OR UPPER(COALESCE(denomination, '')) ILIKE ANY(%s)
              )
            ORDER BY GREATEST(
              similarity(COALESCE(enseigne, ''), %s),
              similarity(COALESCE(denomination, ''), %s)
            ) DESC
            LIMIT 50""",
        (list(chain_hit.nafs), maps_cp, like_patterns, like_patterns,
         chain_hit.chain_name, chain_hit.chain_name),
    )
    rows = await cur.fetchall()
    if not rows:
        return None
    if len(rows) == 1:
        return _row_to_candidate(rows[0])

    from fortress.discovery import _normalize_name  # late import
    canonical_tokens = set(_normalize_name(chain_hit.chain_name).split())
    branded = []
    for r in rows:
        combined = f"{r[2] or ''} {r[1] or ''}"  # enseigne + denomination
        combined_tokens = set(_normalize_name(combined).split())
        if canonical_tokens.issubset(combined_tokens):
            branded.append(r)
    if len(branded) == 1:
        return _row_to_candidate(branded[0])
    return None
