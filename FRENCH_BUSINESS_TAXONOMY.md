# French Business Matcher Taxonomy

*Reference document for the Fortress Maps→SIRENE matcher. April 24, 2026.*

---

## Section 1 — Overview

### Why this document exists

Fortress's Maps→SIRENE matcher catches approximately **60 %** of the companies Google Maps returns on a Cindy-style batch (audit-measured on ws174, April 24, 2026). The remaining 40 % split into two problems:

- **Matcher misses entities that ARE in SIRENE** (~21 % of batch) — the entity exists in the 14.7 M SIRENE snapshot but the cascade fails to find it.
- **Matcher finds a candidate but won't auto-confirm** (~19 % of batch) — pending-review limbo, mostly `fuzzy_name` / `enseigne_weak` with only one corroborating signal.

Historically we have patched these gaps one at a time: a new regex for trailing `EURL`, a franchise HQ blacklist entry, a new `Mas/Château` prefix, a postal-code fallback, etc. Each patch works — but it teaches the matcher about a single sub-case without telling it what the **rest of the French business landscape** looks like. The next edge case is always a surprise.

This document is the **reference map of that landscape**. It catalogues the ten macro-categories of French legal entity our matcher must recognise, links each to the INSEE 4-digit *catégorie juridique* (cat-jur) codes, shows what it looks like on Google Maps vs. in SIRENE vs. on a mentions-légales page, measures our current coverage, and prescribes a matcher strategy. The goal is that every future `/plan` brief can name the category it is targeting, quote the volume share, and pick a strategy that is consistent with what has already been built.

### Stated goal

**Auto-confirm rate ≥ 99 % with the correct NAF code on a Cindy batch.**

Today: ≈ 60 %. Gap: ≈ 39 points. The gap is the sum of:

1. Matcher-internal levers (CP extraction, A2 mentions-légales, CP-restricted disambiguation, chain-detector recall) — tracked in `TASKS.md`.
2. **Structural-category awareness** — this document. Per macro-category, we identify which cat-jur families we do NOT yet recognise and what each costs us in Cindy volume.

### How to use this document

- When designing a new matcher `/plan` brief, first identify **which macro-category** the target entities belong to (Section 2). Read that sub-section end-to-end before drafting the brief. It will list the cat-jur codes, the naming patterns, the mentions-légales conventions, our current coverage, and the recommended next move.
- **The roadmap** (Section 4) is the prioritised queue of new briefs, ordered by Cindy volume impact × complexity. Pick from the top.
- **Cross-cutting concerns** (Section 3) apply to every category: accent normalisation, multi-location businesses, language conventions. Read once, refer back when a category-specific brief touches them.
- If a patch you are about to ship does not fit any category in Section 2, that is a signal to **extend this document first** — adding a 14th category is cheaper than leaving future-you to rediscover it.

### Source primer — the INSEE *catégories juridiques* system

The French statistics institute (INSEE) maintains the official nomenclature of legal entity forms used across the SIRENE registry. It is a three-level hierarchy:

| Level | Positions | Granularity |
|-------|-----------|-------------|
| I   | 9 digits        | Top-level families (1 = personne physique, 5 = société commerciale, 7 = personne morale de droit public, 9 = personne morale de droit privé non commerciale) |
| II  | 41 digits (2-char) | Mid-level groups (54 = SARL, 57 = SAS, 92 = Association) |
| III | 260 active codes (4-char) | Leaf codes (5498 = EURL, 5710 = SAS standard, 9220 = Association loi 1901) |

Our `companies.forme_juridique` column stores the level-III 4-digit code as text. Every 4-digit code in this document links back to the INSEE enumeration — primary source is [xml.insee.fr/schema/cj-enum.html](https://xml.insee.fr/schema/cj-enum.html) (machine-readable complete list) and the human-readable overview is [insee.fr/fr/information/2028129](https://www.insee.fr/fr/information/2028129).

### Matcher architecture — a 30-second primer

The cascade in `fortress/discovery.py` has nine stages in strict priority order. The first stage that returns a candidate short-circuits the rest. Familiarise yourself with this vocabulary — every category section refers to it.

| # | Stage | What it uses | Typical catch rate | `link_method` label |
|---|-------|--------------|--------------------|--------------------|
| 0 | INPI primary (Step 0) | Name → INPI `recherche-entreprises.api.gouv.fr` | High recall, validated by dept/CP overlap | `inpi` |
| 0.5 | Chain detector (Step 0.5) | Brand token + CP → SIRENE storefront | Low recall today (only 2 confirmed historically) | `chain` |
| 1 | Website SIREN (Step 1) | Footer SIREN regex → SIRENE | High precision, blacklist-gated | `siren_website` |
| 2 | Enseigne (Step 2) | Trade-name token match in dept + CP | Dominant method for matched SAS/SARL | `enseigne` / `enseigne_weak` |
| 3 | Phone (Step 3) | Normalised phone match (contacts table) | Unique signal, rarely fires (~5 % coverage) | `phone` / `phone_weak` |
| 4 | Address-first (Step 4) | Street-key + CP match | Good for physical-address stable businesses | `address` |
| 4b | Surname extractor | `DOMAINE/MAS/CHÂTEAU/CAVE/VIGNOBLE/CLOS <surname>` | Agricultural & hospitality only | `surname` |
| 5 | Fuzzy name (Step 5) | Trigram similarity + INPI fallback | Always lands `pending` on its own | `fuzzy_name` |
| A2 | Mentions-légales (Step A2) | Legal name extracted from `/mentions-legales` → INPI | Currently firing zero (bug under investigation) | `inpi_mentions_legales` |
| D1b | Gemini arbiter (D1b Hybrid) | LLM rescue/quarantine of weak/mismatch candidates | Live since Apr 22, 4 confirmed, 43 quarantined | `gemini_judge` / `gemini_quarantine` |

Phase A (`auto_linked_mismatch_accepted`) and Phase B (`auto_linked_inpi_agree`) are **confidence promotions**, not new stages: they look at Step-4b/Step-5 / Step-0 results and decide whether `mismatch`-NAF or `pending`-confidence results are trustworthy enough to auto-confirm.

### Data distribution at a glance

SIRENE contains 14.7 M active French companies. The ten macro-categories below cover **≈ 97 %** of both the SIRENE population and the Cindy matched workload. The remaining 3 % are rare edge categories (cults, mutuelles, syndicats de copropriété, comités d'entreprise) kept in Section 2.J as a single grouped sub-section so nothing is hidden.

---

## Section 2 — Macro categories

Each sub-section is self-contained: you can read one in isolation and design a brief from it. The structure is identical throughout:

1. **Description & cat-jur codes** — what this family is, which 4-digit INSEE codes fall under it.
2. **Volume share** — SIRENE population + Cindy matched-to-date share (ws1).
3. **Maps representation** — what the Maps result typically looks like.
4. **SIRENE representation** — what the SIRENE row typically looks like (denomination, enseigne, NAF).
5. **Mentions-légales representation** — what the legal-notice page typically says.
6. **Current matcher status** — coverage assessment (fully / partial / not handled).
7. **Recommended strategy** — concrete rule(s) to implement.
8. **Edge cases** — known failures that break the rule.
9. **Real-world examples** — SIREN / MAPS samples from our database.

---

### 2.A — Sociétés commerciales classiques

#### Description & cat-jur codes

The standard for-profit commercial companies. Vast majority of French SMBs with a storefront, an online presence, and a Cindy-style B2B profile register here. The family is levels II 54–58:

| Code | Libellé | Notes |
|------|---------|-------|
| 5202 | SNC — Société en nom collectif | Rare, partnership of general partners |
| 5306 | SCS — Société en commandite simple | Rare |
| 5308 | SCA — Société en commandite par actions | Listed big-company niche |
| 5410 | SARL nationale | Historical carve-out |
| 5415 | SARL d'économie mixte | Rare |
| 5498 | EURL — SARL unipersonnelle | Single-partner SARL |
| **5499** | **Autre SARL** | The standard SARL (2.42 M in SIRENE) |
| 5505–5599 | SA à conseil d'administration | Classic corporation, board-governed |
| 5605–5699 | SA à directoire | Two-tier SA |
| **5710** | **SAS — Société par actions simplifiée** | Modern favourite for SMBs (1.83 M) |
| **5720** | **SASU — SAS unipersonnelle** | Single-shareholder SAS |
| 5800 | Société européenne (SE) | European-scope, rare |

See full list [on the INSEE enumeration](https://xml.insee.fr/schema/cj-enum.html).

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | ~30 % of active SIRENEs (5499 alone: 2.42 M; 5710: 1.83 M) |
| Cindy matched | **57 %** (5710=142, 5499=115, 5498 small, 5599=7) |

This is the bread-and-butter of Fortress. If this category breaks, everything breaks.

#### Maps representation

- **Legal name usually visible** but often with variants:
  - `"SARL XXX"` / `"SAS XXX"` / `"XXX SARL"` / `"XXX EURL"` — legal-form token as prefix or suffix
  - Plain trade name (legal form absent) — owner chose commercial identity: `"Le Petit Paris"`, `"Boulangerie Morgant"`, `"Chez Cyril"`
  - Franchise storefront (under commercial-entity legal form) — see 2.H
- **Postal code:** almost always in the Maps address.
- **Phone:** Google displays storefront phone, not siège phone — typically the operational number.
- **Website (if any):** usually the trade-name domain (`lepetitparis-bordeaux.fr`), not the legal name.

#### SIRENE representation

- `denomination` = legal name, all-caps, legal form TOKEN usually present (`"SARL LE PETIT PARIS"`, `"LE PETIT PARIS"`, `"LPP HOLDING"`). The prefix is conventional but inconsistent.
- `enseigne` = trade/commercial name (often `NULL` — only ~15 % of active SIRENEs populate it, but when populated it is Maps-identical).
- `forme_juridique` = 4-digit cat-jur code (most common: `5499` and `5710`).
- `naf_code` = operational sector (not legal status). Varies by activity.

#### Mentions-légales representation

Per French law any company website must publish a *mentions légales* page (art. 6-III of the 2004 LCEN law). For 54xx/57xx entities the typical pattern is:

```
Société [XXX], Société à responsabilité limitée au capital de [nnnn] €,
Siège social : [adresse]
Immatriculée au RCS de [ville] sous le numéro [9 digits].
```

Our A2 lever extracts the legal name via regex on headers like `"Société "`, `"SARL "`, `"dénomination sociale"`, `"Éditeur :"`. The 9-digit RCS number is also often present — caught by our Step 1 `siren_website` regex.

#### Current matcher status

**Mostly handled.** Step 2 (enseigne match) is the workhorse here, backed up by Step 4 (address) and Step 1 (website SIREN). The `_LEGAL_FORM_TOKENS` frozenset already strips SARL/SAS/EURL/SASU before comparison. Phase A + Phase B auto-confirms the remaining mismatch cases when two signals agree.

Coverage: all matched Cindy examples in this category are `confirmed`. Apr 24 audit showed 100 % auto-confirm on 5710/5499 subset (N=257 each).

**Known drip-loss:**

- Trailing-only legal-form names (`"La Rivière EARL"`, `"Les Coteaux de Moissac SARL"`) — the `_normalize_name` regex does strip them, but some edge cases with punctuation remain (7 ws1-maps-only instances spotted April 24).
- Initialism-heavy names (`"M.G. SARL"`, `"LBVL"`, `"CDS"`) — below fuzzy-match threshold.
- Bilingual/foreign-language commercial names in border departments.

#### Recommended strategy

- **No new primary strategy needed.** The category is the happy path.
- **Regression-guard:** every matcher change must preserve Step-2 enseigne performance on this category. Run a regression batch (camping 33 / hôtel 69 / boulangerie 75) after any Step-1/2/3 code change and compare auto-confirm % against baseline.
- **Small polish tickets** (see Roadmap):
  - Harden legal-form stripping for bare `"EURL"`, `"SARL"`, `"SAS"` tokens anywhere in the name.
  - Improve SA variant handling (5505–5699) — small volume but SIRENE denominations often include the governance variant in-name (`"SA à directoire XXXXX"`).

#### Real-world examples

- `5710 | SAS JEAN PIERRE OUVRARD TRANSPORTS | — | Transport routier de fret interurbain`
- `5499 | LE P'TIT GALIBIER | — | Restauration de type rapide`
- `5498 | EURL LYDIE | — | (no NAF)`
- `5710 | AGILE GROUP | AGILE GROUP | Activités de sociétés holding`
- `5499 | LAU.CAS | LE HAVANA CAFE | Débits de boissons` ← trade-name bar, enseigne differs

---

### 2.B — Entrepreneurs individuels (code 1000 + legacy 1100-1900)

#### Description & cat-jur codes

The single largest family in SIRENE by count: a French natural person operating a business *personally*, not through a company shell. The 2023 INSEE reform replaced the previous multi-code split (1100 artisan-commerçant, 1200 commerçant, 1300 artisan, 1400 officier public, 1500 profession libérale, 1600 exploitant agricole, 1700 agent commercial, 1800 associé-gérant, 1900 autre personne physique) with a single code `1000 Entrepreneur individuel`. Legacy codes are retained for historical accuracy.

| Code | Status | Notes |
|------|--------|-------|
| **1000** | Current (since 2023) | Generic — 5.58 M active SIRENEs |
| 1100–1900 | Legacy (pre-2023) | Retained on historical records |

Includes the legal status formerly known as *auto-entrepreneur* (micro-entrepreneur) and the short-lived EIRL — the 2023 reform unified all of these under 1000.

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | **~38 %** of active SIRENEs — the single largest category (5.58 M) |
| Cindy matched | **14 %** (63 / ~450 confirmed) |

Note the **ratio**: 1000 is 38 % of the SIRENE universe but only 14 % of Cindy's confirmed matches. That is a matcher gap — micro-entrepreneurs are almost certainly *over-represented* in Google Maps results relative to what we catch, because half of France's small retail is registered as 1000 and owner names are on storefront signs.

#### Maps representation

- **Trade name / commercial identity almost always shown.** Examples: `"Coiffeur Martinière"`, `"Le pain d'Emmeline"`, `"Chez Sophie"`, `"Aux graines buissonnières"`.
- **The person's own name is rarely visible** on Maps — owner chose a commercial identity.
- Phone and address present.

#### SIRENE representation

- `denomination` = the person's given name + family name in all caps: `"THIERRY JANOYER"`, `"MARIE HONNORAT"`, `"CLAUDE DANSE"`. No commercial name attached.
- `enseigne` = the trade name when declared (only ~15 % do — frustratingly). Example: `"BETRANCOURT"` (pharmacy).
- `forme_juridique = '1000'`.
- `naf_code` = sector-appropriate (often still the generic `"Location de terrains et d'autres biens immobiliers"` when the person is a landlord, which bulks the SIRENE numbers).

#### Mentions-légales representation

When a micro-entrepreneur has a website, mentions légales typically shows:

```
Éditeur du site : [Nom Prénom], entrepreneur individuel
SIRET : [14 digits]
Adresse : [domicile du dirigeant]
```

**Critical:** the displayed address is often the **person's home**, not the storefront. Example surfaced in Apr audit: "L'Avenue" salon in Lyon, SIRENE siège address = owner's home 200 km away.

#### Current matcher status

**Weakest category by design.** The matcher has no reliable way to map `"Le pain d'Emmeline"` to `"EMMELINE DUPONT"` without external context. Step-2 enseigne match fires only on the ~15 % of 1000 entries that declared a trade name. Step-3 phone match works when the person declared their phone in a prior SIRENE contact. Step-4 address match works when the person operates from home. Step 0 (INPI) sometimes hits because INPI indexes trade names.

For the 85 % without declared trade name, the only hope is a correlation chain we do not yet build.

#### Recommended strategy

- **Primary lever: INPI Recherche Entreprises enrichment.** INPI's free `recherche-entreprises.api.gouv.fr` returns `matching_etablissements[].nom_commercial` and `matching_etablissements[].enseigne_1` which are sometimes populated where SIRENE is not. Step 0 already queries by name — broadening to query by `"[trade_name] [commune]"` when Step 0 returns no hit might surface these. **Expected lift: +2-3 pp** on Cindy batches.
- **Secondary lever: Pappers-free-tier / Annuaire-Entreprises enrichment.** Explicitly rejected per Alan's April 24 directive: no new paid data. Skip until internal levers exhausted.
- **Tertiary lever: pattern-recognise the SIRENE-side "NOM PRÉNOM" denomination.** When Step 2 enseigne match finds a single "given-name surname" row within a matching NAF at the maps CP and the Maps name tokens overlap with the surname → escalate to auto-confirm. Carry a new `link_method = "individual_name"` if needed.
- **Fourth lever: crawl-and-extract from the storefront website.** If a Maps business has a website, a mentions-légales page often reveals the owner's legal name even when SIRENE has only the anonymous legal name. This is A2's job — A2 is currently broken (fires zero times, under investigation — see `TASKS.md` TOP PRIORITY 2).

#### Edge cases

- **Multiple micro-entrepreneurs per home address.** SCIs and 1000-landlords share apartment buildings. Address-only match over-matches wildly for this category.
- **Home-address micro-entrepreneurs.** Impossible to match on address when the declared address is a residential street far from the Maps-shown storefront.
- **Post-2023 conversions.** Pre-2023 1100–1900 entities were auto-migrated to 1000 but their metadata (SIRET, declarations) was retained. The denomination format is unchanged but the legal-form-code lookup now consistently returns 1000.

#### Real-world examples

From current SIRENE sample:
- `1000 | THIERRY JANOYER | — | (no NAF)` ← legacy inactive individual
- `1000 | CHRISTIAN CHALLIER | — | Culture de céréales` ← farmer operating as EI
- `1000 | JEAN REPAUX | — | Hôtels et hébergement similaire` ← hotelier operating as EI

From ws1 Maps-only (matcher missed):
- `MAPS | Coiffeur Martinière | — | 14 Rue Hippolyte Flandrin, 69001 Lyon` ← trade name with no legal-form token, no chain match
- `MAPS | Le pain d'Emmeline | — | Paris 11e` ← likely individual baker, trade name unique
- `MAPS | Aux graines buissonnières | — | St-Aignan-Grandlieu` ← likely individual retailer


---

### 2.C — Exploitations agricoles (EARL / GAEC / SCEA / CUMA / Coopératives agricoles)

#### Description & cat-jur codes

Agricultural operators — farms, vineyards, orchards, livestock, forestry. Several legal forms coexist, each with a specific tax and governance regime.

| Code | Libellé | SIRENE count | Notes |
|------|---------|--------------|-------|
| **6598** | **EARL — Exploitation agricole à responsabilité limitée** | 84 K | Single- or multi-person farming entity |
| **6597** | **SCEA — Société civile d'exploitation agricole** | 48 K | Civil-law farming entity |
| 6532 | SICA — Société civile d'intérêt collectif agricole | small | Niche |
| **6533** | **GAEC — Groupement agricole d'exploitation en commun** | 43 K | Multi-family farm co-op |
| **6534** | **GFA — Groupement foncier agricole** | 36 K | Land-ownership vehicle |
| 6535 | GAF — Groupement agricole foncier | very small | |
| 6536 | Groupement forestier | 9 K | Forestry holding |
| 6537 | Groupement pastoral | very small | Transhumance / common pasture |
| 6538 | GFR — Groupement foncier rural | 2 K | Mixed farm-forest |
| 6539 | SCF — Société civile foncière | small | Land-only civil entity |
| **6316** | **CUMA — Coopérative d'utilisation de matériel agricole en commun** | 12 K | Farmer-owned equipment pool |
| **6317** | **Société coopérative agricole** | 12 K | Producer-owned aggregator (e.g., LIMAGRAIN) |
| 6318 | Union de sociétés coopératives agricoles | small | Federation |
| 1000 | Entrepreneur individuel (exploitation agricole) | — | Farmer-as-individual |
| 1600 (legacy) | Exploitant agricole | — | Pre-2023 farmer-as-individual code |

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | ~2 % (250 K total across all codes) |
| Cindy matched | **≈ 10 %** (EARL=26, SCEA=9, GAEC=2, coop agri=3 → 40 of ~450) |
| Cindy *query* volume | **> 50 %** of batches (arboriculture queries dominate — see TASKS.md North Star memory) |

**Mismatch warning:** Cindy runs 50+% of her batch queries against arboriculture/agricultural sectors, but only 10 % of her confirmed matches are agricultural-coded. This is a signal that the matcher is underperforming on Cindy's bread-and-butter work. See Roadmap for why.

#### Maps representation

- **Naming patterns wildly inconsistent:**
  - Legal-form prefix: `"EARL Leprêtre"`, `"GAEC des Trois Chênes"`
  - Legal-form suffix: `"La Rivière Earl"`, `"Les Coteaux de Moissac SARL"` (trailing, no leading token)
  - No legal-form token at all — place-name farm: `"Ferme de Couize"`, `"Ferme des 8 vaches"`, `"Ferme de Pitory"`
  - Prestigious prefixes: `"Domaine de ..."`, `"Château ..."`, `"Vignoble ..."`, `"Mas ..."` (overlap with category 2.H below — treat as 2.H for matching)
  - Family-name farms: `"Maison Cantarel"`, `"Ferme Lou Ranquet LARROQUE GERARD"`
  - CUMA-specific: `"CUMA [Name]"` — equipment pool, only local farmers know it
  - Producer-coop: `"Fruits Rouges"`, `"Perlim"`, `"Terrena"`, `"Vergers du Sud"` — branded wholesale

#### SIRENE representation

- `denomination` typically contains the legal form token: `"EARL LES COTEAUX"`, `"GAEC DES TROIS CHÊNES"`, `"SCEA DOMAINE ..."`, `"SARL LES VOLAILLES DU PAYS DE BROCÉLIANDE"`.
- `enseigne` — often absent.
- `naf_code` — the 01.xx family (01.11Z céréales, 01.13Z légumes, 01.21Z vigne, **01.24Z fruits à pépins et à noyau** ← Cindy's target, 01.25Z noix/châtaignes, 01.30Z reproduction de plantes, 01.41Z élevage bovins lait, 01.47Z volailles), plus 10.xx if the farm also processes (10.71C boulangerie industrielle = impossible, but 10.41A huilerie = possible).
- `forme_juridique` per table above.
- Address: SIRENE siège often = owner's private dwelling, even when the farm has its own on-site address on Maps.

#### Mentions-légales representation

Small farms rarely have a website. When they do, the mentions-légales structure follows the commercial-company template (2.A pattern) with the agricultural form in place of SARL/SAS. Cooperatives and larger operations (`Terrena`, `Perlim`) have proper corporate websites with standard mentions-légales.

#### Current matcher status

**Partial.**

- `_LEGAL_FORM_TOKENS` recognises `earl`, `gaec`, `scea`, `scev`, `sci`, `sarl`, `sas`, `sasu`, `eurl`, `sa`, `snc`, `eirl`, `ei`. ✅ covers main legal-form prefixes at start.
- `_SURNAME_PREFIXES` recognises `domaine, mas, chateau, cave, vignoble, clos`. ✅ covers high-prestige vineyard naming.
- **MISSING:** `ferme` prefix (33 ws1+ws174 maps-only instances on Apr 24 — the single largest prefix gap in the hit-list).
- **MISSING:** `maison` prefix (artisanal/farm context).
- **PARTIAL:** Chain detector arboriculture entries exist (`Blue Whale`, `Perlim`, `Fruits Rouges`, `Vergers du Sud`, `Cofruid'Oc`, `Terrena`) but only match 6 brands against a very branded wholesale scene.
- **MISSING:** CUMA-specific match path (CUMA are rarely on Google Maps — likely fine to deprioritise).
- **MISSING:** Trailing-legal-form handling (e.g. `"La Rivière Earl"` — the current regex handles it as a strippable token but the matcher may not search by the non-legal-form remainder).

#### Recommended strategy

- **Lever 1 (quick win, ~1 h):** Extend `_SURNAME_PREFIXES` to include `ferme`, `maison`, `villa`, `bastide`, `moulin`, `manoir`. Validate on ws1+ws174 maps-only sample first. **Expected lift: 30-40 additional auto-confirms.**
- **Lever 2 (medium, ~3 h):** Dedicated `"legal-form-suffix"` path — a regex pass that detects `<name> (EARL|GAEC|SCEA|SARL|SAS|EURL)$`, strips the token, and re-runs the cascade on the leading name with the appropriate cat-jur as a NAF-gate hint. Would unlock 7-12 farms right now, more as data grows.
- **Lever 3 (longer, ~6 h):** Add a "farmer surname extractor" that handles `"Ferme LOU RANQUET LARROQUE GERARD"` — extract surname (last capitalised token), look up the corresponding 1000/1600 entity in SIRENE at the same CP. Requires careful volume-sampling (could add many low-confidence matches).
- **Lever 4:** Expand chain detector's arboriculture set. Add `Innatura`, `La Morella`, `Les Vergers Boiron`, `Le Verger Sauvage`, `Pomone`, `France Prune`, `Les Crudettes`, `Vergers Est`, `Limagrain`. Requires NAF sampling each before adding.

#### Edge cases

- **Cooperatives with place-name identity.** `"Union des Crus Classés de Graves"` — this is an 8420 syndicat patronal, not an agricultural producer. Cat-jur code determines the right matcher branch.
- **Sole-trader farmers who use a farm-like trade name.** `"Ferme de Richagneux"` is often owned by one person, registered as 1000 — not 6598. The matcher must fall through EARL-expectation to 1000 if 6598 returns no hit at the CP.
- **Multi-farm GAECs.** A single GAEC can operate across 2-3 physical farms with different Maps addresses. SIRENE stores only the siège. Phase 3 (Frankenstein dual storage — see `TASKS.md` PRIORITY 2) will unlock this.

#### Real-world examples

Confirmed matches in ws1:
- `6598 | EARL LES COTEAUX | — | Culture de la vigne`
- `6533 | GAEC DES TROIS CHÊNES | — | Élevage bovins`
- `6317 | LIMAGRAIN | — | Coopérative agricole`

Maps-only (matcher missed) in ws1:
- `MAPS | Ferme de Couize | — | 11330 Palairac`
- `MAPS | Ferme des 8 vaches | — | 11330 Mouthoumet`
- `MAPS | La Rivière Earl | — | 82370 Nohic` ← trailing legal-form token
- `MAPS | GAEC des Vergers de la Chana | — | 42320 Cellieu` ← starts with recognised GAEC token but still miss
- `MAPS | Cuma Adour Proteoil | — | 40250 Mugron` ← CUMA prefix, 6316

---

### 2.D — Sociétés civiles immobilières (SCI & groupements fonciers)

#### Description & cat-jur codes

Vehicles for owning real-estate assets (residential, commercial, agricultural land). Not "businesses" in the B2B lead-gen sense — they are wrappers around property. Crucial because Cindy's Maps results occasionally surface SCIs disguised as commercial storefronts (when an SCI owns the building *and* runs the boutique).

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| **6540** | **SCI — Société civile immobilière** | **1.89 M — 3rd largest category** |
| 6541 | SCI de construction-vente | 48 K |
| 6542 | SC d'attribution | small |
| 6543 | SC coopérative de construction | small |
| 6521 | SCPI — Société civile de placement collectif immobilier | small |
| 6588 | Société civile laitière | small |
| 6589 | SCM — Société civile de moyens | 40 K (shared-expense vehicle for professions libérales) |
| 6595–6599 | Autres sociétés civiles (incl. `6599 Autre SC`) | ~380 K combined |

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | **~16 %** of active SIRENEs (2.4 M combined SCI family) |
| Cindy matched | **< 1 %** (2 confirmed on 6540, negligible on siblings) |

**The 16-vs-1 % gap is the right answer:** SCIs don't operate storefronts and don't appear on Google Maps. Cindy's batches never target them directly. The ones we do match are usually false-positives where an SCI happens to share a building with a Cindy-targeted storefront.

#### Maps representation

Almost never on Maps directly. When they appear, it is usually because:
- A business operator registered their SCI as the Google Business entity by mistake.
- A B&B / holiday rental registered under the owning SCI name.

#### SIRENE representation

- `denomination` = `"SCI [surname]"`, `"SCI [address]"`, `"SCI LES [plural noun]"` — very generic, thousands of `"SCI LES PINS"` exist across France.
- `enseigne` = usually NULL.
- `naf_code` often `68.20A` / `68.20B` (location de biens immobiliers).

#### Mentions-légales representation

Seldom relevant — SCIs rarely have websites. When they do, the mention is typically a short legal notice on the commercial operator's site referencing the SCI as owner.

#### Current matcher status

**Not a target.** No specific logic. The danger is false-positive — see Edge cases.

#### Recommended strategy

- **Primary goal: filter out SCI false-positives**, not match to them.
- **Heuristic:** if a Step-1 `siren_website` match lands on a 6540/6541 SIREN whose NAF is 68.20A/B, and the Maps name does not contain "SCI" or "société civile", **downgrade** the match to `pending` (new `link_method = "sci_suspected"`). Let Alan / Cindy confirm if it is genuinely an SCI storefront.
- **Secondary (low priority):** add an explicit 6540 code path **only if** we start discovering SCIs are eating our auto-confirm accuracy. Today they aren't.

#### Edge cases

- The `siren_website` footer SIREN sometimes leaks the owning SCI's SIREN instead of the operating company's (e.g., building owner's SCI on a rental property's website). Our `siren_website` blacklist already includes a handful of known "hosting" SIRENs (Apr 23 ship `bea0a8b`) — this pattern is the extension.
- SCMs (6589) are a different beast: they pool resources (rent, receptionist) for groups of médecins or avocats. Matcher should NOT confuse them with the medical/legal practice itself (2.I).

#### Real-world examples

- `6540 | SCI LES PINS | — | Location de terrains`
- `6540 | SCI VINCENDON PCS | — | Location de logements`
- `6589 | SCM CABINET DENTAIRE JERENATH | — | (shared-expense dental office)`

---

### 2.E — Collectivités territoriales & EPCI

#### Description & cat-jur codes

Elected-government entities — towns, departments, regions, and the intercommunal cooperation structures built on top. These are *not* companies in the normal sense but they have SIRENs and are present on Google Maps as "mairies", "conseils départementaux", "communautés de communes", etc.

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| **7210** | **Commune** | **35 K** (one per commune in France) |
| 7220 | Département | 101 |
| 7225 | Territoire d'Outre-Mer | very small |
| 7229 | Autre collectivité territoriale | small |
| 7230 | Région | 13 |
| 7312 | Commune associée | small |
| 7313 | Section de commune | small |
| 7314 | Ensemble urbain | small |
| 7343 | Communauté urbaine | small |
| 7345 | SIVOM | small |
| 7346 | Communauté de communes | ~1 K |
| 7348 | Communauté d'agglomération | ~200 |
| 7353 | SIVU | 4 K |
| 7354 | Syndicat mixte communal | small |
| 7355 | Autre syndicat mixte | small |

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | ~0.3 % (40 K combined) |
| Cindy matched | **2 %** (7210=7, 7348=3, 7354=2, etc.) |

Low-volume category, but extremely bad to mis-match: linking a café to the `Mairie de Vitré` is the sort of error Cindy notices immediately.

#### Maps representation

- `"Mairie de [Commune]"` / `"Hôtel de Ville"` / `"Conseil Municipal"`.
- `"Conseil Départemental de [Département]"`, `"Conseil Régional"`.
- `"Communauté de communes du [Pays de ...]"`, `"Communauté d'agglomération [Nom]"`.
- Phone, address usually clean.

#### SIRENE representation

- `denomination` = `"COMMUNE DE [UPPERCASE NAME]"`, `"DEPARTEMENT DE LA [...]"`, `"REGION [...]"`, `"COMMUNAUTE DE COMMUNES [...]"`.
- `enseigne` sometimes = `"MAIRIE DE [NAME]"`.
- `naf_code` = `84.11Z Administration publique générale` (almost always). That is a strong disambiguator.

#### Mentions-légales representation

Public-sector websites have standardised legal notices citing the collectivity name + SIREN. Format is consistent.

#### Current matcher status

**Not explicitly handled.**

Current behaviour: Step 0 (INPI) can find them by name — `"Mairie de Vitré"` → INPI returns the right SIREN. Step 2 enseigne can match on `"MAIRIE DE VITRÉ"`. Auto-confirm depends on Phase A / Phase B paths passing. No NAF-gate exemption for 84.11Z.

7 matches confirmed in ws1 (Cindy occasionally batch-searches municipalities in context of other sectors).

**Known drip-loss:** none documented — low Cindy priority. But a mis-match (matching a commune to a commercial company) would be highly visible.

#### Recommended strategy

- **No new primary lever** — low volume, happy path is adequate.
- **Guardrail:** add an explicit rejection in the matcher if Maps name contains `"Mairie"` / `"Conseil Municipal"` / `"Hôtel de Ville"` and the candidate SIRENE NAF is **not** `84.11Z` or the candidate's cat-jur is not in {7210, 7220, 7230, 7312–7355}. Today nothing prevents `"Mairie de Vitré"` from being matched to some commercial company by enseigne token overlap.
- **Nice-to-have (low priority):** explicit 7xxx-aware cascade short-circuit — if a Maps name contains a cat-jur-7210-shaped pattern, skip to a dedicated 7210-lookup by commune name.

#### Edge cases

- `"Maison de Ma Région [Département]"` — these are outposts of a Région (7230) offering citizen info. 1 instance in ws1 maps-only (Apr 24). Likely sub-entity with its own SIREN at the region SIREN — verify.
- `"Bureau d'Information Touristique de ..."` — typically a SPL (société publique locale — cat-jur in the 4140 EPIC local family) — different category.
- Syndicats mixtes (7354/7355) have opaque names (`"SYMAT"`) and are hard to parse.

#### Real-world examples

- `7210 | COMMUNE DE VITRÉ | MAIRIE DE VITRÉ | 84.11Z`
- `7348 | COMMUNAUTÉ D'AGGLOMÉRATION DU PAYS DE BRIVE | CA DU PAYS DE BRIVE | 84.11Z`
- `7230 | REGION NOUVELLE-AQUITAINE | — | 84.11Z`
- `MAPS | Ambérieu-en-Bugey | — | no address` ← maps_only, likely user error (just a city name, not a business)


---

### 2.F — Établissements publics (locaux + nationaux)

#### Description & cat-jur codes

Public institutions that operate services (hospitals, universities, social services, HLM offices, firefighters, chambers of commerce). Many are Cindy-targeted when she batches `ephad 75`, `ephad 66000`, etc. — because the public EHPAD share the storefront-and-address profile with private ones.

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| 4110 | EPIC national doté d'un comptable | small |
| 4140 | EPIC local | ~5 K |
| 4150 | Régie locale à caractère industriel | small |
| **7361** | **CCAS — Centre communal d'action sociale** | **13 K** |
| 7362 | Caisse des écoles | 4 K |
| 7363 | Caisse de crédit municipal | small |
| **7364** | **Établissement d'hospitalisation public (CHU / CH)** | 5 K |
| 7365 | Syndicat inter-hospitalier | small |
| **7366** | **Établissement public local social et médico-social (EHPAD public)** | 5 K |
| **7371** | **Office public d'habitation à loyer modéré (OPHLM)** | ~1 K |
| 7372 | SDIS — Service départemental d'incendie (pompiers) | 100 |
| 7373 | Établissement public local culturel (bibliothèques, musées) | small |
| 7378 | Régie d'une collectivité locale à caractère administratif | small |
| 7379 | Autre EPA local | small |
| **7381** | **Organisme consulaire (CCI, CMA)** | 200 |
| 7382 | EPN fonction d'administration centrale | small |
| **7383** | **EPN scientifique culturel et professionnel (universités, grandes écoles)** | ~2 K |
| 7384 | Autre EPN d'enseignement | small |
| 7389 | Autre EPN administratif | small |
| 7410 | GIP — Groupement d'intérêt public | 2 K |

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | ~0.25 % (~35 K combined) |
| Cindy matched | **2 %** (7366=8 EHPAD-public, 7361=1 CCAS, 4140=5) |
| Cindy *query* volume | Higher — her `ephad 75` and `ephad 66000` batches target public & private interchangeably |

#### Maps representation

- **Hospitals / EHPAD public:** `"EHPAD [Name]"`, `"E.H.P.A.D. [Name]"`, `"Centre hospitalier [...]"`, `"Hôpital [...]"`, `"Résidence [...]"`, `"Maison de retraite publique"`.
- **HLM offices:** `"OPH [ville]"`, `"Habitat [département]"`.
- **CCAS:** `"CCAS de [commune]"`, `"Centre Communal d'Action Sociale"`.
- **CCI / CMA:** `"CCI [département]"`, `"Chambre de Métiers [...]"`.
- **Universités:** `"Université [...]"`, `"IUT [...]"`, `"ÉSC [...]"`.

#### SIRENE representation

- `denomination` = `"CCAS DE [COMMUNE]"`, `"CENTRE HOSPITALIER DE [...]"`, `"EPSMS [...]"`, `"OFFICE PUBLIC HABITAT DE [...]"`. Standardised formal names.
- `enseigne` sometimes = the popular short name (`"HÔPITAL SAINT-LOUIS"`).
- `naf_code`: `84.11Z` (public admin), `86.10Z` (hospitals), `87.10A` (EHPAD medical), `87.30A` (senior non-medical housing), `85.42Z` (higher ed), `84.25Z` (pompiers).

#### Mentions-légales representation

Legally required and well-maintained for public entities. Full name, SIREN, address, responsible-person name all clearly listed.

#### Current matcher status

**Partial.**

- Step 0 (INPI) handles the bulk — INPI knows all public entities.
- Step 2 (enseigne) works when Maps and SIRENE use the same standard name.
- **EHPAD specifically:** chain detector `_NAF_EHPAD` frozenset covers brand chains (Korian, Emeis/Orpea, DomusVi, Colisée, LNA Santé, Emera, Repotel, Residalya, Argian, Les Jardins d'Arcadie — 11 chains). Does **not** cover public EHPAD which are usually named `"EHPAD [Commune]"` or `"Résidence Personnes Âgées [...]"`. 49 ws1+ws174 maps-only hits have EHPAD-prefix.
- **No explicit rule for CCAS / OPHLM / universities.**

#### Recommended strategy

- **Lever 1 (quick, ~2 h):** Extend chain detector with a "public EHPAD" pseudo-chain — where the `chain_name` is the literal string `"ehpad"` and the NAF set is `{87.10A, 87.10B, 87.30A, 87.30B, 7366 cat-jur via a separate filter}`. Requires adapting `find_chain_siret` to accept a cat-jur filter, not just NAF. Expected to recover 20-30 of the 49 EHPAD-prefix maps-only entities.
- **Lever 2 (medium, ~2-3 h):** Add a "public prefix" token list: `mairie, commune, ccas, ehpad, hôpital, centre hospitalier, residence personnes agees, rpa, cci, cma, universite, iut`. When detected in Maps name, bias the cascade toward the matching cat-jur code space and the 84.11Z / 86.xx / 87.xx NAFs.
- **Lever 3 (deeper):** Consider `forme_juridique`-aware filtering in Step 0 (INPI) — filter INPI results by expected cat-jur family when the Maps name has a structural hint.

#### Edge cases

- **EHPAD private/public ambiguity.** `"EHPAD Alquier-Debrousse"` in 75020 Paris is ws174 maps-only. Could be either `9220 Association` managed, `5499 SARL` private, or `7366 EPLSMS` public. The 75020 catchment needs disambiguation.
- **CCI dual identity.** CCI websites list both the 7381 chamber SIREN (national registry entry) and operational SIRENs for individual services. Don't collapse.
- **University complexes.** A large university has many SIRENs (central + CPGE + IUT + CROUS). Step 0 will usually pick the central one — acceptable but note it.

#### Real-world examples

Confirmed in ws1:
- `7366 | EPLSMS LE MOULIN | EHPAD LE MOULIN | 87.10A`
- `4140 | SEM VITRE TOURISME | — | (local tourism)`
- `7361 | CCAS DE BORDEAUX | — | 84.11Z`

Maps-only in ws174:
- `MAPS | EHPAD Alquier Debrousse | — | 75020 Paris` ← matcher missed — investigate private/public
- `MAPS | E.H.P.A.D. Le Moulin | — | 63000 Clermont-Ferrand` ← dots should be stripped before matching
- `MAPS | Résidence Personnes Agées - LA TOUR ARPAD | — | 66200 Latour-Bas-Elne` ← public RPA, no prefix match

---

### 2.G — Associations & fondations (loi 1901, loi 1908 Alsace-Moselle, fondations)

#### Description & cat-jur codes

Non-profit entities. Two legal regimes: *loi 1901* (most of France) and *loi 1908* (Alsace-Moselle). Fondations are a separate construct.

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| 9210 | Association non déclarée | small |
| **9220** | **Association déclarée (loi 1901)** | **1.24 M — 4th largest category in SIRENE** |
| 9221 | Association déclarée "entreprises d'insertion par l'économique" | small |
| 9222 | Association intermédiaire | small |
| 9223 | Groupement d'employeurs | 10 K |
| 9230 | Association déclarée reconnue d'utilité publique | 2 K |
| 9240 | Congrégation | small (see 2.J) |
| **9260** | **Association de droit local (Alsace-Moselle, loi 1908)** | **35 K** (concentrated in dept 57, 67, 68) |
| **9300** | **Fondation** | 5 K |

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | **~8.6 %** (1.28 M combined — massive by count) |
| Cindy matched | **6 %** (27 / ~450, all 9220) |

Cindy's 6 % is driven by sector overlap — associations often run EHPADs, tourism offices, agricultural fairs, and social-economy businesses she catches in her batches.

#### Maps representation

- **Prefix `"Association "`, `"Asso "`, `"ASS "`** — common but not universal.
- Acronym names: `"ANAS"` (Association Nationale d'Assistance aux Sourds), `"AFPA"`, `"APAJH"`.
- Sector-activity names without association prefix: `"Village Vacances ANAS Rivesaltes"`, `"Les Amis de [lieu]"`, `"Maison des Jeunes et de la Culture"`, `"MJC"`, `"Centre social"`, `"Comité des Fêtes"`, `"Foyer rural"`.
- **Fondation:** `"Fondation [Nom]"` — very standardised prefix.

#### SIRENE representation

- `denomination` = standardised in caps with prefix: `"ASSOCIATION LES AMIS DE ..."`, `"FONDATION PARTAGE ET VIE"`. Asso in Alsace-Moselle (9260) follows the same prefix convention.
- `enseigne` sometimes used for acronym or popular short name.
- `naf_code`: `94.99Z` (autres organisations), `94.91Z` (cultes), `94.92Z` (politiques), `94.99Z` (others). Also sector NAFs when the association operates in a defined sector (e.g. 87.30B senior housing for an association-run EHPAD).

#### Mentions-légales representation

Standard template:
```
Association [name], déclarée au Journal Officiel sous le numéro [W+9 digits]
Siège social : [adresse]
SIREN : [9 digits]
Président : [nom]
```

#### Current matcher status

**Partial.**

- Step 0 (INPI) and Step 2 (enseigne) handle the well-formed associations with clear prefix.
- `_INDUSTRY_WORDS` contains `societe, entreprise, groupe, espace, institut` but not `association, asso, fondation`. Currently these are treated as regular tokens — not noise — which is probably correct behaviour (differs from 2.A).
- 27 ws1 confirmed shows the happy path works for standard 9220.
- Specialized sub-types (9223 groupement d'employeurs, 9230 reconnue d'utilité publique) are matched when they happen to fall in NAF-verified path.

**Gaps:**

- Fondation prefix (9300): 1 ws1+ws174 maps-only sample (`"Fondation Partage et Vie - EHPAD Jean Balat"`). No explicit fondation detector.
- Alsace-Moselle 9260: harder to distinguish from 9220 without cat-jur hint.
- Acronym-only names (ANAS, APAJH) cannot be structurally recognised — fall through to Step 5 fuzzy name.
- Groupement d'employeurs (9223) — 10 K nationally, rare in Cindy.

#### Recommended strategy

- **Lever 1 (quick, ~1 h):** Add `association, asso, fondation` as **category detectors** (not noise). When Maps name starts with one of these, prefer Step-0 INPI and exclude `forme_juridique IN ('5499', '5710', ...)` candidates unless clear evidence.
- **Lever 2 (medium, ~2 h):** Cross-reference with `naf_code = '94.99Z'` — when SIRENE name match at maps-CP lands a 94.99Z candidate, bump confidence.
- **Lever 3 (deeper, low priority):** Expose Alsace-Moselle handling — when `dept ∈ {57, 67, 68}` and Maps name has association-prefix, check both 9220 and 9260 cat-jur families.

#### Edge cases

- **Associations that *operate* EHPADs.** `"Fondation Partage et Vie - EHPAD Jean Balat - Perpignan"` is both an EHPAD and a fondation. The SIRENE row is 9300 with NAF 87.10A or 87.30A. The matcher must not exclude 9300 when looking for an EHPAD.
- **Tourist offices (syndicats d'initiative).** Either an association (9220) or a public-sector EPIC (4140) depending on commune. Cindy's `"Bureau d'Information Touristique de Villeneuve-sur-Lot"` could be either.
- **Yacht clubs / sports clubs.** Almost always 9220 but NAF-filter by 93.xx sports sector.

#### Real-world examples

- `9220 | ASSOCIATION LES AMIS DE SAINT-EMILION | — | 94.99Z`
- `9300 | FONDATION PARTAGE ET VIE | — | Various NAFs across operating EHPADs`
- `9220 | MJC DE FOS-SUR-MER | MAISON DES JEUNES | 94.99Z`
- `9260 | (Alsace-Moselle) | — | 94.99Z` (low volume in Cindy)


---

### 2.H — Franchises & enseignes de réseau (cross-category pattern)

#### Description & cat-jur codes

**This is a naming pattern, not a cat-jur category.** A franchise storefront is usually registered as a commercial company (2.A — 5499/5710/etc.) under an operating-company legal name, while presenting a brand on the storefront (`McDonald's`, `Paul`, `Franck Provost`, `Capfun`, `Huttopia`). Some are directly operated by the brand group (same SIREN as parent), most are independent franchisees.

This is arguably the hardest structural case: the Maps name is the brand; the SIRENE row name is the operator; the two never overlap in tokens. Our chain detector is the dedicated workaround.

#### Volume share

| Scope | Share |
|-------|-------|
| Cindy matched as `chain` | **2** (as of Apr 24) |
| Cindy `camping_prefix` maps-only | 113 |
| Cindy `ehpad_prefix` maps-only | 49 |
| Cindy `hotel_prefix` maps-only | 4 |

**2 confirmed chain matches vs. 113+49+4 = 166 potentially-chain maps-only.** This is the single most neglected category by auto-confirm rate.

#### Maps representation

- **Pure brand:** `"McDonald's"`, `"Franck Provost"`, `"Paul"`, `"Krys"`.
- **Brand + location:** `"Camping Huttopia Lanmary"`, `"Korian Les Magnolias"`, `"Siblu Les Viviers"`.
- **Franchised with extra operator hint:** `"Boulangerie Paul"`, `"Pharmacie Afflelou"`.
- **Brand-less franchise:** `"Camping Le Puigmal"` (might be a Capfun franchise with an independent local name — invisible to chain detector).

#### SIRENE representation

- Operator-registered: `"SARL XYZ"` where XYZ = local owner's initials. Never contains the brand.
- Parent-group HQ SIREN: `"HUTTOPIA"` or `"SIBLU"` — contains the brand in the legal name, usually at the Paris siège.

#### Mentions-légales representation

Standard template. Often reveals the operator name: `"Cette franchise est exploitée par la SARL XYZ au capital de ..."`. Sometimes reveals the parent group (`"Membre du réseau Capfun"`). High info-density page — valuable target for A2.

#### Current matcher status

**Chain detector covers ~130 brands across 11 sectors** (`fortress/matching/chains.py`):

- Boulangerie (9): Paul, Marie Blachère, La Mie Câline, Brioche Dorée, Ange, Banette, Feuillette, Le Fournil de Pierre, Boulangerie Louise
- Coiffure (12): Franck Provost, Jean Louis David, Saint Algue, Jacques Dessange, Dessange, Vog, Fabio Salsa, Coiff&Co, Camille Albane, Tchip, Shampoo, Jean Marc Joubert
- Restauration rapide (14): McDonald's, Burger King, Quick, KFC, Subway, Domino's Pizza, Pizza Hut, O'Tacos, Pitaya, Speed Burger, Pizza Paï, Columbus Café, Five Guys, Prêt à Manger, Bagel Corner
- Restauration traditionnelle (15): Hippopotamus, Buffalo Grill, Courtepaille, Flunch, Léon de Bruxelles, La Boucherie, Del Arte, Bistrot Régent, Au Bureau, Big Mamma, Poivre Rouge, Les 3 Brasseurs, La Pataterie, Taverne de Maitre Kanter, Tablapizza
- Optique (9): Optic 2000, Krys, Afflelou, Grand Optical, Atol, Optical Center, Générale d'Optique, Lissac, Maison du Lunetier
- Camping (18): Siblu, Capfun, Huttopia, Sandaya, Yelloh Village, Homair, Les Castels, Flower Campings, Sunêlia, Campéole, Tohapi, Marvilla, Vacances Directes, Camping Paradis, Ciela Village, OnlyCamp, VacanceSelect, Les Campings Bleus
- Hôtellerie (17): Ibis (+ Budget/Styles), Novotel, Mercure, Sofitel, Pullman, Mama Shelter, B&B Hotels, Première Classe, Campanile, Kyriad, Logis Hôtels, Best Western, Adagio, Citadines, Okko
- Fitness (11): Basic Fit, Fitness Park, L'Orange Bleue, Keepcool, Magic Form, CMG Sports, On Air Fitness, Neoness, Waou, Vita Liberté, L'Appart Fitness
- EHPAD (11): Korian, Emeis, Orpea, DomusVi, Colisée, LNA Santé, Emera, Repotel, Residalya, Argian, Les Jardins d'Arcadie
- Caviste (6): Nicolas, V&B, Cavavin, Le Repaire de Bacchus, Inter Caves, Les Grappes
- Arboriculture (6): Blue Whale, Perlim, Fruits Rouges, Vergers du Sud, Cofruid'Oc, Terrena

**BUT the detector confirmed only 2 matches historically.** Something is deeply wrong. Plausible causes:

1. **CP-extraction bug** (TOP PRIORITY 1 in `TASKS.md`). `find_chain_siret` hard-requires `maps_cp`. If CP is missing from 90 % of maps-only rows (audit finding), the chain detector silently skips.
2. **Branded enseigne mismatch.** The picker requires the chain canonical token to appear in the SIRENE storefront's enseigne OR denomination. Many franchisees register under an operator name that contains no brand reference at all — resulting in `N ambiguous rows → None`.
3. **Normalisation edge case.** "Camping Yelloh! Village Les Tournels" — the `!` might break phrase-token matching somewhere.
4. **NAF gap.** If a franchisee is registered under an unlisted NAF (e.g., Huttopia using 68.20B real-estate rental instead of 55.30Z camping), the candidate query returns 0 rows.

#### Recommended strategy

- **Lever 1 (CRITICAL, ~2-3 h investigation first):** **Instrument chain detector.** Add structured logging at each funnel stage: `chain_matched_on_maps_name`, `chain_queried_sirene`, `chain_sirene_rows_N`, `chain_picked`, `chain_rejected_reason`. Run a test batch of `camping 66` (known chain-heavy). Walk the funnel. Fix whichever stage drops the most.
- **Lever 2 (post-debug):** Relax the picker to allow alternatives when a single candidate at the maps-CP has a matching NAF family — even without brand token agreement. Today's strict picker over-rejects.
- **Lever 3 (expansion):** Add missing brands revealed by audit:
  - Camping: verify Flower Campings, Les Camping des Flots Bleus, any new Apr-24 maps-only brand leakage.
  - Hôtellerie: Accor (parent), Jin Jiang, Choice (brand for Clarion/Comfort/Quality).
  - EHPAD: Chemins d'Espérance, Maisons de Famille, ArpaVie, Floralys, MBV (Maisons Brevet de Vie).
  - Boulangerie: Le Fournil de Anne, Le Fournil des Provinces, Pomme de Pain.
- **Lever 4 (deeper):** Consider a "reverse chain detector" — when INPI primary returns a known chain HQ SIREN, re-query SIRENE for local storefronts at maps-CP. Related to siren_website HQ-leak problem.

#### Edge cases

- **Brand changes mid-contract.** Orpea → Emeis rebrand. Both strings live on signage for years. Chain detector handles both — good.
- **Multi-franchise owners.** One SARL can operate three Basic Fit clubs in the same dept. Disambiguating which storefront Maps refers to requires CP + address, not just CP.
- **Parent-operated vs franchised.** Some Korian are 100 % group-owned; some LNA are franchise. Affects which SIREN exists.
- **Maps name accent weirdness:** `"Yelloh! Village"` — the `!` character needs to survive tokenisation or be reliably normalised.

#### Real-world examples

- ws1 maps-only: `Les Viviers - Camping Siblu`, `Camping Homair - Le Palavas`, `Camping Verte Rive - Onlycamp`, `Camping Yelloh! Village Les Tournels` — ALL recognised by chain detector at `match_chain` but failing in `find_chain_siret`.
- ws1 confirmed chain: 2 rows, details lost in batch_log.

---

### 2.I — Professions libérales réglementées (SELARL, SELAS, SCP, SCM)

#### Description & cat-jur codes

Regulated professionals (lawyers, doctors, pharmacists, notaries, accountants, architects, veterinarians) practising in specialised legal forms that combine commercial/civil law with ordinal rules.

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| 5385 | SELCA — SEL en commandite par actions | small |
| **5485** | **SELARL — SEL à responsabilité limitée** | **60 K** |
| 5585 | SELAFA — SEL à forme anonyme | small |
| 5685 | SELAFA à directoire | small |
| **5785** | **SELAS — SEL par actions simplifiée** | **11 K** |
| 6561–6578 | SCP de professions réglementées — 17 distinct codes (avocats, notaires, huissiers, médecins, dentistes, infirmiers, kinés, labos, vétérinaires, géomètres, architectes, etc.) | ~20 K combined |
| 6585 | Autres SCP | small |
| **6589** | **SCM — Société civile de moyens** | **40 K** (shared-expenses-only vehicle) |
| 8450 | Ordre professionnel | ~500 |

#### Volume share

| Scope | Share |
|-------|-------|
| SIRENE population | ~0.9 % (~130 K combined) |
| Cindy matched | **3 %** (5485=13 SELARL, 5785=6 SELAS) |

Important category for Cindy's coverage of pharmacies (pharmacie = 5485 SELARL almost always), dental, medical, veterinary, legal, and accounting services.

#### Maps representation

- Medical: `"Docteur X"`, `"Dr X Y"`, `"Cabinet Médical X"`, `"Cabinet Dentaire X"`, `"Cabinet Vétérinaire X"`.
- Pharmacy: `"Pharmacie X"` / `"Pharmacie Principale"` / `"Pharmacie de [lieu]"` — pharmacy is almost always declared-enseigne, very match-friendly.
- Legal: `"Cabinet X Avocat"`, `"Maître X Notaire"`, `"Office notarial X"`.
- Accounting: `"Cabinet comptable X"`, `"Expert-comptable X"`.

#### SIRENE representation

- `denomination` = typically starts with form code: `"SELARL DR X"`, `"SELARL PHARMACIE X"`, `"SCP DE NOTAIRES X-Y"`.
- Often all-caps and quite formal.
- `enseigne` sometimes populated with the simpler trade name (`"PHARMACIE DE LUCCIANA"`).
- `naf_code`: 86.xx (medical), 75.00Z (vet), 69.xx (legal/accounting), 47.73Z (pharmacy), 71.11Z (architecture), 71.12B (engineering consulting).

#### Mentions-légales representation

Heavy regulatory content on pro websites — ordinal membership, assurance RC pro, complaint handling URLs. All list SIREN, full legal form name (`"Société d'Exercice Libéral à Responsabilité Limitée"`), capital.

#### Current matcher status

**Mostly handled.**

- Pharmacy and dental are high-confidence via enseigne match (trade name = legal name almost 1:1).
- Legal/medical individuals often match via Step 0 INPI (INPI indexes maitre/docteur names).
- SCM (6589) is recognised as a support vehicle; its storefront SIRENs land fine.

**Gaps:**

- `"Dr X"` / `"Docteur X"` without last-name certainty → confused with 1000 individual code.
- `"Cabinet X"` — when X is a common acronym, ambiguous with office furniture brands or consulting firms.
- SCP of avocats with multi-partner names (`"PROUVOST & MARRIS"`) — SIRENE denomination uses `&`, Maps may use `ET` or a single partner's name.

#### Recommended strategy

- **Lever 1 (quick, ~30 min):** Extend `_LEGAL_FORM_TOKENS` to include `selarl, selas, scp, scm, sela, selafa`. Currently the regex in entities.py has some but not all — audit and align.
- **Lever 2 (medium, ~2 h):** Title-detection pass — if Maps name starts with `"Dr", "Docteur", "Me", "Maître", "Cabinet", "Office"`, treat the remainder as a surname-extractor candidate (reuse Step 4b logic).
- **Lever 3:** Pharmacie-specific regex. Every pharmacy has NAF 47.73Z and the enseigne `"PHARMACIE X"` on both sides. A dedicated Step 2.5 "exact-NAF + exact-prefix-literal" path could auto-confirm them at ~100 % with trivial false-positive risk.

#### Edge cases

- **Multi-professional SCMs.** A medical SCM running 3-4 specialists in one building — each specialist has own SELARL, SCM groups the building. Maps may list the SCM, SIRENE has all three SELARLs at the same address.
- **Retiring partner SCPs.** SIRENE denomination may still show `"SCP X-Y-Z"` years after Z retired. Maps shows `"Cabinet X-Y"`.

#### Real-world examples

- `5485 | SELARL PHARMACIE DE LUCCIANA | PHARMACIE DE LUCCIANA | 47.73Z` ← canonical easy match
- `5485 | DR XAVIER GRUSON | — | 86.23Z dentaire`
- `6565 | SCP MAUDIT ET BIGAND NOTAIRES ASSOCIES | OFFICE NOTARIAL MAUDIT | 69.10Z`
- `6589 | SCM CABINET DENTAIRE JERENATH | — | 86.23Z` ← the SCM supports the dental practice

---

### 2.J — Edge-case categories (cultuel, mutualiste, syndical, étranger)

These categories each represent < 0.5 % of SIRENE and < 0.5 % of Cindy workload. Grouped here for completeness. Most are handled or can be safely ignored.

#### 2.J.1 — Cultuel / religieux

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| 2700 | Paroisse hors zone concordataire | small |
| 7430 | Établissement public des cultes d'Alsace-Lorraine | 2 K |
| 9240 | Congrégation | small |

Rarely on Google Maps under legal name. When present, usually listed as `"Église [nom]"`, `"Paroisse Saint-X"`, `"Abbaye de Y"`, `"Monastère Z"`. Match via Step 0 INPI when name is standard. No dedicated lever recommended.

#### 2.J.2 — Mutualiste et sécurité sociale

| Code | Libellé |
|------|---------|
| 6100 | Caisse d'épargne et de prévoyance |
| 6411 | Société d'assurance mutuelle |
| 8110–8190 | Régimes de sécurité sociale (URSSAF, CPAM, CAF, MSA, régimes spéciaux) |
| 8210 | Mutuelle (santé / prévoyance) |
| 8250 | Assurance mutuelle agricole (MSA locale) |
| 8290 | Autre organisme mutualiste |

These are banks/insurers/social-security agencies. Cindy never batch-targets them. A Mutuelle might appear in an EHPAD batch when it operates a retirement residence. Otherwise not a worry.

#### 2.J.3 — Syndical et ordinal

| Code | Libellé |
|------|---------|
| 8310 | Comité central d'entreprise |
| 8311 | Comité d'établissement |
| 8410 | Syndicat de salariés (CGT, CFDT, FO, etc.) |
| 8420 | Syndicat patronal (MEDEF, CPME, U2P, confréries viticoles like `"Union des Crus Classés de Graves"`) |
| 8450 | Ordre professionnel ou assimilé (Ordre des médecins, barreaux, etc.) |

Confrérie viticole pattern (`"Union des Crus Classés de Graves"`) is the one that brushes Cindy workflow. When `wine` or `vignoble` batch surfaces a 8420, prefer rejection over forcing a match.

#### 2.J.4 — Structures étrangères (3xxx)

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| 3110 | Représentation ou agence commerciale d'état étranger immatriculé au RCS | small |
| 3120 | Société étrangère immatriculée au RCS | 27 K |
| 3205 | Organisation internationale | small |
| 3210 | État étranger / collectivité | small |
| **3220** | **Société étrangère non immatriculée au RCS** | **223 K** (surprising volume) |
| 3290 | Autre personne morale de droit étranger | small |

**3220 is bigger than expected** — these are companies registered abroad but with a French SIREN for French-sourced tax/commercial activity. Most don't have a physical storefront (holdings, IP, intermediary structures). Low Cindy impact.

#### 2.J.5 — Indivisions et groupements sans personnalité morale (2xxx)

| Code | Libellé | SIRENE count |
|------|---------|--------------|
| 2110 | Indivision entre personnes physiques | 143 K |
| 2120 | Indivision avec personne morale | 2 K |
| 2210 | Société créée de fait entre personnes physiques | 32 K |
| 2900 | Autre groupement de droit privé non doté de la personnalité morale | 22 K |

Usually real-estate holding between heirs. Matcher treats them like SCIs (Section 2.D). Rarely targeted by Cindy.

#### 2.J.6 — EPIC / régies / industries d'État (4xxx)

Already partially covered in 2.F. The remaining low-volume variants (Banque de France 4160, régie nationale, etc.) are happy-path Step 0 matches.

#### 2.J.7 — GIE / GEIE / consortiums

- 6210 GEIE — Groupement européen d'intérêt économique (small)
- **6220 GIE — Groupement d'intérêt économique** (10 K)

GIE is a commercial cooperation vehicle. Usually 1-3 member companies sharing resources. Cindy may see one when she targets logistics or agro networks. No dedicated handling needed — Step 0 INPI catches standard GIEs.


---

## Section 3 — Cross-cutting concerns

Rules that apply across every category in Section 2. Refer back to these when a category-specific brief touches them.

### 3.1 — Spelling, accents, case normalisation

- `_normalize_name` strips diacritics (`é→e`, `ô→o`), lowercases, removes punctuation except hyphens.
- SIRENE stores `denomination` all-caps, no diacritics. Maps shows mixed case, with diacritics.
- Our `entities.normalize_address` does the same for addresses.
- **Known edge:** `œ→oe` (`SŒUR` → `SOEUR`) may not be handled — verify when audit flags ligatures.
- **Known edge:** apostrophes, especially `'` vs `'` (ASCII vs typographic), cause token-splitting inconsistencies. `CLAUDE.md` has bitten us on this multiple times — see `_normalize_name` for current handling.

### 3.2 — Multi-location businesses (siège vs établissement)

- SIRENE stores one siège address per SIREN. A chain or franchise has multiple *établissements* each with distinct SIRETs but sharing the SIREN.
- Maps shows the physical storefront — always an établissement, never the siège.
- **Rule:** always match on `companies.adresse` (the siège) for legal identity, but accept `maps.adresse` (operational) for CP/ville.
- The Apr 21 `1f1b893` Frankenstein Phase 1 patch codifies this: `_copy_sirene_reference_data` uses `COALESCE` on code_postal and ville — preserves Maps-derived operational values.
- Phase 3 (dual storage — pending in `TASKS.md` PRIORITY 2) will properly separate `legal_adresse` from `operational_adresse`. Required for clean franchise handling.

### 3.3 — Name transformations over time

- **Rebrands:** `Orpea → Emeis (2024)`, `Pimkie → Grain de Malice`. SIRENE denomination updates slowly after rebranding. Maps updates fast. Both the old and new brand must resolve.
- **Legal-form change:** an EURL becoming a SARL bumps the code (5498 → 5499) but the SIRENE row persists. Matcher should not hard-code cat-jur.
- **M&A:** acquisition merges two SIRENs into one. The acquired SIREN's records close (`statut = 'F'`). Matcher correctly filters `statut = 'A'`.

### 3.4 — Language conventions

- **`Saint-` vs `St-`** — both common. Our normalisation should not distinguish. Current code: hyphens preserved, both tokens fall through.
- **`Saint-` vs `Sainte-`** — gender matters for disambiguation (Saint-Émilion vs Sainte-Emilie). Don't collapse.
- **`Église` vs `Paroisse`** — religious entity synonyms. Church buildings carry the `Église` name; canonical entities are `Paroisse`. Both can be on Maps.
- **Departmental suffixes on Maps queries:** Cindy's queries often end with `- 33000` or `66000`. These are human-friendly dept-postal codes, not tokens to match. Already handled upstream.
- **French vs regional:** some Alsace-Moselle businesses use German names; some Catalunya-border businesses use Catalan. Rare but present — no dedicated rule.

### 3.5 — Gendered legal-form labels

- `SOCIÉTÉ` vs `SOCIETE` (accent) — both are variations of the same word; our normaliser handles it.
- `MADAME/MONSIEUR` prefixes in entrepreneur individuel SIRENE denominations — our normaliser strips titles? Double-check during next review.

### 3.6 — Domain prefixes that are *actually* legal-form signals

When Maps name starts with `"Domaine"`, `"Mas"`, `"Château"` — this is a 2.H hospitality/agricultural surname-extractor case (recognised). But:
- `"Société "` prefix on Maps is itself a noise word — stripping during normalisation is correct.
- `"Groupe "` prefix — corporate-parent language. Strip during normalisation; matcher should not try to match `"Groupe X"` to `"X"` unless evidence converges.
- `"Cie "`, `"Compagnie "` — same as `"Groupe "`. Pass through.

### 3.7 — NAF / cat-jur cross-checks

Rule of thumb for Phase A mismatch_accepted: cat-jur and NAF together provide orthogonal signals. A 7210 Commune with NAF 84.11Z is very high confidence public-sector. A 9220 Association with NAF 87.30A is very high confidence association-run senior housing. A 5710 SAS with NAF 01.24Z is almost impossible (farmers register as 1000 or 6598, not SAS) — would flag for review.

### 3.8 — Dense urban vs rural matching

- Dense-urban depts (75 — Paris only today) use strict CP match. Future extension to 69 (Lyon arrondissements) and 13 (Marseille) is gated on edge-case review.
- Rural matching is tolerant — dept-level match suffices; multiple CPs within a commune are accepted.
- **Critical:** SIRENE siège vs operational CP divergence is highest in rural areas (farmer's home SIREN vs orchard plot). The `_copy_sirene_reference_data` COALESCE approach is mandatory here.

---

## Section 4 — Roadmap

Prioritised list of matcher briefs derived from Section 2 gaps. Ordered by Cindy volume impact × implementation complexity. Each item references the relevant Section-2 sub-section and proposes an effort estimate + expected lift on the north-star confirm rate. **Effort & lift estimates are directional** — tighten them inside each `/plan` session.

### Priority 1 — Debug why the chain detector confirms only 2 matches (Section 2.H)

**Why this is the single biggest lever:** 166 maps-only entities across camping / EHPAD / hotel prefix. Every single one of them matched `match_chain()` semantically but dropped through `find_chain_siret()`. If we recover even 30 % of this pool, Cindy's confirm rate jumps 4-5 pp.

- **Scope:** instrument chain detector funnel (match_chain → query → candidate selection), run camping 66 test batch, trace the drop-off.
- **Expected lift:** +4-6 pp on Cindy fresh batches; +40-60 retrofit hits on ws174 maps-only.
- **Effort:** ~2-3 h investigation, then 1-2 h fix.
- **Dependencies:** TOP PRIORITY 1 in `TASKS.md` (CP extraction fix) should land first — chain detector requires `maps_cp` to fire at all.
- **Cross-ref:** Section 2.H.

### Priority 2 — Add `ferme` / `maison` / `villa` / `bastide` / `moulin` / `manoir` to `_SURNAME_PREFIXES` (Section 2.C)

**Why:** 33 ws1+ws174 maps-only entities with `Ferme` prefix. Single-file change in `discovery.py`. Cindy's agricultural workflow directly benefits.

- **Scope:** extend `_SURNAME_PREFIXES` frozenset; validate the surname picker logic accepts the new prefixes; add tests.
- **Expected lift:** +1-2 pp on Cindy fresh batches; +30-40 retrofit hits.
- **Effort:** ~1 hour including tests.
- **Dependencies:** none.
- **Risk:** low — surname extractor already lands as `method='surname'` → always pending, never auto-confirms. Opens more pending for Cindy review but doesn't introduce accuracy risk.
- **Cross-ref:** Section 2.C.

### Priority 3 — Handle trailing legal-form token (`"La Rivière Earl"`, `"Les Coteaux SARL"`) (Sections 2.A, 2.C)

**Why:** 7 ws1+ws174 maps-only instances with legal-form suffix. Conceptually simple — regex extension — but potentially surfaces more once applied.

- **Scope:** in `_normalize_name` or post-processing, detect `<name_tokens> (EARL|GAEC|SCEA|SARL|SAS|SASU|EURL|SNC)$` and strip trailing form. Then run cascade on `<name_tokens>` alone. Keep the stripped form as a cat-jur hint for verification.
- **Expected lift:** +0.5-1 pp on Cindy fresh batches; +10-20 retrofit hits.
- **Effort:** ~1-2 hours.
- **Dependencies:** none.
- **Cross-ref:** Sections 2.A, 2.C.

### Priority 4 — Public EHPAD / senior-housing pseudo-chain (Section 2.F)

**Why:** 49 ws1+ws174 EHPAD-prefix maps-only. Chain detector handles private branded EHPAD but misses the 50 % public / association-run. Volume is significant in Cindy EHPAD batches (33 ws1 match 87.10A).

- **Scope:** create a "generic ehpad" chain entry — when Maps name starts with `EHPAD`, `E.H.P.A.D.`, `Résidence`, `Résidence Personnes Âgées`, `RPA`, query SIRENE for 87.10A/87.30A/87.30B candidates at maps_cp, pick by enseigne overlap OR cat-jur hint (7366 / 9300 / 5499 all valid).
- **Expected lift:** +1-3 pp on Cindy EHPAD batches (20-30 recoveries).
- **Effort:** ~2-3 hours.
- **Dependencies:** Priority 1 (chain detector debug) should land first — we need the detector to fire reliably before extending.
- **Cross-ref:** Section 2.F.

### Priority 5 — Individual-name matching for code 1000 (Section 2.B)

**Why:** 38 % of SIRENE, 14 % of Cindy matched — the single largest structural gap. Every trade-name Maps storefront that doesn't have a website or declared enseigne falls into this bucket.

- **Scope:** Step 2.5 — when Step 2 enseigne fails and Maps-name suggests individual operator (no legal-form token, not in `_INDUSTRY_WORDS`), query SIRENE for `cat_jur = '1000' AND code_postal = maps_cp AND naf_code ~ <expected>` candidates, rank by token-vs-surname overlap, return top if single.
- **Expected lift:** +3-5 pp on Cindy fresh batches.
- **Effort:** ~4-6 hours — non-trivial. Needs careful false-positive guardrails.
- **Dependencies:** Priority 1 (chain debug).
- **Cross-ref:** Section 2.B.

### Priority 6 — A2 mentions-légales debug + activate (Cross-cutting for 2.A, 2.B, 2.C, 2.H)

**Why:** A2 is wired, enabled, but fires 0 times. Single fix could unlock per-category lift everywhere — legal name recovery from crawled mentions-legales pages.

- **Scope:** instrument A2 funnel — crawl_reached, html_fetched, legal_name_extracted, inpi_queried, candidate_found. Find the break and fix it.
- **Expected lift:** +3-5 pp once unblocked.
- **Effort:** ~1-2 hours instrument + debug.
- **Status:** tracked as TOP PRIORITY 2 in `TASKS.md` already.
- **Cross-ref:** affects 2.A, 2.B, 2.C, 2.H.

### Priority 7 — Association / Fondation prefix handler (Section 2.G)

**Why:** 9220 is the 4th largest SIRENE category (1.24 M). Cindy matches 27 / ~450 today — partial coverage. Low-effort polish.

- **Scope:** detect `Association`, `Asso`, `Fondation` prefix in Maps name, prioritise 9220/9300 cat-jur candidates at maps_cp, relax NAF gate when `naf_code = 94.99Z`.
- **Expected lift:** +1-2 pp.
- **Effort:** ~1-2 hours.
- **Dependencies:** none.
- **Cross-ref:** Section 2.G.

### Priority 8 — Collectivité rejection guardrail (Section 2.E)

**Why:** prevents high-visibility error (matching `"Mairie de X"` to an unrelated company). Low Cindy volume but high reputational risk.

- **Scope:** add rejection in matcher — if Maps name contains `Mairie|Commune|Conseil Municipal|Hôtel de Ville` and candidate is not cat-jur ∈ {7210–7355} or NAF ≠ 84.11Z, downgrade to `pending` with explicit reason.
- **Expected lift:** negligible on rate; material on accuracy.
- **Effort:** ~1 hour.
- **Cross-ref:** Section 2.E.

### Priority 9 — SCI false-positive filter (Section 2.D)

**Why:** SCI siren_website leaks (building owner's SCI on tenant website) are an existing bug category. Blacklist handles the known ones. A structural filter prevents new occurrences.

- **Scope:** when Step 1 siren_website resolves to `cat_jur = '6540'` and Maps name contains no `SCI` token, downgrade to `pending` with `link_method = "sci_suspected"`.
- **Expected lift:** negligible on rate; protects accuracy.
- **Effort:** ~1 hour.
- **Cross-ref:** Section 2.D.

### Priority 10 — Title-based profession libérale disambiguation (Section 2.I)

**Why:** `"Dr X"`, `"Cabinet X"`, `"Maître X"` are hints to prefer 5485/5785/65xx cat-jur. Small but sharp.

- **Scope:** when Maps name starts with medical/legal title token, prefer SELARL/SCP cat-jur families in cascade, relax `_INDUSTRY_WORDS` `cabinet` exclusion.
- **Expected lift:** +0.5-1 pp.
- **Effort:** ~2 hours.
- **Cross-ref:** Section 2.I.

---

## Summary table — effort × lift heatmap

| # | Brief | Section | Effort | Expected lift | Priority rank |
|---|-------|---------|--------|---------------|---------------|
| 1 | Chain detector debug + extend | 2.H | ~3-5 h | +4-6 pp | **#1 (highest)** |
| 2 | Surname-prefix expansion (ferme/maison/...) | 2.C | ~1 h | +1-2 pp | **#2** |
| 3 | Trailing-legal-form suffix handling | 2.A, 2.C | ~1-2 h | +0.5-1 pp | **#3** |
| 4 | Public EHPAD pseudo-chain | 2.F | ~2-3 h | +1-3 pp | **#4** |
| 5 | Individual-name (code 1000) matching | 2.B | ~4-6 h | +3-5 pp | **#5** |
| 6 | A2 mentions-légales debug (already in TASKS.md) | 2.A, 2.B, 2.C, 2.H | ~1-2 h | +3-5 pp | **#6 (parallel)** |
| 7 | Association/Fondation prefix handler | 2.G | ~1-2 h | +1-2 pp | **#7** |
| 8 | Collectivité rejection guardrail | 2.E | ~1 h | minimal rate / better accuracy | **#8** |
| 9 | SCI false-positive filter | 2.D | ~1 h | minimal rate / better accuracy | **#9** |
| 10 | Title-based profession libérale disambiguation | 2.I | ~2 h | +0.5-1 pp | **#10** |

Totals if all ship: ~20-30 h effort; **+15-25 pp** auto-confirm rate lift. Combined with the TOP PRIORITY 1-3 tactical levers already in `TASKS.md` (CP extraction, A2, CP-restricted disambig) — expected ~88-95 % auto-confirm rate across a Cindy-style batch portfolio.

---

## Appendix A — References

- **INSEE** / Official catégories juridiques enumeration: [xml.insee.fr/schema/cj-enum.html](https://xml.insee.fr/schema/cj-enum.html)
- **INSEE overview**: [insee.fr/fr/information/2028129](https://www.insee.fr/fr/information/2028129)
- **SIRENE documentation**: [insee.fr/fr/information/2406147](https://www.insee.fr/fr/information/2406147), [sirene.fr/static-resources/htm/d_sommaire_311.htm](https://www.sirene.fr/static-resources/htm/d_sommaire_311.htm)
- **Recherche Entreprises API (INPI)**: [recherche-entreprises.api.gouv.fr](https://recherche-entreprises.api.gouv.fr/)
- **Fortress matcher code**: `fortress/discovery.py`, `fortress/matching/chains.py`, `fortress/matching/entities.py`, `fortress/matching/inpi.py`, `fortress/matching/gemini.py`
- **CLAUDE.md**: project rules — see NAF gate, _STRONG_METHODS, link_method branches
- **TASKS.md**: current active work queue
- **Related memory**: `project_cindy_workload.md` — Cindy runs 53 % arboriculture

## Appendix B — Audit queries used

Every figure in this document is reproducible via the Neon SQL helper (`/tmp/fortress_sql.py`). Key queries:

```sql
-- SIRENE population per catégorie juridique
SELECT forme_juridique, COUNT(*) AS total
FROM companies
WHERE siren NOT LIKE 'MAPS%' AND statut = 'A'
GROUP BY forme_juridique ORDER BY total DESC LIMIT 50;

-- Cindy (ws1) matched distribution per catégorie juridique
SELECT c.forme_juridique, COUNT(*) AS cnt
FROM companies c
JOIN batch_tags bt ON bt.siren = c.siren
JOIN batch_data bd ON bd.batch_id = bt.batch_id
WHERE bd.workspace_id = 1 AND c.linked_siren IS NOT NULL
GROUP BY c.forme_juridique ORDER BY cnt DESC;

-- Link-method performance across all workspaces
SELECT link_method, link_confidence, COUNT(*) AS cnt
FROM companies
WHERE siren LIKE 'MAPS%' AND link_method IS NOT NULL
GROUP BY link_method, link_confidence ORDER BY cnt DESC;

-- Maps-only structural-pattern analysis
SELECT CASE WHEN LOWER(denomination) LIKE 'camping%' THEN 'camping_prefix'
            WHEN LOWER(denomination) LIKE 'ferme %' OR LOWER(denomination) LIKE 'la ferme%' THEN 'ferme_prefix'
            -- etc. per Section 2 prefixes
       END AS pattern, COUNT(*) AS cnt
FROM companies
WHERE siren LIKE 'MAPS%' AND linked_siren IS NULL AND workspace_id IN (1, 174)
GROUP BY 1 ORDER BY cnt DESC;

-- Cindy's actual sector queries (validates "53% arboriculture" memory)
SELECT jsonb_array_elements_text(search_queries::jsonb) AS q, COUNT(*) AS batches
FROM batch_data
WHERE workspace_id = 1 AND search_queries IS NOT NULL
GROUP BY q ORDER BY batches DESC;
```

All queries in this document are SELECT-only, no DB mutations.
