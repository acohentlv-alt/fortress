from fortress.matching.contacts import (
    _accept_siren,
    _HOSTING_SIRENS,
    _trim_post_capture,
    extract_legal_denomination,
    extract_siren_from_html,
    extract_siret,
)

SAMPLE_MENTIONS_SARL = """
<html><body>
<h1>Mentions Légales</h1>
<p>Raison sociale : SARL IBTISSAM COIFFURE</p>
<p>Capital : 10000 €</p>
<p>SIREN : 123456789</p>
<p>Siège social : 29 Rue Romarin, 69001 Lyon</p>
<h2>Hébergeur</h2>
<p>OVH SAS, 2 rue Kellermann, 59100 Roubaix</p>
</body></html>
"""

SAMPLE_MENTIONS_EDITEUR = """
<html><body>
<p>Éditeur : EURL BETTY GUYONNET, au capital de 5000 euros</p>
<p>RCS Lyon 987654321</p>
<h3>Hosting</h3>
<p>Hébergeur : Amazon Web Services</p>
</body></html>
"""

SAMPLE_MENTIONS_NO_LEGAL = """
<html><body>
<p>Bienvenue sur notre site !</p>
<p>Contact : contact@example.com</p>
</body></html>
"""

SAMPLE_MENTIONS_HEBERGEUR_LEAK = """
<html><body>
<p>Notre site est chez nous.</p>
<h2>Hébergeur</h2>
<p>Raison sociale : OVH SAS</p>
</body></html>
"""

SAMPLE_MENTIONS_DIRECT_PREFIX = """
<html><body>
<p>Bienvenue !</p>
<p>SARL DUPONT COIFFURE, RCS Lyon 111222333, capital 10000€</p>
</body></html>
"""


def test_extract_sarl_from_raison_sociale():
    result = extract_legal_denomination(SAMPLE_MENTIONS_SARL)
    assert result is not None
    assert "IBTISSAM" in result.upper()
    assert "SARL" in result.upper()


def test_extract_eurl_from_editeur():
    result = extract_legal_denomination(SAMPLE_MENTIONS_EDITEUR)
    assert result is not None
    assert "BETTY GUYONNET" in result.upper()


def test_extract_direct_legal_form_prefix():
    result = extract_legal_denomination(SAMPLE_MENTIONS_DIRECT_PREFIX)
    assert result is not None
    assert "DUPONT COIFFURE" in result.upper()


def test_no_legal_form_returns_none():
    result = extract_legal_denomination(SAMPLE_MENTIONS_NO_LEGAL)
    assert result is None


def test_hebergeur_not_captured():
    # Legal name is in the hébergeur section — should NOT be extracted
    result = extract_legal_denomination(SAMPLE_MENTIONS_HEBERGEUR_LEAK)
    assert result is None


def test_empty_html():
    assert extract_legal_denomination("") is None
    assert extract_legal_denomination(None) is None


# ── _HOSTING_SIRENS blacklist (hosting providers + franchise umbrellas) ──

def test_hosting_sirens_blocks_hosting_providers():
    # Existing entries from 2026-04-09
    assert _accept_siren("424761419") is None  # OVH
    assert _accept_siren("431303775") is None  # IONOS
    assert _accept_siren("423093459") is None  # Gandi
    assert _accept_siren("433115904") is None  # Scaleway
    assert _accept_siren("510909807") is None  # o2switch


def test_hosting_sirens_blocks_franchise_umbrellas():
    # Phase 1 blacklist added 2026-04-21 from Gemini D1a disagreement data
    assert _accept_siren("479273161") is None  # SIBLU
    assert _accept_siren("321737736") is None  # SIBLU FRANCE
    assert _accept_siren("388269078") is None  # FRANCE LOCATION / CAPFUN
    assert _accept_siren("424562890") is None  # HUTTOPIA
    assert _accept_siren("790303838") is None  # FONCIERE HUTTOPIA EUROPE


def test_hosting_sirens_blocks_sandaya():
    # Phase 1b addition (Apr 26) — Sandaya camping chain
    assert _accept_siren("533670709") is None  # Sandaya — camping chain


def test_accept_siren_passes_unknown_siren_through():
    assert _accept_siren("123456789") == "123456789"


def test_accept_siren_handles_none_and_empty():
    assert _accept_siren(None) is None
    assert _accept_siren("") is None


def test_hosting_sirens_frozenset_size():
    # Sanity: 5 hosting + 5 franchise umbrella + 2 cross-CP HQ leaks + 1 Phase 1b + 1 Phase 1c = 14 entries.
    # When adding a new entry, bump this count and document the source above.
    assert len(_HOSTING_SIRENS) == 14


def test_extract_siret_rejects_franchise_footer():
    # Camping franchise site with Siblu HQ SIREN in footer should NOT link
    # the local storefront to Siblu's parent SIREN.
    html = """
    <html><body>
    <h1>Camping Les Mathes</h1>
    <p>SIREN : 479273161</p>
    <p>© 2026 Siblu</p>
    </body></html>
    """
    assert extract_siret(html) is None


# ── Bug 4 extractor widening (Apr 24) ──
# Real-world fixtures from live-tested sites (TOMA / Tigermilk / ledauphin).

SAMPLE_TOMA_SARL_WITH_SOUS = """
<html><body>
<h1>Mentions légales</h1>
<p>SARL 19 FORTIA sous le nom commercial TOMA, au capital de 10000 €.</p>
<p>RCS Marseille 987654321</p>
</body></html>
"""

SAMPLE_TIGERMILK_PROPRIETAIRE = """
<html><body>
<p>Propriétaire : SASU 2MS Aboukir Capital social 5000€.</p>
<p>Siège : 2 rue Aboukir, 75002 Paris</p>
</body></html>
"""

SAMPLE_LEDAUPHIN_EDITEUR_BARE = """
<html><body>
<h1>Mentions Légales</h1>
<p>Le site est édité par : LE DAUPHIN ZI DES CARMES 29250 SAINT-POL-DE-LEON</p>
<p>Contact : info@ledauphin.fr</p>
</body></html>
"""

SAMPLE_REGISTRE_COMMERCE_SIREN = """
<html><body>
<p>Société TOMA, Registre du Commerce et des Sociétés sous le numéro Marseille B 929 539 609.</p>
<p>Siège social : Marseille</p>
</body></html>
"""

SAMPLE_LA_CANTINE_NO_PREAMBLE = """
<html><body>
<h1>Bienvenue à LA CANTINE DE MARSEILLE</h1>
<p>Retrouvez nos plats du jour sur notre page Facebook.</p>
</body></html>
"""


def test_extract_sarl_with_sous_terminator():
    # Fix A target — "sous le nom" terminates the capture group.
    # Leading anchor widened to accept digit-starting names like "19 FORTIA".
    result = extract_legal_denomination(SAMPLE_TOMA_SARL_WITH_SOUS)
    assert result is not None
    assert "19 FORTIA" in result.upper()
    assert "SARL" in result.upper()
    assert "sous" not in result.lower()


def test_extract_sasu_from_proprietaire_preamble():
    # Fix B target — "Propriétaire :" is a new preamble keyword.
    # Leading anchor widened to accept digit-starting names like "2MS Aboukir".
    result = extract_legal_denomination(SAMPLE_TIGERMILK_PROPRIETAIRE)
    assert result is not None
    assert "2MS ABOUKIR" in result.upper()
    assert "SASU" in result.upper()


def test_extract_pattern7_editeur_bare_no_legal_form():
    # Fix D target — Pattern 7 WIDE fallback, no legal form required.
    # Regex must catch past-participle "édité par" (not just noun "éditeur").
    result = extract_legal_denomination(SAMPLE_LEDAUPHIN_EDITEUR_BARE)
    assert result is not None
    assert result.upper().startswith("LE DAUPHIN")


def test_extract_siren_from_registre_du_commerce_anchor():
    # Fix C target — "Registre du Commerce … numéro Marseille B 929 539 609"
    # must produce SIREN "929539609" via the widened _SIREN_CONTEXT_RE.
    result = extract_siren_from_html(SAMPLE_REGISTRE_COMMERCE_SIREN)
    assert result == "929539609"


def test_no_preamble_no_legal_form_no_siren_returns_none():
    # Negative case — Pattern 7 should NOT over-fire on plain business names
    # without a formal preamble colon.
    result = extract_legal_denomination(SAMPLE_LA_CANTINE_NO_PREAMBLE)
    assert result is None


# ── Apr 26 post-capture trim (entity-level junk from Apr 25 QA batch CAMPING_33_W174_BATCH_005) ──
# Real extracted legal names that hit INPI no-hit because of trailing
# boilerplate. Each fixture reproduces the HTML shape that caused
# the leak in production.

SAMPLE_FONTAINE_VIEILLE_CAPITAL_SOCIAL = """
<html><body>
<h1>Mentions Légales</h1>
<p>Éditeur : Camping de Fontaine Vieille - Capital social de 45000 €.</p>
<p>SIRET 543210987 00015, 33510 Andernos-les-Bains.</p>
<p>Contact : contact@fontaine-vieille.fr</p>
</body></html>
"""

SAMPLE_GILAMON_CAPITAL_NUMERO_TVA = """
<html><body>
<p>Raison sociale : SARL Centre de soins Gilamon Blanquefort Capital social de 153000 € Numéro de TVA FR00123456789.</p>
<p>Siège : 33290 Blanquefort.</p>
</body></html>
"""

SAMPLE_B2M_LOISIRS_STREET_LEAK = """
<html><body>
<p>SAS B2M Loisirs 488 Rambla Helios 66140 Canet-en-Roussillon, immatriculée au RCS de Perpignan.</p>
</body></html>
"""

SAMPLE_LEDAUPHIN_ZI_ADDRESS = """
<html><body>
<p>Éditeur : LE DAUPHIN ZI DES CARMES 29250 SAINT-POL-DE-LEON</p>
<p>Tél. : 02 98 29 00 00</p>
</body></html>
"""


def test_trim_trailing_capital_social_boilerplate():
    # Real fixture from Apr 25 QA: Pattern captured
    # "Camping de Fontaine Vieille - Capital social" — junk past the name.
    # Post-trim must drop "Capital social" so INPI gets a clean query.
    result = extract_legal_denomination(SAMPLE_FONTAINE_VIEILLE_CAPITAL_SOCIAL)
    assert result is not None
    assert "fontaine vieille" in result.lower()
    assert "capital" not in result.lower()
    assert "social" not in result.lower()


def test_trim_trailing_numero_tva_and_capital():
    # Real fixture from Apr 25 QA: 12 junk tokens past "SC Centre de soins
    # Gilamon Blanquefort". Trim must stop at first "Capital social" boundary.
    result = extract_legal_denomination(SAMPLE_GILAMON_CAPITAL_NUMERO_TVA)
    assert result is not None
    assert "gilamon" in result.lower()
    assert "blanquefort" in result.lower()
    assert "capital" not in result.lower()
    assert "tva" not in result.lower()
    assert "numéro" not in result.lower() and "numero" not in result.lower()


def test_trim_trailing_street_address_prefix():
    # Real fixture from Apr 25 QA: Pattern 3 leaked "488 Rambla Helios" past
    # "SAS B2M Loisirs". Trim must stop at the street-number-prefix boundary
    # so "488 Rambla" (number + street type is not matched here — Rambla
    # isn't in the street-type list) or at the postal code "66140 Canet".
    # Either way "Canet" must be gone.
    result = extract_legal_denomination(SAMPLE_B2M_LOISIRS_STREET_LEAK)
    assert result is not None
    assert "b2m" in result.lower() or "B2M" in result
    assert "loisirs" in result.lower()
    assert "66140" not in result
    assert "canet" not in result.lower()


def test_trim_trailing_ZI_address_block():
    # Real pattern seen in production: ledauphin.fr-style footer with
    # "ZI DES CARMES 29250 SAINT-POL-DE-LEON" after the company name.
    # The ZI (Zone Industrielle) branch must trim everything from " ZI "
    # onward, including the postal code and city.
    result = extract_legal_denomination(SAMPLE_LEDAUPHIN_ZI_ADDRESS)
    assert result is not None
    assert "DAUPHIN" in result.upper()
    assert "ZI" not in result.upper()
    assert "CARMES" not in result.upper()
    assert "29250" not in result
    assert "SAINT-POL" not in result.upper()


# ── Apr 26 TOP 3: legal-preamble boundary tests ──
# Real production junk patterns surfaced by Apr 25 QA.
# Each fixture intentionally includes a preamble token ("Éditeur :",
# "Raison sociale :") so the existing extractor patterns 1 or 2 fire
# FIRST — without that, the extractor returns None and trim never runs.

SAMPLE_TOMESA_DONT_SIEGE_SOCIAL = """
<html><body>
<p>Éditeur : SARL TOMESA dont le siège social est sis chemin du camping, 40170 Lit-et-Mixe.</p>
</body></html>
"""

SAMPLE_AYANT_SON_SIEGE = """
<html><body>
<p>Raison sociale : SARL CAMPING DU LAC ayant son siège à Brive-la-Gaillarde.</p>
</body></html>
"""

SAMPLE_REPRESENTEE_PAR = """
<html><body>
<p>Éditeur : SAS LES PINS représentée par Jean DUPONT, gérant.</p>
</body></html>
"""

SAMPLE_IMMATRICULEE_AU_RCS = """
<html><body>
<p>Éditeur : SARL DOMAINE DE LA PINEDE immatriculée au RCS de Bordeaux sous le n° 123456789.</p>
</body></html>
"""


def test_trim_trailing_dont_siege_social_preamble():
    # Real fixture from Apr 25 QA: "TOMESA dont le siège social est sis
    # chemin du camping" — junk preamble after the name. Trim must stop
    # at "dont le siège social" so INPI gets a clean "SARL TOMESA" query.
    result = extract_legal_denomination(SAMPLE_TOMESA_DONT_SIEGE_SOCIAL)
    assert result is not None
    assert "TOMESA" in result.upper()
    assert "siège" not in result.lower() and "siege" not in result.lower()
    assert "camping" not in result.lower()
    assert "lit-et-mixe" not in result.lower()


def test_trim_trailing_ayant_son_siege_preamble():
    # Variant: "ayant son siège à" instead of "dont le siège social est".
    # Different regex branch but same semantic — strip from "ayant" onward.
    result = extract_legal_denomination(SAMPLE_AYANT_SON_SIEGE)
    assert result is not None
    assert "CAMPING DU LAC" in result.upper()
    assert "ayant" not in result.lower()
    assert "brive" not in result.lower()


def test_trim_trailing_representee_par_director():
    # Director-name suffix after the company is junk for INPI query.
    # The new "représentée par" branch must trim everything from
    # "représentée" onward.
    result = extract_legal_denomination(SAMPLE_REPRESENTEE_PAR)
    assert result is not None
    assert "LES PINS" in result.upper()
    assert "représentée" not in result.lower() and "representee" not in result.lower()
    assert "DUPONT" not in result


def test_trim_trailing_immatriculee_au_rcs():
    # The bare "rcs" branch already exists; this test exercises the longer
    # "immatriculée au RCS de ..." preamble that comes BEFORE "RCS" and
    # would otherwise leak into the INPI query.
    result = extract_legal_denomination(SAMPLE_IMMATRICULEE_AU_RCS)
    assert result is not None
    # Note: PINEDE may render as PINEDE or PINÈDE depending on extractor.
    assert "PINEDE" in result.upper() or "PINÈDE" in result.upper()
    assert "immatriculée" not in result.lower() and "immatriculee" not in result.lower()
    assert "bordeaux" not in result.lower()
    assert "123456789" not in result


def test_trim_phase2_dirty_patterns():
    """A2 phase 2 (Apr 27): new boundaries for agency credits, CTA, dash TVA, spaced SIRET."""
    cases = [
        ("domaine Boutique Nous contacter 0", "domaine Boutique"),
        ("GROUPE SEASONOVA – SARL 538 695 560 00010 – TVA", "GROUPE SEASONOVA – SARL"),
        ("CAMPING X Réalisé par Studio Y", "CAMPING X"),
        ("SARL FOO Réalisation du site bar.fr", "SARL FOO"),
        ("FOO Conception graphique Studio Bar", "FOO"),
        ("FOO Conception et réalisation: Bar", "FOO"),
        ("FOO Développement du site par Bar", "FOO"),
        ("FOO Création du site par Bar", "FOO"),
        ("SARL TEST - TVA payée", "SARL TEST"),  # regular-hyphen TVA variant
    ]
    for input_str, expected in cases:
        assert _trim_post_capture(input_str) == expected, \
            f"Expected {expected!r}, got {_trim_post_capture(input_str)!r} for input {input_str!r}"


def test_trim_phase2_false_positive_guards():
    """Phase 2 must NOT trim legitimate names containing 'Réalisation'/'Conception'/'Création' alone.

    The all-caps RÉALISATION pattern was explicitly rejected during /review because
    'CAMPING DE LA REALISATION' is a real company name and would over-trim.
    """
    clean = [
        "HUTTOPIA SA",
        "ONLYCAMP SAS",
        "PLEIN AIR LOCATIONS SARL",
        "Domaine de Pouchou",
        "Domaine le Poteau",
        "GROUPE SEASONOVA",
        "CAMPING DE LA REALISATION",     # bare all-caps RÉALISATION at end — must NOT trim
        "STUDIO CONCEPTION",              # bare CONCEPTION — must NOT trim
        "AGENCE CREATION",                # bare CREATION — must NOT trim
        "CABINET DE DÉVELOPPEMENT",       # bare DÉVELOPPEMENT — must NOT trim
        "NOUS RESTAURANT",                # 'Nous' as start of name (no 'contacter' follow-up)
        "Studio de Réalisation a",        # mixed-case Réalisation, lowercase next char
        "Atelier Réalisation Bordeaux",   # mixed-case Réalisation, capitalized city
        "Société Réalisation Pro",        # mixed-case Réalisation, capitalized word
        "Cabinet Conception",             # bare Conception, no follow-up
        "Société Création Architecture",  # bare Création, no 'du site'/'par'/etc
    ]
    for name in clean:
        assert _trim_post_capture(name) == name, \
            f"False-positive: {name!r} got trimmed to {_trim_post_capture(name)!r}"


# ── HTML entity decode (Apr 27) ───────────────────────────────────────

SAMPLE_MENTIONS_RSQUO_BLOCKING_TRIM = """
<html><body>
<p>Raison sociale : SARL DUPOND L&rsquo;ATELIER, au capital de 10000 €</p>
<p>RCS Lyon 123456789</p>
</body></html>
"""

SAMPLE_MENTIONS_AMP_IN_NAME = """
<html><body>
<p>Société : SAS PIC&amp;MIE EDITIONS, RCS Paris 999888777, capital 50000€</p>
</body></html>
"""

SAMPLE_MENTIONS_NBSP_BETWEEN_TOKENS = """
<html><body>
<p>Raison sociale : SARL&nbsp;CAMPING&nbsp;DU&nbsp;LAC, capital 50000€</p>
</body></html>
"""

SAMPLE_MENTIONS_EACUTE_ENTITY = """
<html><body>
<p>&Eacute;diteur : SAS &Eacute;DITIONS DUMOULIN, RCS Bordeaux 444555666, capital 100000€</p>
</body></html>
"""


def test_rsquo_entity_decoded_before_trim():
    """The Apr 26 Mairie de Saint-Yrieix case: &rsquo; survived raw and blocked
    the trim regex from finding boundaries. After decode, the apostrophe is
    a real character and the captured name renders cleanly."""
    result = extract_legal_denomination(SAMPLE_MENTIONS_RSQUO_BLOCKING_TRIM)
    assert result is not None
    assert "&rsquo;" not in result
    assert "&" not in result  # No leftover entity fragments
    assert "DUPOND" in result.upper()


def test_amp_entity_decoded_in_legal_name():
    """&amp; in a legal name (e.g. 'PIC&amp;MIE') decodes to '&'."""
    result = extract_legal_denomination(SAMPLE_MENTIONS_AMP_IN_NAME)
    assert result is not None
    assert "&amp;" not in result
    assert "PIC&MIE" in result.upper()


def test_nbsp_entity_collapses_to_space():
    """&nbsp; (U+00A0 after decode) is matched by \\s+ in the subsequent
    whitespace collapse. The captured name ends up with normal spaces."""
    result = extract_legal_denomination(SAMPLE_MENTIONS_NBSP_BETWEEN_TOKENS)
    assert result is not None
    assert "&nbsp;" not in result
    assert " " not in result  # U+00A0 collapsed to regular space
    assert "CAMPING DU LAC" in result.upper()


def test_eacute_entity_decoded_to_accented_char():
    """&Eacute; decodes to É — preserves the accented character in the
    captured legal name. Not previously possible because the entity blocked
    the boundary regex from matching cleanly."""
    result = extract_legal_denomination(SAMPLE_MENTIONS_EACUTE_ENTITY)
    assert result is not None
    assert "&Eacute;" not in result
    assert "&" not in result
    assert "DUMOULIN" in result.upper()


def test_entity_free_html_unchanged():
    """Idempotency check — HTML without entities still works as before."""
    result = extract_legal_denomination(SAMPLE_MENTIONS_SARL)  # existing fixture in this file
    assert result is not None
    assert "IBTISSAM" in result.upper()
    assert "SARL" in result.upper()
