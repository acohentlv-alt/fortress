from fortress.matching.contacts import (
    _accept_siren,
    _HOSTING_SIRENS,
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


def test_accept_siren_passes_unknown_siren_through():
    assert _accept_siren("123456789") == "123456789"


def test_accept_siren_handles_none_and_empty():
    assert _accept_siren(None) is None
    assert _accept_siren("") is None


def test_hosting_sirens_frozenset_size():
    # Sanity: 5 hosting + 5 franchise umbrella + 2 cross-CP HQ leaks = 12 entries.
    # When adding a new entry, bump this count and document the source above.
    assert len(_HOSTING_SIRENS) == 12


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
