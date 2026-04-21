from fortress.matching.contacts import extract_legal_denomination

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
