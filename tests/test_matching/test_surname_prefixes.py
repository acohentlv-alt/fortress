"""Unit tests for surname-prefix matcher (taxonomy 2.C — Apr 27 expansion).

Brief: extend _SURNAME_PREFIXES from 6 (domaine/mas/chateau/cave/vignoble/clos)
to 12 by adding ferme/maison/villa/bastide/moulin/manoir. Surname method
always lands as pending — these tests verify the gate, not the SQL.
"""
from fortress.discovery import _SURNAME_PREFIXES, _INDUSTRY_WORDS, _normalize_name


def test_baseline_prefixes_unchanged():
    """Apr 23 baseline — never regress."""
    for p in ("domaine", "mas", "chateau", "cave", "vignoble", "clos"):
        assert p in _SURNAME_PREFIXES, f"baseline prefix {p!r} missing"


def test_new_prefixes_added():
    """Apr 27 expansion — taxonomy brief #2."""
    for p in ("ferme", "maison", "villa", "bastide", "moulin", "manoir"):
        assert p in _SURNAME_PREFIXES, f"new prefix {p!r} not added"


def test_total_prefix_count_is_12():
    """Tripwire — fail loudly if anyone removes or accidentally adds prefixes."""
    assert len(_SURNAME_PREFIXES) == 12, (
        f"expected 12 prefixes, got {len(_SURNAME_PREFIXES)}: {sorted(_SURNAME_PREFIXES)}"
    )


def test_normalize_then_prefix_lookup_ferme_lambert():
    """End-to-end normalization path: 'Ferme Lambert' → first=ferme, last=lambert."""
    tokens = _normalize_name("Ferme Lambert").split()
    assert tokens[0] == "ferme"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "lambert"
    assert tokens[-1] not in _INDUSTRY_WORDS  # surname candidate admissible


def test_normalize_then_prefix_lookup_maison_dupont():
    tokens = _normalize_name("Maison Dupont").split()
    assert tokens[0] == "maison"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "dupont"
    assert tokens[-1] not in _INDUSTRY_WORDS


def test_normalize_then_prefix_lookup_villa_saint_michel():
    """Hyphens become spaces in normalize — multi-token last-name surname survives."""
    tokens = _normalize_name("VILLA SAINT-MICHEL").split()
    assert tokens[0] == "villa"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "michel"


def test_normalize_then_prefix_lookup_moulin_du_chene():
    """Articles 'du'/'de'/'la' survive normalize (not in _LEGAL strip set).
    Last token is the place/surname candidate, even if generic."""
    tokens = _normalize_name("Moulin du Chêne").split()
    assert tokens[0] == "moulin"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "chene"
    assert tokens[-1] not in _INDUSTRY_WORDS  # generic but admissible; 2-signal floor will filter


def test_industry_word_last_token_blocks_at_gate():
    """'Ferme Hôtel' — last token 'hotel' IS in INDUSTRY_WORDS.
    The Step 4b matcher's last-token guard rejects this, never reaching the SQL.
    Same logic blocks 'Maison Coiffure', 'Villa Restaurant', etc."""
    tokens = _normalize_name("Ferme Hôtel").split()
    assert tokens[0] == "ferme"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "hotel"
    assert tokens[-1] in _INDUSTRY_WORDS  # explicitly blocked


def test_short_last_token_blocked_by_length_guard():
    """Last token shorter than 3 chars is rejected by `len(name_tokens_4b[-1]) >= 3`."""
    tokens = _normalize_name("Ferme Le M").split()
    assert tokens[0] == "ferme"
    assert tokens[0] in _SURNAME_PREFIXES
    assert len(tokens[-1]) < 3  # caught by length guard


def test_real_world_ws174_samples_pass_gate():
    """Sanity check — known maps_only ws174 names should hit the gate."""
    for sample in ("Ferme Roques", "Ferme de Chalonne", "Maison Cantarel", "Maison Bahja"):
        tokens = _normalize_name(sample).split()
        assert tokens[0] in _SURNAME_PREFIXES, f"{sample!r} prefix not recognized"
        assert len(tokens[-1]) >= 3, f"{sample!r} last token too short"
        assert tokens[-1] not in _INDUSTRY_WORDS, f"{sample!r} last token blocked"
