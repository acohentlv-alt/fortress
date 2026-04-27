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
    for tok in ("ferme", "maison", "villa", "bastide", "moulin", "manoir", "verger", "bergerie"):
        assert tok in _SURNAME_PREFIXES, f"missing new prefix: {tok}"


def test_total_prefix_count_is_14():
    """Tripwire — fail loudly if anyone removes or accidentally adds prefixes."""
    assert len(_SURNAME_PREFIXES) == 14, (
        f"expected 14 prefixes, got {len(_SURNAME_PREFIXES)}: {sorted(_SURNAME_PREFIXES)}"
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


def test_normalize_then_prefix_lookup_verger_martin():
    """Verger Martin (regression — ws174 30j) → first token = verger."""
    tokens = _normalize_name("Verger Martin").split()
    assert tokens[0] == "verger"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "martin"
    assert tokens[-1] not in _INDUSTRY_WORDS


def test_normalize_then_prefix_lookup_bergerie_dels_monts():
    """Bergerie dels Monts (ws174, dept 66)."""
    tokens = _normalize_name("Bergerie dels Monts").split()
    assert tokens[0] == "bergerie"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "monts"
    assert tokens[-1] not in _INDUSTRY_WORDS


def test_maison_de_retraite_blocked_by_hard_reject():
    """Maison de Retraite Publique — must NOT trigger Step 4b extraction."""
    from fortress.discovery import _HARD_REJECT_TOKENS
    tokens = _normalize_name("Maison de Retraite Publique").split()
    assert tokens[0] == "maison"
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] == "publique"
    assert tokens[-1] not in _INDUSTRY_WORDS
    assert "retraite" in _HARD_REJECT_TOKENS
    assert "retraite" in tokens
    assert bool(set(tokens) & _HARD_REJECT_TOKENS) is True


def test_maison_artisan_still_works():
    """Maison Cantarel — must STILL trigger extraction (no retraite token)."""
    from fortress.discovery import _HARD_REJECT_TOKENS
    tokens = _normalize_name("Maison Cantarel").split()
    assert tokens[0] in _SURNAME_PREFIXES
    assert tokens[-1] not in _INDUSTRY_WORDS
    assert not (set(tokens) & _HARD_REJECT_TOKENS)
