"""Unit tests for _parse_maps_address helper (Frankenstein fix, Apr 22).

Tests the "last CP wins" strategy that prevents street-prefix digits from
being mistaken for the actual postal code.
"""
import pytest
from fortress.discovery import _parse_maps_address


def test_parse_maps_address_street_starts_with_digits():
    """Real DB regression: street prefix 63200 must not be mistaken for CP."""
    cp, ville = _parse_maps_address("63200 Chem. des Coteaux, 63200 Riom, France")
    assert cp == "63200"
    assert ville == "Riom"


def test_parse_maps_address_strips_trailing_france():
    """99.8% of google_maps addresses end with ', France' — ville must not retain it."""
    cp, ville = _parse_maps_address("12 Rue de la Paix, 75002 Paris, France")
    assert cp == "75002"
    assert ville == "Paris"


def test_parse_maps_address_no_postcode_returns_none():
    """No 5-digit group → return (None, None) gracefully."""
    cp, ville = _parse_maps_address("Canton de Perpignan-6, France")
    assert cp is None
    assert ville is None
