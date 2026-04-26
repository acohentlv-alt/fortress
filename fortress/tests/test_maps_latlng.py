"""Unit tests for parse_maps_lat_lng — GPS extraction from Google Maps URLs.

Covers:
  - Form 1: !3d{lat}!4d{lng} place-detail anchor (preferred)
  - Form 2: @{lat},{lng},{zoom}z camera position (fallback)
  - French mainland + Corsica bounds acceptance
  - DOM-TOM and out-of-bounds rejection
  - Malformed / None input handling
  - Priority: Form 1 wins when both forms are present
"""

import pytest
from fortress.scraping.maps import parse_maps_lat_lng


class TestParseForm1_3d4dPreferred:
    """Form 1 (!3d/!4d) is preferred even when form 2 is also present."""

    def test_parse_form_1_3d4d_preferred(self):
        """Form 1 wins when both !3d/!4d and @lat,lng, are present."""
        url = (
            "https://www.google.com/maps/place/Camping+X/"
            "@43.5,3.2,15z/data=!3m4!4d3.2123!3d43.5456"
        )
        result = parse_maps_lat_lng(url)
        assert result == (43.5456, 3.2123), (
            f"Form 1 should win over form 2; expected (43.5456, 3.2123), got {result}"
        )


class TestParseForm2AtFallback:
    """Form 2 (@lat,lng,zoomz) is used when form 1 is absent."""

    def test_parse_form_2_at_fallback(self):
        """Only form 2 present — parser should fall back to @lat,lng."""
        url = "https://www.google.com/maps/search/camping+47/@44.20,0.62,12z"
        result = parse_maps_lat_lng(url)
        assert result == (44.20, 0.62), (
            f"Expected (44.20, 0.62), got {result}"
        )


class TestBoundsAcceptance:
    """Coordinates within French mainland + Corsica bounds must pass."""

    def test_brittany_negative_longitude(self):
        """Negative longitude for western Brittany must pass bounds."""
        url = (
            "https://www.google.com/maps/place/Quimper+Camping/"
            "@47.99,-4.10,15z/data=!4d-4.0987!3d47.9911"
        )
        result = parse_maps_lat_lng(url)
        assert result == (47.9911, -4.0987), (
            f"Negative lng Brittany should pass; expected (47.9911, -4.0987), got {result}"
        )

    def test_corsica_passes(self):
        """Corsica coords (lat ~42, lng ~9) must pass bounds [41-51, -5-10]."""
        url = (
            "https://www.google.com/maps/place/Camping+Corsica/"
            "@42.0,9.0,15z/data=!4d9.1234!3d42.0567"
        )
        result = parse_maps_lat_lng(url)
        assert result == (42.0567, 9.1234), (
            f"Corsica inside bounds; expected (42.0567, 9.1234), got {result}"
        )


class TestBoundsRejection:
    """Coordinates outside French bounds must return None."""

    def test_dom_tom_rejected(self):
        """Réunion island (lat ~-21, lng ~55) must be rejected — out of bounds."""
        url = (
            "https://www.google.com/maps/place/Reunion/"
            "@-21.115,55.536,15z/data=!4d55.5360!3d-21.1151"
        )
        result = parse_maps_lat_lng(url)
        assert result is None, (
            f"DOM-TOM Réunion should be rejected; expected None, got {result}"
        )

    def test_out_of_france_germany(self):
        """German coords (lat 52, lng 13.4 — Berlin) must be rejected: lat > 51 max."""
        url = (
            "https://www.google.com/maps/place/Berlin/"
            "@52.0,13.4,12z/data=!4d13.4050!3d52.5200"
        )
        result = parse_maps_lat_lng(url)
        assert result is None, (
            f"Germany should be rejected (lat 52 > 51); expected None, got {result}"
        )


class TestEdgeCases:
    """Edge cases: decimal zoom, malformed URL, None input, search-results path."""

    def test_decimal_zoom(self):
        """Decimal zoom (e.g. 12.5z) must not break form 2 regex."""
        url = "https://www.google.com/maps/search/cafe/@45.1234,2.5678,12.5z"
        result = parse_maps_lat_lng(url)
        assert result == (45.1234, 2.5678), (
            f"Decimal zoom should parse fine; expected (45.1234, 2.5678), got {result}"
        )

    def test_malformed_returns_none(self):
        """A bare Maps URL with no coordinates at all must return None."""
        url = "https://www.google.com/maps/"
        result = parse_maps_lat_lng(url)
        assert result is None, (
            f"No coords in URL; expected None, got {result}"
        )

    def test_none_input_returns_none(self):
        """None input must not raise and must return None."""
        result = parse_maps_lat_lng(None)
        assert result is None, (
            f"None input should return None without crashing; got {result}"
        )

    def test_search_results_url_form2(self):
        """Search-results URL (not place path) with form 2 should parse cleanly."""
        url = (
            "https://www.google.com/maps/search/"
            "camping+herault,+France/@43.6,3.8,11z"
        )
        result = parse_maps_lat_lng(url)
        assert result == (43.6, 3.8), (
            f"Search-results form 2 URL; expected (43.6, 3.8), got {result}"
        )
