"""Tests for Module A — SIRENE downloader and ingester.

Run with:
    pytest tests/test_module_a/test_sirene.py -v

These tests do NOT perform any real network I/O or DB access.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fortress.query.sirene_download import (
    ResourceInfo,
    _file_is_recent,
    _verify_sha256,
    download_sirene,
    fetch_dataset_resources,
)
from fortress.query.sirene_ingest import (
    normalize_naf_code,
    transform_row,
)


# ---------------------------------------------------------------------------
# NAF code normalisation
# ---------------------------------------------------------------------------


class TestNafNormalization:
    """Comprehensive tests for normalize_naf_code()."""

    def test_old_format_no_dot(self) -> None:
        """Core requirement: "0121Z" → "01.21Z"."""
        assert normalize_naf_code("0121Z") == "01.21Z"

    def test_canonical_format_unchanged(self) -> None:
        """Already-canonical codes must pass through unchanged."""
        assert normalize_naf_code("01.21Z") == "01.21Z"

    def test_lowercase_letter_normalised(self) -> None:
        """Lowercase letter suffix must be uppercased."""
        assert normalize_naf_code("01.21z") == "01.21Z"
        assert normalize_naf_code("0121z") == "01.21Z"

    def test_whitespace_stripped(self) -> None:
        """Leading/trailing whitespace must be stripped before normalisation."""
        assert normalize_naf_code("  0121Z  ") == "01.21Z"
        assert normalize_naf_code("  01.21Z  ") == "01.21Z"

    def test_none_returns_none(self) -> None:
        assert normalize_naf_code(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert normalize_naf_code("") is None
        assert normalize_naf_code("   ") is None

    def test_various_codes(self) -> None:
        """Spot-check a variety of real SIRENE codes in both formats."""
        cases = [
            ("6201Z", "62.01Z"),
            ("56.10A", "56.10A"),  # already canonical with letter suffix
            ("5610A", "56.10A"),
            ("4711D", "47.11D"),
            ("47.11D", "47.11D"),
            ("0111Z", "01.11Z"),
            ("9609Z", "96.09Z"),
        ]
        for raw, expected in cases:
            assert normalize_naf_code(raw) == expected, (
                f"normalize_naf_code({raw!r}) should be {expected!r}"
            )


# ---------------------------------------------------------------------------
# Row transform
# ---------------------------------------------------------------------------


class TestTransformRow:
    """Tests for the SIRENE row-to-DB-tuple transformation."""

    def _base_row(self) -> dict:
        return {
            "siren": "123456789",
            "nicSiegeUniteLegale": 10,
            "denominationUniteLegale": "ENTREPRISE TEST SA",
            "nomUniteLegale": None,
            "prenomUsuelUniteLegale": None,
            "activitePrincipaleUniteLegale": "0121Z",
            "libelleActivitePrincipaleUniteLegale": "Culture de la vigne",
            "categorieJuridiqueUniteLegale": "5710",
            "dateCreationUniteLegale": "2000-01-15",
            "trancheEffectifsUniteLegale": "11",
            "etatAdministratifUniteLegale": "A",
        }

    def test_basic_transform(self) -> None:
        row = self._base_row()
        result = transform_row(row)
        assert result is not None
        siren, siret, denomination, naf_code, naf_libelle, forme, date_c, tranche, statut = result
        assert siren == "123456789"
        assert siret == "12345678900010"
        assert denomination == "ENTREPRISE TEST SA"
        assert naf_code == "01.21Z"   # normalised from "0121Z"
        assert naf_libelle == "Culture de la vigne"
        assert forme == "5710"
        assert date_c == "2000-01-15"
        assert tranche == "11"
        assert statut == "A"

    def test_missing_denomination_falls_back_to_name_fields(self) -> None:
        row = self._base_row()
        row["denominationUniteLegale"] = None
        row["nomUniteLegale"] = "DUPONT"
        row["prenomUsuelUniteLegale"] = "JEAN"
        result = transform_row(row)
        assert result is not None
        assert result[2] == "JEAN DUPONT"

    def test_missing_both_denomination_uses_siren_placeholder(self) -> None:
        row = self._base_row()
        row["denominationUniteLegale"] = None
        row["nomUniteLegale"] = None
        row["prenomUsuelUniteLegale"] = None
        result = transform_row(row)
        assert result is not None
        assert "123456789" in result[2]

    def test_siren_zero_padded(self) -> None:
        row = self._base_row()
        row["siren"] = "1234"  # too short
        result = transform_row(row)
        assert result is not None
        assert result[0] == "000001234"

    def test_missing_siren_returns_none(self) -> None:
        row = self._base_row()
        row["siren"] = None
        assert transform_row(row) is None

    def test_naf_code_canonical_passthrough(self) -> None:
        row = self._base_row()
        row["activitePrincipaleUniteLegale"] = "62.01Z"
        result = transform_row(row)
        assert result is not None
        assert result[3] == "62.01Z"

    def test_naf_libelle_fallback_to_reference_table(self) -> None:
        """When SIRENE has no libelle, we look it up in naf_codes.py."""
        row = self._base_row()
        row["libelleActivitePrincipaleUniteLegale"] = None
        row["activitePrincipaleUniteLegale"] = "01.21Z"
        result = transform_row(row)
        assert result is not None
        # NAF_CODES["01.21Z"] exists in naf_codes.py
        assert result[4] is not None
        assert "vigne" in result[4].lower()


# ---------------------------------------------------------------------------
# Downloader — skip-recent logic
# ---------------------------------------------------------------------------


class TestDownloaderSkipRecent:
    """Tests for the 'skip if file is recent' guard in download_sirene()."""

    def test_file_is_recent_true(self, tmp_path: Path) -> None:
        """A file written now should be considered recent (age < 32 days)."""
        f = tmp_path / "StockUniteLegale.parquet"
        f.touch()
        assert _file_is_recent(f, max_age_days=32) is True

    def test_file_is_recent_false_old(self, tmp_path: Path) -> None:
        """A file with a mtime 40 days ago must NOT be considered recent."""
        f = tmp_path / "StockUniteLegale.parquet"
        f.touch()
        old_time = time.time() - (40 * 86400)
        import os
        os.utime(f, (old_time, old_time))
        assert _file_is_recent(f, max_age_days=32) is False

    def test_file_is_recent_false_missing(self, tmp_path: Path) -> None:
        """A non-existent file is not recent."""
        f = tmp_path / "does_not_exist.parquet"
        assert _file_is_recent(f, max_age_days=32) is False

    @pytest.mark.asyncio
    async def test_download_skipped_when_recent(self, tmp_path: Path) -> None:
        """download_sirene() should return the existing path without any HTTP call
        when the local file is recent and --force is not set."""
        fake_parquet = tmp_path / "StockUniteLegale.parquet"
        fake_parquet.write_bytes(b"fake parquet content")
        # File was just written → very recent

        with patch(
            "fortress.query.sirene_download.settings"
        ) as mock_settings, patch(
            "fortress.query.sirene_download.fetch_dataset_resources",
            new_callable=AsyncMock,
        ) as mock_fetch:
            mock_settings.sirene_dir = tmp_path

            result = await download_sirene(force=False)

        # The API must NOT have been called
        mock_fetch.assert_not_called()
        assert result == fake_parquet

    @pytest.mark.asyncio
    async def test_download_forced_even_when_recent(self, tmp_path: Path) -> None:
        """download_sirene(force=True) must re-download even if the file is recent."""
        fake_parquet = tmp_path / "StockUniteLegale.parquet"
        fake_parquet.write_bytes(b"old content")

        fake_resource = ResourceInfo(
            url="https://example.com/StockUniteLegale.parquet",
            filename="StockUniteLegale.parquet",
            checksum_sha256=None,
            mime_type="application/octet-stream",
        )

        with patch(
            "fortress.query.sirene_download.settings"
        ) as mock_settings, patch(
            "fortress.query.sirene_download.fetch_dataset_resources",
            new_callable=AsyncMock,
            return_value=[fake_resource],
        ), patch(
            "fortress.query.sirene_download.download_file",
            new_callable=AsyncMock,
        ) as mock_dl:
            mock_settings.sirene_dir = tmp_path
            # Make the file appear to already exist after download
            mock_dl.side_effect = lambda client, url, dest: dest.write_bytes(
                b"new content"
            )

            result = await download_sirene(force=True)

        mock_dl.assert_called_once()
        assert result == fake_parquet


# ---------------------------------------------------------------------------
# Downloader — fetch_dataset_resources (mocked HTTP)
# ---------------------------------------------------------------------------


class TestFetchDatasetResources:
    """Test that we correctly parse the data.gouv.fr API response."""

    @pytest.mark.asyncio
    async def test_parquet_preferred_over_zip(self) -> None:
        """Parquet resources must come before ZIP resources in the returned list."""
        mock_response_data = {
            "resources": [
                {
                    "url": "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip",
                    "title": "StockUniteLegale ZIP",
                    "mime": "application/zip",
                    "checksum": None,
                },
                {
                    "url": "https://files.data.gouv.fr/insee-sirene/StockUniteLegale.parquet",
                    "title": "StockUniteLegale Parquet",
                    "mime": "application/x-parquet",
                    "checksum": {"type": "sha256", "value": "abc123"},
                },
            ]
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_response_data
        mock_resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)

        resources = await fetch_dataset_resources(mock_client)

        assert len(resources) == 2
        # Parquet must be first
        assert resources[0].filename == "StockUniteLegale.parquet"
        assert resources[0].checksum_sha256 == "abc123"
        # ZIP second
        assert resources[1].filename == "StockUniteLegale_utf8.zip"
        assert resources[1].checksum_sha256 is None

    @pytest.mark.asyncio
    async def test_empty_resources_list(self) -> None:
        """An empty resources list returns an empty list (not an error)."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"resources": []}
        mock_resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)

        resources = await fetch_dataset_resources(mock_client)
        assert resources == []


# ---------------------------------------------------------------------------
# Checksum verification
# ---------------------------------------------------------------------------


class TestVerifySha256:
    """Tests for _verify_sha256()."""

    def test_correct_checksum(self, tmp_path: Path) -> None:
        import hashlib
        content = b"hello fortress"
        f = tmp_path / "test.parquet"
        f.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()
        assert _verify_sha256(f, expected) is True

    def test_wrong_checksum(self, tmp_path: Path) -> None:
        f = tmp_path / "test.parquet"
        f.write_bytes(b"hello fortress")
        assert _verify_sha256(f, "0" * 64) is False
