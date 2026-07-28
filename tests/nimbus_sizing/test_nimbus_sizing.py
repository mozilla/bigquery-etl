"""Tests for Nimbus pre-launch population sizing ETL."""

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from bigquery_etl.nimbus_sizing import (
    GCS_FOLDER,
    _col_for_index,
    build_query,
    build_results,
    fetch_experiments,
    write_to_gcs,
)

MOCK_EXPERIMENTS = [
    {
        "slug": "my-experiment",
        "targetingSql": {
            "sql": "firefox_version >= 120",
            "warnings": [],
            "needsUpdate": True,
        },
    },
    {
        "slug": "another-experiment",
        "targetingSql": {
            "sql": "locale = 'en-US' AND firefox_version >= 115",
            "warnings": ["activeRollouts"],
            "needsUpdate": True,
        },
    },
]

MOCK_API_RESPONSE = [
    # Needs update — included
    *MOCK_EXPERIMENTS,
    # needsUpdate=False — excluded
    {
        "slug": "stale-experiment",
        "targetingSql": {
            "sql": "firefox_version >= 100",
            "warnings": [],
            "needsUpdate": False,
        },
    },
    # No targetingSql — excluded
    {"slug": "no-sql-experiment", "targetingSql": None},
    # targetingSql.sql is None — excluded
    {
        "slug": "untranslatable-experiment",
        "targetingSql": {
            "sql": None,
            "warnings": ["attachedFxAOAuthClients"],
            "needsUpdate": True,
        },
    },
]

_RUN_DATE = date(2026, 7, 27)


class TestFetchExperiments:
    def test_filters_to_needs_update_with_sql(self):
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = MOCK_API_RESPONSE
            mock_get.return_value.raise_for_status = MagicMock()

            result = fetch_experiments("https://example.com/api/")

        assert len(result) == 2
        assert {e["slug"] for e in result} == {"my-experiment", "another-experiment"}

    def test_excludes_needs_update_false(self):
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = MOCK_API_RESPONSE
            mock_get.return_value.raise_for_status = MagicMock()
            result = fetch_experiments("https://example.com/api/")

        assert "stale-experiment" not in [e["slug"] for e in result]

    def test_excludes_null_targeting_sql(self):
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = MOCK_API_RESPONSE
            mock_get.return_value.raise_for_status = MagicMock()
            result = fetch_experiments("https://example.com/api/")

        assert "no-sql-experiment" not in [e["slug"] for e in result]
        assert "untranslatable-experiment" not in [e["slug"] for e in result]

    def test_raises_on_non_list_response(self):
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = {"error": "bad"}
            mock_get.return_value.raise_for_status = MagicMock()
            with pytest.raises(RuntimeError, match="expected list"):
                fetch_experiments("https://example.com/api/")


class TestColForIndex:
    def test_returns_exp_prefix(self):
        assert _col_for_index(0) == "exp_0"
        assert _col_for_index(42) == "exp_42"

    def test_no_slug_collision(self):
        # Two slugs that would collide with the old slug→col approach
        # now get distinct column names by index
        assert _col_for_index(0) != _col_for_index(1)


class TestBuildQuery:
    def test_contains_cte(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "WITH latest_per_client AS" in query
        assert "clients AS (SELECT * EXCEPT (rn)" in query

    def test_contains_sample_id_filter(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "sample_id < 10" in query

    def test_uses_indexed_column_aliases(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "`exp_0`" in query
        assert "`exp_1`" in query
        # slug names should NOT appear as column aliases
        assert "`my_experiment`" not in query

    def test_uses_countif_not_count_distinct(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "COUNTIF(" in query
        assert "COUNT(DISTINCT" not in query

    def test_injects_sql_expressions(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "firefox_version >= 120" in query
        assert "locale = 'en-US' AND firefox_version >= 115" in query

    def test_multiplies_by_10(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "* 10" in query

    def test_uses_moz_fx_data_shared_prod_table(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "moz-fx-data-shared-prod" in query
        assert "mozdata" not in query

    def test_submission_date_controls_window(self):
        query = build_query(MOCK_EXPERIMENTS, date(2026, 7, 27))
        assert "2026-07-27" in query


class TestBuildResults:
    def test_maps_row_to_experiments(self):
        rows = [{"exp_0": 40000, "exp_1": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)

        assert results["my-experiment"]["eligible_count"] == 40000
        assert results["another-experiment"]["eligible_count"] == 95000

    def test_does_not_include_enrolled_count(self):
        rows = [{"exp_0": 40000, "exp_1": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)
        assert "enrolled_count" not in results["my-experiment"]

    def test_includes_warnings(self):
        rows = [{"exp_0": 40000, "exp_1": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)

        assert results["my-experiment"]["warnings"] == []
        assert results["another-experiment"]["warnings"] == ["activeRollouts"]

    def test_empty_rows_returns_empty(self):
        results = build_results(MOCK_EXPERIMENTS, [])
        assert results == {}

    def test_skips_missing_columns(self):
        rows = [{"exp_0": 40000}]  # exp_1 missing
        results = build_results(MOCK_EXPERIMENTS, rows)
        assert "my-experiment" in results
        assert "another-experiment" not in results

    def test_slug_collision_safe(self):
        # Two slugs that would have collided with the old _slug_to_col approach
        experiments = [
            {"slug": "my-experiment", "targetingSql": {"sql": "TRUE", "warnings": []}},
            {"slug": "my.experiment", "targetingSql": {"sql": "TRUE", "warnings": []}},
        ]
        rows = [{"exp_0": 1000, "exp_1": 2000}]
        results = build_results(experiments, rows)
        assert results["my-experiment"]["eligible_count"] == 1000
        assert results["my.experiment"]["eligible_count"] == 2000


class TestWriteToGCS:
    RESULTS = {
        "my-experiment": {"eligible_count": 40000, "warnings": []},
        "another-experiment": {"eligible_count": 95000, "warnings": ["activeRollouts"]},
    }

    def _call(self, mock_client):
        write_to_gcs(
            self.RESULTS,
            bucket_name="mozanalysis",
            folder=GCS_FOLDER,
            run_date="2026-07-27",
        )

    def test_wraps_results_in_v1_key(self):
        """GCS JSON must be {"v1": ...} so future schema changes can add v2
        without breaking consumers — matches enrollment_funnel pattern."""
        mock_blob = MagicMock()
        with patch("google.cloud.storage.Client") as mock_client:
            mock_client.return_value.bucket.return_value.blob.return_value = mock_blob
            self._call(mock_client)

        uploaded = json.loads(mock_blob.upload_from_string.call_args[0][0])
        assert "v1" in uploaded
        assert self.RESULTS["my-experiment"] == uploaded["v1"]["my-experiment"]

    def test_writes_dated_and_latest_files(self):
        """Writes dated archive and copies to latest matching enrollment_funnel pattern."""
        mock_blob = MagicMock()
        with patch("google.cloud.storage.Client") as mock_client:
            mock_bucket = mock_client.return_value.bucket.return_value
            mock_bucket.blob.return_value = mock_blob
            self._call(mock_client)

        # dated file uploaded via blob().upload_from_string
        blob_paths = [call[0][0] for call in mock_bucket.blob.call_args_list]
        assert any("2026-07-27" in p and "v1" in p for p in blob_paths)

        # latest file written via copy_blob — third positional arg is dest path
        copy_calls = mock_bucket.copy_blob.call_args_list
        assert len(copy_calls) == 1
        dest_path = copy_calls[0][0][2]
        assert "latest" in dest_path and "v1" in dest_path

    def test_writes_only_current_run_results(self):
        """GCS file is a pure snapshot of this run — Experimenter owns staleness logic."""
        mock_blob = MagicMock()
        with patch("google.cloud.storage.Client") as mock_client:
            mock_client.return_value.bucket.return_value.blob.return_value = mock_blob
            self._call(mock_client)

        uploaded = json.loads(mock_blob.upload_from_string.call_args[0][0])
        assert set(uploaded["v1"].keys()) == {"my-experiment", "another-experiment"}
