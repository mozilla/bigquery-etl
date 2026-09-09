"""Tests for Nimbus experiment population estimates ETL."""

import importlib.util
import json
import os
from unittest.mock import MagicMock, patch

import pytest

# Load query.py by file path since sql/ uses hyphens in directory names
_QUERY_PATH = os.path.join(
    os.path.dirname(__file__),
    "../../sql/moz-fx-data-experiments/monitoring"
    "/experiment_population_estimates_v1/query.py",
)
_spec = importlib.util.spec_from_file_location(
    "experiment_population_estimates_query", _QUERY_PATH
)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

GCS_FOLDER = _mod.GCS_FOLDER
_col_for_index = _mod._col_for_index
_APP_SIZING_TABLE = _mod._APP_SIZING_TABLE
build_query = _mod.build_query
build_results = _mod.build_results
fetch_experiments = _mod.fetch_experiments
write_to_gcs = _mod.write_to_gcs

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
            "sql": "locale IN ('en-US') AND firefox_version >= 115",
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
        assert _col_for_index(0) != _col_for_index(1)


class TestBuildQuery:
    def test_uses_indexed_column_aliases(self):
        query = build_query(MOCK_EXPERIMENTS, "firefox_desktop")
        assert "`exp_0`" in query
        assert "`exp_1`" in query

    def test_uses_countif_not_count_distinct(self):
        query = build_query(MOCK_EXPERIMENTS, "firefox_desktop")
        assert "COUNTIF(" in query
        assert "COUNT(DISTINCT" not in query

    def test_no_cte(self):
        """Dedup/sampling is pre-materialized — build_query should not contain a CTE."""
        query = build_query(MOCK_EXPERIMENTS, "firefox_desktop")
        assert "WITH latest_per_client" not in query
        assert "clients AS" not in query

    @pytest.mark.parametrize(
        "app_name, expected_table",
        [
            (
                "firefox_desktop",
                "moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_sizing_clients_v1",
            ),
            (
                "fenix",
                "moz-fx-data-shared-prod.org_mozilla_fenix_derived.nimbus_sizing_clients_v1",
            ),
            (
                "firefox_ios",
                "moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.nimbus_sizing_clients_v1",
            ),
        ],
    )
    def test_uses_pre_materialized_table(self, app_name, expected_table):
        query = build_query(MOCK_EXPERIMENTS, app_name)
        assert expected_table in query
        assert "mozdata" not in query

    def test_unknown_app_raises(self):
        with pytest.raises(KeyError):
            build_query(MOCK_EXPERIMENTS, "unknown_app")

    def test_generated_sql_is_syntactically_plausible(self):
        """Spot-check that generated SQL doesn't contain known invalid patterns."""
        query = build_query(MOCK_EXPERIMENTS, "firefox_desktop")
        assert " = NULL" not in query
        assert "JSON_ARRAY_LENGTH(" not in query
        assert "= FALSE" not in query
        assert "= TRUE" not in query

    def test_full_query_matches_expected(self):
        """Assert the complete generated SQL for a known input."""
        experiments = [
            {
                "slug": "release-channel-experiment",
                "targetingSql": {
                    "sql": "os_version >= 120",
                    "warnings": [],
                    "needsUpdate": True,
                },
            },
        ]
        query = build_query(experiments, "firefox_desktop")
        expected = (
            "SELECT\n"
            "  COUNTIF(\n"
            "    os_version >= 120\n"
            "  ) * 10 AS `exp_0`\n"
            "FROM `moz-fx-data-shared-prod.firefox_desktop_derived.nimbus_sizing_clients_v1`"
        )
        assert query == expected


class TestBuildResults:
    def _make_mock_bq_row(self, **kwargs):
        row = MagicMock()
        row.keys.return_value = list(kwargs.keys())
        row.__iter__ = lambda self: iter(kwargs.items())
        row.items.return_value = list(kwargs.items())
        row.__getitem__ = lambda self, k: kwargs[k]
        row.get = lambda k, d=None: kwargs.get(k, d)
        return row

    def test_maps_row_to_experiments(self):
        mock_row = self._make_mock_bq_row(exp_0=40000, exp_1=95000)
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value.result.return_value = [mock_row]
            results = build_results(MOCK_EXPERIMENTS, "SELECT 1", "test-project")

        assert results["my-experiment"]["eligible_count"] == 40000
        assert results["another-experiment"]["eligible_count"] == 95000

    def test_includes_warnings(self):
        mock_row = self._make_mock_bq_row(exp_0=40000, exp_1=95000)
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value.result.return_value = [mock_row]
            results = build_results(MOCK_EXPERIMENTS, "SELECT 1", "test-project")

        assert results["my-experiment"]["warnings"] == []
        assert results["another-experiment"]["warnings"] == ["activeRollouts"]

    def test_empty_rows_returns_empty(self):
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value.result.return_value = []
            results = build_results(MOCK_EXPERIMENTS, "SELECT 1", "test-project")
        assert results == {}

    def test_skips_missing_columns(self):
        mock_row = self._make_mock_bq_row(exp_0=40000)
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value.result.return_value = [mock_row]
            results = build_results(MOCK_EXPERIMENTS, "SELECT 1", "test-project")
        assert "my-experiment" in results
        assert "another-experiment" not in results


class TestWriteToGCS:
    RESULTS = {
        "my-experiment": {"eligible_count": 40000, "warnings": []},
        "another-experiment": {
            "eligible_count": 95000,
            "warnings": ["activeRollouts"],
        },
    }

    def _call(self, mock_client):
        write_to_gcs(
            self.RESULTS,
            bucket_name="mozanalysis",
            folder=GCS_FOLDER,
            run_date="2026-07-27",
        )

    def test_wraps_results_in_v1_key(self):
        """GCS JSON must be {"v1": ...} so future schema changes can add v2."""
        mock_blob = MagicMock()
        with patch("google.cloud.storage.Client") as mock_client:
            mock_client.return_value.bucket.return_value.blob.return_value = mock_blob
            self._call(mock_client)

        uploaded = json.loads(mock_blob.upload_from_string.call_args[0][0])
        assert "v1" in uploaded
        assert self.RESULTS["my-experiment"] == uploaded["v1"]["my-experiment"]

    def test_writes_dated_and_latest_files(self):
        """Writes a dated archive and a latest file matching enrollment_funnel pattern."""
        mock_blob = MagicMock()
        with patch("google.cloud.storage.Client") as mock_client:
            mock_bucket = mock_client.return_value.bucket.return_value
            mock_bucket.blob.return_value = mock_blob
            self._call(mock_client)

        blob_paths = [call[0][0] for call in mock_bucket.blob.call_args_list]
        assert any("2026-07-27" in p and "v1" in p for p in blob_paths)

        copy_calls = mock_bucket.copy_blob.call_args_list
        assert len(copy_calls) == 1
        dest_path = copy_calls[0][0][2]
        assert "latest" in dest_path and "v1" in dest_path

    def test_gcs_filename_uses_new_name(self):
        """Filenames should use experiment_population_estimates_v1, not nimbus_draft."""
        mock_blob = MagicMock()
        with patch("google.cloud.storage.Client") as mock_client:
            mock_bucket = mock_client.return_value.bucket.return_value
            mock_bucket.blob.return_value = mock_blob
            self._call(mock_client)

        blob_paths = [call[0][0] for call in mock_bucket.blob.call_args_list]
        assert all("experiment_population_estimates" in p for p in blob_paths)
        assert not any("nimbus_draft" in p for p in blob_paths)
