"""Tests for Nimbus experiment population estimates ETL."""

import importlib.util
import json
import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from datetime import timezone

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
build_query = _mod.build_query
build_results = _mod.build_results
fetch_experiments = _mod.fetch_experiments
write_to_gcs = _mod.write_to_gcs
write_to_bigquery = _mod.write_to_bigquery

MOCK_EXPERIMENTS = [
    {
        "slug": "my-experiment",
        "targetingSql": {
            "sql": "metrics.quantity.nimbus_targeting_context_firefox_version >= 120",
            "warnings": [],
            "needsUpdate": True,
        },
    },
    {
        "slug": "another-experiment",
        "targetingSql": {
            "sql": (
                "metrics.string.nimbus_targeting_context_locale IN ('en-US')"
                " AND metrics.quantity.nimbus_targeting_context_firefox_version >= 115"
            ),
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
            "sql": "metrics.quantity.nimbus_targeting_context_firefox_version >= 100",
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

    def test_uses_countif_not_count_distinct(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "COUNTIF(" in query
        assert "COUNT(DISTINCT" not in query

    def test_always_uses_client_id(self):
        """profile_group_id not yet in nimbus_targeting_context — always client_id."""
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "client_info.client_id" in query
        assert "profile_group_id" not in query

    def test_uses_moz_fx_data_shared_prod_table(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert (
            "moz-fx-data-shared-prod.firefox_desktop.nimbus_targeting_context" in query
        )
        assert "mozdata" not in query

    def test_submission_date_controls_window(self):
        query = build_query(MOCK_EXPERIMENTS, date(2026, 7, 27))
        assert "2026-07-27" in query

    def test_generated_sql_is_syntactically_plausible(self):
        """Spot-check that generated SQL doesn't contain known invalid patterns."""
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
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
                    "sql": (
                        "JSON_VALUE(metrics.object.nimbus_targeting_context_browser_settings,"
                        " '$.update.channel') = 'release'"
                        " AND metrics.quantity.nimbus_targeting_context_firefox_version >= 120"
                    ),
                    "warnings": [],
                    "needsUpdate": True,
                },
            },
        ]
        query = build_query(experiments, date(2026, 7, 27))
        _sql = (
            "JSON_VALUE(metrics.object.nimbus_targeting_context_browser_settings,"
            " '$.update.channel') = 'release'"
            " AND metrics.quantity.nimbus_targeting_context_firefox_version >= 120"
        )
        expected = "\n".join(
            [
                "WITH latest_per_client AS (",
                "  SELECT",
                "    *,",
                "    ROW_NUMBER() OVER (",
                "      PARTITION BY client_info.client_id",
                "      ORDER BY submission_timestamp DESC",
                "    ) AS rn",
                "  FROM `moz-fx-data-shared-prod.firefox_desktop.nimbus_targeting_context`",
                "  WHERE DATE(submission_timestamp) BETWEEN '2026-07-21' AND '2026-07-27'",
                "    AND sample_id < 10",
                "    AND client_info.client_id IS NOT NULL",
                "),",
                "clients AS (SELECT * EXCEPT (rn) FROM latest_per_client WHERE rn = 1)",
                "SELECT",
                f"  COUNTIF(\n    {_sql}\n  ) * 10 AS `exp_0`",
                "FROM clients",
            ]
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


class TestWriteToBigQuery:
    RESULTS = {
        "my-experiment": {"eligible_count": 40000, "warnings": []},
        "another-experiment": {"eligible_count": 95000, "warnings": ["activeRollouts"]},
    }

    def _call(self, mock_client):
        write_to_bigquery(
            self.RESULTS,
            project="moz-fx-data-experiments",
            dataset="monitoring",
            table="experiment_population_estimates_v1",
            submission_date=_RUN_DATE,
        )

    def test_computed_at_is_execution_timestamp_not_midnight(self):
        """computed_at must reflect actual execution time, not submission date midnight."""
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.load_table_from_json.return_value.result.return_value = None
            self._call(mock_client)

        rows = mock_client.return_value.load_table_from_json.call_args[0][0]
        for row in rows:
            computed_at_str = row["computed_at"]
            assert "T" in computed_at_str, "computed_at must be an ISO timestamp"
            assert not computed_at_str.endswith("T00:00:00+00:00"), (
                "computed_at must not be midnight of submission_date"
            )

    def test_computed_at_partition_decorator_matches_computed_at_date(self):
        """Partition decorator must be derived from computed_at, not submission_date."""
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.load_table_from_json.return_value.result.return_value = None
            self._call(mock_client)

        call_args = mock_client.return_value.load_table_from_json.call_args
        rows = call_args[0][0]
        table_ref = call_args[0][1]
        decorator = table_ref.split("$")[1]

        computed_at_date = rows[0]["computed_at"][:10].replace("-", "")
        assert decorator == computed_at_date, (
            f"Partition decorator {decorator} must match computed_at date {computed_at_date}, "
            f"not submission_date {_RUN_DATE.strftime('%Y%m%d')}"
        )

    def test_skips_write_when_no_results(self):
        with patch("google.cloud.bigquery.Client") as mock_client:
            write_to_bigquery(
                {},
                project="moz-fx-data-experiments",
                dataset="monitoring",
                table="experiment_population_estimates_v1",
                submission_date=_RUN_DATE,
            )
        mock_client.return_value.load_table_from_json.assert_not_called()
