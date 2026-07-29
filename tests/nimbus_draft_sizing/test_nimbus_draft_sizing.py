"""Tests for Nimbus pre-launch population sizing ETL."""

import importlib.util
import json
import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

# Load query.py by file path since sql/ uses hyphens in directory names
_QUERY_PATH = os.path.join(
    os.path.dirname(__file__),
    "../../sql/moz-fx-data-experiments/monitoring/nimbus_draft_sizing_v1/query.py",
)
_spec = importlib.util.spec_from_file_location("nimbus_draft_sizing_query", _QUERY_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

GCS_FOLDER = _mod.GCS_FOLDER
_col_for_index = _mod._col_for_index
build_query = _mod.build_query
build_results = _mod.build_results
fetch_experiments = _mod.fetch_experiments
write_to_gcs = _mod.write_to_gcs

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

    def test_injects_sql_expressions(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "firefox_version >= 120" in query
        assert "locale IN ('en-US')" in query

    def test_multiplies_by_10(self):
        query = build_query(MOCK_EXPERIMENTS, _RUN_DATE)
        assert "* 10" in query

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
    def test_maps_row_to_experiments(self):
        rows = [{"exp_0": 40000, "exp_1": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)

        assert results["my-experiment"]["eligible_count"] == 40000
        assert results["another-experiment"]["eligible_count"] == 95000

    def test_includes_warnings(self):
        rows = [{"exp_0": 40000, "exp_1": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)

        assert results["my-experiment"]["warnings"] == []
        assert results["another-experiment"]["warnings"] == ["activeRollouts"]

    def test_empty_rows_returns_empty(self):
        results = build_results(MOCK_EXPERIMENTS, [])
        assert results == {}

    def test_skips_missing_columns(self):
        rows = [{"exp_0": 40000}]
        results = build_results(MOCK_EXPERIMENTS, rows)
        assert "my-experiment" in results
        assert "another-experiment" not in results

    def test_slug_collision_safe(self):
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

    def test_writes_only_current_run_results(self):
        """GCS file is a pure snapshot — Experimenter owns staleness logic."""
        mock_blob = MagicMock()
        with patch("google.cloud.storage.Client") as mock_client:
            mock_client.return_value.bucket.return_value.blob.return_value = mock_blob
            self._call(mock_client)

        uploaded = json.loads(mock_blob.upload_from_string.call_args[0][0])
        assert set(uploaded["v1"].keys()) == {"my-experiment", "another-experiment"}
