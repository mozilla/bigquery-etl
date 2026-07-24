"""Tests for Nimbus pre-launch population sizing ETL."""

from unittest.mock import MagicMock, patch

import pytest

from bigquery_etl.nimbus_sizing import (
    _slug_to_col,
    build_query,
    build_results,
    fetch_experiments,
)

MOCK_EXPERIMENTS = [
    {
        "slug": "my-experiment",
        "populationPercent": "5.0",
        "targetingSql": {
            "sql": "firefox_version >= 120",
            "warnings": [],
            "needsUpdate": True,
        },
    },
    {
        "slug": "another-experiment",
        "populationPercent": "10.0",
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
        "populationPercent": "5.0",
        "targetingSql": {
            "sql": "firefox_version >= 100",
            "warnings": [],
            "needsUpdate": False,
        },
    },
    # No targetingSql — excluded
    {
        "slug": "no-sql-experiment",
        "populationPercent": "5.0",
        "targetingSql": None,
    },
    # targetingSql.sql is None — excluded
    {
        "slug": "untranslatable-experiment",
        "populationPercent": "5.0",
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


class TestSlugToCol:
    def test_replaces_hyphens(self):
        assert _slug_to_col("my-experiment") == "my_experiment"

    def test_replaces_dots(self):
        assert _slug_to_col("my.experiment") == "my_experiment"

    def test_truncates_long_slugs(self):
        long_slug = "a" * 300
        assert len(_slug_to_col(long_slug)) == 256


class TestBuildQuery:
    def test_contains_cte(self):
        query = build_query(MOCK_EXPERIMENTS)
        assert "WITH latest_per_client AS" in query
        assert "clients AS (SELECT * FROM latest_per_client WHERE rn = 1)" in query

    def test_contains_sample_id_filter(self):
        query = build_query(MOCK_EXPERIMENTS)
        assert "sample_id < 10" in query

    def test_contains_one_column_per_experiment(self):
        query = build_query(MOCK_EXPERIMENTS)
        assert "my_experiment" in query
        assert "another_experiment" in query

    def test_injects_sql_expressions(self):
        query = build_query(MOCK_EXPERIMENTS)
        assert "firefox_version >= 120" in query
        assert "locale = 'en-US' AND firefox_version >= 115" in query

    def test_multiplies_by_10(self):
        query = build_query(MOCK_EXPERIMENTS)
        assert "* 10" in query


class TestBuildResults:
    def test_maps_row_to_experiments(self):
        rows = [{"my_experiment": 40000, "another_experiment": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)

        assert results["my-experiment"]["eligible_count"] == 40000
        assert results["another-experiment"]["eligible_count"] == 95000

    def test_does_not_include_enrolled_count(self):
        # enrolled_count is computed by Experimenter using population_percent
        # the ETL only outputs eligible_count
        rows = [{"my_experiment": 40000, "another_experiment": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)
        assert "enrolled_count" not in results["my-experiment"]

    def test_includes_warnings(self):
        rows = [{"my_experiment": 40000, "another_experiment": 95000}]
        results = build_results(MOCK_EXPERIMENTS, rows)

        assert results["my-experiment"]["warnings"] == []
        assert results["another-experiment"]["warnings"] == ["activeRollouts"]

    def test_empty_rows_returns_empty(self):
        results = build_results(MOCK_EXPERIMENTS, [])
        assert results == {}

    def test_skips_missing_columns(self):
        rows = [{"my_experiment": 40000}]  # another_experiment missing
        results = build_results(MOCK_EXPERIMENTS, rows)
        assert "my-experiment" in results
        assert "another-experiment" not in results
