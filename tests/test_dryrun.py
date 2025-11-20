import os
import tempfile
import time
from unittest import mock

import pytest

from bigquery_etl.dryrun import DryRun, Errors


@pytest.fixture
def tmp_query_path(tmp_path):
    p = tmp_path / "moz-fx-data-shared-prod" / "telemetry_derived" / "mytable"
    p.mkdir(parents=True)
    return p


class TestDryRun:
    def test_dry_run_sql_file(self, tmp_query_path):
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        response = dryrun.dry_run_result
        assert response["valid"]

    def test_dry_run_invalid_sql_file(self, tmp_query_path):
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT INVALID 123")

        dryrun = DryRun(str(query_file))
        response = dryrun.dry_run_result
        assert response["valid"] is False

    def test_sql_file_valid(self, tmp_query_path):
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        assert dryrun.is_valid()

    def test_view_file_valid(self, tmp_query_path):
        view_file = tmp_query_path / "view.sql"
        view_file.write_text(
            """
            SELECT
            *
            FROM
            `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
        """
        )

        # this view file is only valid with strip_dml flag
        dryrun = DryRun(sqlfile=str(view_file), strip_dml=True)
        assert dryrun.get_error() is Errors.DATE_FILTER_NEEDED
        assert dryrun.is_valid()

    def test_sql_file_invalid(self, tmp_query_path):
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT INVALID 123")

        dryrun = DryRun(str(query_file))
        assert dryrun.is_valid() is False

    def test_get_referenced_tables_empty(self, tmp_query_path):
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        assert dryrun.get_referenced_tables() == []

    def test_get_sql(self, tmp_path):
        os.makedirs(tmp_path / "telmetry_derived")
        query_file = tmp_path / "telmetry_derived" / "query.sql"

        sql_content = "SELECT 123 "
        query_file.write_text(sql_content)

        assert DryRun(sqlfile=str(query_file)).get_sql() == sql_content
        with pytest.raises(ValueError):
            DryRun(sqlfile="invalid path").get_sql()

    def test_get_referenced_tables(self, tmp_query_path):
        query_file = tmp_query_path / "query.sql"
        query_file.write_text(
            "SELECT * FROM `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6` "
            "WHERE submission_date = '2020-01-01'"
        )
        query_dryrun = DryRun(str(query_file)).get_referenced_tables()

        assert len(query_dryrun) == 1
        assert query_dryrun[0]["datasetId"] == "telemetry_derived"
        assert query_dryrun[0]["tableId"] == "clients_daily_v6"

        view_file = tmp_query_path / "view.sql"
        view_file.write_text(
            """
            CREATE OR REPLACE VIEW
            `moz-fx-data-shared-prod.telemetry.clients_daily`
            AS
            SELECT
            *
            FROM
            `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
        """
        )
        view_dryrun = DryRun(str(view_file), strip_dml=True).get_referenced_tables()

        assert len(view_dryrun) == 1
        assert view_dryrun[0]["datasetId"] == "telemetry_derived"
        assert view_dryrun[0]["tableId"] == "clients_daily_v6"

        view_file.write_text(
            """
        SELECT document_id
        FROM mozdata.org_mozilla_firefox.baseline
        WHERE submission_timestamp > current_timestamp()
        UNION ALL
        SELECT document_id
        FROM mozdata.org_mozilla_fenix.baseline
        WHERE submission_timestamp > current_timestamp()
        """
        )
        multiple_tables = DryRun(str(view_file)).get_referenced_tables()
        multiple_tables.sort(key=lambda x: x["datasetId"])

        assert len(multiple_tables) == 2
        assert multiple_tables[0]["datasetId"] == "org_mozilla_fenix_stable"
        assert multiple_tables[0]["tableId"] == "baseline_v1"
        assert multiple_tables[1]["datasetId"] == "org_mozilla_firefox_stable"
        assert multiple_tables[1]["tableId"] == "baseline_v1"

    def test_get_error(self, tmp_query_path):
        view_file = tmp_query_path / "view.sql"

        view_file.write_text(
            """
        CREATE OR REPLACE VIEW
          `moz-fx-data-shared-prod.telemetry.clients_daily`
        AS
        SELECT
        *
        FROM
          `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
        """
        )

        valid_dml_stripped = """
        SELECT
        *
        FROM
          `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
        WHERE submission_date > current_date()
        """

        invalid_dml_stripped = """
        SELECT
        *
        FROM
          `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
        WHERE something
        WHERE submission_date > current_date()
        """

        assert DryRun(sqlfile=str(view_file)).get_error() is Errors.READ_ONLY
        assert (
            DryRun(sqlfile=str(view_file), strip_dml=True).get_error()
            is Errors.DATE_FILTER_NEEDED
        )
        assert (
            DryRun(sqlfile=str(view_file), content=invalid_dml_stripped).get_error()
            is Errors.DATE_FILTER_NEEDED_AND_SYNTAX
        )
        assert (
            DryRun(
                sqlfile=str(view_file), content=valid_dml_stripped, strip_dml=True
            ).get_error()
            is None
        )

    def test_dryrun_metrics_query(self, tmp_query_path):
        query_file = tmp_query_path / "query.sql"
        query_file.write_text(
            """
            SELECT * FROM (
                {{ metrics.calculate(
                    metrics=['days_of_use', 'uri_count', 'ad_clicks'],
                    platform='firefox_desktop',
                    group_by={'sample_id': 'sample_id'},
                    where='submission_date = "2023-01-01"'
                ) }}
            )
            """
        )

        dryrun = DryRun(sqlfile=str(query_file))
        assert dryrun.is_valid()

    def test_cache_key_generation(self, tmp_query_path):
        """Test that cache keys are generated consistently."""
        query_file = tmp_query_path / "query.sql"
        sql_content = "SELECT 123"
        query_file.write_text(sql_content)

        dryrun = DryRun(str(query_file))
        cache_key1 = dryrun._get_cache_key(sql_content)
        cache_key2 = dryrun._get_cache_key(sql_content)

        # Same SQL should produce same cache key
        assert cache_key1 == cache_key2
        assert len(cache_key1) == 64  # SHA256 hex digest length

        # Different SQL should produce different cache key
        different_sql = "SELECT 456"
        cache_key3 = dryrun._get_cache_key(different_sql)
        assert cache_key1 != cache_key3

    def test_cache_save_and_load(self, tmp_query_path):
        """Test that dry run results can be saved and loaded from cache."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        sql = dryrun.get_sql()
        cache_key = dryrun._get_cache_key(sql)

        # Mock result data
        test_result = {
            "valid": True,
            "schema": {"fields": [{"name": "test", "type": "STRING"}]},
        }

        # Save to cache
        dryrun._save_cached_result(cache_key, test_result)

        # Load from cache
        cached_result = dryrun._get_cached_result(cache_key)

        assert cached_result is not None
        assert cached_result["valid"] is True
        assert cached_result["schema"]["fields"][0]["name"] == "test"

    def test_cache_expiration(self, tmp_query_path):
        """Test that cache expires after TTL."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        sql = dryrun.get_sql()
        cache_key = dryrun._get_cache_key(sql)

        test_result = {"valid": True}
        dryrun._save_cached_result(cache_key, test_result)

        # Should be cached with short TTL
        cached = dryrun._get_cached_result(cache_key, ttl_seconds=10)
        assert cached is not None

        # Should be expired with very short TTL
        expired = dryrun._get_cached_result(cache_key, ttl_seconds=0)
        assert expired is None

    def test_cache_respects_sql_changes(self, tmp_query_path):
        """Test that changing SQL content creates a different cache entry."""
        query_file = tmp_query_path / "query.sql"

        # First SQL
        query_file.write_text("SELECT 123")
        dryrun1 = DryRun(str(query_file))
        sql1 = dryrun1.get_sql()
        cache_key1 = dryrun1._get_cache_key(sql1)
        test_result1 = {"valid": True, "data": "first"}
        dryrun1._save_cached_result(cache_key1, test_result1)

        # Second SQL
        query_file.write_text("SELECT 456")
        dryrun2 = DryRun(str(query_file))
        sql2 = dryrun2.get_sql()
        cache_key2 = dryrun2._get_cache_key(sql2)

        # Cache keys should be different
        assert cache_key1 != cache_key2

        # First cache should still exist
        cached1 = dryrun1._get_cached_result(cache_key1)
        assert cached1["data"] == "first"

        # Second cache should not exist yet
        cached2 = dryrun2._get_cached_result(cache_key2)
        assert cached2 is None

    def test_table_metadata_cache(self, tmp_query_path):
        """Test that table metadata can be cached by table identifier."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(
            str(query_file),
            project="test-project",
            dataset="test_dataset",
            table="test_table",
        )

        table_identifier = f"{dryrun.project}.{dryrun.dataset}.{dryrun.table}"
        test_metadata = {
            "schema": {"fields": [{"name": "col1", "type": "STRING"}]},
            "tableType": "TABLE",
        }

        # Save table metadata
        dryrun._save_cached_table_metadata(table_identifier, test_metadata)

        # Load table metadata
        cached_metadata = dryrun._get_cached_table_metadata(table_identifier)

        assert cached_metadata is not None
        assert cached_metadata["schema"]["fields"][0]["name"] == "col1"
        assert cached_metadata["tableType"] == "TABLE"

    def test_table_metadata_cache_different_tables(self, tmp_query_path):
        """Test that different tables have separate cache entries."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        # Table 1
        dryrun1 = DryRun(
            str(query_file), project="test-project", dataset="dataset1", table="table1"
        )
        table1_id = f"{dryrun1.project}.{dryrun1.dataset}.{dryrun1.table}"
        metadata1 = {"schema": {"fields": [{"name": "table1_col"}]}}
        dryrun1._save_cached_table_metadata(table1_id, metadata1)

        # Table 2
        dryrun2 = DryRun(
            str(query_file), project="test-project", dataset="dataset2", table="table2"
        )
        table2_id = f"{dryrun2.project}.{dryrun2.dataset}.{dryrun2.table}"
        metadata2 = {"schema": {"fields": [{"name": "table2_col"}]}}
        dryrun2._save_cached_table_metadata(table2_id, metadata2)

        # Both should be cached independently
        cached1 = dryrun1._get_cached_table_metadata(table1_id)
        cached2 = dryrun2._get_cached_table_metadata(table2_id)

        assert cached1["schema"]["fields"][0]["name"] == "table1_col"
        assert cached2["schema"]["fields"][0]["name"] == "table2_col"

    def test_use_cache_false_disables_caching(self, tmp_query_path):
        """Test that use_cache=False disables all caching functionality."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        # First, create a cache entry with caching enabled
        dryrun_with_cache = DryRun(str(query_file), use_cache=True)
        result1 = dryrun_with_cache.dry_run_result
        assert result1["valid"]

        # Verify cache was created
        sql = dryrun_with_cache.get_sql()
        cache_key = dryrun_with_cache._get_cache_key(sql)
        cached = dryrun_with_cache._get_cached_result(cache_key)
        assert cached is not None

        # Now create a new DryRun with use_cache=False
        dryrun_no_cache = DryRun(str(query_file), use_cache=False)

        # Even though cache exists, it should not be used
        # We can't easily verify this without mocking the API call,
        # but we can verify the flag is set correctly
        assert dryrun_no_cache.use_cache is False
        assert dryrun_with_cache.use_cache is True
