import os
import time
from unittest.mock import patch

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

    def test_cache_key_includes_ttl(self, tmp_query_path):
        """Test that cache key includes TTL value so different TTLs create different cache entries."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        # Create two DryRun instances with different TTLs
        dry_run1 = DryRun(str(query_file), cache_enabled=True, cache_ttl_hours=1)

        dry_run2 = DryRun(str(query_file), cache_enabled=True, cache_ttl_hours=6)

        # Get cache keys for the same SQL content
        sql = dry_run1.get_sql()
        cache_key1 = dry_run1._get_cache_key(sql)
        cache_key2 = dry_run2._get_cache_key(sql)

        # Cache keys should be different due to different TTL values
        assert cache_key1 != cache_key2

    def test_cache_file_path_generation(self, tmp_query_path):
        """Test that cache file paths are generated correctly."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dry_run = DryRun(str(query_file), cache_enabled=True, cache_ttl_hours=1)

        sql = dry_run.get_sql()
        cache_key = dry_run._get_cache_key(sql)
        cache_file_path = dry_run._get_cache_file_path(cache_key)

        # Should be in temp directory with correct naming pattern
        assert cache_file_path.startswith(dry_run.cache_dir)
        assert f"dryrun_cache_{cache_key}.pkl" in cache_file_path

    def test_cache_validity_checks(self, tmp_query_path):
        """Test cache validity checking logic."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dry_run = DryRun(str(query_file), cache_enabled=True, cache_ttl_hours=1)

        # Test with non-existent file
        assert not dry_run._is_cache_valid("/non/existent/file.pkl")

        # Test with None path
        assert not dry_run._is_cache_valid(None)

        # Test with empty string path
        assert not dry_run._is_cache_valid("")

    @patch("bigquery_etl.dryrun.exists")
    @patch("bigquery_etl.dryrun.getmtime")
    def test_cache_ttl_expiry(self, mock_getmtime, mock_exists, tmp_query_path):
        """Test that cache correctly expires based on TTL."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        # Create metadata.yaml to avoid FileNotFoundError
        metadata_file = tmp_query_path / "metadata.yaml"
        metadata_file.write_text(
            """friendly_name: Test Table
description: Test description
owners:
  - test@example.com
"""
        )

        dry_run = DryRun(
            str(query_file), cache_enabled=True, cache_ttl_hours=1  # 1 hour TTL
        )

        cache_file_path = "/fake/cache/file.pkl"
        current_time = time.time()

        # Test cache within TTL (30 minutes old)
        mock_exists.return_value = True
        mock_getmtime.return_value = current_time - (30 * 60)  # 30 minutes ago
        assert dry_run._is_cache_valid(cache_file_path)

        # Test cache beyond TTL (2 hours old)
        mock_getmtime.return_value = current_time - (2 * 60 * 60)  # 2 hours ago
        assert not dry_run._is_cache_valid(cache_file_path)

    def test_cache_key_different_for_different_content(self, tmp_query_path):
        """Test that different SQL content generates different cache keys."""
        query_file = tmp_query_path / "query.sql"

        # Create metadata.yaml to avoid FileNotFoundError
        metadata_file = tmp_query_path / "metadata.yaml"
        metadata_file.write_text(
            """friendly_name: Test Table
description: Test description
owners:
  - test@example.com
"""
        )

        dry_run = DryRun(str(query_file), cache_enabled=True, cache_ttl_hours=1)

        # Different SQL content should produce different cache keys
        sql1 = "SELECT 123"
        sql2 = "SELECT 456"

        cache_key1 = dry_run._get_cache_key(sql1)
        cache_key2 = dry_run._get_cache_key(sql2)

        assert cache_key1 != cache_key2

    def test_cache_key_includes_parameters(self, tmp_query_path):
        """Test that cache key includes all relevant parameters."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        # Create two DryRun instances with different parameters
        dry_run1 = DryRun(
            str(query_file),
            cache_enabled=True,
            project="project1",
            dataset="dataset1",
            billing_project="billing1",
        )

        dry_run2 = DryRun(
            str(query_file),
            cache_enabled=True,
            project="project2",
            dataset="dataset2",
            billing_project="billing2",
        )

        sql = dry_run1.get_sql()
        cache_key1 = dry_run1._get_cache_key(sql)
        cache_key2 = dry_run2._get_cache_key(sql)

        # Different parameters should produce different cache keys
        assert cache_key1 != cache_key2

    def test_cache_load_success(self, tmp_query_path):
        """Test successful cache loading."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        dry_run = DryRun(
            str(query_file),
            cache_enabled=True,
            project="project1",
            dataset="dataset1",
            billing_project="billing1",
        )

        expected_result = {"valid": True, "cached": True}
        dry_run._save_to_cache("test_cache_key", expected_result)

        result = dry_run._load_from_cache("test_cache_key")
        assert result == expected_result

    @patch("bigquery_etl.metadata.parse_metadata.Metadata.of_query_file")
    def test_cache_disabled_behavior(self, mock_metadata, tmp_query_path):
        """Test that caching is properly disabled when cache_enabled=False."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        # Mock metadata loading to avoid FileNotFoundError
        mock_metadata.return_value = None

        dry_run = DryRun(str(query_file), cache_enabled=False, cache_ttl_hours=1)

        # Cache operations should return None when disabled
        assert dry_run._load_from_cache("test_key") is None

        # Save to cache should do nothing when disabled
        dry_run._save_to_cache("test_key", {"test": "data"})  # Should not raise error

    @patch("bigquery_etl.dryrun.getmtime")
    def test_cache_key_includes_file_modification_time(
        self, mock_getmtime, tmp_query_path
    ):
        """Test that cache key includes file modification time."""
        query_file = tmp_query_path / "query.sql"
        query_file.write_text("SELECT 123")

        # Create metadata.yaml to avoid FileNotFoundError
        metadata_file = tmp_query_path / "metadata.yaml"
        metadata_file.write_text(
            """friendly_name: Test Table
description: Test description
owners:
  - test@example.com
"""
        )

        dry_run = DryRun(str(query_file), cache_enabled=True, cache_ttl_hours=1)

        sql = "SELECT 123"

        # Test with different modification times
        mock_getmtime.return_value = 1000000
        cache_key1 = dry_run._get_cache_key(sql)

        mock_getmtime.return_value = 2000000
        cache_key2 = dry_run._get_cache_key(sql)

        # Different modification times should produce different cache keys
        assert cache_key1 != cache_key2
