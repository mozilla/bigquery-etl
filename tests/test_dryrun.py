import os
from pathlib import Path
from bigquery_etl.dryrun import DryRun

TEST_DIR = Path(__file__).parent


class TestDryRun:
    def test_dry_run_sql_file(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        response = dryrun.dry_run_result
        assert response["valid"]

    def test_dry_run_invalid_sql_file(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT INVALID 123")

        dryrun = DryRun(str(query_file))
        response = dryrun.dry_run_result
        assert response["valid"] is False

    def test_sql_file_valid(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        assert dryrun.is_valid()

    def test_sql_file_invalid(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT INVALID 123")

        dryrun = DryRun(str(query_file))
        assert dryrun.is_valid() is False

    def test_view_file_valid(self):
        view_file = (
            TEST_DIR
            / "sql"
            / "moz-fx-data-shared-prod"
            / "messaging_system"
            / "cfr_users_last_seen"
            / "view.sql"
        )
        dryrun = DryRun(str(view_file))
        assert dryrun.is_valid()

    def test_get_referenced_tables_empty(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT 123")

        dryrun = DryRun(str(query_file))
        assert dryrun.get_referenced_tables() == []

    def test_get_referenced_tables(self, tmp_path):
        os.makedirs(tmp_path / "telmetry_derived")
        query_file = tmp_path / "telmetry_derived" / "query.sql"
        query_file.write_text(
            "SELECT * FROM telemetry_derived.clients_daily_v6 "
            "WHERE submission_date = '2020-01-01'"
        )
        dryrun = DryRun(str(query_file))
        response = dryrun.get_referenced_tables()

        assert len(response) == 1
        assert response[0]["datasetId"] == "telemetry_derived"
        assert response[0]["tableId"] == "clients_daily_v6"

    def test_get_referenced_tables_dml_stripped(self):

        view_file = (
            TEST_DIR / "sql" / "moz-fx-data-shared-prod" / "telemetry" / "view.sql"
        )

        dml_stripped = DryRun(sqlfile=str(view_file), strip_dml=True)
        dml_unstripped = DryRun(str(view_file))

        assert (
            dml_stripped.get_referenced_tables()[0]["datasetId"] == "telemetry_derived"
        )
        assert dml_stripped.get_referenced_tables()[0]["tableId"] == "clients_daily_v6"
        assert dml_unstripped.get_referenced_tables() == []

    def test_get_referenced_tables_partition_filter(self):
        """Test that DryRun returns referenced tables for
        queries that require a partition filter."""

        # this query requires a 'submission_date' filter
        view_file_1 = (
            TEST_DIR
            / "sql"
            / "moz-fx-data-shared-prod"
            / "messaging_system"
            / "cfr_users_last_seen"
            / "view.sql"
        )

        # this query requires a 'submission_timestamp' filter
        view_file_2 = (
            TEST_DIR
            / "sql"
            / "moz-fx-data-shared-prod"
            / "messaging_system"
            / "onboarding_events_amplitude"
            / "view.sql"
        )

        view_1 = DryRun(sqlfile=str(view_file_1), strip_dml=True)
        view_2 = DryRun(sqlfile=str(view_file_2), strip_dml=True)

        assert (
            view_1.get_referenced_tables()[0]["datasetId"] == "messaging_system_derived"
        )
        assert view_1.get_referenced_tables()[0]["tableId"] == "cfr_users_last_seen_v1"

        assert (
            view_2.get_referenced_tables()[0]["datasetId"] == "messaging_system_stable"
        )
        assert view_2.get_referenced_tables()[0]["tableId"] == "onboarding_v1"
