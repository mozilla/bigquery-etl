import os

from bigquery_etl.dryrun import DryRun


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
