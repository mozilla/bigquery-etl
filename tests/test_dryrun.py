import os

from bigquery_etl.dryrun import get_referenced_tables, sql_file_valid, dry_run_sql_file


class TestDryRun:
    def test_dry_run_sql_file(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT 123")

        response = dry_run_sql_file(query_file)
        assert response["valid"]

    def test_dry_run_invalid_sql_file(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT INVALID 123")

        response = dry_run_sql_file(query_file)
        assert response["valid"] is False

    def test_sql_file_valid(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT 123")

        assert sql_file_valid(str(query_file))

    def test_sql_file_invalid(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT INVALID 123")

        assert sql_file_valid(str(query_file)) is False

    def test_get_referenced_tables_empty(self, tmp_path):
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT 123")

        assert get_referenced_tables(query_file) == []

    def test_get_referenced_tables(self, tmp_path):
        os.makedirs(tmp_path / "telmetry_derived")
        query_file = tmp_path / "telmetry_derived" / "query.sql"
        query_file.write_text("SELECT * FROM clients_daily_v6")
        response = get_referenced_tables(query_file)

        assert len(response) == 1
        assert response[0]["datasetId"] == "telemetry_derived"
        assert response[0]["tableId"] == "clients_daily_v6"
