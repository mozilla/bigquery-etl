import os

import pytest

from bigquery_etl.dryrun import DryRun, Errors


@pytest.fixture
def tmp_query_path(tmp_path):
    p = tmp_path / "telemetry_derived" / "mytable"
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
            "SELECT * FROM telemetry_derived.clients_daily_v6 "
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
