from click.testing import CliRunner

from bigquery_etl.cli.alchemer import backfill


def test_cli_alchemer_backfill_inclusive_dates(monkeypatch):
    def nop(*args, **kwargs):
        pass

    monkeypatch.setattr("bigquery_etl.cli.alchemer.get_survey_data", nop)
    monkeypatch.setattr("bigquery_etl.cli.alchemer.insert_to_bq", nop)

    result = CliRunner().invoke(
        backfill,
        [
            "--start-date",
            "2021-01-01",
            "--end-date",
            "2021-01-03",
            "--survey_id",
            "55555",
            "--api_token",
            "token",
            "--api_secret",
            "secret",
            "--destination_table",
            "testing_dataset.testing_table",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # parse the output...
    assert (
        "2021-01-01" in result.stdout
        and "2021-01-02" in result.stdout
        and "2021-01-03" in result.stdout
    ), result.stdout
