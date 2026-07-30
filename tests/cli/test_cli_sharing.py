"""Tests for the `bqetl sharing` CLI command."""

from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from bigquery_etl.cli.sharing import deploy
from tests.sharing.test_sharing import FakeAnalyticsHubClient, FakeBigQueryClient

METADATA = """
friendly_name: Test
description: Test table
owners:
  - test@example.org
labels:
  authorized: true
external_sharing:
  exchange: partner_exchange
  data_review: https://bugzilla.mozilla.org/1
  subscribers:
    - group:partner@example.org
"""

METADATA_NO_SHARING = """
friendly_name: Test
description: Test table
owners:
  - test@example.org
"""


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def not_coding_agent(monkeypatch):
    """Bypass the coding-agent guardrail so the decorated command runs."""
    monkeypatch.setattr(
        "bigquery_etl.util.common.is_running_under_coding_agent", lambda: False
    )


def _write_table(sql_dir: Path, dataset: str, table: str, metadata: str):
    table_dir = sql_dir / "moz-fx-data-shared-prod" / dataset / table
    table_dir.mkdir(parents=True)
    (table_dir / "metadata.yaml").write_text(metadata)


class TestSharingDeploy:
    def test_deploy_configured_view(self, runner, tmp_path):
        sql_dir = tmp_path / "sql"
        _write_table(sql_dir, "my_shared", "my_view", METADATA)

        fake = FakeAnalyticsHubClient()
        with (
            patch("bigquery_etl.cli.sharing.analytics_hub_client", return_value=fake),
            patch(
                "bigquery_etl.cli.sharing.bigquery_client",
                return_value=FakeBigQueryClient(),
            ),
        ):
            result = runner.invoke(
                deploy,
                [str(sql_dir), "--sql-dir", str(sql_dir)],
            )

        assert result.exit_code == 0, result.output
        assert len(fake.created_exchanges) == 1
        assert len(fake.created_listings) == 1
        assert len(fake.set_policy_calls) == 1
        # Sharing is set up on the user-facing project (mozdata), not shared-prod.
        assert "projects/mozdata/" in fake.created_exchanges[0]

    def test_user_facing_project_override(self, runner, tmp_path):
        sql_dir = tmp_path / "sql"
        _write_table(sql_dir, "my_shared", "my_view", METADATA)

        fake = FakeAnalyticsHubClient()
        with (
            patch("bigquery_etl.cli.sharing.analytics_hub_client", return_value=fake),
            patch(
                "bigquery_etl.cli.sharing.bigquery_client",
                return_value=FakeBigQueryClient(),
            ),
        ):
            result = runner.invoke(
                deploy,
                [
                    str(sql_dir),
                    "--sql-dir",
                    str(sql_dir),
                    "--user-facing-project",
                    "moz-fx-data-sandbox",
                ],
            )

        assert result.exit_code == 0, result.output
        # Exchange/listing created in the overridden project, not mozdata.
        assert "projects/moz-fx-data-sandbox/" in fake.created_exchanges[0]

    def test_skips_tables_without_sharing(self, runner, tmp_path):
        sql_dir = tmp_path / "sql"
        _write_table(sql_dir, "my_derived", "tbl_v1", METADATA_NO_SHARING)

        fake = FakeAnalyticsHubClient()
        with (
            patch("bigquery_etl.cli.sharing.analytics_hub_client", return_value=fake),
            patch(
                "bigquery_etl.cli.sharing.bigquery_client",
                return_value=FakeBigQueryClient(),
            ),
        ):
            result = runner.invoke(
                deploy,
                [str(sql_dir), "--sql-dir", str(sql_dir)],
            )

        assert result.exit_code == 0, result.output
        # No configured tables -> nothing deployed.
        assert fake.created_exchanges == []
        assert fake.created_listings == []
        assert "0 table(s)" in result.output

    def test_dry_run_makes_no_changes(self, runner, tmp_path):
        sql_dir = tmp_path / "sql"
        _write_table(sql_dir, "my_shared", "my_view", METADATA)

        fake = FakeAnalyticsHubClient()
        with (
            patch("bigquery_etl.cli.sharing.analytics_hub_client", return_value=fake),
            patch(
                "bigquery_etl.cli.sharing.bigquery_client",
                return_value=FakeBigQueryClient(),
            ),
        ):
            result = runner.invoke(
                deploy,
                [str(sql_dir), "--sql-dir", str(sql_dir), "--dry-run"],
            )

        assert result.exit_code == 0, result.output
        assert fake.created_exchanges == []
        assert fake.created_listings == []
        assert fake.set_policy_calls == []
