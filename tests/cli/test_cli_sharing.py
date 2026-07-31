"""Tests for the `bqetl sharing` CLI command."""

from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from bigquery_etl.cli.sharing import clean, deploy
from bigquery_etl.sharing import MANAGED_DESCRIPTION
from tests.sharing.test_sharing import FakeAnalyticsHubClient, FakeBigQueryClient

METADATA = """
friendly_name: Test
description: Test dataset
dataset_base_acl: view
user_facing: true
external_sharing:
  exchange: partner_exchange
  data_review: https://bugzilla.mozilla.org/1
  subscribers:
    - group:partner@example.org
"""

METADATA_NO_SHARING = """
friendly_name: Test
description: Test dataset
dataset_base_acl: view
user_facing: true
"""

METADATA_EXCHANGE_ID = """
friendly_name: Test
description: Test dataset
dataset_base_acl: view
user_facing: true
external_sharing:
  exchange: Partner Exchange
  exchange_id: custom_exchange_id
  data_review: https://bugzilla.mozilla.org/1
  subscribers:
    - group:partner@example.org
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


def _write_dataset(sql_dir: Path, dataset: str, metadata: str):
    dataset_dir = sql_dir / "moz-fx-data-shared-prod" / dataset
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "dataset_metadata.yaml").write_text(metadata)


class TestSharingDeploy:
    def test_deploy_configured_view(self, runner, tmp_path):
        sql_dir = tmp_path / "sql"
        _write_dataset(sql_dir, "my_shared", METADATA)

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
        _write_dataset(sql_dir, "my_shared", METADATA)

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

    def test_skips_datasets_without_sharing(self, runner, tmp_path):
        sql_dir = tmp_path / "sql"
        _write_dataset(sql_dir, "my_derived", METADATA_NO_SHARING)

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
        # No configured datasets -> nothing deployed.
        assert fake.created_exchanges == []
        assert fake.created_listings == []
        assert "0 dataset(s)" in result.output

    def test_clean_removes_orphans_keeps_configured(self, runner, tmp_path):
        from types import SimpleNamespace

        sql_dir = tmp_path / "sql"
        # One configured `_shared` dataset -> desired listing.
        _write_dataset(sql_dir, "keep_shared", METADATA)

        fake = FakeAnalyticsHubClient()
        base = "projects/mozdata/locations/US/dataExchanges/partner_exchange"
        # Configured (desired) listing + an orphaned managed listing, both under
        # a managed exchange.
        fake.exchanges[base] = SimpleNamespace(description=MANAGED_DESCRIPTION)
        fake.listings[f"{base}/listings/keep_shared"] = SimpleNamespace(
            description=MANAGED_DESCRIPTION
        )
        fake.listings[f"{base}/listings/gone_shared"] = SimpleNamespace(
            description=MANAGED_DESCRIPTION
        )
        # A manually-created exchange that must never be touched.
        manual = "projects/mozdata/locations/US/dataExchanges/manual_exchange"
        fake.exchanges[manual] = SimpleNamespace(description="hand made")

        with (
            patch("bigquery_etl.cli.sharing.analytics_hub_client", return_value=fake),
            patch(
                "bigquery_etl.cli.sharing.bigquery_client",
                return_value=FakeBigQueryClient(),
            ),
        ):
            result = runner.invoke(clean, ["--sql-dir", str(sql_dir)])

        assert result.exit_code == 0, result.output
        assert fake.deleted_listings == [f"{base}/listings/gone_shared"]
        # partner_exchange still has the desired listing -> kept; manual kept.
        assert fake.deleted_exchanges == []

    def test_clean_respects_exchange_id_override(self, runner, tmp_path):
        """clean must key desired on exchange_id (matching deploy), so live
        resources for a dataset with exchange_id set aren't seen as orphans."""
        from types import SimpleNamespace

        sql_dir = tmp_path / "sql"
        _write_dataset(sql_dir, "keep_shared", METADATA_EXCHANGE_ID)

        fake = FakeAnalyticsHubClient()
        base = "projects/mozdata/locations/US/dataExchanges/custom_exchange_id"
        fake.exchanges[base] = SimpleNamespace(description=MANAGED_DESCRIPTION)
        fake.listings[f"{base}/listings/keep_shared"] = SimpleNamespace(
            description=MANAGED_DESCRIPTION
        )

        with (
            patch("bigquery_etl.cli.sharing.analytics_hub_client", return_value=fake),
            patch(
                "bigquery_etl.cli.sharing.bigquery_client",
                return_value=FakeBigQueryClient(),
            ),
        ):
            result = runner.invoke(clean, ["--sql-dir", str(sql_dir)])

        assert result.exit_code == 0, result.output
        # Nothing deleted: the exchange_id-keyed desired set matches the resource.
        assert fake.deleted_listings == []
        assert fake.deleted_exchanges == []
