from unittest.mock import patch

import pytest
import yaml
from google.cloud.exceptions import NotFound

from bigquery_etl import deploy


class TestDeploy:

    @pytest.fixture(scope="session")
    def query_path(self, tmp_path_factory):
        path = (
            tmp_path_factory.mktemp("sql")
            / "moz-fx-data-shared-prod"
            / "test"
            / "test_query_v1"
        )
        path.mkdir(parents=True)
        (path / "query.sql").write_text("SELECT 1")
        (path / "schema.yaml").write_text(
            yaml.dump({"fields": [{"name": "f0_", "type": "INTEGER"}]})
        )
        return path

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.dryrun.DryRun")
    def test_deploy_new_table(self, mock_dryrun, mock_client, query_path):

        mock_dryrun().get_schema.return_value = {
            "fields": [{"name": "f0_", "type": "INTEGER"}]
        }

        client = mock_client.return_value
        client.get_table.side_effect = NotFound("table not found")

        deploy.deploy_table(
            artifact_file=query_path / "query.sql",
        )

        client.create_table.assert_called_once()
        assert client.update_table.call_count == 0

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.dryrun.DryRun")
    def test_deploy_new_table_with_missing_dataset(
        self, mock_dryrun, mock_client, query_path
    ):

        mock_dryrun().get_schema.return_value = {
            "fields": [{"name": "f0_", "type": "INTEGER"}]
        }

        client = mock_client.return_value
        client.get_table.side_effect = NotFound("table not found")
        client.create_table.side_effect = NotFound(
            "404 POST https://bigquery.googleapis.com/..."
        )

        with pytest.raises(
            deploy.FailedDeployException, match="Unable to create table"
        ):
            deploy.deploy_table(artifact_file=query_path / "query.sql")

    def test_deploy_table_without_schema_raises_skip(self, tmp_path):
        query_path = (
            tmp_path / "sql" / "moz-fx-data-shared-prod" / "test" / "test_query_v1"
        )
        query_path.mkdir(parents=True)
        (query_path / "query.sql").write_text("SELECT 1")

        with pytest.raises(deploy.SkippedDeployException, match="Schema missing"):
            deploy.deploy_table(artifact_file=query_path / "query.sql")

    def test_deploy_with_null_destination_raises_skip(self, tmp_path):
        query_path = (
            tmp_path / "sql" / "moz-fx-data-shared-prod" / "test" / "test_query_v1"
        )
        query_path.mkdir(parents=True)
        (query_path / "query.sql").write_text("SELECT 1")
        (query_path / "schema.yaml").write_text(
            yaml.dump({"fields": [{"name": "f0_", "type": "INTEGER"}]})
        )
        (query_path / "metadata.yaml").write_text(
            yaml.dump({"scheduling": {"destination_table": None}})
        )

        with pytest.raises(
            deploy.SkippedDeployException, match="null destination_table configured"
        ):
            deploy.deploy_table(artifact_file=query_path / "query.sql")

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.dryrun.DryRun")
    def test_deploy_table_already_exists(self, mock_dryrun, mock_client, query_path):
        mock_dryrun().get_schema.return_value = {
            "fields": [{"name": "f0_", "type": "INTEGER"}]
        }

        client = mock_client.return_value
        deploy.deploy_table(
            artifact_file=query_path / "query.sql",
        )

        client.update_table.assert_called_once()
        assert client.create_table.call_count == 0

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.dryrun.DryRun")
    def test_deploy_table_already_exists_skip_existing(
        self, mock_dryrun, mock_client, query_path
    ):
        mock_dryrun().get_schema.return_value = {
            "fields": [{"name": "f0_", "type": "INTEGER"}]
        }

        with pytest.raises(deploy.SkippedDeployException, match="already exists"):
            deploy.deploy_table(
                artifact_file=query_path / "query.sql", skip_existing=True
            )

    @patch("bigquery_etl.dryrun.DryRun")
    def test_deploy_fails_when_schemas_dont_match(self, mock_dryrun, query_path):
        mock_dryrun().get_schema.return_value = {
            "fields": [{"name": "doesnt_exist", "type": "STRING"}]
        }
        with pytest.raises(deploy.FailedDeployException, match="does not match schema"):
            deploy.deploy_table(
                artifact_file=query_path / "query.sql",
            )

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.dryrun.DryRun")
    def test_force_deploy_with_not_matching_schemas(
        self, mock_dryrun, mock_client, query_path
    ):
        mock_dryrun().get_schema.return_value = {
            "fields": [{"name": "doesnt_exist", "type": "STRING"}]
        }
        client = mock_client.return_value
        client.get_table.side_effect = NotFound("table not found")

        deploy.deploy_table(artifact_file=query_path / "query.sql", force=True)

        client.create_table.assert_called_once()

    @patch("google.cloud.bigquery.Client")
    def test_deploy_metadata(self, mock_client, tmp_path):
        query_path = (
            tmp_path / "sql" / "moz-fx-data-shared-prod" / "test" / "test_query_v1"
        )
        query_path.mkdir(parents=True)
        (query_path / "schema.yaml").write_text(
            yaml.dump({"fields": [{"name": "f0_", "type": "INTEGER"}]})
        )
        (query_path / "metadata.yaml").write_text(yaml.dump({}))
        client = mock_client.return_value
        client.get_table.side_effect = NotFound("table not found")

        deploy.deploy_table(
            artifact_file=query_path / "metadata.yaml",
        )

        client.create_table.assert_called_once()
        assert client.update_table.call_count == 0
