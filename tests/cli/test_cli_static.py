import os
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.cli.static import publish

VALID_WORKGROUP_ACCESS = [
    dict(
        role="roles/bigquery.dataViewer",
        members=["workgroup:mozilla-confidential"],
    )
]

TABLE_METADATA_CONF = {
    "friendly_name": "test",
    "description": "test",
    "owners": ["test@example.org"],
    "workgroup_access": VALID_WORKGROUP_ACCESS,
}

DATASET_METADATA_CONF = {
    "friendly_name": "test",
    "description": "test",
    "dataset_base_acl": "derived",
    "workgroup_access": VALID_WORKGROUP_ACCESS,
}


class TestStatic:

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @patch("google.cloud.bigquery.Client.load_table_from_file")
    @patch("google.cloud.bigquery.Client.get_table")
    @patch("google.cloud.bigquery.Client.update_table")
    def test_static_publish(
        self, update_table, get_table, load_table_from_file, runner
    ):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_data_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_data_v1/data.csv", "w"
            ) as f:
                f.write("")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_data_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(publish)

            assert result.exit_code == 0
            assert load_table_from_file.call_count == 1
            assert get_table.call_count == 1  # from publish metadata
            assert update_table.call_count == 1  # from publish metadata
