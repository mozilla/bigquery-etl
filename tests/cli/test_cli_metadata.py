import os
import types

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.cli.query import _attach_metadata  # schedule,
from bigquery_etl.metadata.validate_metadata import validate_change_control


class TestMetadata:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_validate_change_control_no_codeowners(self, runner):
        test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"

        with runner.isolated_filesystem():
            os.makedirs(test_path)
            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "labels": {"change_controlled": "true", "foo": "abc"},
            }
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            table = types.SimpleNamespace()
            _attach_metadata(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql",
                table,
            )

            with open("CODEOWNERS1", "w") as f:
                assert validate_change_control(
                    test_path, table, "moz-fx-data-shared-prod", "sql"
                )
