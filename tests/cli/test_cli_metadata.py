import os

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.metadata.validate_metadata import validate_change_control


class TestMetadata:
    test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"
    codeowners_file = os.path.join(test_path, "CODEOWNERS1")

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def check_metadata(
        self, runner, metadata_conf, codeowners_conf=None, expected_result=None
    ):
        with runner.isolated_filesystem():
            os.makedirs(self.test_path)
            with open(
                f"{self.test_path}/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            metadata = Metadata.from_file(f"{self.test_path}/metadata.yaml")
            with open(self.codeowners_file, "w") as codeowners:
                codeowners.write(codeowners_conf or "")

            assert "CODEOWNERS1" in os.listdir(self.test_path)
            assert (
                validate_change_control(
                    file_path=self.test_path,
                    metadata=metadata,
                    codeowners_file=self.codeowners_file,
                )
                is expected_result
            )

    def test_validate_change_control_no_codeowners(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org"],
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        self.check_metadata(runner=runner, metadata_conf=metadata)

    def test_validate_change_control_no_metadataowners(self, runner):
        metadata = {
            "friendly_name": "test",
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        self.check_metadata(runner=runner, metadata_conf=metadata)

    def test_validate_change_control_ok(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org"],
            "labels": {"change_controlled": "true"},
        }
        codeowners = (
            "/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 test@example.org"
        )
        self.check_metadata(
            runner=runner,
            metadata_conf=metadata,
            codeowners_conf=codeowners,
            expected_result=True,
        )

    def test_validate_change_control_at_lest_one_owner(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": [
                "test@example.org",
                "test2@example.org",
                "test3@example.org",
            ],
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        codeowners = (
            "/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 test2@example.org"
        )
        self.check_metadata(
            runner=runner,
            metadata_conf=metadata,
            codeowners_conf=codeowners,
            expected_result=True,
        )

    def test_validate_change_control_no_label(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org"],
            "labels": {"foo": "abc"},
        }
        self.check_metadata(runner=runner, metadata_conf=metadata, expected_result=True)
