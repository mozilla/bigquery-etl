import os

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.metadata.validate_metadata import validate_change_control


class TestMetadata:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_validate_change_control_no_codeowners(self, runner):
        test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"
        codeowners_file = os.path.join(test_path, "CODEOWNERS1")

        with runner.isolated_filesystem():
            os.makedirs(test_path)
            metadata_conf = {
                "friendly_name": "test",
                "owners": ["test@example.org"],
                "labels": {"change_controlled": "true", "foo": "abc"},
            }
            with open(
                f"{test_path}/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            metadata = Metadata.from_file(f"{test_path}/metadata.yaml")
            with open(codeowners_file, "w") as codeowners:
                codeowners.write("EMPTY")

            assert "CODEOWNERS1" in os.listdir(test_path)
            assert (
                validate_change_control(
                    test_path,
                    metadata,
                    "moz-fx-data-shared-prod",
                    "sql",
                    codeowners_file,
                )
                is None
            )

    def test_validate_change_control_no_metadataowners(self, runner):
        test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"
        codeowners_file = os.path.join(test_path, "CODEOWNERS1")

        with runner.isolated_filesystem():
            os.makedirs(test_path)
            metadata_conf = {
                "friendly_name": "test",
                "labels": {"change_controlled": "true", "foo": "abc"},
            }
            with open(
                f"{test_path}/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))
            metadata = Metadata.from_file(f"{test_path}/metadata.yaml")
            with open(codeowners_file, "w") as codeowners:
                codeowners.write("EMPTY")

            assert "CODEOWNERS1" in os.listdir(test_path)
            assert (
                validate_change_control(
                    test_path,
                    metadata,
                    "moz-fx-data-shared-prod",
                    "sql",
                    codeowners_file,
                )
                is None
            )

    def test_validate_change_control_ok(self, runner):
        test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"
        codeowners_file = os.path.join(test_path, "CODEOWNERS1")

        with runner.isolated_filesystem():
            os.makedirs(test_path)
            metadata_conf = {
                "friendly_name": "test",
                "owners": ["test@example.org"],
                "labels": {"change_controlled": "true"},
            }
            with open(
                f"{test_path}/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            metadata = Metadata.from_file(f"{test_path}/metadata.yaml")
            with open(codeowners_file, "w") as codeowners:
                codeowners.write(
                    "/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 test@example.org"
                )

            assert "CODEOWNERS1" in os.listdir(test_path)
            assert validate_change_control(
                test_path, metadata, "moz-fx-data-shared-prod", "sql", codeowners_file
            )

    def test_validate_change_control_at_lest_one_owner(self, runner):
        test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"
        codeowners_file = os.path.join(test_path, "CODEOWNERS1")

        with runner.isolated_filesystem():
            os.makedirs(test_path)
            metadata_conf = {
                "friendly_name": "test",
                "owners": [
                    "test@example.org",
                    "test2@example.org",
                    "test3@example.org",
                ],
                "labels": {"change_controlled": "true", "foo": "abc"},
            }
            with open(
                f"{test_path}/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            metadata = Metadata.from_file(f"{test_path}/metadata.yaml")
            with open(codeowners_file, "w") as codeowners:
                codeowners.write(
                    "/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 test3@example.org"
                )

            assert "CODEOWNERS1" in os.listdir(test_path)
            assert validate_change_control(
                test_path, metadata, "moz-fx-data-shared-prod", "sql", codeowners_file
            )

    def test_validate_change_control_no_label(self, runner):
        test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"
        codeowners_file = os.path.join(test_path, "CODEOWNERS1")

        with runner.isolated_filesystem():
            os.makedirs(test_path)
            metadata_conf = {
                "friendly_name": "test",
                "owners": ["test@example.org"],
                "labels": {"foo": "abc"},
            }
            with open(
                f"{test_path}/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            metadata = Metadata.from_file(f"{test_path}/metadata.yaml")
            with open(codeowners_file, "w") as codeowners:
                codeowners.write("EMPTY")

            assert validate_change_control(
                test_path, metadata, "moz-fx-data-shared-prod", "sql", codeowners_file
            )
