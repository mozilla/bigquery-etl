import distutils
import os
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.cli.metadata import update
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.metadata.validate_metadata import validate_change_control

TEST_DIR = Path(__file__).parent.parent


class TestMetadata:
    test_path = "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1"

    @property
    def codeowners_filename(self) -> str:
        return "CODEOWNERS_TEST"

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def check_metadata(
        self, runner, metadata_conf, codeowners_conf=None, expected_result=None
    ):
        codeowners_file = os.path.join(self.test_path, self.codeowners_filename)
        with runner.isolated_filesystem():
            os.makedirs(self.test_path)
            with open(
                f"{self.test_path}/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            metadata = Metadata.from_file(f"{self.test_path}/metadata.yaml")
            with open(codeowners_file, "w") as codeowners:
                codeowners.write(codeowners_conf or "")

            assert self.codeowners_filename in os.listdir(self.test_path)
            assert (
                validate_change_control(
                    file_path=self.test_path,
                    metadata=metadata,
                    codeowners_file=codeowners_file,
                )
                is expected_result
            )

    def test_validate_change_control_no_owners_in_codeowners(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org"],
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        self.check_metadata(
            runner=runner, metadata_conf=metadata, expected_result=False
        )

    def test_validate_change_control_no_owners_in_metadata(self, runner):
        metadata = {
            "friendly_name": "test",
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        self.check_metadata(
            runner=runner, metadata_conf=metadata, expected_result=False
        )

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

    def test_validate_change_control_all_owners(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": [
                "test@example.org",
                "test2@example.org",
                "test3@example.org",
            ],
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        codeowners = "/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 test@example.org test2@example.org test3@example.org"
        self.check_metadata(
            runner=runner,
            metadata_conf=metadata,
            codeowners_conf=codeowners,
            expected_result=True,
        )

    def test_validate_change_control_github_identity(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": [
                "test@example.org",
                "test2@example.org",
                "mozilla/reviewers",
            ],
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        codeowners = (
            "/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 @mozilla/reviewers"
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

    def test_validate_change_control_more_than_one_owner_but_not_all(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org", "test2@example.org", "test3@example.org"],
            "labels": {"change_controlled": "true"},
        }
        codeowners = "/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 test@example.org test2@example.org"
        self.check_metadata(
            runner=runner,
            metadata_conf=metadata,
            codeowners_conf=codeowners,
            expected_result=True,
        )

    def test_validate_change_control_commented_line(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org", "test2@example.org"],
            "labels": {"change_controlled": "true"},
        }
        codeowners = (
            "#/sql/moz-fx-data-shared-prod/telemetry_derived/query_v1 test@example.org"
        )
        self.check_metadata(
            runner=runner,
            metadata_conf=metadata,
            codeowners_conf=codeowners,
            expected_result=False,
        )

    def test_metadata_update_with_no_deprecation(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))
            name = [
                str(tmpdirname)
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/"
            ]
            runner.invoke(update, name, "--sql_dir=" + str(tmpdirname) + "/sql")
            with open(
                tmpdirname
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/metadata.yaml",
                "r",
            ) as stream:
                metadata = yaml.safe_load(stream)
        assert metadata["workgroup_access"][0]["role"] == "roles/bigquery.dataViewer"
        assert metadata["workgroup_access"][0]["members"] == [
            "workgroup:mozilla-confidential"
        ]
        assert not metadata["deprecated"]

    def test_metadata_update_with_deprecation(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))
            name = [
                str(tmpdirname)
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v1/"
            ]
            runner.invoke(update, name, "--sql_dir=" + str(tmpdirname) + "/sql")
            with open(
                tmpdirname
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v1/metadata.yaml",
                "r",
            ) as stream:
                metadata = yaml.safe_load(stream)

            with open(
                tmpdirname
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/dataset_metadata.yaml",
                "r",
            ) as stream:
                dataset_metadata = yaml.safe_load(stream)

        assert metadata["workgroup_access"] == []
        assert metadata["deprecated"]
        assert dataset_metadata["workgroup_access"] == []
        assert dataset_metadata["default_table_workgroup_access"] == [
            {
                "members": ["workgroup:mozilla-confidential"],
                "role": "roles/bigquery.dataViewer",
            }
        ]

    def test_metadata_update_do_not_update(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))
            name = [
                str(tmpdirname)
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_scalar_aggregates_v1/"
            ]
            runner.invoke(update, name, "--sql_dir=" + str(tmpdirname) + "/sql")
            with open(
                tmpdirname
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_scalar_aggregates_v1/metadata.yaml",
                "r",
            ) as stream:
                metadata = yaml.safe_load(stream)
                print(metadata)
        assert metadata["workgroup_access"][0]["role"] == "roles/bigquery.dataViewer"
        assert metadata["workgroup_access"][0]["members"] == ["workgroup:revenue/cat4"]
        assert "deprecated" not in metadata
