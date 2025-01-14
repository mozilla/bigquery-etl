import distutils
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner
from dateutil.relativedelta import relativedelta

from bigquery_etl.cli.metadata import deprecate, publish, update
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.metadata.validate_metadata import (
    validate,
    validate_change_control,
    validate_shredder_mitigation,
)

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
            "workgroup:test/test",
            "workgroup:mozilla-confidential",
        ]
        assert "deprecated" not in metadata

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
        assert dataset_metadata["workgroup_access"] == [
            {
                "members": ["workgroup:mozilla-test"],
                "role": "roles/bigquery.dataEditor",
            }
        ]
        assert dataset_metadata["default_table_workgroup_access"] == [
            {
                "members": ["workgroup:mozilla-confidential"],
                "role": "roles/bigquery.dataViewer",
            }
        ]

    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery.Table")
    def test_metadata_publish(self, mock_bigquery_table, mock_bigquery_client, runner):
        mock_bigquery_client().get_table.return_value = mock_bigquery_table()
        from distutils import log

        log.set_verbosity(log.WARN)
        log.set_threshold(log.WARN)

        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))
            name = (
                str(tmpdirname)
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v1/"
            )

            runner.invoke(
                publish,
                [name, "--parallelism=0", "--sql_dir=" + str(tmpdirname) + "/sql"],
                catch_exceptions=False,
            )

        assert mock_bigquery_client().update_table.call_count == 1
        assert (
            mock_bigquery_client().update_table.call_args[0][0].friendly_name
            == "Test metadata.yaml"
        )
        assert (
            mock_bigquery_client().update_table.call_args[0][0].description
            == "Clustering fields: `column1`"
        )
        assert mock_bigquery_client().update_table.call_args[0][0].labels == {
            "deletion_date": "2024-03-02",
            "deprecated": "true",
            "owner1": "test",
            "monitoring": "true",
        }
        assert (
            mock_bigquery_client()
            .get_table(
                "moz-fx-data-shared-prod.telemetry_derived.clients_daily_scalar_aggregates_v1"
            )
            .friendly_name
            == "Test metadata.yaml"
        )
        assert mock_bigquery_table().friendly_name == "Test metadata.yaml"
        assert mock_bigquery_table().description == "Clustering fields: `column1`"
        assert mock_bigquery_table().labels == {
            "deletion_date": "2024-03-02",
            "deprecated": "true",
            "owner1": "test",
            "monitoring": "true",
        }

    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery.Table")
    def test_metadata_publish_with_no_metadata_file(
        self, mock_bigquery_table, mock_bigquery_client, runner
    ):
        mock_bigquery_client().get_table.return_value = mock_bigquery_table()

        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))
            name = [
                str(tmpdirname)
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v2/"
            ]
            runner.invoke(publish, name, "--sql_dir=" + str(tmpdirname) + "/sql")

        assert mock_bigquery_client().update_table.call_count == 0

    def test_metadata_deprecate_default_deletion_date(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))

            qualified_table_name = (
                "moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6"
            )
            result = runner.invoke(
                deprecate,
                [qualified_table_name, "--sql_dir=" + str(tmpdirname) + "/sql"],
            )
            with open(
                tmpdirname
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/metadata.yaml",
                "r",
            ) as stream:
                metadata = yaml.safe_load(stream)

        default_deletion_date = (datetime.today() + relativedelta(months=+3)).date()

        assert result.exit_code == 0
        assert metadata["deprecated"]
        assert metadata["deletion_date"] == default_deletion_date

    def test_metadata_deprecate_set_deletion_date(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))

            qualified_table_name = (
                "moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6"
            )
            result = runner.invoke(
                deprecate,
                [
                    qualified_table_name,
                    "--deletion_date=2024-03-02",
                    "--sql_dir=" + str(tmpdirname) + "/sql",
                ],
            )
            with open(
                tmpdirname
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/metadata.yaml",
                "r",
            ) as stream:
                metadata = yaml.safe_load(stream)

        assert result.exit_code == 0
        assert metadata["deprecated"]
        assert metadata["deletion_date"] == datetime(2024, 3, 2).date()

    def test_metadata_deprecate_set_invalid_deletion_date_should_fail(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))

            qualified_table_name = (
                "moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6"
            )
            result = runner.invoke(
                deprecate,
                [
                    qualified_table_name,
                    "--deletion_date=2024-02",
                    "--sql_dir=" + str(tmpdirname) + "/sql",
                ],
            )
            with open(
                tmpdirname
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/metadata.yaml",
                "r",
            ) as stream:
                metadata = yaml.safe_load(stream)

        assert result.exit_code == 2
        assert "deprecated" not in metadata
        assert "deletion_date" not in metadata
        assert "Invalid value for '--deletion_date'" in result.output

    def test_metadata_deprecate_no_metadata(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            distutils.dir_util.copy_tree(str(TEST_DIR), str(tmpdirname))

            qualified_table_name = "moz-fx-data-shared-prod.telemetry_derived.clients_daily_scalar_aggregates_v2"
            result = runner.invoke(
                deprecate,
                [
                    qualified_table_name,
                    "--deletion_date=2024-03-02",
                    "--sql_dir=" + str(tmpdirname) + "/sql",
                ],
            )

            assert result.exit_code == 1
            assert (
                str(result.exception)
                == f"No metadata file(s) were found for: {qualified_table_name}"
            )

    def test_validate_shredder_mitigation_schema_columns(self, runner, capfd):
        """Test that validation fails when query contains id-level columns or descriptions are missing."""
        metadata = {
            "friendly_name": "Test",
            "labels": {"shredder_mitigation": "true"},
        }
        schema_1 = {
            "fields": [
                {
                    "mode": "REQUIRED",
                    "name": "profile_id",
                    "type": "STRING",
                    "description": "description 1",
                },
                {
                    "mode": "REQUIRED",
                    "name": "column_3",
                    "type": "STRING",
                    "description": "description 3",
                },
            ]
        }
        schema_2 = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "description 3",
                },
            ]
        }

        with runner.isolated_filesystem():
            query_path = Path(self.test_path) / "query.sql"
            metadata_path = Path(self.test_path) / "metadata.yaml"
            schema_path = Path(self.test_path) / "schema.yaml"
            os.makedirs(self.test_path, exist_ok=True)

            with open(query_path, "w") as f:
                f.write("SELECT column_1 FROM test_table group by column_1")
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema_1))
            metadata_from_file = Metadata.from_file(metadata_path)

            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is False
            assert (
                "Shredder mitigation validation failed, profile_id is an id-level column "
                "that is not allowed for this type of backfill."
            ) in captured.out

            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema_2))
            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is False
            assert (
                "Shredder mitigation validation failed, column_1 does not have a description"
                " in the schema."
            ) in captured.out

    def test_validate_shredder_mitigation_group_by(self, runner, capfd):
        """Test that validation fails and prints a notification when group by contains numbers."""
        metadata = {
            "friendly_name": "Test",
            "labels": {"shredder_mitigation": "true"},
        }

        schema = {
            "fields": [
                {
                    "mode": "REQUIRED",
                    "name": "column_1",
                    "type": "STRING",
                },
                {
                    "mode": "REQUIRED",
                    "name": "column_2",
                    "type": "STRING",
                    "description": "description 2",
                },
            ]
        }

        with runner.isolated_filesystem():
            query_path = Path(self.test_path) / "query.sql"
            metadata_path = Path(self.test_path) / "metadata.yaml"
            schema_path = Path(self.test_path) / "schema.yaml"
            os.makedirs(self.test_path, exist_ok=True)

            with open(query_path, "w") as f:
                f.write("SELECT column_1 FROM test_table GROUP BY ALL")
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))

            metadata_from_file = Metadata.from_file(metadata_path)
            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is False
            assert (
                "Shredder mitigation validation failed, GROUP BY must use an explicit list of"
                " columns. Avoid expressions like `GROUP BY ALL` or `GROUP BY 1, 2, 3`."
            ) in captured.out

            with open(query_path, "w") as f:
                f.write("SELECT column_1 FROM test_table GROUP BY column_1, 2")
            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is False
            assert (
                "Shredder mitigation validation failed, GROUP BY must use an explicit list "
                "of columns. Avoid expressions like `GROUP BY ALL` or `GROUP BY 1, 2, 3`."
            ) in captured.out

    def test_validate_metadata_without_labels(self, runner, capfd):
        """Test that metadata validation doesn't fail when labels are not present."""
        metadata = {
            "friendly_name": "Test",
            "labels": {},
        }
        schema = """
            fields:
              - mode: NULLABLE
                name: column_1
                type: DATE
            """

        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            with open(Path(self.test_path) / "metadata.yaml", "w") as f:
                f.write(yaml.safe_dump(metadata))
            with open(Path(self.test_path) / "schema.yaml", "w") as f:
                f.write(schema)

            result = validate(self.test_path)
            captured = capfd.readouterr()
            assert result is None
            assert captured.out == ""
