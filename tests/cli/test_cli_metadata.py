import os
import shutil
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
    BIGEYE_PREDEFINED_FILE,
    CODEOWNERS_FILE,
    count_schema_fields,
    find_bigeye_checks,
    validate,
    validate_asset_level,
    validate_change_control,
    validate_col_desc_enforced,
    validate_shredder_mitigation,
)
from bigquery_etl.schema import Schema

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

    @patch("bigquery_etl.metadata.validate_metadata.validate_change_control")
    def test_validate_change_control_called_with_correct_project(
        self, mock_validate_change_control, runner
    ):
        project = "test-project"
        test_path = f"sql/{project}/fenix_derived/query_v1"
        metadata = {
            "friendly_name": "test",
            "owners": [
                "test@example.org",
                "test2@example.org",
                "test3@example.org",
            ],
            "labels": {"change_controlled": "true", "foo": "abc"},
        }
        with runner.isolated_filesystem():
            os.makedirs(test_path)
            with open(f"{test_path}/metadata.yaml", "w") as f:
                f.write(yaml.dump(metadata))

            validate(target=test_path)

            metadata = Metadata.from_file(f"{test_path}/metadata.yaml")

            mock_validate_change_control.assert_called_once()
            mock_validate_change_control.assert_called_once_with(
                file_path=test_path,
                metadata=metadata,
                codeowners_file=CODEOWNERS_FILE,
                project_id=project,
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

    def test_validate_change_control_all_owners_using_wildcard(self, runner):
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
            "/sql/**/query_v1 test@example.org test2@example.org test3@example.org"
        )
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

    def test_count_schema_fields_empty_file(self):
        schema = ""
        assert count_schema_fields(schema) == (0, 0)
        schema = []
        assert count_schema_fields(schema) == (0, 0)

    def test_count_schema_fields_empty_schema(self, runner):
        schema = {"fields": []}
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            schema_path = Path(self.test_path) / "schema.yaml"
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))
            assert count_schema_fields(Schema.from_schema_file(schema_path)) == (0, 0)

    def test_count_schema_fields_flat_schema(self, runner):
        schema = {
            "fields": [
                {"name": "column_1", "type": "STRING", "description": "Description 1"},
                {"name": "column_2", "type": "INTEGER", "description": "Description 2"},
            ]
        }
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            schema_path = Path(self.test_path) / "schema.yaml"
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))
            schema_from_file = Schema.from_schema_file(schema_path).to_bigquery_schema()
            assert count_schema_fields(schema_from_file) == (2, 2)

    def test_count_schema_fields_missing_description(self, runner):
        schema = {
            "fields": [
                {"name": "column_1", "type": "STRING"},
                {"name": "column_2", "type": "INTEGER", "description": "Description 2"},
            ]
        }
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            schema_path = Path(self.test_path) / "schema.yaml"
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))
            schema_from_file = Schema.from_schema_file(schema_path).to_bigquery_schema()
            assert count_schema_fields(schema_from_file) == (2, 1)

    def test_count_schema_fields_nested_schema(self, runner):
        schema = {
            "fields": [
                {
                    "name": "column_1",
                    "type": "RECORD",
                    "description": "Parent field",
                    "fields": [
                        {
                            "name": "column_1_1",
                            "type": "STRING",
                            "description": "Description 1.1",
                        },
                        {"name": "column_1_2", "type": "INTEGER"},
                    ],
                }
            ]
        }
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            schema_path = Path(self.test_path) / "schema.yaml"
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))
            schema_from_file = Schema.from_schema_file(schema_path).to_bigquery_schema()
            assert count_schema_fields(schema_from_file) == (3, 2)

    def test_count_schema_fields_deep_nested_schema(self, runner):
        schema = {
            "fields": [
                {
                    "name": "column_1",
                    "type": "RECORD",
                    "description": "Description 1",
                    "fields": [
                        {
                            "name": "column_1_1",
                            "type": "RECORD",
                            "fields": [
                                {
                                    "name": "column_1_1_1",
                                    "description": "Description 1_1",
                                },
                            ],
                        }
                    ],
                }
            ]
        }
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            schema_path = Path(self.test_path) / "schema.yaml"
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))
            schema_from_file = Schema.from_schema_file(schema_path).to_bigquery_schema()
            assert count_schema_fields(schema_from_file) == (3, 2)

    def test_find_bigeye_checks_missing(self, runner):
        assert find_bigeye_checks(self.test_path) is False

    def test_find_bigeye_checks_missing_volume_check(self, runner, capfd):
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            bigeye_file_path = Path(self.test_path) / BIGEYE_PREDEFINED_FILE

            data = {
                "type": "BIGCONFIG_FILE",
                "tag_deployments": [
                    {
                        "column_selectors": [
                            {
                                "name": [{"name": f"{self.test_path}.*"}],
                                "metrics": [
                                    {
                                        "metric_type": {
                                            "type": "PREDEFINED",
                                            "predefined_metric": "FRESHNESS",
                                        },
                                        "metric_name": "FRESHNESS",
                                    },
                                ],
                            }
                        ]
                    }
                ],
            }
            with open(bigeye_file_path, "w") as f:
                yaml.safe_dump(data, f)
            assert find_bigeye_checks(self.test_path) is False
            captured = capfd.readouterr()
            assert "ERROR: Missing Bigeye metrics: {'VOLUME'}." in captured.out

    def test_find_bigeye_checks_missing_freshness_check(self, runner, capfd):
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            bigeye_file_path = Path(self.test_path) / BIGEYE_PREDEFINED_FILE

            data = {
                "type": "BIGCONFIG_FILE",
                "tag_deployments": [
                    {
                        "column_selectors": [
                            {
                                "name": [{"name": f"{self.test_path}.*"}],
                                "metrics": [
                                    {
                                        "metric_type": {
                                            "type": "PREDEFINED",
                                            "predefined_metric": "VOLUME",
                                        },
                                        "metric_name": "VOLUME",
                                    },
                                ],
                            }
                        ]
                    }
                ],
            }
            with open(bigeye_file_path, "w") as f:
                yaml.safe_dump(data, f)
            assert find_bigeye_checks(self.test_path) is False
            captured = capfd.readouterr()
            assert "ERROR: Missing Bigeye metrics: {'FRESHNESS'}." in captured.out

    def test_find_bigeye_checks_ok(self, runner):
        with runner.isolated_filesystem():
            bigeye_metrics = {
                "type": "BIGCONFIG_FILE",
                "tag_deployments": [
                    {
                        "column_selectors": {"name": "{query_path}.*"},
                        "metrics": [
                            {
                                "metric_type": {
                                    "type": "PREDEFINED",
                                    "predefined_metric": "FRESHNESS",
                                },
                                "metric_name": "FRESHNESS",
                            },
                            {
                                "metric_type": {
                                    "type": "PREDEFINED",
                                    "predefined_metric": "VOLUME",
                                },
                                "metric_name": "VOLUME [fail]",
                            },
                        ],
                    }
                ],
            }
            os.makedirs(self.test_path, exist_ok=True)
            bigeye_file_path = Path(self.test_path) / BIGEYE_PREDEFINED_FILE

            with open(bigeye_file_path, "w") as f:
                yaml.safe_dump(bigeye_metrics, f)
            result = find_bigeye_checks(str(self.test_path))
            assert result is True

    def check_test_level(
        self,
        runner,
        metadata,
        query=None,
        schema=None,
        with_unittests=None,
        with_bigeye_metrics=None,
        expected_result=None,
        expected_exception=None,
        capfd=None,
    ):
        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            metadata_path = Path(self.test_path) / "metadata.yaml"
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))

            if query:
                query_path = Path(self.test_path) / "query.sql"
                with open(query_path, "w") as f:
                    f.write(query)
            if schema:
                schema_path = Path(self.test_path) / "schema.yaml"
                with open(schema_path, "w") as f:
                    f.write(yaml.safe_dump(schema))

            if with_unittests:
                unittest_path = os.path.join("tests", self.test_path, "test_query_v1")
                os.makedirs(unittest_path, exist_ok=True)

            if with_bigeye_metrics:
                bigeye_metrics = {
                    "type": "BIGCONFIG_FILE",
                    "tag_deployments": [
                        {
                            "column_selectors": {"name": "{query_path}.*"},
                            "metrics": [
                                {
                                    "metric_type": {
                                        "type": "PREDEFINED",
                                        "predefined_metric": "FRESHNESS",
                                    },
                                    "metric_name": "FRESHNESS",
                                },
                                {
                                    "metric_type": {
                                        "type": "PREDEFINED",
                                        "predefined_metric": "VOLUME",
                                    },
                                    "metric_name": "VOLUME [fail]",
                                },
                            ],
                        }
                    ],
                }

                bigeye_file_path = Path(self.test_path) / BIGEYE_PREDEFINED_FILE
                with open(bigeye_file_path, "w") as f:
                    yaml.safe_dump(bigeye_metrics, f)

            metadata_from_file = Metadata.from_file(metadata_path)
            result = validate_asset_level(self.test_path, metadata_from_file)
            assert result is expected_result

            if capfd:
                captured = capfd.readouterr()
                assert expected_exception in captured.out

    def test_level_is_not_set(self, runner):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org", "test2@example.org"],
        }

        self.check_test_level(runner=runner, metadata=metadata, expected_result=True)

    def test_level_is_not_string(self, runner, capfd):
        metadata = {"friendly_name": "test", "labels": {"level": ["gold", "silver"]}}

        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            metadata_path = Path(self.test_path) / "metadata.yaml"
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))

            with pytest.raises(ValueError) as e:
                _ = Metadata.from_file(metadata_path)
            expected_exc = "Invalid label format: ['gold', 'silver']"
            assert (str(e.value)) == expected_exc

    def test_level_multiple_values(self, runner, capfd):
        metadata = {"friendly_name": "test", "labels": {"level": "silver, gold"}}

        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            metadata_path = Path(self.test_path) / "metadata.yaml"
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))

            with pytest.raises(ValueError) as e:
                _ = Metadata.from_file(metadata_path)
            expected_exc = "Invalid label format: silver, gold"
            assert (str(e.value)) == expected_exc

    def test_level_unknown_value(self, runner, capfd):
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }
        metadata = {"friendly_name": "test", "labels": {"level": "kpi"}}

        with runner.isolated_filesystem():
            os.makedirs(self.test_path, exist_ok=True)
            metadata_path = Path(self.test_path) / "metadata.yaml"
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))

            Metadata.from_file(metadata_path)
            expected_exception = "Invalid level in metadata: kpi. Must be one of "
            self.check_test_level(
                runner=runner,
                query=query,
                schema=schema,
                metadata=metadata,
                with_unittests=True,
                with_bigeye_metrics=True,
                expected_result=False,
                expected_exception=expected_exception,
                capfd=capfd,
            )

    def test_level_gold_comply_is_table(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "scheduling": {"dag_name": "bqetl_default"},
            "labels": {"change_controlled": "true", "foo": "abc", "level": "gold"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = "Metadata level gold achieved!"
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=True,
            with_bigeye_metrics=True,
            expected_result=True,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_gold_comply_is_view(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "labels": {"change_controlled": "true", "foo": "abc", "level": "gold"},
        }
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = "Metadata level gold achieved!"
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            schema=schema,
            with_unittests=True,
            with_bigeye_metrics=True,
            expected_result=True,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_gold_not_comply_missing_bigeye_metrics(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "scheduling": {"dag_name": "bqetl_default"},
            "labels": {"change_controlled": "true", "foo": "abc", "level": "gold"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = "ERROR. Metadata Level 'gold' not achieved. Missing: bigeye_predefined_metrics"
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=True,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_gold_not_comply_missing_column_descriptions(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "scheduling": {"dag_name": "bqetl_default"},
            "labels": {"change_controlled": "true", "foo": "abc", "level": "gold"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                },
            ]
        }

        expected_exception = "ERROR. Metadata Level 'gold' not achieved. Missing: column_descriptions_all"
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=True,
            with_bigeye_metrics=True,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_silver_not_comply_missing_column_descriptions(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "scheduling": {"dag_name": "bqetl_default"},
            "labels": {"change_controlled": "true", "foo": "abc", "level": "silver"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                },
            ]
        }

        expected_exception = "ERROR. Metadata Level 'silver' not achieved. Missing: column_descriptions_70_percent"

        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=True,
            with_bigeye_metrics=True,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_gold_not_comply_missing_unittests(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "scheduling": {"dag_name": "bqetl_default"},
            "labels": {"change_controlled": "true", "foo": "abc", "level": "gold"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = (
            "ERROR. Metadata Level 'gold' not achieved. Missing: unittests"
        )
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=False,
            with_bigeye_metrics=True,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_gold_not_comply_missing_description(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "owners": ["test@example.org", "test2@example.org"],
            "labels": {"change_controlled": "true", "foo": "abc", "level": "gold"},
            "scheduling": {"dag_name": "bqetl_default"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = (
            "ERROR. Metadata Level 'gold' not achieved. Missing: description"
        )
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=True,
            with_bigeye_metrics=True,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_gold_not_comply_missing_scheduler(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "labels": {"change_controlled": "true", "foo": "abc", "level": "gold"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "mode": "NULLABLE",
                    "name": "column_1",
                    "type": "STRING",
                    "description": "Description 1",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = (
            "ERROR. Metadata Level 'gold' not achieved. Missing: scheduler"
        )
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=True,
            with_bigeye_metrics=True,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_silver_comply_nested_fields(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "owners": ["test@example.org", "test2@example.org"],
            "scheduling": {"dag_name": "bqetl_default"},
            "labels": {"level": "silver"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "name": "column_1",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "column 1a",
                            "type": "STRING",
                            "description": "Description 1",
                        },
                        {
                            "name": "column 1b",
                            "type": "STRING",
                            "description": "Description 1",
                        },
                    ],
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = "Metadata level silver achieved!"
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            with_unittests=True,
            with_bigeye_metrics=True,
            expected_result=True,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_silver_not_comply_missing_all(self, runner, capfd):
        metadata = {"friendly_name": "test", "labels": {"level": "silver"}}
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "name": "column_1",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "column 1a",
                            "type": "STRING",
                            "description": "Description 1",
                        },
                        {
                            "name": "column 1b",
                            "type": "STRING",
                        },
                    ],
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                    "description": "Description 2",
                },
            ]
        }

        expected_exception = (
            "ERROR. Metadata Level 'silver' not achieved. "
            "Missing: description, unittests, scheduler, "
            "bigeye_predefined_metrics, column_descriptions_70_percent"
        )
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_bronze_fail_missing_schema(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "labels": {"level": "bronze"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"

        expected_exception = "missing the required schema.yaml"
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            expected_result=False,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_level_bronze_comply(self, runner, capfd):
        metadata = {
            "friendly_name": "test",
            "description": "Table description.",
            "labels": {"level": "bronze"},
        }
        query = "SELECT column_1, column_2 FROM test_table group by column_1, column_2"
        schema = {
            "fields": [
                {
                    "name": "column_1",
                    "type": "STRING",
                },
                {
                    "name": "column_2",
                    "type": "STRING",
                },
            ]
        }

        expected_exception = "Metadata level bronze achieved!"
        self.check_test_level(
            runner=runner,
            metadata=metadata,
            query=query,
            schema=schema,
            expected_result=True,
            expected_exception=expected_exception,
            capfd=capfd,
        )

    def test_metadata_update_with_no_deprecation(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)
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
            "workgroup:mozilla-confidential/data-viewers",
            "workgroup:test/test",
        ]
        assert "deprecated" not in metadata

    def test_metadata_update_with_deprecation(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)
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

        assert metadata["workgroup_access"] == [
            {
                "members": ["workgroup:mozilla-foo"],
                "role": "roles/bigquery.dataEditor",
            }
        ]
        assert metadata["deprecated"]
        assert dataset_metadata["workgroup_access"] == [
            {
                "members": ["workgroup:mozilla-test"],
                "role": "roles/bigquery.dataEditor",
            },
            {
                "members": ["workgroup:mozilla-confidential/data-viewers"],
                "role": "roles/bigquery.metadataViewer",
            },
        ]
        assert dataset_metadata["default_table_workgroup_access"] == [
            {
                "members": ["workgroup:mozilla-confidential/data-viewers"],
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
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)
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
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)
            name = [
                str(tmpdirname)
                + "/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v2/"
            ]
            runner.invoke(publish, name, "--sql_dir=" + str(tmpdirname) + "/sql")

        assert mock_bigquery_client().update_table.call_count == 0

    @patch("bigquery_etl.cli.metadata._publish_metadata")
    def test_metadata_publish_skip_ingestion_true(self, mock_publish, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            shutil.copytree(str(TEST_DIR / "sql"), f"{tmpdirname}", dirs_exist_ok=True)

            def invoke_command(project_id, table_id):
                runner.invoke(
                    publish,
                    [
                        table_id,
                        f"--sql-dir={str(tmpdirname)}",
                        "--skip-stable-datasets=true",
                        "--parallelism=1",
                        f"--project-id={project_id}",
                    ],
                    catch_exceptions=False,
                )

            invoke_command("moz-fx-data-shared-prod", "test_derived.test_v1")
            invoke_command("moz-fx-data-shared-prod", "test_stable.test_v1")
            invoke_command("glam-fenix-dev", "test_stable.test_v1")

        assert mock_publish.call_count == 2
        mock_publish.assert_any_call(
            "moz-fx-data-shared-prod",
            credentials=None,
            metadata_file=Path(tmpdirname)
            / "moz-fx-data-shared-prod"
            / "test_derived"
            / "test_v1"
            / "metadata.yaml",
        )
        mock_publish.assert_any_call(
            "glam-fenix-dev",
            credentials=None,
            metadata_file=Path(tmpdirname)
            / "glam-fenix-dev"
            / "test_stable"
            / "test_v1"
            / "metadata.yaml",
        )

    @patch("bigquery_etl.cli.metadata._publish_metadata")
    def test_metadata_publish_skip_ingestion_false(self, mock_publish, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            shutil.copytree(str(TEST_DIR / "sql"), f"{tmpdirname}", dirs_exist_ok=True)

            def invoke_command(project_id, table_id):
                runner.invoke(
                    publish,
                    [
                        table_id,
                        f"--sql-dir={str(tmpdirname)}",
                        "--skip-stable-datasets=false",
                        "--parallelism=1",
                        f"--project-id={project_id}",
                    ],
                    catch_exceptions=False,
                )

            invoke_command("moz-fx-data-shared-prod", "test_derived.test_v1")
            invoke_command("moz-fx-data-shared-prod", "test_stable.test_v1")
            invoke_command("glam-fenix-dev", "test_stable.test_v1")

        assert mock_publish.call_count == 3
        mock_publish.assert_any_call(
            "moz-fx-data-shared-prod",
            credentials=None,
            metadata_file=Path(tmpdirname)
            / "moz-fx-data-shared-prod"
            / "test_stable"
            / "test_v1"
            / "metadata.yaml",
        )

    def test_metadata_deprecate_default_deletion_date(self, runner):
        with tempfile.TemporaryDirectory() as tmpdirname:
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)

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
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)

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
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)

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
            shutil.copytree(str(TEST_DIR), str(tmpdirname), dirs_exist_ok=True)

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

    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery.Table")
    def test_validate_shredder_mitigation_schema_columns(
        self, mock_bigquery_table, mock_bigquery_client, runner, capfd
    ):
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

        mock_bigquery_client().get_table.return_value = mock_bigquery_table()

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

    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery.Table")
    def test_validate_shredder_mitigation_group_by(
        self, mock_bigquery_table, mock_bigquery_client, runner, capfd
    ):
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

        mock_bigquery_client().get_table.return_value = mock_bigquery_table()

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

    @patch("google.cloud.bigquery.Client")
    @patch("google.cloud.bigquery.Table")
    def test_validate_shredder_mitigation_table_empty_or_not_exists(
        self, mock_bigquery_table, mock_bigquery_client, runner, capfd
    ):
        """Test that validation fails when the table doesn't exist."""
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
                    "description": "column 1.",
                },
            ]
        }

        mock_instance = mock_bigquery_client.return_value
        mock_instance.query.return_value.result.return_value = False

        expected_exc = (
            "The shredder-mitigation label can only be applied to existing and non-empty tables.\n"
            "Ensure that the table"
        )

        with runner.isolated_filesystem():
            query_path = Path(self.test_path) / "query.sql"
            metadata_path = Path(self.test_path) / "metadata.yaml"
            schema_path = Path(self.test_path) / "schema.yaml"
            os.makedirs(self.test_path, exist_ok=True)

            with open(query_path, "w") as f:
                f.write("SELECT column_1 FROM test_table GROUP BY column_1")
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))

            metadata_from_file = Metadata.from_file(metadata_path)
            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is False
            assert expected_exc in captured.out

            mock_instance_true = mock_bigquery_client.return_value
            mock_instance_true.query.return_value.result.return_value = True

            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is True
            assert captured.out == ""

            mock_instance_true = mock_bigquery_client.return_value
            mock_instance_true.query.return_value.result.return_value = None

            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is False
            assert expected_exc in captured.out

    @patch("google.cloud.bigquery.Client")
    def test_capture_validate_exception(self, mock_bigquery_client, runner, capfd):
        mock_instance = mock_bigquery_client.return_value
        mock_instance.query.side_effect = Exception("Test")

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
                    "description": "column 1.",
                },
            ]
        }

        expected_exc = (
            "Please check that the name is correct and if the table is in "
            "a private repository, ensure that it exists and has data before"
            " running a backfill with shredder mitigation."
        )

        with runner.isolated_filesystem():
            query_path = Path(self.test_path) / "query.sql"
            metadata_path = Path(self.test_path) / "metadata.yaml"
            schema_path = Path(self.test_path) / "schema.yaml"
            os.makedirs(self.test_path, exist_ok=True)

            with open(query_path, "w") as f:
                f.write("SELECT column_1 FROM test_table GROUP BY column_1")
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))

            metadata_from_file = Metadata.from_file(metadata_path)

            result = validate_shredder_mitigation(self.test_path, metadata_from_file)
            captured = capfd.readouterr()
            assert result is True
            assert expected_exc in captured.out

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

    def test_validate_col_desc_enforced(self, runner):
        """Test that validation fails when metadata says enforce column descriptions but a column desc is missing"""
        metadata = {"friendly_name": "Test", "require_column_descriptions": True}
        schema = {
            "fields": [
                {
                    "mode": "REQUIRED",
                    "name": "profile_id",
                    "type": "STRING",
                    "description": "description 1",
                },
                {"mode": "REQUIRED", "name": "column_3", "type": "STRING"},
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
                f.write(yaml.safe_dump(schema))
            metadata_from_file = Metadata.from_file(metadata_path)

            result = validate_col_desc_enforced(self.test_path, metadata_from_file)
            assert result is False

    def test_validate_col_desc_passes_when_not_enforced(self, runner):
        """Test that validation passes when metadata says enforce column descriptions is False and a col desc is missing"""
        metadata = {"friendly_name": "Test", "require_column_descriptions": False}
        schema = {
            "fields": [
                {
                    "mode": "REQUIRED",
                    "name": "profile_id",
                    "type": "STRING",
                    "description": "description 1",
                },
                {"mode": "REQUIRED", "name": "column_3", "type": "STRING"},
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
                f.write(yaml.safe_dump(schema))
            metadata_from_file = Metadata.from_file(metadata_path)

            result = validate_col_desc_enforced(self.test_path, metadata_from_file)
            assert result is True

    def test_validate_col_desc_passes_with_all_col_desc_and_enforcement(self, runner):
        """Test that validation passes when metadata says enforce column descriptions is True and all cols have a desc"""
        metadata = {"friendly_name": "Test", "require_column_descriptions": True}
        schema = {
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
                f.write("SELECT column_1 FROM test_table group by column_1")
            with open(metadata_path, "w") as f:
                f.write(yaml.safe_dump(metadata))
            with open(schema_path, "w") as f:
                f.write(yaml.safe_dump(schema))
            metadata_from_file = Metadata.from_file(metadata_path)

            result = validate_col_desc_enforced(self.test_path, metadata_from_file)
            assert result is True

    def test_nested_validate_col_desc1(self, runner):
        """Test that validation fails when metadata says enforce column descriptions is True and no desc on nested"""
        metadata = {"friendly_name": "Test", "require_column_descriptions": True}

        schema = {
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
                    "description": "description 2",
                },
                {
                    "mode": "REQUIRED",
                    "name": "metadata",
                    "type": "RECORD",
                    "description": "description 2",
                    "fields": [
                        {"mode": "REQUIRED", "name": "column_3", "type": "STRING"}
                    ],
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
                f.write(yaml.safe_dump(schema))
            metadata_from_file = Metadata.from_file(metadata_path)

            result = validate_col_desc_enforced(self.test_path, metadata_from_file)
            assert result is False

    def test_nested_validate_col_desc2(self, runner):
        """Test that validation passes when metadata says enforce column descriptions is True and there are desc"""
        metadata = {"friendly_name": "Test", "require_column_descriptions": True}

        schema = {
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
                    "description": "description 2",
                },
                {
                    "mode": "REQUIRED",
                    "name": "metadata",
                    "type": "RECORD",
                    "description": "description 2",
                    "fields": [
                        {
                            "mode": "REQUIRED",
                            "name": "column_3",
                            "type": "STRING",
                            "description": "nested desc",
                        }
                    ],
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
                f.write(yaml.safe_dump(schema))
            metadata_from_file = Metadata.from_file(metadata_path)

            result = validate_col_desc_enforced(self.test_path, metadata_from_file)
            assert result is True
