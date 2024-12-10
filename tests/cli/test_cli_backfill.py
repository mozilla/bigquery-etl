import json
import os
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner
from google.api_core.exceptions import NotFound

from bigquery_etl.backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from bigquery_etl.backfill.utils import (
    BACKFILL_DESTINATION_DATASET,
    BACKFILL_DESTINATION_PROJECT,
    get_backfill_backup_table_name,
    get_backfill_file_from_qualified_table_name,
    get_backfill_staging_qualified_table_name,
    get_entries_from_qualified_table_name,
    get_qualified_table_name_to_entries_map_by_project,
    qualified_table_name_matching,
    validate_metadata_workgroups,
)
from bigquery_etl.cli.backfill import (
    complete,
    create,
    info,
    initiate,
    scheduled,
    validate,
)
from bigquery_etl.cli.stage import QUERY_FILE
from bigquery_etl.deploy import FailedDeployException

DEFAULT_STATUS = BackfillStatus.INITIATE
VALID_REASON = "test_reason"
VALID_WATCHER = "test@example.org"
VALID_BACKFILL = Backfill(
    date(2021, 5, 4),
    date(2021, 1, 3),
    date(2021, 5, 3),
    [date(2021, 2, 3)],
    VALID_REASON,
    [VALID_WATCHER],
    DEFAULT_STATUS,
)
BACKFILL_YAML_TEMPLATE = (
    "2021-05-04:\n"
    "  start_date: 2021-01-03\n"
    "  end_date: 2021-05-03\n"
    "  excluded_dates:\n"
    "  - 2021-02-03\n"
    "  reason: test_reason\n"
    "  watchers:\n"
    "  - test@example.org\n"
    "  status: Initiate\n"
)

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

TABLE_METADATA_CONF_EMPTY_WORKGROUP = {
    "friendly_name": "test",
    "description": "test",
    "owners": ["test@example.org"],
    "workgroup_access": [],
}

TABLE_METADATA_CONF_DEPENDS_ON_PAST = {
    "friendly_name": "test",
    "description": "test",
    "owners": ["test@example.org"],
    "workgroup_access": VALID_WORKGROUP_ACCESS,
    "scheduling": {"depends_on_past": True},
}

DATASET_METADATA_CONF = {
    "friendly_name": "test",
    "description": "test",
    "dataset_base_acl": "derived",
    "workgroup_access": VALID_WORKGROUP_ACCESS,
}

DATASET_METADATA_CONF_EMPTY_WORKGROUP = {
    "friendly_name": "test",
    "description": "test",
    "dataset_base_acl": "derived",
    "workgroup_access": [],
    "default_table_workgroup_access": VALID_WORKGROUP_ACCESS,
}

PARTITIONED_TABLE_METADATA = {
    "friendly_name": "test",
    "description": "test",
    "owners": ["test@example.org"],
    "workgroup_access": VALID_WORKGROUP_ACCESS,
    "bigquery": {
        "time_partitioning": {
            "type": "day",
            "field": "submission_date",
            "require_partition_filter": True,
        }
    },
}

DEFAULT_BILLING_PROJECT = "moz-fx-data-backfill-slots"
VALID_BILLING_PROJECT = "moz-fx-data-backfill-1"
INVALID_BILLING_PROJECT = "mozdata"


class TestBackfill:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_create_backfill(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfill_file = SQL_DIR + "/" + BACKFILL_FILE
            backfill = Backfill.entries_from_file(backfill_file)[0]

            assert backfill.entry_date == date.today()
            assert backfill.start_date == date(2021, 3, 1)
            assert backfill.end_date == date.today()
            assert backfill.watchers == [DEFAULT_WATCHER]
            assert backfill.reason == DEFAULT_REASON
            assert backfill.status == DEFAULT_STATUS
            assert backfill.billing_project is None

    def test_create_backfill_with_billing_project(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                    f"--billing_project={VALID_BILLING_PROJECT}",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfill_file = SQL_DIR + "/" + BACKFILL_FILE
            backfill = Backfill.entries_from_file(backfill_file)[0]

            assert backfill.entry_date == date.today()
            assert backfill.start_date == date(2021, 3, 1)
            assert backfill.end_date == date.today()
            assert backfill.watchers == [DEFAULT_WATCHER]
            assert backfill.reason == DEFAULT_REASON
            assert backfill.status == DEFAULT_STATUS
            assert backfill.billing_project == VALID_BILLING_PROJECT

    def test_create_backfill_with_invalid_billing_project_should_fail(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                    f"--billing_project={INVALID_BILLING_PROJECT}",
                ],
            )

            assert result.exit_code == 1
            assert "Invalid billing project" in str(result.exception)

    def test_create_backfill_depends_on_past_should_fail(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF_DEPENDS_ON_PAST))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                ],
            )

            assert result.exit_code == 1
            assert (
                "Tables that depend on past are currently not supported."
                in result.output
            )

    def test_create_backfill_with_invalid_watcher(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            invalid_watcher = "test.org"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                    "--watcher=" + invalid_watcher,
                ],
            )
            assert result.exit_code == 1
            assert "Invalid" in str(result.exception)
            assert "watchers" in str(result.exception)

    def test_create_backfill_with_invalid_path(self, runner):
        with runner.isolated_filesystem():
            invalid_path = "test.test_query_v1"
            result = runner.invoke(create, [invalid_path, "--start_date=2021-03-01"])
            assert result.exit_code == 2
            assert "Invalid" in result.output
            assert "path" in result.output

    def test_create_backfill_with_invalid_start_date_greater_than_end_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            invalid_start_date = "2021-05-01"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=" + invalid_start_date,
                    "--end_date=2021-03-01",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid start date" in str(result.exception)

    def test_create_backfill_with_invalid_excluded_dates_before_start_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            invalid_exclude_date = "2021-03-01"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-05-01",
                    "--exclude=" + invalid_exclude_date,
                ],
            )
            assert result.exit_code == 1
            assert "Invalid excluded dates" in str(result.exception)

    def test_create_backfill_with_excluded_dates_after_end_date(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            invalid_exclude_date = "2021-07-01"
            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-05-01",
                    "--end_date=2021-06-01",
                    "--exclude=" + invalid_exclude_date,
                ],
            )
            assert result.exit_code == 1
            assert "Invalid excluded dates" in str(result.exception)

    def test_create_backfill_entry_with_all_params(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2021-03-01",
                    "--end_date=2021-03-10",
                    "--exclude=2021-03-05",
                    "--watcher=test@example.org",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfill_file = SQL_DIR + "/" + BACKFILL_FILE
            backfill = Backfill.entries_from_file(backfill_file)[0]

            assert backfill.start_date == date(2021, 3, 1)
            assert backfill.end_date == date(2021, 3, 10)
            assert backfill.watchers == [VALID_WATCHER]
            assert backfill.reason == DEFAULT_REASON
            assert backfill.status == DEFAULT_STATUS

    def test_create_backfill_with_exsting_entry(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_entry_1 = Backfill(
                date(2021, 5, 3),
                date(2021, 1, 3),
                date(2021, 5, 3),
                [date(2021, 2, 3)],
                VALID_REASON,
                [VALID_WATCHER],
                BackfillStatus.COMPLETE,
            )

            backfill_entry_2 = Backfill(
                date.today(),
                date(2023, 3, 1),
                date(2023, 3, 10),
                [],
                DEFAULT_REASON,
                [DEFAULT_WATCHER],
                DEFAULT_STATUS,
            )

            backfill_file = (
                Path("sql/moz-fx-data-shared-prod/test/test_query_v1") / BACKFILL_FILE
            )
            backfill_file.write_text(backfill_entry_1.to_yaml())
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfills = Backfill.entries_from_file(backfill_file)
            assert backfills[0] == backfill_entry_1

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2023-03-01",
                    "--end_date=2023-03-10",
                ],
            )

            assert result.exit_code == 0

            backfills = Backfill.entries_from_file(backfill_file)

            assert backfills[1] == backfill_entry_1
            assert backfills[0] == backfill_entry_2

    def test_create_backfill_with_existing_entry_with_initiate_status_should_fail(
        self, runner
    ):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_entry_1 = Backfill(
                date(2021, 5, 3),
                date(2021, 1, 3),
                date(2021, 5, 3),
                [date(2021, 2, 3)],
                VALID_REASON,
                [VALID_WATCHER],
                DEFAULT_STATUS,
            )

            backfill_file = (
                Path("sql/moz-fx-data-shared-prod/test/test_query_v1") / BACKFILL_FILE
            )
            backfill_file.write_text(backfill_entry_1.to_yaml())
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfills = Backfill.entries_from_file(backfill_file)
            assert backfills[0] == backfill_entry_1

            result = runner.invoke(
                create,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--start_date=2023-03-01",
                    "--end_date=2023-03-10",
                ],
            )

            assert result.exit_code == 1

            assert (
                "Backfill entries cannot contain more than one entry with Initiate status"
                in str(result.exception)
            )

    def test_validate_backfill(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(BACKFILL_YAML_TEMPLATE)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 0

    def test_validate_backfill_with_billing_project(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_text = (
                BACKFILL_YAML_TEMPLATE + f"  billing_project: {VALID_BILLING_PROJECT}"
            )
            backfill_file.write_text(backfill_text)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 0

    def test_validate_backfill_with_invalid_billing_project_should_fail(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_text = (
                BACKFILL_YAML_TEMPLATE + f"  billing_project: {INVALID_BILLING_PROJECT}"
            )
            backfill_file.write_text(backfill_text)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid billing project" in str(result.exception)

    def test_validate_backfill_depends_on_past_should_fail(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF_DEPENDS_ON_PAST))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(BACKFILL_YAML_TEMPLATE)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert (
                "Tables that depend on past are currently not supported"
                in result.output
            )

    def test_validate_backfill_invalid_table_name(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.",
                ],
            )
            assert result.exit_code == 1
            assert (
                "Qualified table name must be named like: <project>.<dataset>.<table>"
                in str(result.exception)
            )

    def test_validate_backfill_non_existing_table_name(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v2",
                ],
            )
            assert result.exit_code == 1
            assert "does not exist" in result.output

    def test_validate_backfill_invalid_default_reason(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_REASON, DEFAULT_REASON
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Default reason" in result.output

    def test_validate_backfill_empty_reason(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_reason = ""
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_REASON, invalid_reason
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert (
                "Reason in backfill entry should not be empty"
                in result.exception.args[0]
            )

    def test_validate_backfill_invalid_watcher(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_watcher = "test@example"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_WATCHER, invalid_watcher
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid" in str(result.exception)
            assert "watchers" in str(result.exception)

    def test_validate_backfill_empty_watcher(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_watcher = ""
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                VALID_WATCHER, invalid_watcher
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid" in str(result.exception)
            assert "watchers" in str(result.exception)

    def test_validate_backfill_watchers_duplicated(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_watchers = "  - test@example.org\n" "  - test@example.org\n"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                "  - " + VALID_WATCHER, invalid_watchers
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Duplicate watcher" in result.exception.args[0]

    def test_validate_backfill_invalid_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_status = "INVALIDSTATUS"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                DEFAULT_STATUS.value, invalid_status
            )
            backfill_file.write_text(invalid_backfill)
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert invalid_status in str(result.exception)

    def test_validate_backfill_duplicate_entry_dates(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE

            duplicate_entry_date = "2021-05-05"
            invalid_backfill = BACKFILL_YAML_TEMPLATE.replace(
                "2021-05-04", duplicate_entry_date
            )
            backfill_file.write_text(invalid_backfill + "\n" + invalid_backfill)

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Backfill entry already exists" in str(result.exception)

    def test_validate_backfill_invalid_entry_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_entry_date = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-05-04", invalid_entry_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "can't be in the future" in str(result.exception)

    def test_validate_backfill_invalid_start_date_greater_than_end_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_start_date = "2021-05-04"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-01-03", invalid_start_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid start date" in str(result.exception)

    def test_validate_backfill_invalid_start_date_greater_than_entry_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_start_date = "2021-05-05"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-01-03", invalid_start_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid start date" in str(result.exception)

    def test_validate_backfill_invalid_end_date_greater_than_entry_date(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_end_date = "2021-05-05"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-05-03", invalid_end_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid end date" in str(result.exception)

    def test_validate_backfill_invalid_excluded_dates_less_than_start_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_excluded_date = "2021-01-02"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-02-03", invalid_excluded_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid excluded dates" in str(result.exception)

    def test_validate_backfill_invalid_excluded_dates_greater_than_end_date(
        self, runner
    ):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            invalid_excluded_date = "2021-05-04"
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE.replace("2021-02-03", invalid_excluded_date)
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid excluded dates" in str(result.exception)

    def test_validate_backfill_invalid_excluded_dates_duplicated(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            duplicate_excluded_dates = "  - 2021-02-03\n" "  - 2021-02-03\n"
            backfill_file.write_text(
                (
                    "2021-05-04:\n"
                    "  start_date: 2021-01-03\n"
                    "  end_date: 2021-05-03\n"
                    "  excluded_dates:\n"
                    + duplicate_excluded_dates
                    + "  reason: test_reason\n"
                    "  watchers:\n"
                    "  - test@example.org\n"
                    "  status: Initiate\n"
                )
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "duplicate excluded dates" in result.exception.args[0]

    def test_validate_backfill_invalid_excluded_dates_not_sorted(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            duplicate_excluded_dates = "  - 2021-02-04\n" "  - 2021-02-03\n"
            backfill_file.write_text(
                "2021-05-04:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  excluded_dates:\n"
                + duplicate_excluded_dates
                + "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Initiate\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "excluded dates not sorted" in result.exception.args[0]

    def test_validate_backfill_entries_not_sorted(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2023-05-04:\n"
                "  start_date: 2020-01-03\n"
                "  end_date: 2020-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Complete\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                validate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )
            assert result.exit_code == 1
            assert "entries are not sorted" in result.output

    def test_backfill_info_one_table_all_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            qualified_table_name_1 = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Complete\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v2/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v2/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            qualified_table_name_2 = "moz-fx-data-shared-prod.test.test_query_v2"

            result = runner.invoke(
                create,
                [
                    qualified_table_name_2,
                    "--start_date=2021-04-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [qualified_table_name_1],
            )

            assert result.exit_code == 0
            assert qualified_table_name_1 in result.output
            assert BackfillStatus.INITIATE.value in result.output
            assert "total of 2 backfill(s)" in result.output
            assert qualified_table_name_2 not in result.output
            assert BackfillStatus.COMPLETE.value in result.output

    def test_backfill_info_one_table_initiate_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Initiate\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [qualified_table_name, "--status=Initiate"],
            )

            assert result.exit_code == 0
            assert qualified_table_name in result.output
            assert BackfillStatus.INITIATE.value in result.output
            assert "total of 2 backfill(s)" in result.output
            assert BackfillStatus.COMPLETE.value not in result.output

    def test_backfill_info_all_tables_all_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            qualified_table_name_1 = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Complete\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v2/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v2/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            qualified_table_name_2 = "moz-fx-data-shared-prod.test.test_query_v2"

            result = runner.invoke(
                create,
                [
                    qualified_table_name_2,
                    "--start_date=2021-04-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [],
            )

            assert result.exit_code == 0
            assert qualified_table_name_1 in result.output
            assert qualified_table_name_2 in result.output
            assert BackfillStatus.INITIATE.value in result.output
            assert "total of 3 backfill(s)" in result.output
            assert BackfillStatus.COMPLETE.value in result.output

    def test_backfill_info_all_tables_with_initiate_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            qualified_table_name_1 = "moz-fx-data-shared-prod.test.test_query_v1"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Initiate\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            qualified_table_name_2 = "moz-fx-data-shared-prod.test.test_query_v2"
            os.makedirs(SQL_DIR)

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Initiate\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [
                    "--status=Initiate",
                ],
            )

            assert result.exit_code == 0
            assert qualified_table_name_1 in result.output
            assert qualified_table_name_2 in result.output
            assert BackfillStatus.INITIATE.value in result.output
            assert "total of 4 backfill(s)" in result.output
            assert BackfillStatus.COMPLETE.value not in result.output

    def test_backfill_info_with_invalid_path(self, runner):
        with runner.isolated_filesystem():
            invalid_path = "moz-fx-data-shared-prod.test.test_query_v2"
            result = runner.invoke(info, [invalid_path])

            assert result.exit_code == 2
            assert "Invalid" in result.output
            assert "path" in result.output

    def test_backfill_info_with_invalid_qualified_table_name(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            invalid_qualified_table_name = "mozdata.test_query_v2"
            result = runner.invoke(info, [invalid_qualified_table_name])

            assert result.exit_code == 1
            assert "Qualified table name must be named like:" in str(result.exception)

    def test_backfill_info_one_table_invalid_status(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE + "\n"
                "2021-05-03:\n"
                "  start_date: 2021-01-03\n"
                "  end_date: 2021-05-03\n"
                "  reason: test_reason\n"
                "  watchers:\n"
                "  - test@example.org\n"
                "  status: Complete\n"
            )

            assert BACKFILL_FILE in os.listdir(SQL_DIR)

            result = runner.invoke(
                info,
                [qualified_table_name, "--status=testing"],
            )

            assert result.exit_code == 2
            assert "Invalid value for '--status'" in result.output

    def test_get_entries_from_qualified_table_name(self, runner):
        with runner.isolated_filesystem():
            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(
                create,
                [
                    qualified_table_name,
                    "--start_date=2021-03-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            backfill_file = SQL_DIR + "/" + BACKFILL_FILE
            backfill = Backfill.entries_from_file(backfill_file)[0]

            assert backfill.entry_date == date.today()
            assert backfill.start_date == date(2021, 3, 1)
            assert backfill.end_date == date.today()
            assert backfill.watchers == [DEFAULT_WATCHER]
            assert backfill.reason == DEFAULT_REASON
            assert backfill.status == DEFAULT_STATUS

            backfills = get_entries_from_qualified_table_name(
                "sql", qualified_table_name
            )

            expected_backfill = Backfill(
                date.today(),
                date(2021, 3, 1),
                date.today(),
                [],
                DEFAULT_REASON,
                [DEFAULT_WATCHER],
                DEFAULT_STATUS,
            )

            assert backfills == [expected_backfill]

    def test_get_qualified_table_name_to_entries_map_by_project(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            qualified_table_name_1 = "moz-fx-data-shared-prod.test.test_query_v1"

            result = runner.invoke(
                create,
                [
                    qualified_table_name_1,
                    "--start_date=2021-03-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            qualified_table_name_2 = "moz-fx-data-shared-prod.test.test_query_v2"
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v2"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v2/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v2/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = runner.invoke(
                create,
                [
                    qualified_table_name_2,
                    "--start_date=2021-03-01",
                ],
            )

            assert result.exit_code == 0
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v2"
            )

            backfills_dict = get_qualified_table_name_to_entries_map_by_project(
                "sql", "moz-fx-data-shared-prod"
            )

            expected_backfill = Backfill(
                date.today(),
                date(2021, 3, 1),
                date.today(),
                [],
                DEFAULT_REASON,
                [DEFAULT_WATCHER],
                DEFAULT_STATUS,
            )

            assert qualified_table_name_1 in backfills_dict
            assert backfills_dict[qualified_table_name_1] == [expected_backfill]
            assert qualified_table_name_2 in backfills_dict

    def test_get_backfill_file_from_qualified_table_name(self, runner):
        qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"
        actual_backfill_file = get_backfill_file_from_qualified_table_name(
            "sql", qualified_table_name
        )
        expected_backfill_file = Path(
            "sql/moz-fx-data-shared-prod/test/test_query_v1/backfill.yaml"
        )

        assert actual_backfill_file == expected_backfill_file

    def test_get_backfill_staging_qualified_table_name(self, runner):
        qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"
        backfill_table_id = "test__test_query_v1_2023_05_30"

        actual_backfill_staging = get_backfill_staging_qualified_table_name(
            qualified_table_name, date.fromisoformat("2023-05-30")
        )
        expected_backfill_staging = f"{BACKFILL_DESTINATION_PROJECT}.{BACKFILL_DESTINATION_DATASET}.{backfill_table_id}"

        assert actual_backfill_staging == expected_backfill_staging

    def test_get_backfill_backup_table_name(self, runner):
        qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"
        cloned_table_id = "test__test_query_v1_backup_2023_05_30"

        actual_backfill_staging = get_backfill_backup_table_name(
            qualified_table_name, date.fromisoformat("2023-05-30")
        )
        expected_backfill_backup = f"{BACKFILL_DESTINATION_PROJECT}.{BACKFILL_DESTINATION_DATASET}.{cloned_table_id}"

        assert actual_backfill_staging == expected_backfill_backup

    def test_validate_metadata_workgroups_invalid_table_workgroup_and_valid_dataset_workgroup(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            invalid_workgroup_access = [
                dict(
                    role="roles/bigquery.dataViewer",
                    members=["workgroup:invalid_workgroup"],
                )
            ]

            metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "owners": ["test@example.org"],
                "workgroup_access": invalid_workgroup_access,
            }

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(metadata_conf))

            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = validate_metadata_workgroups("sql", qualified_table_name)
            assert result

    def test_validate_metadata_workgroups_empty_table_workgroup_and_valid_dataset_workgroups(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF_EMPTY_WORKGROUP))

            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            result = validate_metadata_workgroups("sql", qualified_table_name)
            assert result

    def test_validate_metadata_workgroups_valid_table_workgroup_and_invalid_dataset_workgroup(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            invalid_workgroup_access = [
                dict(
                    role="roles/bigquery.dataViewer",
                    members=["workgroup:invalid_workgroup"],
                )
            ]

            dataset_metadata_conf = {
                "friendly_name": "test",
                "description": "test",
                "dataset_base_acl": "derived",
                "workgroup_access": invalid_workgroup_access,
            }

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(dataset_metadata_conf))

            assert "dataset_metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test"
            )

            result = validate_metadata_workgroups("sql", qualified_table_name)
            assert result

    def test_validate_metadata_workgroups_valid_table_workgroup_and_empty_dataset_workgroup(
        self, runner
    ):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            assert "dataset_metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test"
            )

            result = validate_metadata_workgroups("sql", qualified_table_name)
            assert result

    def test_validate_metadata_workgroups_missing_table_metadata(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            assert "metadata.yaml" not in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            assert "dataset_metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test"
            )

            result = validate_metadata_workgroups("sql", qualified_table_name)
            assert result

    def test_validate_metadata_workgroups_missing_dataset_metadata(self, runner):
        with pytest.raises(ValueError) as e:
            with runner.isolated_filesystem():
                os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
                with open(
                    "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
                ) as f:
                    f.write("SELECT 1")

                assert "query.sql" in os.listdir(
                    "sql/moz-fx-data-shared-prod/test/test_query_v1"
                )

                qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

                with open(
                    "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                    "w",
                ) as f:
                    f.write(yaml.dump(TABLE_METADATA_CONF))

                assert "metadata.yaml" in os.listdir(
                    "sql/moz-fx-data-shared-prod/test/test_query_v1"
                )

                assert "dataset_metadata.yaml" not in os.listdir(
                    "sql/moz-fx-data-shared-prod/test"
                )

                validate_metadata_workgroups("sql", qualified_table_name)

        assert e.type == ValueError

    def test_validate_metadata_workgroups_empty_workgroups(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/test/test_query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            assert "query.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF_EMPTY_WORKGROUP))

            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            assert "dataset_metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/test"
            )

            result = validate_metadata_workgroups("sql", qualified_table_name)
            assert not result

    def test_qualified_table_name_matching(self, runner):
        qualified_table_name = "moz-fx-data-shared-prod.test.test_query_v1"
        project_id, dataset_id, table_id = qualified_table_name_matching(
            qualified_table_name
        )

        assert project_id == "moz-fx-data-shared-prod"
        assert dataset_id == "test"
        assert table_id == "test_query_v1"

    def test_qualified_table_name_matching_invalid_name(self, runner):
        with pytest.raises(AttributeError) as e:
            invalid_name = "test.test_query_v1"
            qualified_table_name_matching(invalid_name)

        assert "Qualified table name must be named like" in str(e.value)

    @patch("google.cloud.bigquery.Client.get_table")
    def test_backfill_scheduled(self, get_table, runner):
        get_table.side_effect = [
            None,  # Check that staging data exists
            NotFound(  # Check that clone does not exist
                "moz-fx-data-shared-prod.backfills_staging_derived.test_query_v1_backup_2021_05_03"
                "not found"
            ),
            NotFound(  # Check that staging data does not exist
                "moz-fx-data-shared-prod.backfills_staging_derived.test_query_v1_2021_05_04"
                "not found"
            ),
        ]
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE
                + """
2021-05-03:
  start_date: 2021-01-03
  end_date: 2021-05-03
  reason: test_reason
  watchers:
  - test@example.org
  status: Complete"""
            )

            result = runner.invoke(
                scheduled,
                [
                    "--json_path=tmp.json",
                    "--status=Complete",
                ],
            )

            assert result.exit_code == 0
            assert "1 backfill(s) require processing." in result.output
            assert Path("tmp.json").exists()
            assert len(json.loads(Path("tmp.json").read_text())) == 1

            result = runner.invoke(
                scheduled,
                [
                    "--json_path=tmp.json",
                    "--status=Initiate",
                ],
            )

            assert result.exit_code == 0
            assert "1 backfill(s) require processing." in result.output

    @patch("google.cloud.bigquery.Client.get_table")
    def test_backfill_scheduled_depends_on_past_should_fail(self, get_table, runner):
        get_table.side_effect = [
            None,  # Check that staging data exists
            NotFound(  # Check that clone does not exist
                "moz-fx-data-shared-prod.backfills_staging_derived.test_query_v1_backup_2021_05_03"
                "not found"
            ),
            NotFound(  # Check that staging data does not exist
                "moz-fx-data-shared-prod.backfills_staging_derived.test_query_v1_2021_05_04"
                "not found"
            ),
        ]
        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(TABLE_METADATA_CONF_DEPENDS_ON_PAST))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                BACKFILL_YAML_TEMPLATE
                + """
2021-05-03:
  start_date: 2021-01-03
  end_date: 2021-05-03
  reason: test_reason
  watchers:
  - test@example.org
  status: Complete"""
            )

            result = runner.invoke(
                scheduled,
                [
                    "--json_path=tmp.json",
                    "--status=Complete",
                ],
            )

            assert result.exit_code == 0
            assert "0 backfill(s) require processing." in result.output
            assert Path("tmp.json").exists()
            assert len(json.loads(Path("tmp.json").read_text())) == 0

            result = runner.invoke(
                scheduled,
                [
                    "--json_path=tmp.json",
                    "--status=Initiate",
                ],
            )

            assert result.exit_code == 0
            assert "0 backfill(s) require processing." in result.output

    @patch("google.cloud.bigquery.Client.get_table")
    @patch("google.cloud.bigquery.Client.copy_table")
    @patch("google.cloud.bigquery.Client.delete_table")
    def test_complete_partitioned_backfill(
        self, delete_table, copy_table, get_table, runner
    ):
        get_table.side_effect = [
            None,  # Check that staging data exists
            NotFound(  # Check that clone does not exist
                "moz-fx-data-shared-prod.backfills_staging_derived.test__test_query_v1_backup_2021_05_03"
                "not found"
            ),
        ]
        copy_table.side_effect = None
        delete_table.side_effect = None

        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(PARTITIONED_TABLE_METADATA))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                """
2021-05-03:
  start_date: 2021-01-03
  end_date: 2021-01-13
  reason: test_reason
  watchers:
  - test@example.org
  status: Complete"""
            )

            result = runner.invoke(
                complete,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                ],
            )

            assert result.exit_code == 0
            assert copy_table.call_count == 12  # one for backup, 11 for partitions
            for i, call in enumerate(copy_table.call_args_list[1:]):
                d = date(2021, 1, 3) + timedelta(days=i)
                assert call.args == (
                    f'moz-fx-data-shared-prod.backfills_staging_derived.test__test_query_v1_2021_05_03${d.strftime("%Y%m%d")}',
                    f'moz-fx-data-shared-prod.test.test_query_v1${d.strftime("%Y%m%d")}',
                )
            assert delete_table.call_count == 1

    @patch("google.cloud.bigquery.Client")
    @patch("subprocess.check_call")
    @patch("bigquery_etl.cli.backfill.deploy_table")
    @patch("bigquery_etl.cli.backfill.Schema.from_query_file")
    def test_initiate_partitioned_backfill(
        self,
        mock_from_query_file,
        mock_deploy_table,
        check_call,
        mock_client,
        runner,
    ):
        backfill_staging_table_name = (
            "moz-fx-data-shared-prod.backfills_staging_derived.test__test_query_v1"
        )

        mock_client().get_table.side_effect = [
            NotFound(  # Check that staging data does not exist
                f"{backfill_staging_table_name}_backup_2021_05_03" "not found"
            ),
            None,  # Check that production data exists during dry run
            None,  # Check that production data exists
        ]

        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            query_path = Path(SQL_DIR) / QUERY_FILE

            with open(query_path, "w") as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/schema.yaml", "w"
            ) as f:
                f.write(yaml.dump({"fields": [{"name": "f0_", "type": "INTEGER"}]}))

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(PARTITIONED_TABLE_METADATA))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                """
            2021-05-03:
              start_date: 2021-01-03
              end_date: 2021-01-08
              reason: test_reason
              watchers:
              - test@example.org
              status: Initiate
              override_retention_limit: True"""
            )

            result = runner.invoke(
                initiate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--parallelism=0",
                ],
            )

            assert result.exit_code == 0

            assert not mock_from_query_file.called  # schema exists

            mock_deploy_table.assert_called_with(
                artifact_file=query_path,
                destination_table=f"{backfill_staging_table_name}_2021_05_03",
                respect_dryrun_skip=False,
            )

            expected_submission_date_params = [
                f"--parameter=submission_date:DATE:2021-01-0{day}"
                for day in range(3, 9)
            ]

            expected_destination_table_params = [
                f"--destination_table=moz-fx-data-shared-prod:backfills_staging_derived.test__test_query_v1_2021_05_03$2021010{day}"
                for day in range(3, 9)
            ]

            # this is inspecting calls to the underlying subprocess.check_call(["bq]"...)
            assert check_call.call_count == 12  # 6 for dry run, 6 for backfill
            for call in check_call.call_args_list:
                submission_date_params = [
                    arg for arg in call.args[0] if "--parameter=submission_date" in arg
                ]
                assert len(submission_date_params) == 1
                assert submission_date_params[0] in expected_submission_date_params
                assert f"--project_id={DEFAULT_BILLING_PROJECT}" in call.args[0]
                destination_table_params = [
                    arg for arg in call.args[0] if "--destination_table" in arg
                ]
                assert destination_table_params[0] in expected_destination_table_params

    @patch("google.cloud.bigquery.Client")
    @patch("subprocess.check_call")
    @patch("bigquery_etl.cli.backfill.deploy_table")
    @patch("bigquery_etl.cli.backfill.Schema.from_query_file")
    def test_initiate_partitioned_backfill_without_schema_should_pass(
        self, mock_from_query_file, mock_deploy_table, check_call, mock_client, runner
    ):
        backfill_staging_table_name = (
            "moz-fx-data-shared-prod.backfills_staging_derived.test__test_query_v1"
        )

        mock_client().get_table.side_effect = [
            NotFound(  # Check that staging data does not exist
                f"{backfill_staging_table_name}_backup_2021_05_03" "not found"
            ),
            None,  # Check that production data exists during dry run
            None,  # Check that production data exists
        ]

        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            query_path = Path(SQL_DIR) / QUERY_FILE

            with open(query_path, "w") as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(PARTITIONED_TABLE_METADATA))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                """
            2021-05-03:
              start_date: 2021-01-03
              end_date: 2021-01-08
              reason: test_reason
              watchers:
              - test@example.org
              status: Initiate
              override_retention_limit: true"""
            )

            result = runner.invoke(
                initiate,
                ["moz-fx-data-shared-prod.test.test_query_v1", "--parallelism=0"],
            )

            assert result.exit_code == 0

            mock_from_query_file.assert_called_with(
                query_file=query_path,
                respect_skip=False,
                sql_dir="sql/",
            )

            mock_deploy_table.assert_called_with(
                artifact_file=query_path,
                destination_table=f"{backfill_staging_table_name}_2021_05_03",
                respect_dryrun_skip=False,
            )

            expected_submission_date_params = [
                f"--parameter=submission_date:DATE:2021-01-0{day}"
                for day in range(3, 9)
            ]

            expected_destination_table_params = [
                f"--destination_table=moz-fx-data-shared-prod:backfills_staging_derived.test__test_query_v1_2021_05_03$2021010{day}"
                for day in range(3, 9)
            ]

            # this is inspecting calls to the underlying subprocess.check_call(["bq]"...)
            assert check_call.call_count == 12  # 6 for dry run, 6 for backfill
            for call in check_call.call_args_list:
                submission_date_params = [
                    arg for arg in call.args[0] if "--parameter=submission_date" in arg
                ]
                assert len(submission_date_params) == 1
                assert submission_date_params[0] in expected_submission_date_params
                assert f"--project_id={DEFAULT_BILLING_PROJECT}" in call.args[0]
                destination_table_params = [
                    arg for arg in call.args[0] if "--destination_table" in arg
                ]
                assert destination_table_params[0] in expected_destination_table_params

    @patch("google.cloud.bigquery.Client")
    @patch("subprocess.check_call")
    @patch("bigquery_etl.cli.backfill.deploy_table")
    @patch("bigquery_etl.cli.backfill.Schema.from_query_file")
    def test_initiate_partitioned_backfill_with_valid_billing_project_from_entry(
        self, mock_from_query_file, mock_deploy_table, check_call, mock_client, runner
    ):
        backfill_staging_table_name = (
            "moz-fx-data-shared-prod.backfills_staging_derived.test__test_query_v1"
        )

        mock_client().get_table.side_effect = [
            NotFound(  # Check that staging data does not exist
                f"{backfill_staging_table_name}_backup_2021_05_03" "not found"
            ),
            None,  # Check that production data exists during dry run
            None,  # Check that production data exists
        ]

        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            query_path = Path(SQL_DIR) / QUERY_FILE

            with open(query_path, "w") as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/schema.yaml", "w"
            ) as f:
                f.write(yaml.dump({"fields": [{"name": "f0_", "type": "INTEGER"}]}))

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(PARTITIONED_TABLE_METADATA))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                f"""
            2021-05-03:
              start_date: 2021-01-03
              end_date: 2021-01-08
              reason: test_reason
              watchers:
              - test@example.org
              status: Initiate
              billing_project: {VALID_BILLING_PROJECT}
              override_retention_limit: true
              """
            )

            result = runner.invoke(
                initiate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--parallelism=0",
                ],
            )

            assert not mock_from_query_file.called  # schema exists

            mock_deploy_table.assert_called_with(
                artifact_file=query_path,
                destination_table=f"{backfill_staging_table_name}_2021_05_03",
                respect_dryrun_skip=False,
            )
            assert result.exit_code == 0

            expected_submission_date_params = [
                f"--parameter=submission_date:DATE:2021-01-0{day}"
                for day in range(3, 9)
            ]

            expected_destination_table_params = [
                f"--destination_table=moz-fx-data-shared-prod:backfills_staging_derived.test__test_query_v1_2021_05_03$2021010{day}"
                for day in range(3, 9)
            ]

            # this is inspecting calls to the underlying subprocess.check_call(["bq]"...)
            assert check_call.call_count == 12  # 6 for dry run, 6 for backfill
            for call in check_call.call_args_list:
                submission_date_params = [
                    arg for arg in call.args[0] if "--parameter=submission_date" in arg
                ]
                assert len(submission_date_params) == 1
                assert submission_date_params[0] in expected_submission_date_params
                assert f"--project_id={VALID_BILLING_PROJECT}" in call.args[0]
                destination_table_params = [
                    arg for arg in call.args[0] if "--destination_table" in arg
                ]
                assert destination_table_params[0] in expected_destination_table_params

    @patch("google.cloud.bigquery.Client")
    @patch("bigquery_etl.cli.backfill.deploy_table")
    @patch("bigquery_etl.cli.backfill.Schema.from_query_file")
    def test_initiate_backfill_with_failed_deploy(
        self, mock_from_query_file, mock_deploy_table, mock_client, runner
    ):
        backfill_staging_table_name = (
            "moz-fx-data-shared-prod.backfills_staging_derived.test__test_query_v1"
        )

        mock_deploy_table.side_effect = FailedDeployException("Unable to deploy table")

        mock_client().get_table.side_effect = [
            NotFound(  # Check that staging data does not exist
                f"{backfill_staging_table_name}_backup_2021_05_03" "not found"
            ),
            None,  # Check that production data exists during dry run
            None,  # Check that production data exists
        ]

        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)
            query_path = Path(SQL_DIR) / QUERY_FILE

            with open(query_path, "w") as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(PARTITIONED_TABLE_METADATA))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                """
            2021-05-03:
              start_date: 2021-01-03
              end_date: 2021-01-08
              reason: test_reason
              watchers:
              - test@example.org
              status: Initiate
              """
            )

            result = runner.invoke(
                initiate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--parallelism=0",
                ],
            )

            mock_from_query_file.assert_called_with(
                query_file=query_path,
                respect_skip=False,
                sql_dir="sql/",
            )

            mock_deploy_table.assert_called_with(
                artifact_file=query_path,
                destination_table=f"{backfill_staging_table_name}_2021_05_03",
                respect_dryrun_skip=False,
            )
            assert result.exit_code == 1
            assert "Backfill initiate failed to deploy" in str(result.exception)

    @patch("google.cloud.bigquery.Client")
    def test_initiate_partitioned_backfill_with_invalid_billing_project_from_entry_should_fail(
        self, mock_client, runner
    ):
        backfill_staging_table_name = (
            "moz-fx-data-shared-prod.backfills_staging_derived.test__test_query_v1"
        )

        mock_client().get_table.side_effect = [
            NotFound(  # Check that staging data does not exist
                f"{backfill_staging_table_name}_backup_2021_05_03" "not found"
            ),
            None,  # Check that production data exists during dry run
            None,  # Check that production data exists
        ]

        with runner.isolated_filesystem():
            SQL_DIR = "sql/moz-fx-data-shared-prod/test/test_query_v1"
            os.makedirs(SQL_DIR)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/metadata.yaml",
                "w",
            ) as f:
                f.write(yaml.dump(PARTITIONED_TABLE_METADATA))

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF_EMPTY_WORKGROUP))

            backfill_file = Path(SQL_DIR) / BACKFILL_FILE
            backfill_file.write_text(
                f"""
            2021-05-03:
              start_date: 2021-01-03
              end_date: 2021-01-08
              reason: test_reason
              watchers:
              - test@example.org
              status: Initiate
              billing_project: {INVALID_BILLING_PROJECT}
              """
            )

            result = runner.invoke(
                initiate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--parallelism=0",
                ],
            )

            assert result.exit_code == 1
            assert "Invalid billing project" in str(result.exception)

    @patch("bigquery_etl.cli.backfill.deploy_table")
    @patch("bigquery_etl.backfill.utils._should_initiate")
    def test_dont_initiate_if_label_and_backfill_entry_dont_match(
        self, mock_should_initiate, mock_deploy_table, runner
    ):
        """Test that process stops if the table has label shredder_mitigation and the backfill doesn't."""
        path = Path("sql/moz-fx-data-shared-prod/test/test_query_v1")
        mock_should_initiate.return_value = True
        mock_deploy_table.return_value = None
        expected_error_output = (
            "This backfill cannot continue.\nManaged backfills for "
            "tables with metadata label shredder_mitigation require "
            "using --shredder_mitigation."
        )

        with runner.isolated_filesystem():
            os.makedirs(path, exist_ok=True)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(path / "metadata.yaml", "w") as f:
                f.write(
                    "friendly_name: Test\ndescription: Test\nlabels:\n  incremental: true\n  shredder_mitigation: true"
                )

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_entry_1 = Backfill(
                date(2021, 5, 3),
                date(2021, 1, 3),
                date(2021, 5, 3),
                [date(2021, 2, 3)],
                VALID_REASON,
                [VALID_WATCHER],
                BackfillStatus.INITIATE,
                shredder_mitigation=False,
            )

            backfill_file = (
                Path("sql/moz-fx-data-shared-prod/test/test_query_v1") / BACKFILL_FILE
            )
            backfill_file.write_text(backfill_entry_1.to_yaml())
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            backfills = Backfill.entries_from_file(backfill_file)
            assert backfills[0] == backfill_entry_1

            result = runner.invoke(
                initiate,
                [
                    "moz-fx-data-shared-prod.test.test_query_v1",
                    "--parallelism=0",
                ],
            )

            assert result.exit_code == 1
            assert expected_error_output in result.output

    @patch("bigquery_etl.cli.backfill.deploy_table")
    @patch("bigquery_etl.backfill.utils._should_initiate")
    def test_initiate_if_label_and_backfill_entry_match(
        self, mock_should_initiate, mock_deploy_table, runner
    ):
        """Test that the process continues if both table & backfill use shredder_mitigation."""
        path = Path("sql/moz-fx-data-shared-prod/test/test_query_v1")
        mock_should_initiate.return_value = True
        mock_deploy_table.return_value = None

        with runner.isolated_filesystem():
            os.makedirs(path, exist_ok=True)

            with open(
                "sql/moz-fx-data-shared-prod/test/test_query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT 1")

            with open(path / "metadata.yaml", "w") as f:
                f.write(
                    "friendly_name: Test\ndescription: Test\nlabels:\n  incremental: true\n  shredder_mitigation: true"
                )

            with open(
                "sql/moz-fx-data-shared-prod/test/dataset_metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(DATASET_METADATA_CONF))

            backfill_entry_1 = Backfill(
                date(2021, 5, 3),
                date(2021, 1, 3),
                date(2021, 5, 3),
                [date(2021, 2, 3)],
                VALID_REASON,
                [VALID_WATCHER],
                BackfillStatus.INITIATE,
                shredder_mitigation=True,
            )

            backfill_file = (
                Path("sql/moz-fx-data-shared-prod/test/test_query_v1") / BACKFILL_FILE
            )
            backfill_file.write_text(backfill_entry_1.to_yaml())
            assert BACKFILL_FILE in os.listdir(
                "sql/moz-fx-data-shared-prod/test/test_query_v1"
            )
            backfills = Backfill.entries_from_file(backfill_file)
            assert backfills[0] == backfill_entry_1

            with patch(
                "bigquery_etl.cli.backfill.query_backfill", return_value=None
            ) as mock_backfill:
                with patch(
                    "bigquery_etl.cli.backfill.generate_query_with_shredder_mitigation",
                    return_value=("a", "b"),
                ) as mock_shredder_mitigation:
                    result = runner.invoke(
                        initiate,
                        [
                            "moz-fx-data-shared-prod.test.test_query_v1",
                            "--parallelism=0",
                        ],
                    )

                assert result.exit_code == 0
                mock_shredder_mitigation.call_count == 2
                mock_backfill.call_count == 2
