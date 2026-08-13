import os
from pathlib import Path
from typing import NewType

import pytest

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.query_scheduling.task import (
    MAX_TASK_NAME_LENGTH,
    Task,
    TaskParseException,
    TaskRef,
    UnscheduledTask,
)

TEST_DIR = Path(__file__).parent.parent


class TestTask:
    default_scheduling = {
        "dag_name": "bqetl_test_dag",
        "default_args": {"owner": "test@example.org"},
    }

    def test_of_query(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        task = Task.of_query(query_file)

        assert task.query_file == str(query_file)
        assert task.dataset == "test"
        assert task.project == "moz-fx-data-test-project"
        assert task.table == "incremental_query"
        assert task.version == "v1"
        assert task.task_name == "test__incremental_query__v1"
        assert task.dag_name == "bqetl_events"
        assert task.depends_on_past is False
        assert task.public_json
        assert task.arguments == ["--append_table"]

    def test_description_markdown(self):
        from bigquery_etl.metadata.parse_metadata import WorkgroupAccessMetadata

        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        # labels as parsed by Metadata.from_file: synthesized `dag`/`owner<N>`
        # keys and a list-valued `review_bugs` label.
        metadata = Metadata(
            friendly_name="Incremental Query",
            description="Daily incremental aggregate.",
            owners=["test@example.org", "test2@example.org"],
            labels={
                "incremental": "",
                "schedule": "daily",
                "review_bugs": ["123456", "789"],
                "dag": "bqetl_test_dag",
                "owner1": "test",
                "owner2": "test2",
            },
            scheduling={"dag_name": "bqetl_test_dag"},
        )
        metadata.workgroup_access = [
            WorkgroupAccessMetadata(
                role="roles/bigquery.dataViewer",
                members=["workgroup:mozilla-confidential"],
            )
        ]

        task = Task.of_query(query_file, metadata)

        # metadata fields are captured on the task
        assert task.friendly_name == "Incremental Query"
        assert task.description == "Daily incremental aggregate."
        assert task.owners == ["test@example.org", "test2@example.org"]
        assert task.workgroups == ["workgroup:mozilla-confidential"]

        markdown = task.description_markdown
        assert "### Incremental Query" in markdown
        assert "Daily incremental aggregate." in markdown
        assert "**Owners:** test@example.org, test2@example.org" in markdown
        # bool-style labels render as key only, key/value as "key: value", and
        # list-valued labels are flattened rather than shown as a list literal.
        assert (
            "**Labels:** incremental, review_bugs: 123456, 789, schedule: daily"
            in markdown
        )
        # synthesized `dag` and `owner<N>` labels are filtered out
        assert "dag: bqetl_test_dag" not in markdown
        assert "owner1" not in markdown
        assert "**Workgroup access:** workgroup:mozilla-confidential" in markdown

    def test_description_markdown_check_headings(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "checks.sql"
        )
        metadata = Metadata(
            friendly_name="Incremental Query",
            description="Daily incremental aggregate.",
            owners=["test@example.org"],
            labels={},
            scheduling={"dag_name": "bqetl_test_dag"},
        )

        dq_check = Task.of_dq_check(query_file, is_check_fail=True, metadata=metadata)
        assert "### Incremental Query (data checks)" in dq_check.description_markdown

        bigeye = Task.of_bigeye_check(query_file, metadata=metadata)
        assert (
            "### Incremental Query (Bigeye monitoring)" in bigeye.description_markdown
        )

    def test_source_url(self):
        query_file = "sql/moz-fx-data-shared-prod/telemetry_derived/foo_v1/query.sql"

        # repo is taken from Dag.repo, not inferred from the DAG name prefix
        public_task = Task(
            dag_name="bqetl_test_dag",
            query_file=query_file,
            owner="test@example.org",
            repo="bigquery-etl",
        )
        assert public_task.source_url == (
            "https://github.com/mozilla/bigquery-etl/blob/generated-sql/"
            "sql/moz-fx-data-shared-prod/telemetry_derived/foo_v1/query.sql"
        )

        # a `bqetl_*`-named DAG that actually lives in private-bigquery-etl still
        # links to the private repo, because the repo comes from Dag.repo
        private_task = Task(
            dag_name="bqetl_test_dag",
            query_file=query_file,
            owner="test@example.org",
            repo="private-bigquery-etl",
        )
        assert private_task.source_url == (
            "https://github.com/mozilla/private-bigquery-etl/blob/"
            "private-generated-sql/"
            "sql/moz-fx-data-shared-prod/telemetry_derived/foo_v1/query.sql"
        )

        # without a resolved repo, no source link is emitted
        no_repo_task = Task(
            dag_name="bqetl_test_dag", query_file=query_file, owner="test@example.org"
        )
        assert no_repo_task.source_url is None

    def test_of_multipart_query(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "multipart_query_v1"
            / "part1.sql"
        )

        task = Task.of_multipart_query(query_file)

        assert task.query_file == str(query_file)
        assert task.dataset == "test"
        assert task.table == "multipart_query"
        assert task.project == "moz-fx-data-test-project"
        assert task.version == "v1"
        assert task.task_name == "test__multipart_query__v1"
        assert task.dag_name == "bqetl_core"
        assert task.depends_on_past is False
        assert task.multipart
        assert task.query_file_path == os.path.dirname(query_file)

    def test_of_python_script(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        task = Task.of_python_script(query_file)

        assert task.query_file == str(query_file)
        assert task.dataset == "test"
        assert task.project == "moz-fx-data-test-project"
        assert task.table == "incremental_query"
        assert task.version == "v1"
        assert task.task_name == "test__incremental_query__v1"
        assert task.dag_name == "bqetl_events"
        assert task.depends_on_past is False
        assert task.is_python_script

    def test_of_non_existing_query(self):
        with pytest.raises(FileNotFoundError):
            Task.of_query("non_existing_query/query.sql")

    def test_invalid_path_format(self):
        with pytest.raises(TaskParseException):
            metadata = Metadata("test", "test", [], {}, self.default_scheduling)
            Task.of_query(TEST_DIR / "data" / "query.sql", metadata)

    def test_unscheduled_task(self, tmp_path):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata("test", "test", [], {}, {})

        with pytest.raises(UnscheduledTask):
            Task.of_query(query_file, metadata)

    def test_no_dag_name(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata("test", "test", [], {}, self.default_scheduling)

        with pytest.raises(TaskParseException):
            Task.of_query(query_file, metadata)

    def test_task_instantiation(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)
        assert task.dag_name == "bqetl_test_dag"
        assert task.depends_on_past is False
        assert task.task_name == "test__incremental_query__v1"
        assert task.public_json is False
        assert task.arguments == []

    def test_task_instantiation_custom_name(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        scheduling = {
            "dag_name": "bqetl_test_dag",
            "default_args": {"owner": "test@example.org"},
            "task_name": "custom_task_name",
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        task = Task.of_query(query_file, metadata)
        assert task.dag_name == "bqetl_test_dag"
        assert task.task_name == "custom_task_name"

    def test_validate_custom_task_name(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        scheduling = {
            "dag_name": "bqetl_test_dag",
            "default_args": {"owner": "test@example.org"},
            "task_name": "a" * (MAX_TASK_NAME_LENGTH + 1),
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        with pytest.raises(ValueError):
            Task.of_query(query_file, metadata)

        scheduling = {
            "dag_name": "bqetl_test_dag",
            "default_args": {"owner": "test@example.org"},
            "task_name": "",
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        with pytest.raises(ValueError):
            Task.of_query(query_file, metadata)

        scheduling = {
            "dag_name": "bqetl_test_dag",
            "default_args": {"owner": "test@example.org"},
            "task_name": "a" * MAX_TASK_NAME_LENGTH,
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        task = Task.of_query(query_file, metadata)
        assert task.task_name == "a" * MAX_TASK_NAME_LENGTH

    def test_validate_task_name(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / (("a" * MAX_TASK_NAME_LENGTH) + "_v1")
            / "query.sql"
        )

        scheduling = {
            "dag_name": "bqetl_test_dag",
            "default_args": {"owner": "test@example.org"},
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        task = Task.of_query(query_file, metadata)
        assert task.task_name == ("a" * (MAX_TASK_NAME_LENGTH - 4)) + "__v1"

        with pytest.raises(ValueError):
            task.task_name = ("a" * MAX_TASK_NAME_LENGTH) + "__v1"
            Task.validate_task_name(task, "task_name", task.task_name)

    def test_dag_name_validation(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        with pytest.raises(ValueError):
            Task(
                dag_name="test_dag",
                query_file=str(query_file),
                owner="test@example.com",
            )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
        )

    def test_owner_validation(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        with pytest.raises(ValueError):
            assert Task(
                dag_name="bqetl_test_dag", query_file=str(query_file), owner="invalid"
            )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
        )

    def test_email_validation(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        with pytest.raises(ValueError):
            assert Task(
                dag_name="bqetl_test_dag",
                query_file=str(query_file),
                owner="test@example.com",
                email=["valid@example.com", "invalid", "valid2@example.com"],
            )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
            email=[],
        )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
            email=["test@example.org", "test2@example.org"],
        )

    def test_start_date_validation(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        with pytest.raises(ValueError):
            assert Task(
                dag_name="bqetl_test_dag",
                query_file=str(query_file),
                owner="test@example.com",
                start_date="March 12th 2020",
            )

        with pytest.raises(ValueError):
            assert Task(
                dag_name="bqetl_test_dag",
                query_file=str(query_file),
                owner="test@example.com",
                start_date="2020-13-12",
            )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
            start_date="2020-01-01",
        )

    def test_date_partition_parameter(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
            date_partition_parameter=NewType("Ignore", None),
        )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
            date_partition_parameter=None,
        )

        assert Task(
            dag_name="bqetl_test_dag",
            query_file=str(query_file),
            owner="test@example.com",
            date_partition_parameter="import_date",
        )

    def test_task_get_dependencies_none(self, tmp_path):
        query_file = tmp_path / "test-project" / "test" / "query_v1" / "query.sql"
        os.makedirs(query_file.parent)
        query_file.write_text("SELECT 123423")

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)
        dags = DagCollection.from_dict({})
        task.with_upstream_dependencies(dags)
        assert task.upstream_dependencies == []
        assert task.downstream_dependencies == []

    def test_task_get_multiple_dependencies(self, tmp_path):
        query_file = tmp_path / "test-project" / "test" / "query_v1" / "query.sql"
        os.makedirs(query_file.parent)
        query_file.write_text(
            "SELECT * FROM `test-project`.test.table1_v1 "
            "UNION ALL SELECT * FROM `test-project`.test.table2_v1"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table1_file = tmp_path / "test-project" / "test" / "table1_v1" / "query.sql"
        os.makedirs(table1_file.parent)
        table1_file.write_text("SELECT 12345")
        table1_task = Task.of_query(table1_file, metadata)

        table2_file = tmp_path / "test-project" / "test" / "table2_v1" / "query.sql"
        os.makedirs(table2_file.parent)
        table2_file.write_text("SELECT 67890")
        table2_task = Task.of_query(table2_file, metadata)

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                }
            }
        ).with_tasks([task, table1_task, table2_task])

        task.with_upstream_dependencies(dags)
        result = task.upstream_dependencies

        tables = [t.task_id for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables

    def test_task_date_partition_offset(self, tmp_path):
        query_file = tmp_path / "test-project" / "test" / "query_v1" / "query.sql"
        os.makedirs(query_file.parent)
        query_file.write_text(
            "SELECT * FROM `test-project`.test.table1_v1 "
            "UNION ALL SELECT * FROM `test-project`.test.table2_v1"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        metadata_task1 = Metadata(
            "test",
            "test",
            ["test@example.org"],
            {},
            {**self.default_scheduling, **{"date_partition_offset": -2}},
        )

        metadata_task2 = Metadata(
            "test",
            "test",
            ["test@example.org"],
            {},
            {**self.default_scheduling, **{"date_partition_offset": -1}},
        )

        task = Task.of_query(query_file, metadata)

        table1_file = tmp_path / "test-project" / "test" / "table1_v1" / "query.sql"
        os.makedirs(table1_file.parent)
        table1_file.write_text("SELECT 12345")
        table1_task = Task.of_query(table1_file, metadata_task1)

        table2_file = tmp_path / "test-project" / "test" / "table2_v1" / "query.sql"
        os.makedirs(table2_file.parent)
        table2_file.write_text("SELECT 67890")
        table2_task = Task.of_query(table2_file, metadata_task2)

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                }
            }
        ).with_tasks([task, table1_task, table2_task])

        task.with_upstream_dependencies(dags)
        result = task.upstream_dependencies

        assert task.date_partition_offset == -2
        assert [
            t.date_partition_offset for t in result if t.task_id == "test__table1__v1"
        ] == [-2]
        assert [
            t.date_partition_offset for t in result if t.task_id == "test__table2__v1"
        ] == [-1]
        assert task.date_partition_parameter == "submission_date"

    def test_task_date_partition_offset_recursive(self, tmp_path):
        query_file = tmp_path / "test-project" / "test" / "query_v1" / "query.sql"
        os.makedirs(query_file.parent)
        query_file.write_text("SELECT * FROM `test-project`.test.table1_v1")

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        table2_metadata = Metadata(
            "test",
            "test",
            ["test@example.org"],
            {},
            {**self.default_scheduling, **{"date_partition_offset": -2}},
        )

        task = Task.of_query(query_file, metadata)

        table1_file = tmp_path / "test-project" / "test" / "table1_v1" / "query.sql"
        os.makedirs(table1_file.parent)
        table1_file.write_text("SELECT * FROM `test-project`.test.table2_v1")
        table1_task = Task.of_query(table1_file, metadata)

        table2_file = tmp_path / "test-project" / "test" / "table2_v1" / "query.sql"
        os.makedirs(table2_file.parent)
        table2_file.write_text("SELECT 67890")
        table2_task = Task.of_query(table2_file, table2_metadata)

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                }
            }
        ).with_tasks([task, table1_task, table2_task])

        task.with_upstream_dependencies(dags)
        result = task.upstream_dependencies

        assert task.date_partition_offset == -2
        assert [
            t.date_partition_offset for t in result if t.task_id == "test__table1__v1"
        ] == [-2]
        assert task.date_partition_parameter == "submission_date"

    def test_multipart_task_get_dependencies(self, tmp_path):
        query_dir = tmp_path / "test-project" / "test" / "query_v1"
        os.makedirs(query_dir)

        query_file_part1 = query_dir / "part1.sql"
        query_file_part1.write_text("SELECT * FROM `test-project`.test.table1_v1")

        query_file_part2 = query_dir / "part2.sql"
        query_file_part2.write_text("SELECT * FROM `test-project`.test.table2_v1")

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_multipart_query(query_file_part1, metadata)

        table1_file = tmp_path / "test-project" / "test" / "table1_v1" / "query.sql"
        os.makedirs(table1_file.parent)
        table1_file.write_text("SELECT 12345")
        table1_task = Task.of_query(table1_file, metadata)

        table2_file = tmp_path / "test-project" / "test" / "table2_v1" / "query.sql"
        os.makedirs(table2_file.parent)
        table2_file.write_text("SELECT 67890")
        table2_task = Task.of_query(table2_file, metadata)

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                }
            }
        ).with_tasks([task, table1_task, table2_task])

        task.with_upstream_dependencies(dags)
        result = task.upstream_dependencies

        tables = [t.task_id for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables

    def test_task_get_view_dependencies(self, tmp_path):
        query_file = tmp_path / "test-project" / "test" / "query_v1" / "query.sql"
        os.makedirs(query_file.parent)
        query_file.write_text(
            "SELECT * FROM `test-project`.test.table1_v1 "
            "UNION ALL SELECT * FROM `test-project`.test.test_view"
        )

        view_file = tmp_path / "test-project" / "test" / "test_view" / "view.sql"
        os.makedirs(view_file.parent)
        view_file.write_text(
            "CREATE OR REPLACE VIEW `test-project`.test.test_view "
            "AS SELECT * FROM `test-project`.test.table2_v1"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table1_file = tmp_path / "test-project" / "test" / "table1_v1" / "query.sql"
        os.makedirs(table1_file.parent)
        table1_file.write_text("SELECT 12345")
        table1_task = Task.of_query(table1_file, metadata)

        table2_file = tmp_path / "test-project" / "test" / "table2_v1" / "query.sql"
        os.makedirs(table2_file.parent)
        table2_file.write_text("SELECT 67890")
        table2_task = Task.of_query(table2_file, metadata)

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                }
            }
        ).with_tasks([task, table1_task, table2_task])

        task.with_upstream_dependencies(dags)
        result = task.upstream_dependencies

        tables = [t.task_id for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables

    def test_task_get_nested_view_dependencies(self, tmp_path):
        query_file = tmp_path / "test-project" / "test" / "query_v1" / "query.sql"
        os.makedirs(query_file.parent)
        query_file.write_text(
            "SELECT * FROM `test-project`.test.table1_v1 "
            "UNION ALL SELECT * FROM `test-project`.test.test_view"
        )

        view_file = tmp_path / "test-project" / "test" / "test_view" / "view.sql"
        os.makedirs(view_file.parent)
        view_file.write_text(
            "CREATE OR REPLACE VIEW `test-project`.test.test_view "
            "AS SELECT * FROM `test-project`.test.test_view2"
        )

        view2_file = tmp_path / "test-project" / "test" / "test_view2" / "view.sql"
        os.makedirs(view2_file.parent)
        view2_file.write_text(
            "CREATE OR REPLACE VIEW `test-project`.test.test_view2 "
            "AS SELECT * FROM `test-project`.test.table2_v1"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table1_file = tmp_path / "test-project" / "test" / "table1_v1" / "query.sql"
        os.makedirs(table1_file.parent)
        table1_file.write_text("SELECT 12345")
        table1_task = Task.of_query(table1_file, metadata)

        table2_file = tmp_path / "test-project" / "test" / "table2_v1" / "query.sql"
        os.makedirs(table2_file.parent)
        table2_file.write_text("SELECT 67890")
        table2_task = Task.of_query(table2_file, metadata)

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                }
            }
        ).with_tasks([task, table1_task, table2_task])

        task.with_upstream_dependencies(dags)
        result = task.upstream_dependencies
        tables = [t.task_id for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables

    def test_task_depends_on(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        scheduling = {
            "dag_name": "bqetl_test_dag",
            "default_args": {"owner": "test@example.org"},
            "depends_on": [
                {"dag_name": "external_dag", "task_id": "external_task"},
                {
                    "dag_name": "external_dag2",
                    "task_id": "external_task2",
                    "execution_delta": "15m",
                    "poke_interval": "30m",
                    "timeout": "8h",
                },
            ],
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        task = Task.of_query(query_file, metadata)
        assert task.dag_name == "bqetl_test_dag"
        assert len(task.depends_on) == 2
        assert task.depends_on[0].dag_name == "external_dag"
        assert task.depends_on[0].task_id == "external_task"

        assert task.depends_on[1].dag_name == "external_dag2"
        assert task.depends_on[1].task_id == "external_task2"
        assert task.depends_on[1].execution_delta == "15m"
        assert task.depends_on[1].poke_interval == "30m"
        assert task.depends_on[1].timeout == "8h"

    def test_task_trigger_rule(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        task = Task(
            dag_name="bqetl_test_dag",
            owner="test@example.org",
            query_file=str(query_file),
            trigger_rule="all_success",
        )

        assert task.trigger_rule == "all_success"

        with pytest.raises(ValueError, match=r"Invalid trigger rule an_invalid_rule"):
            assert Task(
                dag_name="bqetl_test_dag",
                owner="test@example.org",
                query_file=str(query_file),
                trigger_rule="an_invalid_rule",
            )

    def test_task_ref(self):
        task_ref = TaskRef(dag_name="test_dag", task_id="task")

        assert task_ref.dag_name == "test_dag"
        assert task_ref.task_id == "task"
        assert task_ref.execution_delta is None

        task_ref = TaskRef(dag_name="test_dag", task_id="task", execution_delta="15m")

        assert task_ref.execution_delta == "15m"

    def test_task_ref_execution_delta_validation(self):
        with pytest.raises(ValueError):
            TaskRef(dag_name="test_dag", task_id="task", execution_delta="invalid")

        assert TaskRef(dag_name="test_dag", task_id="task", execution_delta="1h15m")

    def test_check_get_dependencies(self, tmp_path):
        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        table1_file = tmp_path / "test-project" / "test" / "table1_v1" / "query.sql"
        os.makedirs(table1_file.parent)
        table1_file.write_text("SELECT 12345")
        metadata.write(
            tmp_path / "test-project" / "test" / "table1_v1" / "metadata.yaml"
        )
        table1_task = Task.of_query(table1_file, metadata)

        table2_file = tmp_path / "test-project" / "test" / "table2_v1" / "query.sql"
        os.makedirs(table2_file.parent)
        table2_file.write_text("SELECT 67890")
        table2_task = Task.of_query(table2_file, metadata)

        check_file = tmp_path / "test-project" / "test" / "table1_v1" / "checks.sql"
        check_file.write_text("SELECT * FROM `test-project`.test.table2_v1")
        checks_task = Task.of_dq_check(
            check_file, is_check_fail=True, metadata=metadata
        )

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-01-01",
                    },
                }
            }
        ).with_tasks([checks_task, table1_task, table2_task])

        checks_task.with_upstream_dependencies(dags)
        result = checks_task.upstream_dependencies

        tables = [t.task_id for t in result]

        assert len(tables) == 2
        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables
