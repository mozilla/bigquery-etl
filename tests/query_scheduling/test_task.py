import os
from pathlib import Path
from typing import NewType

import pytest

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.query_scheduling.task import (
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
            "task_name": "a" * 63,
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
            "task_name": "a" * 62,
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        task = Task.of_query(query_file, metadata)
        assert task.task_name == "a" * 62

    def test_validate_task_name(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / (("a" * 63) + "_v1")
            / "query.sql"
        )

        scheduling = {
            "dag_name": "bqetl_test_dag",
            "default_args": {"owner": "test@example.org"},
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)

        with pytest.raises(ValueError):
            Task.of_query(query_file, metadata)

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

    @pytest.mark.java
    def test_task_get_dependencies_none(self, tmp_path):
        query_file_path = tmp_path / "test-project" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 123423")

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)
        dags = DagCollection.from_dict({})
        task.with_dependencies(dags)
        assert task.dependencies == []

    @pytest.mark.java
    def test_task_get_multiple_dependencies(self, tmp_path):
        query_file_path = tmp_path / "test-project" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            "SELECT * FROM `test-project`.test.table1_v1 "
            "UNION ALL SELECT * FROM `test-project`.test.table2_v1"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table_task1 = Task.of_query(
            tmp_path / "test-project" / "test" / "table1_v1" / "query.sql",
            metadata,
        )
        table_task2 = Task.of_query(
            tmp_path / "test-project" / "test" / "table2_v1" / "query.sql",
            metadata,
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
        ).with_tasks([task, table_task1, table_task2])

        task.with_dependencies(dags)
        result = task.dependencies

        tables = [t.task_id for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables

    @pytest.mark.java
    def test_multipart_task_get_dependencies(self, tmp_path):
        query_file_path = tmp_path / "test-project" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file_part1 = query_file_path / "part1.sql"
        query_file_part1.write_text("SELECT * FROM `test-project`.test.table1_v1")

        query_file_part2 = query_file_path / "part2.sql"
        query_file_part2.write_text("SELECT * FROM `test-project`.test.table2_v1")

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_multipart_query(query_file_part1, metadata)

        table_task1 = Task.of_query(
            tmp_path / "test-project" / "test" / "table1_v1" / "query.sql",
            metadata,
        )
        table_task2 = Task.of_query(
            tmp_path / "test-project" / "test" / "table2_v1" / "query.sql",
            metadata,
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
        ).with_tasks([task, table_task1, table_task2])

        task.with_dependencies(dags)
        result = task.dependencies

        tables = [t.task_id for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables

    @pytest.mark.java
    def test_task_get_view_dependencies(self, tmp_path):
        query_file_path = tmp_path / "test-project" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            "SELECT * FROM `test-project`.test.table1_v1 "
            "UNION ALL SELECT * FROM `test-project`.test.test_view"
        )

        view_file_path = tmp_path / "test-project" / "test" / "test_view"
        os.makedirs(view_file_path)

        view_file = view_file_path / "view.sql"
        view_file.write_text(
            "CREATE OR REPLACE VIEW `test-project`.test.test_view "
            "AS SELECT * FROM `test-project`.test.table2_v1"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table_task1 = Task.of_query(
            tmp_path / "test-project" / "test" / "table1_v1" / "query.sql",
            metadata,
        )
        table_task2 = Task.of_query(
            tmp_path / "test-project" / "test" / "table2_v1" / "query.sql",
            metadata,
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
        ).with_tasks([task, table_task1, table_task2])

        task.with_dependencies(dags)
        result = task.dependencies

        tables = [t.task_id for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables

    @pytest.mark.java
    def test_task_get_nested_view_dependencies(self, tmp_path):
        query_file_path = tmp_path / "test-project" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            "SELECT * FROM `test-project`.test.table1_v1 "
            "UNION ALL SELECT * FROM `test-project`.test.test_view"
        )

        view_file_path = tmp_path / "test-project" / "test" / "test_view"
        os.makedirs(view_file_path)

        view_file = view_file_path / "view.sql"
        view_file.write_text(
            "CREATE OR REPLACE VIEW `test-project`.test.test_view "
            "AS SELECT * FROM `test-project`.test.test_view2"
        )

        view2_file_path = tmp_path / "test-project" / "test" / "test_view2"
        os.makedirs(view2_file_path)

        view2_file = view2_file_path / "view.sql"
        view2_file.write_text(
            "CREATE OR REPLACE VIEW `test-project`.test.test_view2 "
            "AS SELECT * FROM `test-project`.test.table2_v1"
        )

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table_task1 = Task.of_query(
            tmp_path / "test-project" / "test" / "table1_v1" / "query.sql",
            metadata,
        )
        table_task2 = Task.of_query(
            tmp_path / "test-project" / "test" / "table2_v1" / "query.sql",
            metadata,
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
        ).with_tasks([task, table_task1, table_task2])

        task.with_dependencies(dags)
        result = task.dependencies
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
