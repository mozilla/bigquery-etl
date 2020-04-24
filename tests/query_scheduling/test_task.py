from pathlib import Path
import pytest

from bigquery_etl.query_scheduling.task import Task, UnscheduledTask, TaskParseException
from bigquery_etl.metadata.parse_metadata import Metadata

TEST_DIR = Path(__file__).parent.parent


class TestTask:
    def test_of_query(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        task = Task.of_query(query_file)

        assert task.query_file == str(query_file)
        assert task.dataset == "test"
        assert task.table == "incremental_query"
        assert task.version == "v1"
        assert task.task_name == "test__incremental_query__v1"
        assert task.dag_name == "bqetl_events"
        assert task.args == {"depends_on_past": False}

    def test_of_non_existing_query(self):
        with pytest.raises(FileNotFoundError):
            Task.of_query("non_existing_query/query.sql")

    def test_invalid_path_format(self):
        with pytest.raises(ValueError):
            Task.of_query(TEST_DIR / "data" / "query.sql")

    def test_unscheduled_task(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata("test", "test", {}, {})

        with pytest.raises(UnscheduledTask):
            Task(query_file, metadata)

    def test_no_dag_name(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata("test", "test", {}, {"foo": "bar"})

        with pytest.raises(TaskParseException):
            Task(query_file, metadata)

    def test_task_instantiation(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata(
            "test",
            "test",
            {},
            {"dag_name": "test_dag", "depends_on_past": True, "param": "test_param"},
        )

        task = Task(query_file, metadata)
        assert task.dag_name == "test_dag"
        assert task.args["depends_on_past"]
        assert task.args["param"] == "test_param"

    def test_task_dry_run(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata(
            "test",
            "test",
            {},
            {"dag_name": "test_dag", "depends_on_past": True, "param": "test_param"},
        )

        task = Task(query_file, metadata)
        task._dry_run()