from google.cloud import bigquery
from pathlib import Path
import os
import pytest

from bigquery_etl.query_scheduling.task import Task, UnscheduledTask, TaskParseException
from bigquery_etl.parse_metadata import Metadata
from bigquery_etl.query_scheduling.dag_collection import DagCollection

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
        task._dry_run()    @pytest.mark.integration
    def test_task_get_dependencies_none(self, tmp_path):
        client = bigquery.Client(os.environ["GOOGLE_PROJECT_ID"])

        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 123423")

        metadata = Metadata(
            "test",
            "test",
            {},
            {"dag_name": "test_dag", "depends_on_past": True, "param": "test_param"},
        )

        task = Task(query_file, metadata)
        dags = DagCollection.from_dict({})
        assert task.get_dependencies(client, dags) == []

    @pytest.mark.integration
    def test_task_get_multiple_dependencies(self, tmp_path):
        project_id = os.environ["GOOGLE_PROJECT_ID"]
        client = bigquery.Client(os.environ["GOOGLE_PROJECT_ID"])

        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            f"SELECT * FROM {project_id}.test.table1_v1 "
            + f"UNION ALL SELECT * FROM {project_id}.test.table2_v1"
        )

        schema = [bigquery.SchemaField("a", "STRING", mode="NULLABLE")]

        table = bigquery.Table(f"{project_id}.test.table1_v1", schema=schema)
        client.create_table(table)
        table = bigquery.Table(f"{project_id}.test.table2_v1", schema=schema)
        client.create_table(table)

        metadata = Metadata(
            "test",
            "test",
            {},
            {"dag_name": "test_dag", "depends_on_past": True, "param": "test_param"},
        )

        task = Task(query_file, metadata)

        table1_task = Task(
            tmp_path / "sql" / "test" / "table1_v1" / "query.sql", metadata
        )
        table2_task = Task(
            tmp_path / "sql" / "test" / "table2_v1" / "query.sql", metadata
        )

        dags = DagCollection.from_dict(
            {"test_dag": {"schedule_interval": "daily", "default_args": {}}}
        ).with_tasks([task, table1_task, table2_task])

        result = task.get_dependencies(client, dags)

        client.delete_table(f"{project_id}.test.table1_v1")
        client.delete_table(f"{project_id}.test.table2_v1")

        tables = [f"{t.dataset}__{t.table}__{t.version}" for t in result]

        assert "test__table1__v1" in tables
        assert "test__table2__v1" in tables


# todo: test queries with views
