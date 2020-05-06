from google.cloud import bigquery
from pathlib import Path
import os
import pytest

from bigquery_etl.query_scheduling.task import Task, UnscheduledTask, TaskParseException
from bigquery_etl.metadata.parse_metadata import Metadata
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

    @pytest.mark.integration
    def test_task_get_dependencies_none(self, tmp_path, bigquery_client):
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
        assert task.get_dependencies(bigquery_client, dags) == []

    @pytest.mark.integration
    def test_task_get_multiple_dependencies(
        self, tmp_path, bigquery_client, project_id, temporary_dataset
    ):
        query_file_path = tmp_path / "sql" / temporary_dataset / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            f"SELECT * FROM {project_id}.{temporary_dataset}.table1_v1 "
            + f"UNION ALL SELECT * FROM {project_id}.{temporary_dataset}.table2_v1"
        )

        schema = [bigquery.SchemaField("a", "STRING", mode="NULLABLE")]
        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.table1_v1", schema=schema
        )
        bigquery_client.create_table(table)
        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.table2_v1", schema=schema
        )
        bigquery_client.create_table(table)

        metadata = Metadata(
            "test",
            "test",
            {},
            {"dag_name": "test_dag", "depends_on_past": True, "param": "test_param"},
        )

        task = Task(query_file, metadata)

        table_task1 = Task(
            tmp_path / "sql" / temporary_dataset / "table1_v1" / "query.sql", metadata
        )
        table_task2 = Task(
            tmp_path / "sql" / temporary_dataset / "table2_v1" / "query.sql", metadata
        )

        dags = DagCollection.from_dict(
            {"test_dag": {"schedule_interval": "daily", "default_args": {}}}
        ).with_tasks([task, table_task1, table_task2])

        result = task.get_dependencies(bigquery_client, dags)

        tables = [f"{t.dataset}__{t.table}__{t.version}" for t in result]

        assert f"{temporary_dataset}__table1__v1" in tables
        assert f"{temporary_dataset}__table2__v1" in tables

    @pytest.mark.integration
    def test_task_get_view_dependencies(
        self, tmp_path, bigquery_client, project_id, temporary_dataset
    ):
        query_file_path = tmp_path / "sql" / temporary_dataset / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            f"SELECT * FROM {project_id}.{temporary_dataset}.table1_v1 "
            + f"UNION ALL SELECT * FROM {project_id}.{temporary_dataset}.test_view"
        )

        schema = [bigquery.SchemaField("a", "STRING", mode="NULLABLE")]
        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.table1_v1", schema=schema
        )
        bigquery_client.create_table(table)
        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.table2_v1", schema=schema
        )
        bigquery_client.create_table(table)
        view = bigquery.Table(f"{project_id}.{temporary_dataset}.test_view")
        view.view_query = f"SELECT * FROM {project_id}.{temporary_dataset}.table2_v1"
        bigquery_client.create_table(view)

        metadata = Metadata(
            "test",
            "test",
            {},
            {"dag_name": "test_dag", "depends_on_past": True, "param": "test_param"},
        )

        task = Task(query_file, metadata)

        table_task1 = Task(
            tmp_path / "sql" / temporary_dataset / "table1_v1" / "query.sql", metadata
        )
        table_task2 = Task(
            tmp_path / "sql" / temporary_dataset / "table2_v1" / "query.sql", metadata
        )

        dags = DagCollection.from_dict(
            {"test_dag": {"schedule_interval": "daily", "default_args": {}}}
        ).with_tasks([task, table_task1, table_task2])

        result = task.get_dependencies(bigquery_client, dags)

        tables = [f"{t.dataset}__{t.table}__{t.version}" for t in result]

        assert f"{temporary_dataset}__table1__v1" in tables
        assert f"{temporary_dataset}__table2__v1" in tables

    @pytest.mark.integration
    def test_task_get_nested_view_dependencies(
        self, tmp_path, bigquery_client, project_id, temporary_dataset
    ):
        query_file_path = tmp_path / "sql" / temporary_dataset / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            f"SELECT * FROM {project_id}.{temporary_dataset}.table1_v1 "
            + f"UNION ALL SELECT * FROM {project_id}.{temporary_dataset}.test_view"
        )

        schema = [bigquery.SchemaField("a", "STRING", mode="NULLABLE")]
        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.table1_v1", schema=schema
        )
        bigquery_client.create_table(table)
        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.table2_v1", schema=schema
        )
        bigquery_client.create_table(table)
        view = bigquery.Table(f"{project_id}.{temporary_dataset}.test_view2")
        view.view_query = f"SELECT * FROM {project_id}.{temporary_dataset}.table2_v1"
        bigquery_client.create_table(view)
        view = bigquery.Table(f"{project_id}.{temporary_dataset}.test_view")
        view.view_query = f"SELECT * FROM {project_id}.{temporary_dataset}.test_view2"
        bigquery_client.create_table(view)

        metadata = Metadata(
            "test",
            "test",
            {},
            {"dag_name": "test_dag", "depends_on_past": True, "param": "test_param"},
        )

        task = Task(query_file, metadata)

        table_task1 = Task(
            tmp_path / "sql" / temporary_dataset / "table1_v1" / "query.sql", metadata
        )
        table_task2 = Task(
            tmp_path / "sql" / temporary_dataset / "table2_v1" / "query.sql", metadata
        )

        dags = DagCollection.from_dict(
            {"test_dag": {"schedule_interval": "daily", "default_args": {}}}
        ).with_tasks([task, table_task1, table_task2])

        result = task.get_dependencies(bigquery_client, dags)
        tables = [f"{t.dataset}__{t.table}__{t.version}" for t in result]

        assert f"{temporary_dataset}__table1__v1" in tables
        assert f"{temporary_dataset}__table2__v1" in tables
