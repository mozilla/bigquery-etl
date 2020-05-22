from google.cloud import bigquery
from pathlib import Path
import os
import pytest

from bigquery_etl.query_scheduling.task import Task, UnscheduledTask, TaskParseException
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.dag_collection import DagCollection

TEST_DIR = Path(__file__).parent.parent


class TestTask:
    default_scheduling = {
        "dag_name": "bqetl_test_dag",
        "schedule_interval": "daily",
        "default_args": {"owner": "test@example.org"},
    }

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
        assert task.depends_on_past is False

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

    @pytest.mark.integration
    def test_task_get_dependencies_none(self, tmp_path, bigquery_client):
        query_file_path = tmp_path / "sql" / "test" / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text("SELECT 123423")

        metadata = Metadata(
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)
        dags = DagCollection.from_dict({})
        task.with_dependencies(bigquery_client, dags)
        assert task.dependencies == []

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
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table_task1 = Task.of_query(
            tmp_path / "sql" / temporary_dataset / "table1_v1" / "query.sql", metadata
        )
        table_task2 = Task.of_query(
            tmp_path / "sql" / temporary_dataset / "table2_v1" / "query.sql", metadata
        )

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {"owner": "test@example.org"},
                }
            }
        ).with_tasks([task, table_task1, table_task2])

        task.with_dependencies(bigquery_client, dags)
        result = task.dependencies

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
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table_task1 = Task.of_query(
            tmp_path / "sql" / temporary_dataset / "table1_v1" / "query.sql", metadata
        )
        table_task2 = Task.of_query(
            tmp_path / "sql" / temporary_dataset / "table2_v1" / "query.sql", metadata
        )

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {"owner": "test@example.org"},
                }
            }
        ).with_tasks([task, table_task1, table_task2])

        task.with_dependencies(bigquery_client, dags)
        result = task.dependencies

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
            "test", "test", ["test@example.org"], {}, self.default_scheduling
        )

        task = Task.of_query(query_file, metadata)

        table_task1 = Task.of_query(
            tmp_path / "sql" / temporary_dataset / "table1_v1" / "query.sql", metadata
        )
        table_task2 = Task.of_query(
            tmp_path / "sql" / temporary_dataset / "table2_v1" / "query.sql", metadata
        )

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {"owner": "test@example.org"},
                }
            }
        ).with_tasks([task, table_task1, table_task2])

        task.with_dependencies(bigquery_client, dags)
        result = task.dependencies
        tables = [f"{t.dataset}__{t.table}__{t.version}" for t in result]

        assert f"{temporary_dataset}__table1__v1" in tables
        assert f"{temporary_dataset}__table2__v1" in tables
