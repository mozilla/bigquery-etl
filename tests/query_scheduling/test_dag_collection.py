from google.cloud import bigquery
from jinja2 import Environment, PackageLoader
import os
from pathlib import Path
import pytest
from unittest import mock

from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.query_scheduling.dag import InvalidDag, DagParseException
from bigquery_etl.query_scheduling.task import Task
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.dryrun import DryRun

TEST_DIR = Path(__file__).parent.parent
# This Cloud Function has been manually deployed and might be outdated compared
# to the shared-prod version
TEST_DRY_RUN_URL = (
    "https://us-central1-bigquery-etl-integration-test.cloudfunctions.net/dryrun"
)


class TestDagCollection:
    default_args = {
        "start_date": "2020-05-15",
        "owner": "test@example.org",
        "email": ["test@example.org"],
        "depends_on_past": False,
    }

    def test_dags_from_file(self):
        dags_file = TEST_DIR / "data" / "dags.yaml"
        dags = DagCollection.from_file(dags_file)

        assert len(dags.dags) == 2
        assert dags.dag_by_name("not existing") is None
        assert dags.dag_by_name("bqetl_events") is not None
        assert dags.dag_by_name("bqetl_core") is not None

        events_dag = dags.dag_by_name("bqetl_events")
        assert len(events_dag.tasks) == 0

        core_dag = dags.dag_by_name("bqetl_core")
        assert len(core_dag.tasks) == 0

    def test_dags_from_empty_file(self, tmp_path):
        dags_file = tmp_path / "dags.yaml"
        dags_file.write_text("")
        dags = DagCollection.from_file(dags_file)

        assert len(dags.dags) == 0

    def test_dags_from_dict(self):
        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag1": {
                    "schedule_interval": "daily",
                    "default_args": self.default_args,
                },
                "bqetl_test_dag2": {
                    "schedule_interval": "daily",
                    "default_args": self.default_args,
                },
            }
        )

        assert len(dags.dags) == 2
        assert dags.dag_by_name("bqetl_test_dag1") is not None
        assert dags.dag_by_name("bqetl_test_dag2") is not None

        dag1 = dags.dag_by_name("bqetl_test_dag1")
        assert len(dag1.tasks) == 0
        assert dag1.schedule_interval == "daily"
        assert dag1.default_args.owner == "test@example.org"

        dag2 = dags.dag_by_name("bqetl_test_dag2")
        assert len(dag2.tasks) == 0
        assert dag2.schedule_interval == "daily"

    def test_dags_from_empty_dict(self):
        dags = DagCollection.from_dict({})
        assert len(dags.dags) == 0

    def test_dags_from_invalid_dict(self):
        with pytest.raises(DagParseException):
            DagCollection.from_dict({"foo": "bar"})

    def test_dag_by_name(self):
        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag1": {
                    "schedule_interval": "daily",
                    "default_args": self.default_args,
                }
            }
        )

        assert dags.dag_by_name("bqetl_test_dag1") is not None
        assert dags.dag_by_name("bqetl_test_dag1").name == "bqetl_test_dag1"
        assert dags.dag_by_name("non_existing") is None

    def test_task_for_table(self):
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
            "test",
            "test",
            ["test@example.org"],
            {},
            {"dag_name": "bqetl_test_dag", "depends_on_past": True},
        )

        tasks = [Task.of_query(query_file, metadata)]

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": self.default_args,
                }
            }
        ).with_tasks(tasks)

        task = dags.task_for_table(
            "moz-fx-data-test-project", "test", "incremental_query_v1"
        )

        assert task
        assert task.dag_name == "bqetl_test_dag"

    def test_task_for_non_existing_table(self):
        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": self.default_args,
                }
            }
        ).with_tasks([])

        assert (
            dags.task_for_table(
                "moz-fx-data-test-project", "test", "non_existing_table"
            )
            is None
        )

    def test_dags_with_tasks(self):
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
            "test",
            "test",
            ["test@example.org"],
            {},
            {"dag_name": "bqetl_test_dag", "depends_on_past": True},
        )

        tasks = [Task.of_query(query_file, metadata)]

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": self.default_args,
                }
            }
        ).with_tasks(tasks)

        assert len(dags.dags) == 1

        dag = dags.dag_by_name("bqetl_test_dag")
        assert len(dag.tasks) == 1
        assert dag.tasks[0].dag_name == "bqetl_test_dag"

    def test_dags_with_invalid_tasks(self):
        with pytest.raises(InvalidDag):
            query_file = (
                TEST_DIR
                / "data"
                / "moz-fx-data-test-project"
                / "test_sql"
                / "test"
                / "incremental_query_v1"
                / "query.sql"
            )

            metadata = Metadata(
                "test",
                "test",
                ["test@example.org"],
                {},
                {
                    "dag_name": "bqetl_non_exisiting_dag",
                    "depends_on_past": True,
                    "param": "test_param",
                },
            )

            tasks = [Task.of_query(query_file, metadata)]

            DagCollection.from_dict(
                {
                    "bqetl_test_dag": {
                        "schedule_interval": "daily",
                        "default_args": self.default_args,
                    }
                }
            ).with_tasks(tasks)

    @mock.patch.object(DryRun, "DRY_RUN_URL", TEST_DRY_RUN_URL)
    def test_to_airflow(self, tmp_path):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "non_incremental_query_v1"
            / "query.sql"
        )

        metadata = Metadata(
            "test",
            "test",
            ["test@example.com"],
            {},
            {
                "dag_name": "bqetl_test_dag",
                "depends_on_past": True,
                "param": "test_param",
                "arguments": ["--append_table"],
            },
        )

        tasks = [Task.of_query(query_file, metadata)]

        default_args = {
            "depends_on_past": False,
            "owner": "test@example.org",
            "email": ["test@example.org"],
            "start_date": "2020-01-01",
            "retry_delay": "1h",
        }
        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": default_args,
                }
            }
        ).with_tasks(tasks)

        dags.to_airflow_dags(tmp_path)
        result = (tmp_path / "bqetl_test_dag.py").read_text().strip()
        expected = (TEST_DIR / "data" / "dags" / "simple_test_dag").read_text().strip()
        assert result == expected

    @mock.patch.object(DryRun, "DRY_RUN_URL", TEST_DRY_RUN_URL)
    def test_python_script_to_airflow(self, tmp_path):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "python_script_query_v1"
            / "query.py"
        )

        metadata = Metadata(
            "test",
            "test",
            ["test@example.com"],
            {},
            {
                "dag_name": "bqetl_test_dag",
                "depends_on_past": True,
                "arguments": ["--date", "{{ds}}"],
            },
        )

        tasks = [Task.of_python_script(query_file, metadata)]

        default_args = {
            "depends_on_past": False,
            "owner": "test@example.org",
            "email": ["test@example.org"],
            "start_date": "2020-01-01",
            "retry_delay": "1h",
        }
        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": default_args,
                }
            }
        ).with_tasks(tasks)

        dags.to_airflow_dags(tmp_path)
        result = (tmp_path / "bqetl_test_dag.py").read_text().strip()
        expected = (
            (TEST_DIR / "data" / "dags" / "python_script_test_dag").read_text().strip()
        )

        assert result == expected

    @pytest.mark.integration
    @mock.patch.object(DryRun, "DRY_RUN_URL", TEST_DRY_RUN_URL)
    def test_to_airflow_with_dependencies(
        self, tmp_path, project_id, temporary_dataset, bigquery_client
    ):
        query_file_path = tmp_path / project_id / temporary_dataset / "query_v1"
        os.makedirs(query_file_path)

        query_file = query_file_path / "query.sql"
        query_file.write_text(
            f"SELECT * FROM {project_id}.{temporary_dataset}.table1_v1 "
            + f"UNION ALL SELECT * FROM {project_id}.{temporary_dataset}.table2_v1 "
            + "UNION ALL SELECT * FROM "
            + f"{project_id}.{temporary_dataset}.external_table_v1"
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

        table = bigquery.Table(
            f"{project_id}.{temporary_dataset}.external_table_v1", schema=schema
        )
        bigquery_client.create_table(table)

        metadata = Metadata(
            "test",
            "test",
            ["test@example.org"],
            {},
            {
                "dag_name": "bqetl_test_dag",
                "default_args": {"owner": "test@example.org"},
            },
        )

        task = Task.of_query(query_file, metadata)

        table_task1 = Task.of_query(
            tmp_path / project_id / temporary_dataset / "table1_v1" / "query.sql",
            metadata,
        )

        os.makedirs(tmp_path / project_id / temporary_dataset / "table1_v1")
        query_file = (
            tmp_path / project_id / temporary_dataset / "table1_v1" / "query.sql"
        )
        query_file.write_text("SELECT 1")

        table_task2 = Task.of_query(
            tmp_path / project_id / temporary_dataset / "table2_v1" / "query.sql",
            metadata,
        )

        os.makedirs(tmp_path / project_id / temporary_dataset / "table2_v1")
        query_file = (
            tmp_path / project_id / temporary_dataset / "table2_v1" / "query.sql"
        )
        query_file.write_text("SELECT 2")

        metadata = Metadata(
            "test",
            "test",
            ["test@example.org"],
            {},
            {
                "dag_name": "bqetl_external_test_dag",
                "default_args": {"owner": "test@example.org"},
            },
        )

        external_table_task = Task.of_query(
            tmp_path
            / project_id
            / temporary_dataset
            / "external_table_v1"
            / "query.sql",
            metadata,
        )

        os.makedirs(tmp_path / project_id / temporary_dataset / "external_table_v1")
        query_file = (
            tmp_path
            / project_id
            / temporary_dataset
            / "external_table_v1"
            / "query.sql"
        )
        query_file.write_text("SELECT 3")

        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-05-25",
                    },
                },
                "bqetl_external_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.org",
                        "start_date": "2020-05-25",
                    },
                },
            }
        ).with_tasks([task, table_task1, table_task2, external_table_task])

        dags.to_airflow_dags(tmp_path)

        # we need to use templates since the temporary dataset name changes between runs
        env = Environment(loader=PackageLoader("tests", "data/dags"))

        dag_template_with_dependencies = env.get_template("test_dag_with_dependencies")
        dag_template_external_dependency = env.get_template(
            "test_dag_external_dependency"
        )

        args = {"temporary_dataset": temporary_dataset}

        expected_dag_with_dependencies = dag_template_with_dependencies.render(args)
        expected_dag_external_dependency = dag_template_external_dependency.render(args)

        dag_with_dependencies = (tmp_path / "bqetl_test_dag.py").read_text().strip()
        dag_external_dependency = (
            (tmp_path / "bqetl_external_test_dag.py").read_text().strip()
        )

        assert dag_with_dependencies == expected_dag_with_dependencies
        assert dag_external_dependency == expected_dag_external_dependency

    @pytest.mark.integration
    @mock.patch.object(DryRun, "DRY_RUN_URL", TEST_DRY_RUN_URL)
    def test_public_json_dag_to_airflow(self, tmp_path):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "non_incremental_query_v1"
            / "query.sql"
        )

        tasks = [Task.of_query(query_file)]

        default_args = {
            "depends_on_past": False,
            "owner": "test@example.org",
            "email": ["test@example.org"],
            "start_date": "2020-01-01",
            "retry_delay": "1h",
        }

        dags = DagCollection.from_dict(
            {
                "bqetl_public_data_json": {
                    "schedule_interval": "daily",
                    "default_args": default_args,
                },
                "bqetl_core": {
                    "schedule_interval": "daily",
                    "default_args": default_args,
                },
            }
        ).with_tasks(tasks)

        dags.to_airflow_dags(tmp_path)
        result = (tmp_path / "bqetl_public_data_json.py").read_text().strip()
        expected_dag = (
            (TEST_DIR / "data" / "dags" / "test_public_data_json_dag")
            .read_text()
            .strip()
        )

        assert result == expected_dag

    @pytest.mark.integration
    @mock.patch.object(DryRun, "DRY_RUN_URL", TEST_DRY_RUN_URL)
    def test_to_airflow_duplicate_dependencies(self, tmp_path):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "non_incremental_query_v1"
            / "query.sql"
        )

        query_file2 = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "no_metadata_query_v1"
            / "query.sql"
        )

        metadata = Metadata(
            "test",
            "test",
            ["test@example.com"],
            {},
            {
                "dag_name": "bqetl_test_dag",
                "depends_on_past": True,
                "depends_on": [{"dag_name": "external", "task_id": "task1"}],
            },
        )

        tasks = [
            Task.of_query(query_file, metadata),
            Task.of_query(query_file2, metadata),
        ]

        default_args = {
            "owner": "test@example.org",
            "start_date": "2020-01-01",
        }
        dags = DagCollection.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": default_args,
                }
            }
        ).with_tasks(tasks)

        dags.to_airflow_dags(tmp_path)
        result = (tmp_path / "bqetl_test_dag.py").read_text().strip()
        expected = (
            (TEST_DIR / "data" / "dags" / "test_dag_duplicate_dependencies")
            .read_text()
            .strip()
        )

        assert result == expected
