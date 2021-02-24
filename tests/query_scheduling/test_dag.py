from pathlib import Path

import pytest

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.dag import (
    Dag,
    DagDefaultArgs,
    DagParseException,
    PublicDataJsonDag,
)
from bigquery_etl.query_scheduling.dag_collection import DagCollection
from bigquery_etl.query_scheduling.task import Task

TEST_DIR = Path(__file__).parent.parent


class TestDag:
    default_args = {
        "owner": "test@example.org",
        "email": ["test@example.org"],
        "depends_on_past": False,
    }

    def test_dag_instantiation(self):
        dag = Dag("bqetl_test_dag", "daily", self.default_args)

        assert dag.name == "bqetl_test_dag"
        assert dag.schedule_interval == "daily"
        assert dag.tasks == []
        assert dag.default_args == self.default_args

    def test_add_tasks(self):
        dag = Dag("bqetl_test_dag", "daily", self.default_args)

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

        task1 = Task.of_query(query_file)
        task2 = Task.of_query(query_file, metadata)

        assert dag.tasks == []

        dag.add_tasks([task1, task2])

        assert len(dag.tasks) == 2

    def test_add_tasks_duplicate_names(self):
        dag = Dag("bqetl_test_dag", "daily", self.default_args)

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
            "task_name": "duplicate_task_name",
        }

        metadata = Metadata("test", "test", ["test@example.org"], {}, scheduling)
        task = Task.of_query(query_file, metadata)

        with pytest.raises(ValueError):
            dag.add_tasks([task, task])

    def test_from_dict(self):
        dag = Dag.from_dict(
            {
                "bqetl_test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "start_date": "2020-05-12",
                        "owner": "test@example.com",
                        "email": ["test@example.com"],
                    },
                }
            }
        )

        assert dag.name == "bqetl_test_dag"
        assert dag.schedule_interval == "daily"
        assert dag.default_args.owner == "test@example.com"
        assert dag.default_args.email == ["test@example.com"]
        assert dag.default_args.depends_on_past is False

    def test_from_empty_dict(self):
        with pytest.raises(DagParseException):
            Dag.from_dict({})

    def test_from_dict_multiple_dags(self):
        with pytest.raises(DagParseException):
            Dag.from_dict({"bqetl_test_dag1": {}, "bqetl_test_dag2": {}})

    def test_from_dict_without_scheduling_interval(self):
        with pytest.raises(DagParseException):
            Dag.from_dict({"bqetl_test_dag": {}})

    def test_invalid_dag_name(self):
        with pytest.raises(ValueError):
            Dag("test_dag", "daily", self.default_args)

    def test_schedule_interval_format(self):
        assert Dag("bqetl_test_dag", "daily", self.default_args)
        assert Dag("bqetl_test_dag", "weekly", self.default_args)
        assert Dag("bqetl_test_dag", "once", self.default_args)

        with pytest.raises(ValueError):
            assert Dag("bqetl_test_dag", "never", self.default_args)

        assert Dag("bqetl_test_dag", "* * * * *", self.default_args)
        assert Dag("bqetl_test_dag", "1 * * * *", self.default_args)

        with pytest.raises(TypeError):
            assert Dag("bqetl_test_dag", 323, self.default_args)

    def test_empty_dag_default_args(self):
        with pytest.raises(TypeError):
            assert DagDefaultArgs()

    def test_dag_default_args_owner_validation(self):
        with pytest.raises(ValueError):
            assert DagDefaultArgs(owner="invalid_email", start_date="2020-12-12")

        with pytest.raises(ValueError):
            assert DagDefaultArgs(owner="test@example-com.", start_date="2020-12-12")

        with pytest.raises(TypeError):
            assert DagDefaultArgs(owner=None, start_date="2020-12-12")

        assert DagDefaultArgs(owner="test@example.org", start_date="2020-12-12")

    def test_dag_default_args_email_validation(self):
        with pytest.raises(ValueError):
            assert DagDefaultArgs(
                owner="test@example.com",
                start_date="2020-12-12",
                email=["valid@example.com", "invalid", "valid2@example.com"],
            )

        assert DagDefaultArgs(
            owner="test@example.com", start_date="2020-12-12", email=[]
        )

        assert DagDefaultArgs(
            owner="test@example.com",
            start_date="2020-12-12",
            email=["test@example.org", "test2@example.org"],
        )

    def test_dag_default_args_start_date_validation(self):
        with pytest.raises(ValueError):
            assert DagDefaultArgs(
                owner="test@example.com", start_date="March 12th 2020"
            )

        with pytest.raises(ValueError):
            assert DagDefaultArgs(owner="test@example.com", start_date="foo")

        with pytest.raises(ValueError):
            assert DagDefaultArgs(owner="test@example.com", start_date="2020-13-01")

        assert DagDefaultArgs(owner="test@example.com", start_date="2020-12-12")

    def test_dag_default_args_retry_delay_validation(self):
        with pytest.raises(ValueError):
            assert DagDefaultArgs(
                owner="test@example.com", start_date="2020-12-12", retry_delay="90"
            )

        with pytest.raises(ValueError):
            assert DagDefaultArgs(
                owner="test@example.com", start_date="2020-12-12", retry_delay="1d1h1m"
            )

        assert DagDefaultArgs(
            owner="test@example.com", start_date="2020-12-12", retry_delay="3h15m1s"
        )

        assert DagDefaultArgs(
            owner="test@example.com", start_date="2020-12-12", retry_delay="3h"
        )

    def test_public_data_json_dag_add_task(self):
        public_json_dag = PublicDataJsonDag(
            "bqetl_public_data_json_dag", "daily", self.default_args
        )

        assert len(public_json_dag.tasks) == 0

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
        dag = Dag(
            "bqetl_events",
            "0 1 * * *",
            DagDefaultArgs("test@example.org", "2020-01-01"),
            [task],
        )
        public_json_dag.add_export_tasks([task], DagCollection([dag]))

        assert len(public_json_dag.tasks) == 1
        assert (
            public_json_dag.tasks[0].task_name
            == "export_public_data_json_test__incremental_query__v1"
        )
        assert public_json_dag.tasks[0].dag_name == "bqetl_public_data_json_dag"
        assert len(public_json_dag.tasks[0].dependencies) == 1
