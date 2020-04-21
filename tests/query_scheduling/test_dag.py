from pathlib import Path
import pytest

from bigquery_etl.query_scheduling.dag import Dag, DagParseException
from bigquery_etl.query_scheduling.task import Task

TEST_DIR = Path(__file__).parent.parent


class TestDag:
    def test_dag_instantiation(self):
        dag = Dag("test_dag", "daily", {})

        assert dag.name == "test_dag"
        assert dag.schedule_interval == "daily"
        assert dag.tasks == []
        assert dag.default_args == {}

    def test_add_tasks(self):
        dag = Dag("test_dag", "daily", {})

        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "incremental_query_v1"
            / "query.sql"
        )

        tasks = [Task.of_query(query_file), Task.of_query(query_file)]

        assert dag.tasks == []

        dag.add_tasks(tasks)

        assert len(dag.tasks) == 2

    def test_from_dict(self):
        dag = Dag.from_dict(
            {
                "test_dag": {
                    "schedule_interval": "daily",
                    "default_args": {
                        "owner": "test@example.com",
                        "param": "test_param",
                    },
                }
            }
        )

        assert dag.name == "test_dag"
        assert dag.schedule_interval == "daily"
        assert dag.default_args == {"owner": "test@example.com", "param": "test_param"}

    def test_from_empty_dict(self):
        with pytest.raises(DagParseException):
            Dag.from_dict({})

    def test_from_dict_multiple_dags(self):
        with pytest.raises(DagParseException):
            Dag.from_dict({"test_dag1": {}, "test_dag2": {}})

    def test_from_dict_without_scheduling_interval(self):
        with pytest.raises(DagParseException):
            Dag.from_dict({"test_dag": {}})
