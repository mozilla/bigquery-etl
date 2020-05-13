"""Represents a collection of configured Airflow DAGs."""

from itertools import groupby
import yaml

from bigquery_etl.query_scheduling.dag import Dag, InvalidDag


class DagCollection:
    """Representation of all configured DAGs."""

    def __init__(self, dags):
        """Instantiate DAGs."""
        self.dags = dags

    @classmethod
    def from_dict(cls, d):
        """
        Parse DAG configurations from a dict and create new instances.

        Expected dict format:
        {
            "dag_name1": {
                "schedule_interval": string,
                "default_args": dict
            },
            "dag_name2": {
                "schedule_interval": string,
                "default_args": dict
            },
            ...
        }
        """
        dags = [Dag.from_dict({k: v}) for k, v in d.items()]
        return cls(dags)

    @classmethod
    def from_file(cls, config_file):
        """Instantiate DAGs based on the provided configuration file."""
        with open(config_file, "r") as yaml_stream:
            dags_config = yaml.safe_load(yaml_stream)
            return DagCollection.from_dict(dags_config)

    def dag_by_name(self, name):
        """Return the DAG with the provided name."""
        for dag in self.dags:
            if dag.name == name:
                return dag

        return None

    def task_for_table(self, dataset, table):
        """Return the task that schedules the query for the provided table."""
        for dag in self.dags:
            for task in dag.tasks:
                if dataset == task.dataset and table == f"{task.table}_{task.version}":
                    return task

        return None

    def with_tasks(self, tasks):
        """Assign tasks to their corresponding DAGs."""
        for dag_name, tasks in groupby(tasks, lambda t: t.dag_name):
            dag = self.dag_by_name(dag_name)

            if dag is None:
                raise InvalidDag(
                    f"DAG {dag_name} does not exist in dags.yaml"
                    "but used in task definition {tasks[0].name}."
                )
            else:
                dag.add_tasks(tasks)

        return self

    def to_airflow_dags(self, output_dir, client):
        """Write DAG representation as Airflow dags to file."""
        for dag in self.dags:
            output_file = output_dir / (dag.name + ".py")
            output_file.write_text(dag.to_airflow_dag(client, self))
