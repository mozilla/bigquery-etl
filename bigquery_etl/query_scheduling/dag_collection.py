"""Represents a collection of configured Airflow DAGs."""

from black import format_file_contents, FileMode
from itertools import groupby
from operator import attrgetter
from pathlib import Path
import yaml

from bigquery_etl.query_scheduling.dag import Dag, InvalidDag, PublicDataJsonDag
from functools import partial
from multiprocessing.pool import ThreadPool


class DagCollection:
    """Representation of all configured DAGs."""

    def __init__(self, dags):
        """Instantiate DAGs."""
        self.dags = dags
        self.dags_by_name = {dag.name: dag for dag in dags}

    @classmethod
    def from_dict(cls, d):
        """
        Parse DAG configurations from a dict and create new instances.

        Expected dict format:
        {
            "bqetl_dag_name1": {
                "schedule_interval": string,
                "default_args": {
                    "owner": string,
                    "start_date": "YYYY-MM-DD",
                    ...
                }
            },
            "bqetl_dag_name2": {
                "schedule_interval": string,
                "default_args": {
                    "owner": string,
                    "start_date": "YYYY-MM-DD",
                    ...
                }
            },
            ...
        }
        """
        if d is None:
            return cls([])

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
        return self.dags_by_name.get(name)

    def task_for_table(self, dataset, table):
        """Return the task that schedules the query for the provided table."""
        for dag in self.dags:
            for task in dag.tasks:
                if dataset == task.dataset and table == f"{task.table}_{task.version}":
                    return task

        return None

    def with_tasks(self, tasks):
        """Assign tasks to their corresponding DAGs."""
        public_data_json_dag = None

        get_dag_name = attrgetter("dag_name")
        for dag_name, group in groupby(sorted(tasks, key=get_dag_name), get_dag_name):
            dag = self.dag_by_name(dag_name)
            if dag is None:
                raise InvalidDag(
                    f"DAG {dag_name} does not exist in dags.yaml"
                    "but used in task definition {next(group).task_name}."
                )
            dag.add_tasks(list(group))

        public_json_tasks = [task for task in tasks if task.public_json]
        if public_json_tasks:
            for dag in self.dags:
                if dag.__class__ == PublicDataJsonDag:
                    public_data_json_dag = dag
            if public_data_json_dag:
                public_data_json_dag.add_export_tasks(public_json_tasks, self)

        return self

    def dag_to_airflow(self, output_dir, dag):
        """Generate the Airflow DAG representation for the provided DAG."""
        output_file = Path(output_dir) / (dag.name + ".py")
        formatted_dag = format_file_contents(
            dag.to_airflow_dag(self), fast=False, mode=FileMode()
        )
        output_file.write_text(formatted_dag)

    def to_airflow_dags(self, output_dir):
        """Write DAG representation as Airflow dags to file."""
        with ThreadPool(8) as p:
            p.map(partial(self.dag_to_airflow, output_dir), self.dags, chunksize=1)
