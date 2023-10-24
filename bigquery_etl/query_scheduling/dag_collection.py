"""Represents a collection of configured Airflow DAGs."""

from collections import defaultdict
from functools import partial
from itertools import groupby
from multiprocessing import get_context, set_start_method
from operator import attrgetter
from pathlib import Path

import yaml
from black import FileMode, format_file_contents

from bigquery_etl.query_scheduling.dag import Dag, InvalidDag, PublicDataJsonDag


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

    def task_for_table(self, project, dataset, table):
        """Return the task that schedules the query for the provided table."""
        for dag in self.dags:
            for task in dag.tasks:
                if (
                    project == task.project
                    and dataset == task.dataset
                    and table == f"{task.table}_{task.version}"
                    and not task.is_dq_check
                ):
                    return task

        return None

    def fail_checks_task_for_table(self, project, dataset, table):
        """Return the task that schedules the checks for the provided table."""
        for dag in self.dags:
            for task in dag.tasks:
                if (
                    project == task.project
                    and dataset == task.dataset
                    and table == f"{task.table}_{task.version}"
                    and task.is_dq_check
                    and task.is_dq_check_fail
                ):
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
                    f"DAG {dag_name} does not exist in dags.yaml "
                    f"but used in task definition {next(group).task_name}."
                )
            dag.add_tasks(list(group))

        public_json_tasks = [
            task for task in tasks if task.public_json and not task.is_dq_check
        ]
        if public_json_tasks:
            for dag in self.dags:
                if dag.__class__ == PublicDataJsonDag:
                    public_data_json_dag = dag
            if public_data_json_dag:
                public_data_json_dag.add_export_tasks(public_json_tasks, self)

        return self

    def get_task_downstream_dependencies(self, task):
        """Return all direct downstream dependencies of the task."""
        # Cache the downstream dependencies for faster lookups.
        if not hasattr(self, "_downstream_dependencies"):
            downstream_dependencies = defaultdict(list)
            for dag in self.dags:
                for _task in dag.tasks:
                    _task.with_upstream_dependencies(self)
                    for upstream_dependency in (
                        _task.depends_on + _task.upstream_dependencies
                    ):
                        downstream_dependencies[upstream_dependency.task_key].append(
                            _task.to_ref(self)
                        )
            self._downstream_dependencies = downstream_dependencies

        return self._downstream_dependencies[task.task_key]

    def dag_to_airflow(self, output_dir, dag):
        """Generate the Airflow DAG representation for the provided DAG."""
        output_file = Path(output_dir) / (dag.name + ".py")

        try:
            formatted_dag = format_file_contents(
                dag.to_airflow_dag(), fast=False, mode=FileMode()
            )
            output_file.write_text(formatted_dag)
        except InvalidDag as e:
            print(e)

    def to_airflow_dags(self, output_dir, dag_to_generate=None):
        """Write DAG representation as Airflow dags to file."""
        # https://pythonspeed.com/articles/python-multiprocessing/
        # when running tests on CI that call this function, we need
        # to create a custom pool to prevent processes from getting stuck

        # Generate a single DAG:
        if dag_to_generate is not None:
            dag_to_generate.with_upstream_dependencies(self)
            dag_to_generate.with_downstream_dependencies(self)
            self.dag_to_airflow(output_dir, dag_to_generate)
            return

        # Generate all DAGs:
        try:
            set_start_method("spawn")
        except Exception:
            pass

        for dag in self.dags:
            dag.with_upstream_dependencies(self)
            dag.with_downstream_dependencies(self)

        to_airflow_dag = partial(self.dag_to_airflow, output_dir)
        with get_context("spawn").Pool(8) as p:
            p.map(to_airflow_dag, self.dags)
