"""Represents an Airflow DAG."""

import attr
import cattr
from jinja2 import Environment, PackageLoader
from typing import List

from bigquery_etl.query_scheduling.task import Task


AIRFLOW_DAG_TEMPLATE = "airflow_dag.j2"


class DagParseException(Exception):
    """Raised when DAG config is invalid."""

    def __init__(self, message):
        """Throw DagParseException."""
        message = f"""
        {message}

        Expected yaml format:
        name:
            schedule_interval: string,
            default_args: map
        """

        super(DagParseException, self).__init__(message)


class InvalidDag(Exception):
    """Raised when the resulting DAG is invalid."""

    pass


@attr.s(auto_attribs=True)
class DagDefaultArgs:
    owner: str
    email: List[str]
    depends_on_past: bool = False
    # todo: more attributes and validation


@attr.s(auto_attribs=True)
class Dag:
    """Representation of a DAG configuration."""

    name: str
    schedule_interval: str
    default_args: DagDefaultArgs
    tasks: List[Task] = []

    # todo validate dag naming

    def add_tasks(self, tasks):
        """Add tasks to be scheduled as part of the DAG."""
        self.tasks += tasks

    @classmethod
    def from_dict(cls, d):
        """
        Parse the DAG configuration from a dict and create a new Dag instance.

        Expected dict format:
        {
            "name": {
                "schedule_interval": string,
                "default_args": dict
            }
        }
        """
        if len(d.keys()) != 1:
            raise DagParseException(f"Invalid DAG configuration format in {d}")

        converter = cattr.Converter()
        try:
            name = list(d.keys())[0]
            return converter.structure({"name": name, **d[name]}, cls)
        except TypeError as e:
            raise DagParseException(f"Invalid DAG configuration format in {d}: {e}")

    def to_airflow_dag(self, client, dag_collection):
        """Convert the DAG to its Airflow representation and return the python code."""
        env = Environment(
            loader=PackageLoader("bigquery_etl", "query_scheduling/templates")
        )
        dag_template = env.get_template(AIRFLOW_DAG_TEMPLATE)

        args = self.__dict__

        for task in args["tasks"]:
            task.with_dependencies(client, dag_collection)

        return dag_template.render(args)
