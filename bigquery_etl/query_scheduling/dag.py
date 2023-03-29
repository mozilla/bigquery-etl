"""Represents an Airflow DAG."""

from typing import List, Optional

import attr
import cattrs
from jinja2 import Environment, PackageLoader

from bigquery_etl.query_scheduling import formatters
from bigquery_etl.query_scheduling.task import Task, TaskRef
from bigquery_etl.query_scheduling.utils import (
    is_date_string,
    is_email_or_github_identity,
    is_schedule_interval,
    is_timedelta_string,
    is_valid_dag_name,
    schedule_interval_delta,
)

AIRFLOW_DAG_TEMPLATE = "airflow_dag.j2"
PUBLIC_DATA_JSON_DAG_TEMPLATE = "public_data_json_airflow_dag.j2"
PUBLIC_DATA_JSON_DAG = "bqetl_public_data_json"


class DagParseException(Exception):
    """Raised when DAG config is invalid."""

    def __init__(self, message):
        """Throw DagParseException."""
        message = f"""
        {message}

        Expected yaml format:
        name:
            schedule_interval: string,
            default_args:
                owner: string
                start_date: 'YYYY-MM-DD'
                ...
        """

        super(DagParseException, self).__init__(message)


class InvalidDag(Exception):
    """Raised when the resulting DAG is invalid."""

    pass


@attr.s(auto_attribs=True)
class DagDefaultArgs:
    """
    Representation of Airflow DAG default_args.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    owner: str = attr.ib()
    start_date: str = attr.ib()
    end_date: Optional[str] = attr.ib(None)
    email: List[str] = attr.ib([])
    depends_on_past: bool = attr.ib(False)
    retry_delay: str = attr.ib("30m")
    email_on_failure: bool = attr.ib(True)
    email_on_retry: bool = attr.ib(True)
    retries: int = attr.ib(2)

    @owner.validator
    def validate_owner(self, attribute, value):
        """Check that owner is a valid email address."""
        if not is_email_or_github_identity(value):
            raise ValueError(
                f"Invalid email or github identity for DAG owner: {value}."
            )

    @email.validator
    def validate_email(self, attribute, value):
        """Check that provided email addresses are valid."""
        if not all(map(lambda e: is_email_or_github_identity(e), value)):
            raise ValueError(f"Invalid email or github identity in DAG email: {value}.")

    @retry_delay.validator
    def validate_retry_delay(self, attribute, value):
        """Check that retry_delay is in a valid timedelta format."""
        if not is_timedelta_string(value):
            raise ValueError(
                f"Invalid timedelta definition for {attribute}: {value}."
                "Timedeltas should be specified like: 1h, 30m, 1h15m, 1d4h45m, ..."
            )

    @end_date.validator
    @start_date.validator
    def validate_date(self, attribute, value):
        """Check that start_date and end_date has YYYY-MM-DD format."""
        if value is not None and not is_date_string(value):
            raise ValueError(
                f"Invalid date definition for {attribute}: {value}."
                "Dates should be specified as YYYY-MM-DD."
            )

    def to_dict(self):
        """Return class as a dict."""
        return self.__dict__


@attr.s(auto_attribs=True)
class Dag:
    """
    Representation of a DAG configuration.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    name: str = attr.ib()
    schedule_interval: str = attr.ib()
    default_args: DagDefaultArgs
    tasks: List[Task] = attr.ib([])
    description: str = attr.ib("")
    repo: str = attr.ib("bigquery-etl")
    tags: List[str] = attr.ib([])

    @name.validator
    def validate_dag_name(self, attribute, value):
        """Validate the DAG name."""
        if not is_valid_dag_name(value):
            raise ValueError(
                f"Invalid DAG name {value}. Name must start with 'bqetl_' "
                f"or 'private_bqetl_'."
            )

    @tasks.validator
    def validate_tasks(self, attribute, value):
        """Validate tasks."""
        task_names = list(map(lambda t: t.task_name, value))
        duplicate_task_names = set(
            [task_name for task_name in task_names if task_names.count(task_name) > 1]
        )

        if len(duplicate_task_names) > 0:
            raise ValueError(
                f"Duplicate task names encountered: {duplicate_task_names}."
            )

    @schedule_interval.validator
    def validate_schedule_interval(self, attribute, value):
        """Validate the schedule_interval format."""
        if not is_schedule_interval(value):
            raise ValueError(f"Invalid schedule_interval {value}.")

    def add_tasks(self, tasks):
        """Add tasks to be scheduled as part of the DAG."""
        self.tasks = self.tasks.copy() + tasks
        self.validate_tasks(None, self.tasks)

    def with_upstream_dependencies(self, dag_collection):
        """Perform a dry_run to get upstream dependencies."""
        for task in self.tasks:
            task.with_upstream_dependencies(dag_collection)

    def with_downstream_dependencies(self, dag_collection):
        """Get downstream tasks by looking up upstream dependencies in DAG collection."""
        for task in self.tasks:
            task.with_downstream_dependencies(dag_collection)

    def to_dict(self):
        """Return class as a dict."""
        d = self.__dict__
        name = d["name"]
        del d["name"]
        del d["tasks"]
        d["default_args"] = self.default_args.to_dict()

        return {name: d}

    @classmethod
    def from_dict(cls: type, d: dict):
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

        converter = cattrs.BaseConverter()
        try:
            name = list(d.keys())[0]
            tags: set[str] = set(d[name].get("tags", []))
            if not any(tag.startswith("repo/") for tag in tags):
                tags.add("repo/" + d[name].get("repo", "bigquery-etl"))
            d[name]["tags"] = sorted(tags)

            if name == PUBLIC_DATA_JSON_DAG:
                return converter.structure({"name": name, **d[name]}, PublicDataJsonDag)
            else:
                return converter.structure({"name": name, **d[name]}, cls)
        except (TypeError, AttributeError) as e:
            raise DagParseException(f"Invalid DAG configuration format in {d}: {e}")

    def _jinja_env(self):
        """Prepare and load custom formatters into the jinja environment."""
        env = Environment(
            loader=PackageLoader("bigquery_etl", "query_scheduling/templates"),
            extensions=["jinja2.ext.do"],
        )

        # load custom formatters into Jinja env
        for name in dir(formatters):
            func = getattr(formatters, name)
            if not callable(func):
                continue

            env.filters[name] = func

        return env

    def to_airflow_dag(self):
        """Convert the DAG to its Airflow representation and return the python code."""
        if len(self.tasks) == 0:
            raise InvalidDag(
                f"DAG {self.name} has no tasks - cannot convert it to a valid .py DAG "
                f"file. Does it appear under `scheduling` in any metadata.yaml files?"
            )

        env = self._jinja_env()
        dag_template = env.get_template(AIRFLOW_DAG_TEMPLATE)
        args = self.__dict__

        return dag_template.render(args)


class PublicDataJsonDag(Dag):
    """Special DAG with tasks exporting public json data to GCS."""

    def to_airflow_dag(self):
        """Convert the DAG to its Airflow representation and return the python code."""
        env = self._jinja_env()
        dag_template = env.get_template(PUBLIC_DATA_JSON_DAG_TEMPLATE)
        args = self.__dict__

        return dag_template.render(args)

    def _create_export_task(self, task, dag_collection):
        if not task.public_json:
            raise ValueError(f"Task {task.task_name} not marked as public JSON.")

        converter = cattrs.BaseConverter()
        task_dict = converter.unstructure(task)

        del task_dict["dataset"]
        del task_dict["table"]
        del task_dict["version"]
        del task_dict["project"]

        export_task = converter.structure(task_dict, Task)
        export_task.dag_name = self.name
        export_task.task_name = f"export_public_data_json_{export_task.task_name}"

        task_schedule_interval = dag_collection.dag_by_name(
            task.dag_name
        ).schedule_interval

        execution_delta = schedule_interval_delta(
            task_schedule_interval, self.schedule_interval
        )

        if execution_delta == "0s":
            execution_delta = None

        export_task.dependencies = [
            TaskRef(
                dag_name=task.dag_name,
                task_id=task.task_name,
                execution_delta=execution_delta,
            )
        ]

        return export_task

    def add_export_tasks(self, tasks, dag_collection):
        """Add new tasks for exporting data of the original queries to GCS."""
        self.add_tasks(
            [self._create_export_task(task, dag_collection) for task in tasks]
        )
