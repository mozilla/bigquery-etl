"""Represents an Airflow DAG."""

import attr
import cattr
from jinja2 import Environment, PackageLoader
import re
from typing import List, Optional

from bigquery_etl.query_scheduling.task import Task
from bigquery_etl.query_scheduling import formatters
from bigquery_etl.query_scheduling.utils import (
    is_timedelta_string,
    is_date_string,
    is_email,
)


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
    """Representation of Airflow DAG default_args."""

    owner: str = attr.ib()
    email: List[str] = attr.ib([])
    depends_on_past: bool = attr.ib(False)
    start_date: Optional[str] = attr.ib(None)
    retry_delay: str = attr.ib("30m")
    email_on_failure: bool = attr.ib(True)
    email_on_retry: bool = attr.ib(True)
    retries: int = attr.ib(2)

    @owner.validator
    def validate_owner(self, attribute, value):
        if not is_email(value):
            raise ValueError(f"Invalid email for DAG owner: {value}.")

    @email.validator
    def validate_email(self, attribute, value):
        if not all(map(lambda e: is_email(e), value)):
            raise ValueError(f"Invalid email in DAG email: {value}.")

    @retry_delay.validator
    def validate_retry_delay(self, attribute, value):
        if not is_timedelta_string(value):
            raise ValueError(
                f"Invalid timedelta definition for {attribute}: {value}."
                "Timedeltas should be specified like: 1h, 30m, 1h15m, 1d4h45m, ..."
            )

    @start_date.validator
    def validate_start_date(self, attribute, value):
        if not is_date_string(value):
            raise ValueError(
                f"Invalid date definition for {attribute}: {value}."
                "Dates should be specified as YYYY-MM-DD."
            )

    def to_dict(self):
        """Return class as a dict."""
        return self.__dict__


@attr.s(auto_attribs=True)
class Dag:
    """Representation of a DAG configuration."""

    name: str = attr.ib()
    schedule_interval: str = attr.ib()
    default_args: DagDefaultArgs
    tasks: List[Task] = attr.ib([])

    @name.validator
    def validate_dag_name(self, attribute, value):
        """Validate the DAG name."""
        dag_name_pattern = re.compile("^bqetl_.+$")
        if not dag_name_pattern.match(value):
            raise ValueError(
                f"Invalid DAG name {value}. Name must start with 'bqetl_'."
            )

    @schedule_interval.validator
    def validate_schedule_interval(self, attribute, value):
        """
        Validate the schedule_interval format.
        Schedule intervals can be either in CRON format or one of:
        @once, @hourly, @daily, @weekly, @monthly, @yearly
        or a timedelta []d[]h[]m
        """
        # https://stackoverflow.com/questions/14203122/create-a-regular-expression-for-cron-statement
        pattern = re.compile(
            r"^(once|hourly|daily|weekly|monthly|yearly|"
            r"((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*) ?){5,7})|"
            r"((\d+h)?(\d+m)?(\d+s)?))$"
        )
        if not pattern.match(value):
            raise ValueError(f"Invalid schedule_interval {value}.")

    def add_tasks(self, tasks):
        """Add tasks to be scheduled as part of the DAG."""
        self.tasks = self.tasks.copy() + tasks

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

        # load custom formatters into Jinja env
        for name in dir(formatters):
            func = getattr(formatters, name)
            if not callable(func):
                continue

            env.filters[name] = func

        dag_template = env.get_template(AIRFLOW_DAG_TEMPLATE)

        args = self.__dict__

        for task in args["tasks"]:
            task.with_dependencies(client, dag_collection)

        return dag_template.render(args)
