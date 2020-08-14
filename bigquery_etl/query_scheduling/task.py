"""Represents a scheduled Airflow task."""

import attr
import cattr
from fnmatch import fnmatchcase
import glob
import os
import re
import logging
from typing import List, Optional, Tuple


from bigquery_etl.dryrun import DryRun
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.utils import (
    is_date_string,
    is_email,
    is_valid_dag_name,
    is_timedelta_string,
    schedule_interval_delta,
    is_schedule_interval,
)


AIRFLOW_TASK_TEMPLATE = "airflow_task.j2"
QUERY_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)_(v[0-9]+)/(?:query\.sql|part1\.sql)$"
)
DEFAULT_PROJECT = "moz-fx-data-shared-prod"


class TaskParseException(Exception):
    """Raised when task scheduling config is invalid."""

    def __init__(self, message):
        """Throw TaskParseException."""
        message = f"""
        {message}

        Expected yaml format in metadata.yaml:
        scheduling:
            dag_name: string [required]
            depends_on_past: bool [optional]
            ... <more config parameters> ...
        """

        super(TaskParseException, self).__init__(message)


class UnscheduledTask(Exception):
    """Raised when a task is not scheduled."""

    pass


@attr.s(auto_attribs=True, frozen=True)
class TaskRef:
    """
    Representation of a reference to another task.

    The task can be defined in bigquery-etl or in telemetry-airflow.
    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    dag_name: str = attr.ib()
    task_id: str = attr.ib()
    execution_delta: Optional[str] = attr.ib(None)
    schedule_interval: Optional[str] = attr.ib(None)

    @execution_delta.validator
    def validate_execution_delta(self, attribute, value):
        """Check that execution_delta is in a valid timedelta format."""
        if value is not None and not is_timedelta_string(value):
            raise ValueError(
                f"Invalid timedelta definition for {attribute}: {value}."
                "Timedeltas should be specified like: 1h, 30m, 1h15m, 1d4h45m, ..."
            )

    @schedule_interval.validator
    def validate_schedule_interval(self, attribute, value):
        """
        Validate the schedule_interval format.

        Schedule intervals can be either in CRON format or one of:
        @once, @hourly, @daily, @weekly, @monthly, @yearly
        or a timedelta []d[]h[]m
        """
        if value is not None and not is_schedule_interval(value):
            raise ValueError(f"Invalid schedule_interval {value}.")


# Know tasks in telemetry-airflow, like stable table tasks
# https://github.com/mozilla/telemetry-airflow/blob/master/dags/copy_deduplicate.py
EXTERNAL_TASKS = {
    TaskRef(
        dag_name="copy_deduplicate",
        task_id="copy_deduplicate_main_ping",
        schedule_interval="0 1 * * *",
    ): ["telemetry_stable.main_v4"],
    TaskRef(
        dag_name="copy_deduplicate",
        task_id="bq_main_events",
        schedule_interval="0 1 * * *",
    ): ["telemetry_derived.main_events_v1"],
    TaskRef(
        dag_name="copy_deduplicate",
        task_id="event_events",
        schedule_interval="0 1 * * *",
    ): ["telemetry_derived.event_events_v1"],
    TaskRef(
        dag_name="copy_deduplicate",
        task_id="baseline_clients_last_seen",
        schedule_interval="0 1 * * *",
    ): ["*.baseline_clients_last_seen*"],
    TaskRef(
        dag_name="copy_deduplicate",
        task_id="copy_deduplicate_all",
        schedule_interval="0 1 * * *",
    ): ["*_stable.*"],
}


@attr.s(auto_attribs=True)
class Task:
    """
    Representation of a task scheduled in Airflow.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    dag_name: str = attr.ib()
    query_file: str
    owner: str = attr.ib()
    email: List[str] = attr.ib([])
    task_name: Optional[str] = attr.ib(None)
    dataset: str = attr.ib(init=False)
    table: str = attr.ib(init=False)
    version: str = attr.ib(init=False)
    depends_on_past: bool = attr.ib(False)
    start_date: Optional[str] = attr.ib(None)
    date_partition_parameter: Optional[str] = "submission_date"
    # indicate whether data should be published as JSON
    public_json: bool = attr.ib(False)
    depends_on: List[TaskRef] = attr.ib([])
    arguments: List[str] = attr.ib([])
    parameters: List[str] = attr.ib([])
    multipart: bool = attr.ib(False)
    sql_file_path: Optional[str] = None
    priority: Optional[int] = None
    referenced_tables: Optional[List[Tuple[str, str]]] = attr.ib(None)
    allow_field_addition_on_date: Optional[str] = attr.ib(None)

    @owner.validator
    def validate_owner(self, attribute, value):
        """Check that owner is a valid email address."""
        if not is_email(value):
            raise ValueError(f"Invalid email for task owner: {value}.")

    @email.validator
    def validate_email(self, attribute, value):
        """Check that provided email addresses are valid."""
        if not all(map(lambda e: is_email(e), value)):
            raise ValueError(f"Invalid email in DAG email: {value}.")

    @start_date.validator
    def validate_start_date(self, attribute, value):
        """Check that start_date has YYYY-MM-DD format."""
        if value is not None and not is_date_string(value):
            raise ValueError(
                f"Invalid date definition for {attribute}: {value}."
                "Dates should be specified as YYYY-MM-DD."
            )

    @dag_name.validator
    def validate_dag_name(self, attribute, value):
        """Validate the DAG name."""
        if not is_valid_dag_name(value):
            raise ValueError(
                f"Invalid DAG name {value} for task. Name must start with 'bqetl_'."
            )

    @task_name.validator
    def validate_task_name(self, attribute, value):
        """Validate the task name."""
        if value is not None:
            if len(value) < 1 or len(value) > 62:
                raise ValueError(
                    f"Invalid task name {value}. "
                    + "The task name has to be 1 to 62 characters long."
                )

    def __attrs_post_init__(self):
        """Extract information from the query file name."""
        query_file_re = re.search(QUERY_FILE_RE, self.query_file)
        if query_file_re:
            self.dataset = query_file_re.group(1)
            self.table = query_file_re.group(2)
            self.version = query_file_re.group(3)

            if self.task_name is None:
                self.task_name = f"{self.dataset}__{self.table}__{self.version}"
                self.validate_task_name(None, self.task_name)
        else:
            raise ValueError(
                "query_file must be a path with format:"
                " ../<dataset>/<table>_<version>/(query.sql|part1.sql)"
                f" but is {self.query_file}"
            )

    @classmethod
    def of_query(cls, query_file, metadata=None, dag_collection=None):
        """
        Create task that schedules the corresponding query in Airflow.

        Raises FileNotFoundError if not metadata file exists for query.
        If `metadata` is set, then it is used instead of the metadata.yaml
        file that might exist alongside the query file.
        """
        converter = cattr.Converter()
        if metadata is None:
            metadata = Metadata.of_sql_file(query_file)

        dag_name = metadata.scheduling.get("dag_name")
        if dag_name is None:
            raise UnscheduledTask(
                f"Metadata for {query_file} does not contain scheduling information."
            )

        task_config = {"query_file": str(query_file)}
        task_config.update(metadata.scheduling)

        if len(metadata.owners) <= 0:
            raise TaskParseException(
                f"No owner specified in metadata for {query_file}."
            )

        # Airflow only allows to set one owner, so we just take the first
        task_config["owner"] = metadata.owners[0]

        # Get default email from default_args if available
        default_email = []
        if dag_collection is not None:
            dag = dag_collection.dag_by_name(dag_name)
            if dag is not None:
                default_email = dag.default_args.email
        email = task_config.get("email", default_email)
        # owners get added to the email list
        task_config["email"] = list(set(email + metadata.owners))

        # data processed in task should be published
        if metadata.is_public_json():
            task_config["public_json"] = True

        try:
            return converter.structure(task_config, cls)
        except TypeError as e:
            raise TaskParseException(
                f"Invalid scheduling information format for {query_file}: {e}"
            )

    @classmethod
    def of_multipart_query(cls, query_file, metadata=None, dag_collection=None):
        """
        Create task that schedules the corresponding multipart query in Airflow.

        Raises FileNotFoundError if not metadata file exists for query.
        If `metadata` is set, then it is used instead of the metadata.yaml
        file that might exist alongside the query file.
        """
        task = cls.of_query(query_file, metadata, dag_collection)
        task.multipart = True
        task.sql_file_path = os.path.dirname(query_file)
        return task

    def _get_referenced_tables(self):
        """
        Perform a dry_run to get tables the query depends on.

        Queries that reference more than 50 tables will not have a complete list
        of dependencies. See https://cloud.google.com/bigquery/docs/reference/
        rest/v2/Job#JobStatistics2.FIELDS.referenced_tables
        """
        logging.info(f"Get dependencies for {self.task_name}")

        if self.referenced_tables is None:
            table_names = set()
            query_files = [self.query_file]

            if self.multipart:
                # dry_run all files if query is split into multiple parts
                query_files = glob.glob(self.sql_file_path + "/*.sql")

            for query_file in query_files:
                referenced_tables = DryRun(query_file).get_referenced_tables()

                if len(referenced_tables) >= 50:
                    logging.warn(
                        "Query has 50 or more tables. Queries that reference more "
                        "than 50 tables will not have a complete list of "
                        "dependencies."
                    )

                for t in referenced_tables:
                    table_names.add((t["datasetId"], t["tableId"]))

            # the order of table dependencies changes between requests
            # sort to maintain same order between DAG generation runs
            self.referenced_tables = sorted(table_names)
        return self.referenced_tables

    def with_dependencies(self, dag_collection):
        """Perfom a dry_run to get upstream dependencies."""
        dependencies = []

        for table in self._get_referenced_tables():
            upstream_task = dag_collection.task_for_table(table[0], table[1])
            task_schedule_interval = dag_collection.dag_by_name(
                self.dag_name
            ).schedule_interval

            if upstream_task is not None:
                # ensure there are no duplicate dependencies
                # manual dependency definitions overwrite automatically detected ones
                if not any(
                    d.dag_name == upstream_task.dag_name
                    and d.task_id == upstream_task.task_name
                    for d in self.depends_on
                ):
                    upstream_schedule_interval = dag_collection.dag_by_name(
                        upstream_task.dag_name
                    ).schedule_interval

                    execution_delta = schedule_interval_delta(
                        upstream_schedule_interval, task_schedule_interval
                    )

                    if execution_delta == "0s":
                        execution_delta = None

                    dependencies.append(
                        TaskRef(
                            dag_name=upstream_task.dag_name,
                            task_id=upstream_task.task_name,
                            execution_delta=execution_delta,
                        )
                    )
            else:
                # see if there are some static dependencies
                for task, patterns in EXTERNAL_TASKS.items():
                    if any(fnmatchcase(f"{table[0]}.{table[1]}", p) for p in patterns):
                        # ensure there are no duplicate dependencies
                        # manual dependency definitions overwrite automatically detected
                        if not any(
                            d.dag_name == task.dag_name and d.task_id == task.task_id
                            for d in self.depends_on + dependencies
                        ):
                            execution_delta = schedule_interval_delta(
                                task.schedule_interval, task_schedule_interval
                            )

                            if execution_delta:
                                dependencies.append(
                                    TaskRef(
                                        dag_name=task.dag_name,
                                        task_id=task.task_id,
                                        execution_delta=execution_delta,
                                    )
                                )
                        break  # stop after the first match

        self.dependencies = dependencies
