"""Represents a scheduled Airflow task."""

import attr
import cattr
import glob
import os
import re
import logging
from google.cloud import bigquery
from typing import List, Optional


from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.utils import (
    is_date_string,
    is_email,
    is_valid_dag_name,
    is_timedelta_string,
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


@attr.s(auto_attribs=True)
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

    @execution_delta.validator
    def validate_execution_delta(self, attribute, value):
        """Check that execution_delta is in a valid timedelta format."""
        if value is not None and not is_timedelta_string(value):
            raise ValueError(
                f"Invalid timedelta definition for {attribute}: {value}."
                "Timedeltas should be specified like: 1h, 30m, 1h15m, 1d4h45m, ..."
            )


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
    def of_query(cls, query_file, metadata=None):
        """
        Create task that schedules the corresponding query in Airflow.

        Raises FileNotFoundError if not metadata file exists for query.
        If `metadata` is set, then it is used instead of the metadata.yaml
        file that might exist alongside the query file.
        """
        converter = cattr.Converter()
        if metadata is None:
            metadata = Metadata.of_sql_file(query_file)

        if metadata.scheduling == {} or "dag_name" not in metadata.scheduling:
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

        # owners get added to the email list
        if "email" not in task_config:
            task_config["email"] = []

        task_config["email"] = list(set(task_config["email"] + metadata.owners))

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
    def of_multipart_query(cls, query_file, metadata=None):
        """
        Create task that schedules the corresponding multipart query in Airflow.

        Raises FileNotFoundError if not metadata file exists for query.
        If `metadata` is set, then it is used instead of the metadata.yaml
        file that might exist alongside the query file.
        """
        task = cls.of_query(query_file, metadata)
        task.multipart = True
        task.sql_file_path = os.path.dirname(query_file)
        return task

    def _get_referenced_tables(self, client):
        """
        Perform a dry_run to get tables the query depends on.

        Queries that reference more than 50 tables will not have a complete list
        of dependencies. See https://cloud.google.com/bigquery/docs/reference/
        rest/v2/Job#JobStatistics2.FIELDS.referenced_tables
        """
        logging.info(f"Get dependencies for {self.task_name}")

        # the submission_date parameter needs to be set to make the dry run faster
        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
            default_dataset=f"{DEFAULT_PROJECT}.{self.dataset}",
            query_parameters=[
                bigquery.ScalarQueryParameter("submission_date", "DATE", "2019-01-01")
            ],
        )

        table_names = set()
        query_files = [self.query_file]

        if self.multipart:
            # dry_run all files if query is split into multiple parts
            query_files = glob.glob(self.sql_file_path + "/*.sql")

        for query_file in query_files:
            with open(query_file) as query_stream:
                query = query_stream.read()
                query_job = client.query(query, job_config=job_config)
                referenced_tables = query_job.referenced_tables

                if len(referenced_tables) >= 50:
                    logging.warn(
                        "Query has 50 or more tables. Queries that reference more than"
                        "50 tables will not have a complete list of dependencies."
                    )

                for t in referenced_tables:
                    table_names.add((t.dataset_id, t.table_id))

        # the order of table dependencies changes between requests
        # sort to maintain same order between DAG generation runs
        sorted_table_names = list(table_names)
        sorted_table_names.sort()
        return sorted_table_names

    def with_dependencies(self, client, dag_collection):
        """Perfom a dry_run to get upstream dependencies."""
        dependencies = []

        for table in self._get_referenced_tables(client):
            upstream_task = dag_collection.task_for_table(table[0], table[1])

            if upstream_task is not None:
                # ensure there are no duplicate dependencies
                # manual dependency definitions overwrite automatically detected ones
                if not any(
                    d.dag_name == upstream_task.dag_name
                    and d.task_id == upstream_task.task_name
                    for d in self.depends_on
                ):
                    dependencies.append(upstream_task)

        self.dependencies = dependencies
