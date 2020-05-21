"""Represents a scheduled Airflow task."""

import attr
import cattr
import re
import logging
from google.cloud import bigquery
from typing import List, Optional


from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.query_scheduling.utils import is_date_string, is_email


AIRFLOW_TASK_TEMPLATE = "airflow_task.j2"
QUERY_FILE_RE = re.compile(r"^.*/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)_(v[0-9]+)/query\.sql$")


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
class Task:
    """Representation of a task scheduled in Airflow."""

    dag_name: str
    query_file: str
    owner: str = attr.ib()
    email: List[str] = attr.ib([])
    dataset: str = attr.ib(init=False)
    table: str = attr.ib(init=False)
    version: str = attr.ib(init=False)
    task_name: str = attr.ib(init=False)
    depends_on_past: bool = attr.ib(False)
    start_date: Optional[str] = attr.ib(None)

    @owner.validator
    def validate_owner(self, attribute, value):
        if not is_email(value):
            raise ValueError(f"Invalid email for task owner: {value}.")

    @email.validator
    def validate_email(self, attribute, value):
        if not all(map(lambda e: is_email(e), value)):
            raise ValueError(f"Invalid email in DAG email: {value}.")

    @start_date.validator
    def validate_start_date(self, attribute, value):
        if value is not None and not is_date_string(value):
            raise ValueError(
                f"Invalid date definition for {attribute}: {value}."
                "Dates should be specified as YYYY-MM-DD."
            )

    def __attrs_post_init__(self):
        """Extract information from the query file name."""
        query_file_re = re.search(QUERY_FILE_RE, self.query_file)
        if query_file_re:
            self.dataset = query_file_re.group(1)
            self.table = query_file_re.group(2)
            self.version = query_file_re.group(3)
            self.task_name = f"{self.dataset}__{self.table}__{self.version}"
        else:
            raise ValueError(
                "query_file must be a path with format:"
                "../<dataset>/<table>_<version>/query.sql"
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

        if metadata.scheduling == {}:
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

        try:
            return converter.structure(task_config, cls)
        except TypeError as e:
            raise TaskParseException(
                f"Invalid scheduling information format for {query_file}: {e}"
            )

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
            query_parameters=[
                bigquery.ScalarQueryParameter("submission_date", "DATE", "2019-01-01")
            ],
        )

        with open(self.query_file) as query_stream:
            query = query_stream.read()
            query_job = client.query(query, job_config=job_config)
            referenced_tables = query_job.referenced_tables

            if len(referenced_tables) >= 50:
                logging.warn(
                    "Query has 50 or more tables. Queries that reference more than"
                    "50 tables will not have a complete list of dependencies."
                )

            table_names = [(t.dataset_id, t.table_id) for t in referenced_tables]
            return table_names

    def with_dependencies(self, client, dag_collection):
        """Perfom a dry_run to get upstream dependencies."""
        dependencies = []

        for table in self._get_referenced_tables(client):
            upstream_task = dag_collection.task_for_table(table[0], table[1])

            if upstream_task is not None:
                dependencies.append(upstream_task)

        self.dependencies = dependencies
