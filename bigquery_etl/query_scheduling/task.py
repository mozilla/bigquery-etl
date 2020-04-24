"""Represents a scheduled Airflow task."""

import re
from google.cloud import bigquery

from bigquery_etl.metadata.parse_metadata import Metadata


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


class Task:
    """Representation of a task scheduled in Airflow."""

    def __init__(self, query_file, metadata):
        """Instantiate a new task."""
        self.query_file = str(query_file)

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

        scheduling = metadata.scheduling

        if scheduling == {}:
            raise UnscheduledTask()

        if "dag_name" not in scheduling:
            raise TaskParseException(
                f"dag_name not defined in task config for {self.query_file}"
            )

        self.dag_name = scheduling["dag_name"]
        self.args = scheduling.copy()
        del self.args["dag_name"]

    @classmethod
    def of_query(cls, query_file):
        """
        Create task that schedules the corresponding query in Airflow.

        Raises FileNotFoundError if not metadata file exists for query.
        """
        metadata = Metadata.of_sql_file(query_file)
        return cls(query_file, metadata)

    def _get_referenced_tables(self, client):
        """Perform a dry_run to get tables the query depends on.
        
        Queries that reference more than 50 tables will not have a complete list
        of dependencies. See https://cloud.google.com/bigquery/docs/reference/
        rest/v2/Job#JobStatistics2.FIELDS.referenced_tables
        """

        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

        with open(self.query_file) as query_stream:
            query = query_stream.read()
            query_job = client.query(query, job_config=job_config)
            return query_job.referenced_tables

    def get_dependencies(self, client, dag_collection):
        """Perfom a dry_run to get upstream dependencies."""
        dependencies = []

        for table in self._get_referenced_tables(client):
            upstream_task = dag_collection.task_for_table()

            if upstream_task is not None:
                dependencies.append(upstream_task)

    def to_airflow(self, client, dag_collection):
        """Convert the task configuration into the Airflow representation."""
        dependencies = self.get_dependencies()

        # todo
