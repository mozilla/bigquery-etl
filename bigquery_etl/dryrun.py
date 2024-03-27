"""
Dry run query files.

Passes all queries to a Cloud Function that will run the
queries with the dry_run option enabled.

We could provision BigQuery credentials to the CircleCI job to allow it to run
the queries directly, but there is no way to restrict permissions such that
only dry runs can be performed. In order to reduce risk of CI or local users
accidentally running queries during tests and overwriting production data, we
proxy the queries through the dry run service endpoint.
"""

import glob
import json
import re
import sys
from enum import Enum
from os.path import basename, dirname, exists
from pathlib import Path
from typing import Optional, Set
from urllib.request import Request, urlopen

import click
import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import bigquery
from google.oauth2.id_token import fetch_id_token

from .config import ConfigLoader
from .metadata.parse_metadata import Metadata
from .util.common import render

try:
    from functools import cached_property  # type: ignore
except ImportError:
    # python 3.7 compatibility
    from backports.cached_property import cached_property  # type: ignore


class Errors(Enum):
    """DryRun errors that require special handling."""

    READ_ONLY = 1
    DATE_FILTER_NEEDED = 2
    DATE_FILTER_NEEDED_AND_SYNTAX = 3


class DryRun:
    """Dry run SQL files."""

    def __init__(
        self,
        sqlfile,
        content=None,
        strip_dml=False,
        use_cloud_function=True,
        client=None,
        respect_skip=True,
        sql_dir=ConfigLoader.get("default", "sql_dir"),
    ):
        """Instantiate DryRun class."""
        self.sqlfile = sqlfile
        self.content = content
        self.strip_dml = strip_dml
        self.use_cloud_function = use_cloud_function
        self.client = client if use_cloud_function or client else bigquery.Client()
        self.respect_skip = respect_skip
        self.dry_run_url = ConfigLoader.get("dry_run", "function")
        self.sql_dir = sql_dir
        try:
            self.metadata = Metadata.of_query_file(self.sqlfile)
        except FileNotFoundError:
            self.metadata = None

        from bigquery_etl.cli.utils import is_authenticated

        if not is_authenticated():
            print(
                "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
                "and check that the project is set correctly."
            )
            sys.exit(1)

    @staticmethod
    def skipped_files(sql_dir=ConfigLoader.get("default", "sql_dir")) -> Set[str]:
        """Return files skipped by dry run."""
        default_sql_dir = Path(ConfigLoader.get("default", "sql_dir"))
        sql_dir = Path(sql_dir)
        file_pattern_re = re.compile(rf"^{re.escape(str(default_sql_dir))}/")

        skip_files = {
            file
            for skip in ConfigLoader.get("dry_run", "skip", fallback=[])
            for file in glob.glob(
                file_pattern_re.sub(f"{str(sql_dir)}/", skip),
                recursive=True,
            )
        }

        # update skip list to include renamed queries in stage.
        test_project = ConfigLoader.get("default", "test_project", fallback="")
        file_pattern_re = re.compile(r"sql/([^\/]+)/([^/]+)(/?.*|$)")

        skip_files.update(
            [
                file
                for skip in ConfigLoader.get("dry_run", "skip", fallback=[])
                for file in glob.glob(
                    file_pattern_re.sub(
                        lambda x: f"sql/{test_project}/{x.group(2)}_{x.group(1).replace('-', '_')}*{x.group(3)}",
                        skip,
                    ),
                    recursive=True,
                )
            ]
        )

        return skip_files

    def skip(self):
        """Determine if dry run should be skipped."""
        return self.respect_skip and self.sqlfile in self.skipped_files(
            sql_dir=self.sql_dir
        )

    def get_sql(self):
        """Get SQL content."""
        if exists(self.sqlfile):
            file_path = Path(self.sqlfile)
            sql = render(
                file_path.name,
                format=False,
                template_folder=file_path.parent.absolute(),
            )
        else:
            raise ValueError(f"Invalid file path: {self.sqlfile}")
        if self.strip_dml:
            sql = re.sub(
                "CREATE OR REPLACE VIEW.*?AS",
                "",
                sql,
                flags=re.DOTALL,
            )
            sql = re.sub(
                "CREATE MATERIALIZED VIEW.*?AS",
                "",
                sql,
                flags=re.DOTALL,
            )

        return sql

    @cached_property
    def dry_run_result(self):
        """Dry run the provided SQL file."""
        if self.content:
            sql = self.content
        else:
            sql = self.get_sql()
        if self.metadata:
            # use metadata to rewrite date-type query params as submission_date
            date_params = [
                query_param
                for query_param in (
                    self.metadata.scheduling.get("date_partition_parameter"),
                    *(
                        param.split(":", 1)[0]
                        for param in self.metadata.scheduling.get("parameters", [])
                        if re.fullmatch(r"[^:]+:DATE:{{.*ds.*}}", param)
                    ),
                )
                if query_param and query_param != "submission_date"
            ]
            if date_params:
                pattern = re.compile(
                    "@("
                    + "|".join(date_params)
                    # match whole query parameter names
                    + ")(?![a-zA-Z0-9_])"
                )
                sql = pattern.sub("@submission_date", sql)
        project = basename(dirname(dirname(dirname(self.sqlfile))))
        dataset = basename(dirname(dirname(self.sqlfile)))
        try:
            if self.use_cloud_function:
                auth_req = GoogleAuthRequest()
                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                creds.refresh(auth_req)
                if hasattr(creds, "id_token"):
                    # Get token from default credentials for the current environment created via Cloud SDK run
                    id_token = creds.id_token
                else:
                    # If the environment variable GOOGLE_APPLICATION_CREDENTIALS is set to service account JSON file,
                    # then ID token is acquired using this service account credentials.
                    id_token = fetch_id_token(auth_req, self.dry_run_url)

                r = urlopen(
                    Request(
                        self.dry_run_url,
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {id_token}",
                        },
                        data=json.dumps(
                            {
                                "project": project,
                                "dataset": dataset,
                                "query": sql,
                            }
                        ).encode("utf8"),
                        method="POST",
                    )
                )
                return json.load(r)
            else:
                self.client.project = project
                job_config = bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                    default_dataset=f"{project}.{dataset}",
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "submission_date", "DATE", "2019-01-01"
                        )
                    ],
                )
                job = self.client.query(sql, job_config=job_config)
                try:
                    dataset_labels = self.client.get_dataset(job.default_dataset).labels
                except Exception as e:
                    # Most users do not have bigquery.datasets.get permission in
                    # moz-fx-data-shared-prod
                    # This should not prevent the dry run from running since the dataset
                    # labels are usually not required
                    if "Permission bigquery.datasets.get denied on dataset" in str(e):
                        dataset_labels = []
                    else:
                        raise e

                return {
                    "valid": True,
                    "referencedTables": [
                        ref.to_api_repr() for ref in job.referenced_tables
                    ],
                    "schema": (
                        job._properties.get("statistics", {})
                        .get("query", {})
                        .get("schema", {})
                    ),
                    "datasetLabels": dataset_labels,
                }
        except Exception as e:
            print(f"{self.sqlfile!s:59} ERROR\n", e)
            return None

    def get_referenced_tables(self):
        """Return referenced tables by dry running the SQL file."""
        if not self.skip() and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.skip():
            print(f"\t...Ignoring dryrun results for {self.sqlfile}")

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "referencedTables" in self.dry_run_result
        ):
            return self.dry_run_result["referencedTables"]

        # Handle views that require a date filter
        if (
            self.dry_run_result
            and self.strip_dml
            and self.get_error() == Errors.DATE_FILTER_NEEDED
        ):
            # Since different queries require different partition filters
            # (submission_date, crash_date, timestamp, submission_timestamp, ...)
            # We can extract the filter name from the error message
            # (by capturing the next word after "column(s)")

            # Example error:
            # "Cannot query over table <table_name> without a filter over column(s)
            # <date_filter_name> that can be used for partition elimination."

            error = self.dry_run_result["errors"][0].get("message", "")
            date_filter = find_next_word("column(s)", error)

            if "date" in date_filter:
                filtered_content = (
                    f"{self.get_sql()}WHERE {date_filter} > current_date()"
                )
                if (
                    DryRun(self.sqlfile, filtered_content).get_error()
                    == Errors.DATE_FILTER_NEEDED_AND_SYNTAX
                ):
                    # If the date filter (e.g. WHERE crash_date > current_date())
                    # is added to a query that already has a WHERE clause,
                    # it will throw an error. To fix this, we need to
                    # append 'AND' instead of 'WHERE'
                    filtered_content = (
                        f"{self.get_sql()}AND {date_filter} > current_date()"
                    )

            if "timestamp" in date_filter:
                filtered_content = (
                    f"{self.get_sql()}WHERE {date_filter} > current_timestamp()"
                )
                if (
                    DryRun(sqlfile=self.sqlfile, content=filtered_content).get_error()
                    == Errors.DATE_FILTER_NEEDED_AND_SYNTAX
                ):
                    filtered_content = (
                        f"{self.get_sql()}AND {date_filter} > current_timestamp()"
                    )

            stripped_dml_result = DryRun(sqlfile=self.sqlfile, content=filtered_content)
            if (
                stripped_dml_result.get_error() is None
                and "referencedTables" in stripped_dml_result.dry_run_result
            ):
                return stripped_dml_result.dry_run_result["referencedTables"]

        return []

    def get_schema(self):
        """Return the query schema by dry running the SQL file."""
        if not self.skip() and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.skip():
            print(f"\t...Ignoring dryrun results for {self.sqlfile}")
            return {}

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "schema" in self.dry_run_result
        ):
            return self.dry_run_result["schema"]

        return {}

    def get_dataset_labels(self):
        """Return the labels on the default dataset by dry running the SQL file."""
        if not self.skip() and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.skip():
            print(f"\t...Ignoring dryrun results for {self.sqlfile}")
            return {}

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "datasetLabels" in self.dry_run_result
        ):
            return self.dry_run_result["datasetLabels"]

        return {}

    def is_valid(self):
        """Dry run the provided SQL file and check if valid."""
        if self.dry_run_result is None:
            return False

        if self.dry_run_result["valid"]:
            print(f"{self.sqlfile!s:59} OK")
        elif self.get_error() == Errors.READ_ONLY:
            # We want the dryrun service to only have read permissions, so
            # we expect CREATE VIEW and CREATE TABLE to throw specific
            # exceptions.
            print(f"{self.sqlfile!s:59} OK")
        elif self.get_error() == Errors.DATE_FILTER_NEEDED and self.strip_dml:
            # With strip_dml flag, some queries require a partition filter
            # (submission_date, submission_timestamp, etc.) to run
            # We mark these requests as valid and add a date filter
            # in get_referenced_table()
            print(f"{self.sqlfile!s:59} OK but DATE FILTER NEEDED")
        else:
            print(f"{self.sqlfile!s:59} ERROR\n", self.dry_run_result["errors"])
            return False

        return True

    def errors(self):
        """Dry run the provided SQL file and return errors."""
        if self.dry_run_result is None:
            return []
        return self.dry_run_result.get("errors", [])

    def get_error(self) -> Optional[Errors]:
        """Get specific errors for edge case handling."""
        errors = self.errors()
        if len(errors) != 1:
            return None

        error = errors[0]
        if error and error.get("code") in [400, 403]:
            error_message = error.get("message", "")
            if (
                "does not have bigquery.tables.create permission for dataset"
                in error_message
                or "Permission bigquery.tables.create denied" in error_message
                or "Permission bigquery.datasets.update denied" in error_message
            ):
                return Errors.READ_ONLY
            if "without a filter over column(s)" in error_message:
                return Errors.DATE_FILTER_NEEDED
            if (
                "Syntax error: Expected end of input but got keyword WHERE"
                in error_message
            ):
                return Errors.DATE_FILTER_NEEDED_AND_SYNTAX
        return None

    def validate_schema(self):
        """Check whether schema is valid."""
        # delay import to prevent circular imports in 'bigquery_etl.schema'
        from .schema import SCHEMA_FILE, Schema

        if (
            self.skip()
            or basename(self.sqlfile) == "script.sql"
            or str(self.sqlfile).endswith(".py")
        ):  # noqa E501
            print(f"\t...Ignoring schema validation for {self.sqlfile}")
            return True

        query_file_path = Path(self.sqlfile)
        query_schema = Schema.from_json(self.get_schema())
        if self.errors():
            # ignore file when there are errors that self.get_schema() did not raise
            click.echo(f"\t...Ignoring schema validation for {self.sqlfile}")
            return True
        existing_schema_path = query_file_path.parent / SCHEMA_FILE

        if not existing_schema_path.is_file():
            click.echo(f"No schema file defined for {query_file_path}", err=True)
            return True

        table_name = query_file_path.parent.name
        dataset_name = query_file_path.parent.parent.name
        project_name = query_file_path.parent.parent.parent.name

        partitioned_by = None
        if (
            self.metadata
            and self.metadata.bigquery
            and self.metadata.bigquery.time_partitioning
        ):
            partitioned_by = self.metadata.bigquery.time_partitioning.field

        table_schema = Schema.for_table(
            project_name, dataset_name, table_name, partitioned_by
        )

        # This check relies on the new schema being deployed to prod
        if not query_schema.compatible(table_schema):
            click.echo(
                click.style(
                    f"ERROR: Schema for query in {query_file_path} "
                    f"incompatible with schema deployed for "
                    f"{project_name}.{dataset_name}.{table_name}\n"
                    f"Did you deploy new the schema to prod yet?",
                    fg="red",
                ),
                err=True,
            )
            return False
        else:
            existing_schema = Schema.from_schema_file(existing_schema_path)

            if not existing_schema.equal(query_schema):
                click.echo(
                    click.style(
                        f"ERROR: Schema defined in {existing_schema_path} "
                        f"incompatible with query {query_file_path}",
                        fg="red",
                    ),
                    err=True,
                )
                return False

        click.echo(f"Schemas for {query_file_path} are valid.")
        return True


def sql_file_valid(sqlfile):
    """Dry run SQL files."""
    return DryRun(sqlfile).is_valid()


def find_next_word(target, source):
    """Find the next word in a string."""
    split = source.split()
    for i, w in enumerate(split):
        if w == target:
            # get the next word, and remove quotations from column name
            return split[i + 1].replace("'", "")
