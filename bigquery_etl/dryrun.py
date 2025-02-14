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
from datetime import datetime, timedelta, timezone
from enum import Enum
from os.path import basename, dirname, exists
from pathlib import Path
from typing import Any, Iterable, Optional, Set
from urllib.request import Request, urlopen

import click
import google.auth
import sqlglot
import sqlglot.optimizer.scope
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


TMP_DATASET = "bigquery-etl-integration-test.tmp"

YESTERDAY = datetime.now(timezone.utc).date() - timedelta(days=1)

QUERY_PARAMETER_TYPE_VALUES: dict[str, str | bool | int] = {
    # Use yesterday's date in case the date/time parameters are used in time travel queries.
    "DATE": YESTERDAY.isoformat(),
    "DATETIME": YESTERDAY.isoformat() + " 00:00:00",
    "TIMESTAMP": YESTERDAY.isoformat() + " 00:00:00",
    "STRING": "foo",
    "BOOL": True,
    "FLOAT64": 1,
    "FLOAT": 1,
    "INT64": 1,
    "INTEGER": 1,
    "NUMERIC": 1,
    "BIGNUMERIC": 1,
}


def get_credentials(auth_req: Optional[GoogleAuthRequest] = None):
    """Get GCP credentials."""
    auth_req = auth_req or GoogleAuthRequest()
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(auth_req)
    return credentials


def get_id_token(dry_run_url=ConfigLoader.get("dry_run", "function"), credentials=None):
    """Get token to authenticate against Cloud Function."""
    auth_req = GoogleAuthRequest()
    credentials = credentials or get_credentials(auth_req)

    if hasattr(credentials, "id_token"):
        # Get token from default credentials for the current environment created via Cloud SDK run
        id_token = credentials.id_token
    else:
        # If the environment variable GOOGLE_APPLICATION_CREDENTIALS is set to service account JSON file,
        # then ID token is acquired using this service account credentials.
        id_token = fetch_id_token(auth_req, dry_run_url)
    return id_token


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
        id_token=None,
        credentials=None,
        project=None,
        dataset=None,
        table=None,
    ):
        """Instantiate DryRun class."""
        self.sqlfile = sqlfile
        self.content = content
        self.strip_dml = strip_dml
        self.use_cloud_function = use_cloud_function
        self.bq_client = client
        self.respect_skip = respect_skip
        self.dry_run_url = ConfigLoader.get("dry_run", "function")
        self.sql_dir = sql_dir
        self.id_token = (
            id_token
            if not use_cloud_function or id_token
            else get_id_token(self.dry_run_url)
        )
        self.credentials = credentials
        self.project = project
        self.dataset = dataset
        self.table = table
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

    @cached_property
    def client(self):
        """Get BigQuery client instance."""
        if self.use_cloud_function:
            return None
        return self.bq_client or bigquery.Client(credentials=self.credentials)

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

    def get_query_parameters(self) -> list[bigquery.ScalarQueryParameter]:
        """Get query parameters to use for the dry run."""
        query_parameters = []
        scheduling_metadata = self.metadata.scheduling if self.metadata else {}
        if date_partition_parameter := scheduling_metadata.get(
            "date_partition_parameter", "submission_date"
        ):
            query_parameters.append(
                bigquery.ScalarQueryParameter(
                    date_partition_parameter,
                    "DATE",
                    QUERY_PARAMETER_TYPE_VALUES["DATE"],
                )
            )
        for parameter in scheduling_metadata.get("parameters", []):
            parameter_name, parameter_type, _ = parameter.strip().split(":", 2)
            parameter_type = parameter_type.upper() or "STRING"
            query_parameters.append(
                bigquery.ScalarQueryParameter(
                    parameter_name,
                    parameter_type,
                    QUERY_PARAMETER_TYPE_VALUES.get(parameter_type),
                )
            )
        return query_parameters

    def _dry_run_query(
        self,
        sql: str,
        query_parameters: Iterable[bigquery.ScalarQueryParameter],
        target_project: str,
        target_dataset: str,
    ) -> dict[str, Any]:
        """Dry run the provided query."""
        if self.use_cloud_function:
            json_data = {
                "project": self.project or target_project,
                "dataset": self.dataset or target_dataset,
                "query": sql,
                "query_parameters": [
                    query_parameter.to_api_repr()
                    for query_parameter in query_parameters
                ],
            }

            if self.table:
                json_data["table"] = self.table

            r = urlopen(
                Request(
                    self.dry_run_url,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.id_token}",
                    },
                    data=json.dumps(json_data).encode("utf8"),
                    method="POST",
                )
            )
            return json.load(r)
        else:
            self.client.project = target_project
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
                default_dataset=f"{target_project}.{target_dataset}",
                query_parameters=list(query_parameters),
            )
            job = self.client.query(sql, job_config=job_config)
            return {
                "valid": True,
                "referencedTables": [
                    ref.to_api_repr() for ref in job.referenced_tables
                ],
                "schema": {"fields": [field.to_api_repr() for field in job.schema]},
            }

    @cached_property
    def dry_run_result(self):
        """Dry run the provided SQL file."""
        if self.content:
            sql = self.content
        else:
            sql = self.get_sql()

        query_parameters = self.get_query_parameters()

        project = basename(dirname(dirname(dirname(self.sqlfile))))
        dataset = basename(dirname(dirname(self.sqlfile)))

        try:
            result = self._dry_run_query(sql, query_parameters, project, dataset)

            if not self.use_cloud_function:
                try:
                    result["datasetLabels"] = self.client.get_dataset(
                        f"{project}.{dataset}"
                    ).labels
                except Exception as e:
                    # Most users do not have bigquery.datasets.get permission in
                    # moz-fx-data-shared-prod
                    # This should not prevent the dry run from running since the dataset
                    # labels are usually not required
                    if "Permission bigquery.datasets.get denied on dataset" in str(e):
                        result["datasetLabels"] = []
                    else:
                        raise

                if (
                    self.project is not None
                    and self.table is not None
                    and self.dataset is not None
                ):
                    table = self.client.get_table(
                        f"{self.project}.{self.dataset}.{self.table}"
                    )
                    result["tableMetadata"] = {
                        "tableType": table.table_type,
                        "friendlyName": table.friendly_name,
                        "schema": {
                            "fields": [field.to_api_repr() for field in table.schema]
                        },
                    }

            return result

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
                    DryRun(
                        self.sqlfile,
                        filtered_content,
                        client=self.client,
                        id_token=self.id_token,
                    ).get_error()
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
                    DryRun(
                        sqlfile=self.sqlfile,
                        content=filtered_content,
                        client=self.client,
                        id_token=self.id_token,
                    ).get_error()
                    == Errors.DATE_FILTER_NEEDED_AND_SYNTAX
                ):
                    filtered_content = (
                        f"{self.get_sql()}AND {date_filter} > current_timestamp()"
                    )

            stripped_dml_result = DryRun(
                sqlfile=self.sqlfile,
                content=filtered_content,
                client=self.client,
                id_token=self.id_token,
            )
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

    def get_table_schema(self):
        """Return the schema of the provided table."""
        if not self.skip() and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.skip():
            print(f"\t...Ignoring dryrun results for {self.sqlfile}")
            return {}

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "tableMetadata" in self.dry_run_result
        ):
            return self.dry_run_result["tableMetadata"]["schema"]

        return []

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
            project_name,
            dataset_name,
            table_name,
            client=self.client,
            id_token=self.id_token,
            partitioned_by=partitioned_by,
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

    def validate_union_schemas(self) -> bool:
        """Check whether subqueries being unioned have exactly matching schemas."""
        # Delay import to prevent circular imports in `bigquery_etl.schema`.
        from .schema import Schema, SchemaAssertError

        result = True

        target_project = basename(dirname(dirname(dirname(self.sqlfile))))
        target_dataset = basename(dirname(dirname(self.sqlfile)))

        query_parameters_by_name = {
            param.name: param for param in self.get_query_parameters()
        }

        def _replace_parameters(node: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
            if (
                isinstance(node, sqlglot.exp.Parameter)
                and node.name in query_parameters_by_name
            ):
                query_parameter = query_parameters_by_name[node.name]
                value_literal = sqlglot.exp.Literal(
                    this=query_parameter.value,
                    is_string=isinstance(query_parameter.value, str),
                )
                if query_parameter.type_ in ("STRING", "BOOL", "INT64", "INTEGER"):
                    return value_literal
                return sqlglot.cast(
                    value_literal, query_parameter.type_, dialect="bigquery"
                )
            return node

        function_defs_by_name: dict[str, sqlglot.exp.Create] = {}

        def _inline_functions(node: sqlglot.exp.Expression) -> sqlglot.exp.Expression:
            if (
                isinstance(node, sqlglot.exp.Anonymous)
                and node.name in function_defs_by_name
                and not (
                    isinstance(node.parent, sqlglot.exp.Dot)
                    and node == node.parent.right
                )
            ):
                function_def = function_defs_by_name[node.name]
                function_params: list[sqlglot.exp.ColumnDef] = (
                    function_def.this.expressions
                )
                function_args: list[sqlglot.exp.Expression] = node.expressions
                function_body: sqlglot.exp.Expression = function_def.expression

                # If we know the return type, replace the function call with null cast as the return type.
                for function_property in function_def.args["properties"].expressions:
                    if isinstance(function_property, sqlglot.exp.ReturnsProperty):
                        return sqlglot.cast(sqlglot.exp.Null(), function_property.this)

                # If the function has no arguments, replace the function call with the function body as is.
                if not function_args:
                    return function_body

                # Replace the function call with the function body modified to inline the function arguments.
                function_args_by_name = {}
                for function_arg_index, function_arg in enumerate(function_args):
                    if isinstance(function_arg, sqlglot.exp.Kwarg):
                        function_args_by_name[function_arg.name] = (
                            function_arg.expression
                        )
                    elif function_arg_index < len(function_params):
                        function_args_by_name[
                            function_params[function_arg_index].name
                        ] = function_arg

                def _inline_function_args(
                    node: sqlglot.exp.Expression,
                ) -> sqlglot.exp.Expression:
                    if (
                        isinstance(node, sqlglot.exp.Column)
                        and node.parts[0].name in function_args_by_name
                    ):
                        function_arg_name = node.parts[0].name
                        function_arg = function_args_by_name[function_arg_name]
                        if isinstance(
                            function_arg,
                            (
                                sqlglot.exp.Column,
                                sqlglot.exp.Dot,
                                sqlglot.exp.Literal,
                                sqlglot.exp.Paren,
                            ),
                        ):
                            inline_function_arg = function_arg
                        else:
                            inline_function_arg = sqlglot.exp.paren(function_arg)
                        if len(node.parts) == 1:
                            if isinstance(
                                node.parent, (sqlglot.exp.Select, sqlglot.exp.Struct)
                            ):
                                return sqlglot.alias(
                                    inline_function_arg, function_arg_name
                                )
                            return inline_function_arg
                        # Recursively transform the final column expression part in case it's a
                        # `* REPLACE (...)` with embedded function parameter references.
                        return sqlglot.exp.Dot.build(
                            [
                                inline_function_arg,
                                *node.parts[1:-1],
                                node.parts[-1].transform(_inline_function_args),
                            ]
                        )
                    return node

                return function_body.transform(_inline_function_args)
            return node

        for sql_expression in sqlglot.parse(self.get_sql(), dialect="bigquery"):
            if not sql_expression:
                continue
            if (
                isinstance(sql_expression, sqlglot.exp.Create)
                and sql_expression.kind == "FUNCTION"
            ):
                function_defs_by_name[sql_expression.this.name] = sql_expression
                continue

            sql_scope = sqlglot.optimizer.scope.build_scope(sql_expression)
            if not sql_scope:
                continue

            union_expression_scopes: list[sqlglot.optimizer.scope.Scope] = [
                scope
                for scope in sql_scope.traverse()
                if isinstance(scope.expression, sqlglot.exp.Union)
            ]
            for union_number, union_scope in enumerate(
                union_expression_scopes, start=1
            ):
                union: sqlglot.exp.Union = union_scope.expression
                left_select = union.left.copy()
                # Always use the very first `SELECT` statement in the union as the basis for comparison.
                while isinstance(left_select, sqlglot.exp.Union):
                    left_select = left_select.left
                right_select = union.right.copy()
                if union_scope.cte_sources:
                    for cte_name, cte_scope in union_scope.cte_sources.items():
                        left_select.with_(cte_name, cte_scope.expression, copy=False)
                        right_select.with_(cte_name, cte_scope.expression, copy=False)

                if query_parameters_by_name:
                    left_select.transform(_replace_parameters, copy=False)
                    right_select.transform(_replace_parameters, copy=False)

                if function_defs_by_name:
                    left_select.transform(_inline_functions, copy=False)
                    right_select.transform(_inline_functions, copy=False)

                left_select_sql = left_select.sql(dialect="bigquery", pretty=True)
                right_select_sql = right_select.sql(dialect="bigquery", pretty=True)

                # Use `CREATE VIEW` statements to avoid having to add `WHERE` clauses for tables' partition columns.
                left_dryrun_sql = f"CREATE OR REPLACE VIEW `{TMP_DATASET}.union_left` AS\n{left_select_sql}"
                right_dryrun_sql = f"CREATE OR REPLACE VIEW `{TMP_DATASET}.union_right` AS\n{right_select_sql}"

                try:
                    left_dryrun_result = self._dry_run_query(
                        left_dryrun_sql, [], target_project, target_dataset
                    )
                except Exception as e:
                    raise Exception(
                        f"Dryrun error for left side of union #{union_number}."
                    ) from e
                if not left_dryrun_result["valid"]:
                    raise Exception(
                        f"Dryrun error for left side of union #{union_number}: {left_dryrun_result['errors']}"
                    )

                try:
                    right_dryrun_result = self._dry_run_query(
                        right_dryrun_sql, [], target_project, target_dataset
                    )
                except Exception as e:
                    raise Exception(
                        f"Dryrun error for right side of union #{union_number}."
                    ) from e
                if not right_dryrun_result["valid"]:
                    raise Exception(
                        f"Dryrun error for right side of union #{union_number}: {right_dryrun_result['errors']}"
                    )

                left_select_schema = Schema.from_json(left_dryrun_result["schema"])
                right_select_schema = Schema.from_json(right_dryrun_result["schema"])
                try:
                    left_select_schema.assert_exactly_unionable_with(
                        right_select_schema
                    )
                except SchemaAssertError as e:
                    click.echo(
                        click.style(
                            f"ERROR: Schema mismatch in union #{union_number} in {self.sqlfile}: {e}",
                            fg="red",
                        ),
                        err=True,
                    )
                    result = False
        return result


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
