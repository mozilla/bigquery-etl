"""bigquery-etl CLI query command."""

import concurrent.futures
import copy
import datetime
import json
import logging
import multiprocessing
import os
import re
import string
import subprocess
import sys
import tempfile
from concurrent import futures
from datetime import date, timedelta
from functools import partial
from glob import glob
from multiprocessing.pool import Pool, ThreadPool
from pathlib import Path
from traceback import print_exc
from typing import Optional

import rich_click as click
import sqlparse
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..backfill.date_range import BackfillDateRange, get_backfill_partition
from ..backfill.utils import QUALIFIED_TABLE_NAME_RE, qualified_table_name_matching
from ..cli import check
from ..cli.format import format
from ..cli.utils import (
    billing_project_option,
    is_authenticated,
    is_valid_project,
    no_dryrun_option,
    parallelism_option,
    paths_matching_name_pattern,
    project_id_option,
    respect_dryrun_skip_option,
    sql_dir_option,
    temp_dataset_option,
    use_cloud_function_option,
)
from ..config import ConfigLoader
from ..dependency import get_dependency_graph
from ..deploy import (
    FailedDeployException,
    SkippedDeployException,
    SkippedExternalDataException,
    deploy_table,
)
from ..dryrun import DryRun, get_credentials, get_id_token
from ..format_sql.format import skip_format
from ..format_sql.formatter import reformat
from ..metadata import validate_metadata
from ..metadata.parse_metadata import (
    METADATA_FILE,
    BigQueryMetadata,
    ClusteringMetadata,
    DatasetMetadata,
    Metadata,
    PartitionMetadata,
    PartitionType,
)
from ..query_scheduling.dag_collection import DagCollection
from ..query_scheduling.generate_airflow_dags import get_dags
from ..schema import SCHEMA_FILE, Schema
from ..util import extract_from_query_path
from ..util.bigquery_id import sql_table_id
from ..util.common import random_str
from ..util.common import render as render_template
from ..util.parallel_topological_sorter import ParallelTopologicalSorter
from .dryrun import dryrun
from .generate import generate_all

QUERY_NAME_RE = re.compile(r"(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)")
VERSION_RE = re.compile(r"_v[0-9]+")
DESTINATION_TABLE_RE = re.compile(r"^[a-zA-Z0-9_$]{0,1024}$")
DEFAULT_DAG_NAME = "bqetl_default"
DEFAULT_INIT_PARALLELISM = 10
DEFAULT_CHECKS_FILE_NAME = "checks.sql"
VIEW_FILE = "view.sql"
MATERIALIZED_VIEW = "materialized_view.sql"
NBR_DAYS_RETAINED = 775


@click.group(help="Commands for managing queries.")
@click.pass_context
def query(ctx):
    """Create the CLI group for the query command."""
    # create temporary directory generated content is written to
    # the directory will be deleted automatically after the command exits
    ctx.ensure_object(dict)
    ctx.obj["TMP_DIR"] = ctx.with_resource(tempfile.TemporaryDirectory())


@query.command(
    help="""Create a new query with name
    <dataset>.<query_name>, for example: telemetry_derived.active_profiles.
    Use the `--project_id` option to change the project the query is added to;
    default is `moz-fx-data-shared-prod`. Views are automatically generated
    in the publicly facing dataset.

    Examples:

    \b
    ./bqetl query create telemetry_derived.deviations_v1 \\
      --owner=example@mozilla.com

    \b
    # The query version gets autocompleted to v1. Queries are created in the
    # _derived dataset and accompanying views in the public dataset.
    ./bqetl query create telemetry.deviations --owner=example@mozilla.com
    """,
)
@click.argument("name")
@sql_dir_option
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@click.option(
    "--owner",
    "-o",
    help="Owner of the query (email address)",
    default="example@mozilla.com",
)
@click.option(
    "--dag",
    "-d",
    help=(
        f"Name of the DAG the query should be scheduled under."
        "If there is no DAG name specified, the query is"
        f"scheduled by default in DAG {DEFAULT_DAG_NAME}."
        "To skip the automated scheduling use --no_schedule."
        "To see available DAGs run `bqetl dag info`."
        "To create a new DAG run `bqetl dag create`."
    ),
    default=DEFAULT_DAG_NAME,
)
@click.option(
    "--no_schedule",
    "--no-schedule",
    help=(
        "Using this option creates the query without scheduling information."
        " Use `bqetl query schedule` to add it manually if required."
    ),
    default=False,
    is_flag=True,
)
@click.pass_context
def create(ctx, name, sql_dir, project_id, owner, dag, no_schedule):
    """CLI command for creating a new query."""
    # create directory structure for query
    try:
        match = QUERY_NAME_RE.match(name)
        name = match.group("name")
        dataset = match.group("dataset")

        version = "_" + name.split("_")[-1]
        if not VERSION_RE.match(version):
            version = "_v1"
        else:
            name = "_".join(name.split("_")[:-1])
    except AttributeError:
        click.echo(
            "New queries must be named like:"
            + " <dataset>.<table> or <dataset>.<table>_v[n]"
        )
        sys.exit(1)

    derived_path = None
    view_path = None
    path = Path(sql_dir)

    if dataset.endswith("_derived"):
        # create directory for this table
        derived_path = path / project_id / dataset / (name + version)
        derived_path.mkdir(parents=True)

        # create a directory for the corresponding view
        view_path = path / project_id / dataset.replace("_derived", "") / name
        # new versions of existing tables may already have a view
        view_path.mkdir(parents=True, exist_ok=True)
    else:
        # check if there is a corresponding derived dataset
        if (path / project_id / (dataset + "_derived")).exists():
            derived_path = path / project_id / (dataset + "_derived") / (name + version)
            derived_path.mkdir(parents=True)
            view_path = path / project_id / dataset / name
            view_path.mkdir(parents=True)

            dataset = dataset + "_derived"
        else:
            # some dataset that is not specified as _derived
            # don't automatically create views
            derived_path = path / project_id / dataset / (name + version)
            derived_path.mkdir(parents=True)

    click.echo(f"Created query in {derived_path}")

    if view_path and not (view_file := view_path / "view.sql").exists():
        # Don't overwrite the view_file if it already exists
        click.echo(f"Created corresponding view in {view_path}")
        view_dataset = dataset.replace("_derived", "")
        view_file.write_text(
            reformat(
                f"""CREATE OR REPLACE VIEW
                  `{project_id}.{view_dataset}.{name}`
                AS SELECT * FROM
                  `{project_id}.{dataset}.{name}{version}`"""
            )
            + "\n"
        )

    # create query.sql file
    query_file = derived_path / "query.sql"
    query_file.write_text(
        reformat(
            f"""-- Query for {dataset}.{name}{version}
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
            SELECT * FROM table WHERE submission_date = @submission_date"""
        )
        + "\n"
    )

    # create default metadata.yaml
    metadata_file = derived_path / "metadata.yaml"
    metadata = Metadata(
        friendly_name=string.capwords(name.replace("_", " ")),
        description="Please provide a description for the query",
        owners=[owner],
        labels={"incremental": True},
        bigquery=BigQueryMetadata(
            time_partitioning=PartitionMetadata(field="", type=PartitionType.DAY),
            clustering=ClusteringMetadata(fields=[]),
        ),
    )
    metadata.write(metadata_file)

    dataset_metadata_file = derived_path.parent / "dataset_metadata.yaml"
    if not dataset_metadata_file.exists():
        dataset_name = str(dataset_metadata_file.parent.name)
        dataset_metadata = DatasetMetadata(
            friendly_name=string.capwords(dataset_name.replace("_", " ")),
            description="Please provide a description for the dataset",
            dataset_base_acl="derived",
            user_facing=False,
        )
        dataset_metadata.write(dataset_metadata_file)
        click.echo(f"Created dataset metadata in {dataset_metadata_file}")

    if view_path:
        dataset_metadata_file = view_path.parent / "dataset_metadata.yaml"
        if not dataset_metadata_file.exists():
            dataset_name = str(dataset_metadata_file.parent.name)
            dataset_metadata = DatasetMetadata(
                friendly_name=string.capwords(dataset_name.replace("_", " ")),
                description="Please provide a description for the dataset",
                dataset_base_acl="view",
                user_facing=True,
            )
            dataset_metadata.write(dataset_metadata_file)
            click.echo(f"Created dataset metadata in {dataset_metadata_file}")

    if no_schedule:
        click.echo(
            click.style(
                "WARNING: This query has been created without "
                "scheduling information. Use `bqetl query schedule`"
                " to manually add it to a DAG or "
                "`bqetl query create --help` for more options.",
                fg="yellow",
            )
        )
    else:
        ctx.invoke(schedule, name=derived_path, dag=dag)


@query.command(
    help="""Schedule an existing query

    Examples:

    \b
    ./bqetl query schedule telemetry_derived.deviations_v1 \\
      --dag=bqetl_deviations

    \b
    # Set a specific name for the task
    ./bqetl query schedule telemetry_derived.deviations_v1 \\
      --dag=bqetl_deviations \\
      --task-name=deviations
    """,
)
@click.argument("name")
@sql_dir_option
@project_id_option()
@click.option(
    "--dag",
    "-d",
    help=(
        "Name of the DAG the query should be scheduled under. "
        "To see available DAGs run `bqetl dag info`. "
        "To create a new DAG run `bqetl dag create`."
    ),
)
@click.option(
    "--depends_on_past",
    "--depends-on-past",
    help="Only execute query if previous scheduled run succeeded.",
    default=False,
    type=bool,
)
@click.option(
    "--task_name",
    "--task-name",
    help=(
        "Custom name for the Airflow task. By default the task name is a "
        "combination of the dataset and table name."
    ),
)
def schedule(name, sql_dir, project_id, dag, depends_on_past, task_name):
    """CLI command for scheduling a query."""
    query_files = paths_matching_name_pattern(name, sql_dir, project_id)

    if query_files == []:
        click.echo(f"Name doesn't refer to any queries: {name}", err=True)
        sys.exit(1)

    sql_dir = Path(sql_dir)

    dags = DagCollection.from_file(sql_dir.parent / "dags.yaml")

    for query_file in query_files:
        try:
            metadata = Metadata.of_query_file(query_file)
        except FileNotFoundError:
            click.echo(f"Cannot schedule {query_file}. No metadata.yaml found.")
            continue

        if dag:
            # check if DAG already exists
            existing_dag = dags.dag_by_name(dag)
            if not existing_dag:
                click.echo(
                    (
                        f"DAG {dag} does not exist. "
                        "To see available DAGs run `bqetl dag info`. "
                        "To create a new DAG run `bqetl dag create`."
                    ),
                    err=True,
                )
                sys.exit(1)

            # write scheduling information to metadata file
            metadata.scheduling = {}
            metadata.scheduling["dag_name"] = dag

            if depends_on_past:
                metadata.scheduling["depends_on_past"] = depends_on_past

            if task_name:
                metadata.scheduling["task_name"] = task_name

            metadata.write(query_file.parent / METADATA_FILE)
            logging.info(
                f"Updated {query_file.parent / METADATA_FILE} with scheduling"
                " information. For more information about scheduling queries see: "
                "https://github.com/mozilla/bigquery-etl#scheduling-queries-in-airflow"
            )

            # update dags since new task has been added
            dags = get_dags(None, sql_dir.parent / "dags.yaml", sql_dir=sql_dir)
        else:
            dags = get_dags(None, sql_dir.parent / "dags.yaml", sql_dir=sql_dir)
            if metadata.scheduling == {}:
                click.echo(f"No scheduling information for: {query_file}", err=True)
                sys.exit(1)


@query.command(
    help="""Get information about all or specific queries.

    Examples:

    \b
    # Get info for specific queries
    ./bqetl query info telemetry_derived.*

    \b
    # Get cost and last update timestamp information
    ./bqetl query info telemetry_derived.clients_daily_v6 \\
      --cost --last_updated
    """,
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option()
@click.pass_context
def info(ctx, name, sql_dir, project_id):
    """Return information about all or specific queries."""
    if name is None:
        name = "*.*"

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    if query_files == []:
        # run SQL generators if no matching query has been found
        ctx.invoke(
            generate_all,
            output_dir=ctx.obj["TMP_DIR"],
            ignore=["derived_view_schemas", "stable_views"],
        )
        query_files = paths_matching_name_pattern(name, ctx.obj["TMP_DIR"], project_id)
        if query_files == []:
            raise click.ClickException(f"No queries matching `{name}` were found.")

    for query_file in query_files:
        query_file_path = Path(query_file)
        table = query_file_path.parent.name
        dataset = query_file_path.parent.parent.name
        project = query_file_path.parent.parent.parent.name

        try:
            metadata = Metadata.of_query_file(query_file)
        except FileNotFoundError:
            metadata = None

        click.secho(f"{project}.{dataset}.{table}", bold=True)
        click.echo(f"path: {query_file}")

        if metadata is None:
            click.echo("No metadata")
        else:
            click.echo(f"description: {metadata.description}")
            click.echo(f"owners: {metadata.owners}")

            if metadata.scheduling == {}:
                click.echo("scheduling: not scheduled")
            else:
                click.echo("scheduling:")
                click.echo(f"  dag_name: {metadata.scheduling['dag_name']}")

        # TODO: Add costs and last_updated info

        click.echo("")


def _parse_parameter(parameter: str, param_date: str) -> str:
    # TODO: Parse more complex parameters such as macro_ds_add
    param_name, param_type, param_value = parameter.split(":")
    if param_type == "DATE" and param_value != "{{ds}}":
        raise ValueError(f"Unable to parse parameter {parameter}")

    return f"--parameter={parameter.replace('{{ds}}', param_date)}"


def _backfill_query(
    query_file_path,
    project_id,
    date_partition_parameter,
    date_partition_offset,
    max_rows,
    dry_run,
    scheduling_parameters,
    args,
    partitioning_type,
    backfill_date,
    destination_table,
    run_checks,
    checks_file_name,
    billing_project,
):
    """Run a query backfill for a specific date."""
    project, dataset, table = extract_from_query_path(query_file_path)
    if destination_table is None:
        destination_table = f"{project}.{dataset}.{table}"

    # For partitioned tables, get the partition to write to the correct destination:
    if partitioning_type is not None:
        if (
            partition := get_backfill_partition(
                backfill_date,
                date_partition_parameter,
                date_partition_offset,
                partitioning_type,
            )
        ) is not None:
            destination_table = f"{destination_table}${partition}"

    if not QUALIFIED_TABLE_NAME_RE.match(destination_table):
        click.echo("Destination table must be named like: <project>.<dataset>.<table>")
        sys.exit(1)

    backfill_date_str = backfill_date.strftime("%Y-%m-%d")
    query_parameters = [
        _parse_parameter(param, backfill_date_str) for param in scheduling_parameters
    ]

    if date_partition_parameter is not None:
        offset_param = backfill_date + timedelta(days=date_partition_offset)
        query_parameters.append(
            f"--parameter={date_partition_parameter}:DATE:{offset_param.strftime('%Y-%m-%d')}"
        )

    arguments = (
        [
            "query",
            "--use_legacy_sql=false",
            "--replace",
            f"--max_rows={max_rows}",
            f"--project_id={project_id}",
            "--format=none",
        ]
        + args
        + query_parameters
    )
    if dry_run:
        arguments += ["--dry_run"]

    click.echo(
        f"Backfill run: {backfill_date_str} "
        f"Destination_table: {destination_table} "
        f"Scheduling Parameters: {query_parameters}"
    )

    _run_query(
        [query_file_path],
        project_id=project_id,
        dataset_id=dataset,
        destination_table=destination_table,
        public_project_id=ConfigLoader.get(
            "default", "public_project", fallback="mozilla-public-data"
        ),
        query_arguments=arguments,
        billing_project=billing_project,
    )

    # Run checks on the query
    checks_file = query_file_path.parent / checks_file_name
    if run_checks and checks_file.exists():
        table_name = checks_file.parent.name
        # query_args have things like format, which we don't want to push
        # to the check; so we just take the query parameters
        check_args = [qa for qa in arguments if qa.startswith("--parameter")]
        check._run_check(
            checks_file=checks_file,
            project_id=project_id,
            dataset_id=dataset,
            table=table_name,
            query_arguments=check_args,
            dry_run=dry_run,
        )

    return True


@query.command(
    help="""Run a backfill for a query. Additional parameters will get passed to bq.

    Examples:

    \b
    # Backfill for specific date range
    # second comment line
    ./bqetl query backfill telemetry_derived.ssl_ratios_v1 \\
      --start_date=2021-03-01 \\
      --end_date=2021-03-31

    \b
    # Dryrun backfill for specific date range and exclude date
    ./bqetl query backfill telemetry_derived.ssl_ratios_v1 \\
      --start_date=2021-03-01 \\
      --end_date=2021-03-31 \\
      --exclude=2021-03-03 \\
      --dry_run
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@sql_dir_option
@project_id_option(required=True)
@billing_project_option()
@click.option(
    "--start_date",
    "--start-date",
    "-s",
    help="First date to be backfilled",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
)
@click.option(
    "--end_date",
    "--end-date",
    "-e",
    help="Last date to be backfilled",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(date.today()),
)
@click.option(
    "--exclude",
    "-x",
    multiple=True,
    help="Dates excluded from backfill. Date format: yyyy-mm-dd",
    default=[],
)
@click.option(
    "--dry_run/--no_dry_run", "--dry-run/--no-dry-run", help="Dry run the backfill"
)
@click.option(
    "--max_rows",
    "-n",
    type=int,
    default=100,
    help="How many rows to return in the result",
)
@click.option(
    "--parallelism",
    "-p",
    type=int,
    default=8,
    help="How many threads to run backfill in parallel",
)
@click.option(
    "--destination_table",
    "--destination-table",
    required=False,
    help=(
        "Destination table name results are written to. "
        + "If not set, determines destination table based on query."
    ),
)
@click.option(
    "--checks/--no-checks", help="Whether to run checks during backfill", default=False
)
@click.option(
    "--custom_query_path",
    "--custom-query-path",
    help="Name of a custom query to run the backfill. If not given, the proces runs as usual.",
    default=None,
)
@click.option(
    "--checks_file_name",
    "--checks_file_name",
    help="Name of a custom data checks file to run after each partition backfill. E.g. custom_checks.sql. Optional.",
    default=None,
)
@click.option(
    "--scheduling_overrides",
    "--scheduling-overrides",
    required=False,
    type=str,
    default="{}",
    help=(
        "Pass overrides as a JSON string for scheduling sections: "
        "parameters and/or date_partition_parameter as needed."
    ),
)
@click.option(
    "--override-retention-range-limit",
    required=False,
    type=bool,
    is_flag=True,
    help="True to allow running a backfill outside the retention policy limit.",
    default=False,
)
@click.pass_context
def backfill(
    ctx,
    name,
    sql_dir,
    project_id,
    billing_project,
    start_date,
    end_date,
    exclude,
    dry_run,
    max_rows,
    parallelism,
    destination_table,
    checks,
    checks_file_name,
    custom_query_path,
    scheduling_overrides,
    override_retention_range_limit,
):
    """Run a backfill."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    # If override retention policy is False, and the start date is less than NBR_DAYS_RETAINED
    if (
        not override_retention_range_limit
        and start_date.date() < date.today() - timedelta(days=NBR_DAYS_RETAINED)
    ):
        # Exit - cannot backfill due to risk of losing data
        click.echo(
            f"Cannot backfill more than {NBR_DAYS_RETAINED} days prior to current date due to retention policies"
        )
        sys.exit(1)

    # If override retention policy is true, continue to run the backfill
    if override_retention_range_limit:
        click.echo("Over-riding retention limit - ensure data exists in source tables")

    if custom_query_path:
        query_files = paths_matching_name_pattern(
            custom_query_path, sql_dir, project_id
        )
    else:
        query_files = paths_matching_name_pattern(name, sql_dir, project_id)

    if query_files == []:
        if custom_query_path:
            click.echo(f"Custom query file '{custom_query_path}' not found in {name}")
            sys.exit(1)

        # run SQL generators if no matching query has been found
        ctx.invoke(
            generate_all,
            output_dir=ctx.obj["TMP_DIR"],
            ignore=["derived_view_schemas", "stable_views", "country_code_lookup"],
        )
        query_files = paths_matching_name_pattern(name, ctx.obj["TMP_DIR"], project_id)
        if query_files == []:
            raise click.ClickException(f"No queries matching `{name}` were found.")

    for query_file in query_files:
        query_file_path = Path(query_file)

        try:
            metadata = Metadata.of_query_file(str(query_file_path))
        except FileNotFoundError:
            click.echo(f"Can't run backfill without metadata for {query_file_path}.")
            continue

        depends_on_past = metadata.scheduling.get("depends_on_past", False)
        # If date_partition_parameter isn't set it's assumed to be submission_date:
        # https://github.com/mozilla/telemetry-airflow/blob/dbc2782fa23a34ae8268e7788f9621089ac71def/utils/gcp.py#L194C48-L194C48

        # adding copy logic for cleaner handling of overrides
        scheduling_metadata = metadata.scheduling.copy()
        scheduling_metadata.update(json.loads(scheduling_overrides))
        date_partition_parameter = scheduling_metadata.get(
            "date_partition_parameter", "submission_date"
        )
        scheduling_parameters = scheduling_metadata.get("parameters", [])
        date_partition_offset = scheduling_metadata.get("date_partition_offset", 0)

        partitioning_type = None
        if metadata.bigquery and metadata.bigquery.time_partitioning:
            partitioning_type = metadata.bigquery.time_partitioning.type

        # null date_partition_parameter explicitly set to null will
        # cause each backfill query to overwrite the entire table
        if partitioning_type is not None and date_partition_parameter is None:
            raise ValueError(
                f"Cannot backfill partitioned table with date_partition_parameter set to null: {query_file.parent}"
            )

        date_range = BackfillDateRange(
            start_date.date(),
            end_date.date(),
            excludes=[date.fromisoformat(x) for x in exclude],
            range_type=partitioning_type or PartitionType.DAY,
        )

        if depends_on_past and exclude:
            click.echo(
                f"Warning: depends_on_past = True for {query_file_path} but the"
                f"following dates will be excluded from the backfill: {exclude}"
            )

        client = bigquery.Client(project=project_id)
        try:
            project, dataset, table = extract_from_query_path(query_file_path)
            client.get_table(f"{project}.{dataset}.{table}")
        except NotFound:
            ctx.invoke(
                initialize,
                name=query_file,
                dry_run=dry_run,
                billing_project=billing_project,
            )

        backfill_query = partial(
            _backfill_query,
            query_file_path,
            project_id,
            date_partition_parameter,
            date_partition_offset,
            max_rows,
            dry_run,
            scheduling_parameters,
            ctx.args,
            partitioning_type,
            destination_table=destination_table,
            run_checks=checks,
            checks_file_name=checks_file_name or DEFAULT_CHECKS_FILE_NAME,
            billing_project=billing_project,
        )

        if not depends_on_past and parallelism > 0:
            # run backfill for dates in parallel if depends_on_past is false
            failed_backfills = []
            with futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
                future_to_date = {
                    executor.submit(backfill_query, backfill_date): backfill_date
                    for backfill_date in date_range
                }
                for future in futures.as_completed(future_to_date):
                    backfill_date = future_to_date[future]
                    try:
                        future.result()
                    except Exception as e:  # TODO: More specific exception(s)
                        print(f"Encountered exception {e}: {backfill_date}.")
                        failed_backfills.append(backfill_date)
                    else:
                        print(f"Completed processing: {backfill_date}.")
            if failed_backfills:
                raise RuntimeError(
                    f"Backfill processing failed for the following backfill dates: {failed_backfills}"
                )
        else:
            # if data depends on previous runs, then execute backfill sequentially
            for backfill_date in date_range:
                backfill_query(backfill_date)


@query.command(
    help="""Run a query. Additional parameters will get passed to bq.<br />
    If a destination_table is set, the query result will be written to BigQuery. Without a destination_table specified, the results are not stored.<br />
    If the `name` is not found within the `sql/` folder bqetl assumes it hasn't been generated yet
    and will start the generating process for all `sql_generators/` files.
    This generation process will take some time and run dryrun calls against BigQuery but this is expected. <br />
    Additional parameters (all parameters that are not specified in the Options) must come after the query-name.
    Otherwise the first parameter that is not an option is interpreted as the query-name and since it can't be found the generation process will start.

    Examples:

    \b
    # Run a query by name
    ./bqetl query run telemetry_derived.ssl_ratios_v1

    \b
    # Run a query file
    ./bqetl query run /path/to/query.sql

    \b
    # Run a query and save the result to BigQuery
    ./bqetl query run telemetry_derived.ssl_ratios_v1 \
        --project_id=moz-fx-data-shared-prod \
        --dataset_id=telemetry_derived \
        --destination_table=ssl_ratios_v1
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@sql_dir_option
@project_id_option()
@billing_project_option()
@click.option(
    "--public_project_id",
    "--public-project-id",
    default=ConfigLoader.get(
        "default", "public_project", fallback="mozilla-public-data"
    ),
    help="Project with publicly accessible data",
)
@click.option(
    "--destination_table",
    "--destination-table",
    required=False,
    help=(
        "Destination table name results are written to. "
        + "If not set, the query result will not be written to BigQuery."
    ),
)
@click.option(
    "--dataset_id",
    "--dataset-id",
    required=False,
    help=(
        "Destination dataset results are written to. "
        + "If not set, determines destination dataset based on query."
    ),
)
@click.pass_context
def run(
    ctx,
    name,
    sql_dir,
    project_id,
    billing_project,
    public_project_id,
    destination_table,
    dataset_id,
):
    """Run a query."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    if query_files == []:
        # run SQL generators if no matching query has been found
        ctx.invoke(
            generate_all,
            output_dir=ctx.obj["TMP_DIR"],
            ignore=["derived_view_schemas", "stable_views", "country_code_lookup"],
        )
        query_files = paths_matching_name_pattern(name, ctx.obj["TMP_DIR"], project_id)
        if query_files == []:
            raise click.ClickException(f"No queries matching `{name}` were found.")

    _run_query(
        query_files,
        project_id,
        public_project_id,
        destination_table,
        dataset_id,
        ctx.args,
        billing_project=billing_project,
    )


def _run_query(
    query_files,
    project_id,
    public_project_id,
    destination_table,
    dataset_id,
    query_arguments,
    addl_templates: Optional[dict] = None,
    billing_project: Optional[str] = None,
):
    """Run a query.

    project_id is the default project to use with table/view/udf references in the query that
    do not have a project id qualifier.
    billing_project is the project to run the query in for the purposes of billing and
    slot reservation selection.  This is project_id if billing_project is not set
    """
    if billing_project is not None:
        query_arguments.append(f"--project_id={billing_project}")
    elif project_id is not None:
        query_arguments.append(f"--project_id={project_id}")

    if addl_templates is None:
        addl_templates = {}

    for query_file in query_files:
        use_public_table = False

        query_file = Path(query_file)
        try:
            metadata = Metadata.of_query_file(query_file)
            if metadata.is_public_bigquery():
                if not validate_metadata.validate_public_data(metadata, query_file):
                    sys.exit(1)

                # change the destination table to write results to the public dataset;
                # a view to the public table in the internal dataset is created
                # when CI runs
                if (
                    dataset_id is not None
                    and destination_table is not None
                    and re.match(DESTINATION_TABLE_RE, destination_table)
                ):
                    destination_table = "{}:{}.{}".format(
                        public_project_id, dataset_id, destination_table
                    )
                    query_arguments.append(
                        "--destination_table={}".format(destination_table)
                    )
                    use_public_table = True
                else:
                    print(
                        "ERROR: Cannot run public dataset query. Parameters"
                        " --destination_table=<table without dataset ID> and"
                        " --dataset_id=<dataset> required"
                    )
                    sys.exit(1)
        except yaml.YAMLError as e:
            logging.error(e)
            sys.exit(1)
        except FileNotFoundError:
            logging.warning("No metadata.yaml found for %s", query_file)

        if not use_public_table and destination_table is not None:
            # destination table was parsed by argparse, however if it wasn't modified to
            # point to a public table it needs to be passed as parameter for the query

            if re.match(QUALIFIED_TABLE_NAME_RE, destination_table):
                project, dataset, table = qualified_table_name_matching(
                    destination_table
                )
                destination_table = "{}:{}.{}".format(project, dataset, table)
            elif billing_project is not None:
                # add project and dataset to destination table if it isn't qualified
                if project_id is None or dataset_id is None:
                    raise ValueError(
                        "Cannot determine destination table without project_id and dataset_id"
                    )
                destination_table = "{}:{}.{}".format(
                    project_id, dataset_id, destination_table
                )

            query_arguments.append("--destination_table={}".format(destination_table))

        if bool(list(filter(lambda x: x.startswith("--parameter"), query_arguments))):
            # need to do this as parameters are not supported with legacy sql
            query_arguments.append("--use_legacy_sql=False")

        # this assumed query command should always be passed inside query_arguments
        if "query" not in query_arguments:
            query_arguments = ["query"] + query_arguments

        query_text = render_template(
            query_file.name,
            template_folder=str(query_file.parent),
            templates_dir="",
            format=False,
            **addl_templates,
        )

        # create a session, setting default project and dataset for the query
        # this is needed if the project the query is run in (billing_project) doesn't match the
        # project directory the query is in
        if billing_project is not None and billing_project != project_id:
            default_project, default_dataset, _ = extract_from_query_path(query_file)

            session_id = create_query_session(
                session_project=billing_project,
                default_project=project_id or default_project,
                default_dataset=dataset_id or default_dataset,
            )
            query_arguments.append(f"--session_id={session_id}")

            # temp udfs cannot be used in a session when destination table is set
            if destination_table is not None and query_file.name != "script.sql":
                query_text = extract_and_run_temp_udfs(
                    query_text=query_text,
                    project_id=billing_project,
                    session_id=session_id,
                )

        # if billing_project is set, default dataset is set with the @@dataset_id variable instead
        elif dataset_id is not None:
            # dataset ID was parsed by argparse but needs to be passed as parameter
            # when running the query
            query_arguments.append(f"--dataset_id={dataset_id}")

        # write rendered query to a temporary file;
        # query string cannot be passed directly to bq as SQL comments will be interpreted as CLI arguments
        with tempfile.NamedTemporaryFile(mode="w+") as query_stream:
            query_stream.write(query_text)
            query_stream.seek(0)

            # run the query as shell command so that passed parameters can be used as is
            subprocess.check_call(["bq"] + query_arguments, stdin=query_stream)


def create_query_session(
    session_project: str,
    default_project: Optional[str] = None,
    default_dataset: Optional[str] = None,
):
    """Create a bigquery session and return the session id.

    Optionally set the system variables @@dataset_project_id and @@dataset_id
    if project_id and dataset_id are given. This sets the default project_id or dataset_id
    for table/view/udf references that do not have a project or dataset qualifier.

    :param session_project: Project to create the session in
    :param default_project: Optional project to use in queries for object
        that do not have a project qualifier.
    :param default_dataset: Optional dataset to use in queries for object
        that do not have a project qualifier.
    """
    query_parts = []
    if default_project is not None:
        query_parts.append(f"SET @@dataset_project_id = '{default_project}'")
    if default_dataset is not None:
        query_parts.append(f"SET @@dataset_id = '{default_dataset}'")

    if len(query_parts) == 0:  # need to run a non-empty query
        session_query = "SELECT 1"
    else:
        session_query = ";\n".join(query_parts)

    client = bigquery.Client(project=session_project)

    job_config = bigquery.QueryJobConfig(
        create_session=True,
        use_legacy_sql=False,
    )
    job = client.query(session_query, job_config)
    job.result()

    if job.session_info is None:
        raise RuntimeError(f"Failed to get session id with job id {job.job_id}")

    return job.session_info.session_id


def extract_and_run_temp_udfs(query_text: str, project_id: str, session_id: str) -> str:
    """Create temp udfs in the session and return the query without udf definitions.

    Does not support dry run because the query will fail dry run if udfs aren't defined.
    """
    sql_statements = sqlparse.split(query_text)

    if len(sql_statements) == 1:
        return query_text

    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        connection_properties=[bigquery.ConnectionProperty("session_id", session_id)],
    )

    # assume query files only have temp udfs as additional statements
    udf_def_statement = "\n".join(sql_statements[:-1])
    client.query_and_wait(udf_def_statement, job_config=job_config)

    return sql_statements[-1]


@query.command(
    help="""Run a multipart query.

    Examples:

    \b
    # Run a multipart query
    ./bqetl query run_multipart /path/to/query.sql
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument(
    "query_dir",
    type=click.Path(file_okay=False),
)
@click.option(
    "--using",
    default="document_id",
    help="comma separated list of join columns to use when combining results",
)
@click.option(
    "--parallelism",
    default=4,
    type=int,
    help="Maximum number of queries to execute concurrently",
)
@click.option(
    "--dataset_id",
    "--dataset-id",
    help="Default dataset, if not specified all tables must be qualified with dataset",
)
@project_id_option()
@temp_dataset_option()
@click.option(
    "--destination_table",
    required=True,
    help="table where combined results will be written",
)
@click.option(
    "--time_partitioning_field",
    type=lambda f: bigquery.TimePartitioning(field=f),
    help="time partition field on the destination table",
)
@click.option(
    "--clustering_fields",
    type=lambda f: f.split(","),
    help="comma separated list of clustering fields on the destination table",
)
@click.option(
    "--dry_run",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print bytes that would be processed for each part and don't run queries",
)
@click.option(
    "--parameters",
    "--parameter",
    multiple=True,
    default=[],
    type=lambda p: bigquery.ScalarQueryParameter(*p.split(":", 2)),
    metavar="NAME:TYPE:VALUE",
    help="query parameter(s) to pass when running parts",
)
@click.option(
    "--priority",
    default="INTERACTIVE",
    type=click.Choice(["BATCH", "INTERACTIVE"]),
    help=(
        "Priority for BigQuery query jobs; BATCH priority will significantly slow "
        "down queries if reserved slots are not enabled for the billing project; "
        "defaults to INTERACTIVE"
    ),
)
@click.option(
    "--schema_update_options",
    "--schema_update_option",
    multiple=True,
    type=click.Choice(
        [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            # Airflow passes an empty string when the field addition date doesn't
            # match the run date.
            # See https://github.com/mozilla/telemetry-airflow/blob/
            # e49fa7e6b3f5ec562dd248d257770c2303cf0cba/dags/utils/gcp.py#L515
            "",
        ]
    ),
    default=[],
    help="Optional options for updating the schema.",
)
def run_multipart(
    query_dir,
    using,
    parallelism,
    dataset_id,
    project_id,
    temp_dataset,
    destination_table,
    time_partitioning_field,
    clustering_fields,
    dry_run,
    parameters,
    priority,
    schema_update_options,
):
    """Run a multipart query."""
    if dataset_id is not None and "." not in dataset_id and project_id is not None:
        dataset_id = f"{project_id}.{dataset_id}"
    if "." not in destination_table and dataset_id is not None:
        destination_table = f"{dataset_id}.{destination_table}"
    client = bigquery.Client(project_id)
    with ThreadPool(parallelism) as pool:
        parts = pool.starmap(
            _run_part,
            [
                (
                    client,
                    part,
                    query_dir,
                    temp_dataset,
                    dataset_id,
                    dry_run,
                    parameters,
                    priority,
                )
                for part in sorted(next(os.walk(query_dir))[2])
                if part.startswith("part") and part.endswith(".sql")
            ],
            chunksize=1,
        )
    if not dry_run:
        total_bytes = sum(job.total_bytes_processed for _, job in parts)
        query = (
            f"SELECT\n  *\nFROM\n  `{sql_table_id(parts[0][1].destination)}`"
            + "".join(
                f"\nFULL JOIN\n  `{sql_table_id(job.destination)}`"
                f"\nUSING\n  ({using})"
                for _, job in parts[1:]
            )
        )
        try:
            job = client.query(
                query=query,
                job_config=bigquery.QueryJobConfig(
                    destination=destination_table,
                    time_partitioning=time_partitioning_field,
                    clustering_fields=clustering_fields,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    use_legacy_sql=False,
                    priority=priority,
                    schema_update_options=schema_update_options,
                ),
            )
            job.result()
            logging.info(
                f"Processed {job.total_bytes_processed:,d} bytes to combine results"
            )
            total_bytes += job.total_bytes_processed
            logging.info(f"Processed {total_bytes:,d} bytes in total")
        finally:
            for _, job in parts:
                client.delete_table(sql_table_id(job.destination).split("$")[0])
            logging.info(f"Deleted {len(parts)} temporary tables")


def _run_part(
    client, part, query_dir, temp_dataset, dataset_id, dry_run, parameters, priority
):
    """Run a query part."""
    with open(os.path.join(query_dir, part)) as sql_file:
        query = sql_file.read()
    job_config = bigquery.QueryJobConfig(
        destination=temp_dataset.temp_table(),
        default_dataset=dataset_id,
        use_legacy_sql=False,
        dry_run=dry_run,
        query_parameters=parameters,
        priority=priority,
        allow_large_results=True,
    )
    job = client.query(query=query, job_config=job_config)
    if job.dry_run:
        logging.info(f"Would process {job.total_bytes_processed:,d} bytes for {part}")
    else:
        job.result()
        logging.info(f"Processed {job.total_bytes_processed:,d} bytes for {part}")
    return part, job


@query.command(
    help="""Validate a query.
    Checks formatting, scheduling information and dry runs the query.

    Examples:

    ./bqetl query validate telemetry_derived.clients_daily_v6

    \b
    # Validate query not in shared-prod
    ./bqetl query validate \\
      --use_cloud_function=false \\
      --project_id=moz-fx-data-marketing-prod \\
      ga_derived.blogs_goals_v1
    """,
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option()
@use_cloud_function_option
@click.option(
    "--validate_schemas",
    "--validate-schemas",
    help="Require dry run schema to match destination table and file if present.",
    is_flag=True,
    default=False,
)
@respect_dryrun_skip_option(default=False)
@no_dryrun_option(default=False)
@click.option("--skip_format_sql", "--skip-format-sql", is_flag=True, default=False)
@click.pass_context
def validate(
    ctx,
    name,
    sql_dir,
    project_id,
    use_cloud_function,
    validate_schemas,
    respect_dryrun_skip,
    no_dryrun,
    skip_format_sql,
):
    """Validate queries by dry running, formatting and checking scheduling configs."""
    if name is None:
        name = "*.*"

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    dataset_dirs = set()
    errors = []
    for query in query_files:
        click.echo(f"Validating metadata for {query}")
        if not skip_format_sql:
            ctx.invoke(format, paths=[str(query)])

        if not no_dryrun:
            ctx.invoke(
                dryrun,
                paths=[str(query)],
                use_cloud_function=use_cloud_function,
                project=project_id,
                validate_schemas=validate_schemas,
                respect_skip=respect_dryrun_skip,
            )

        try:
            validate_metadata.validate(query.parent)
        except validate_metadata.MetadataValidationError as e:
            errors.append(str(e))
        dataset_dirs.add(query.parent.parent)

    if no_dryrun:
        click.echo("Dry run skipped for query files.")

    for dataset_dir in dataset_dirs:
        try:
            validate_metadata.validate_datasets(dataset_dir)
        except validate_metadata.MetadataValidationError as e:
            errors.append(str(e))

    if len(errors) > 0:
        click.echo(
            f"Failed to validate {len(errors)} metadata files (see above for error messages):"
        )
        click.echo("\n".join(errors))
        sys.exit(1)


def _initialize_in_parallel(
    project,
    table,
    dataset,
    query_file,
    arguments,
    parallelism,
    sample_ids,
    addl_templates,
    billing_project,
):
    with ThreadPool(parallelism) as pool:
        # Process all sample_ids in parallel.
        pool.map(
            partial(
                _run_query,
                [query_file],
                project,
                None,
                table,
                dataset,
                addl_templates=addl_templates,
                billing_project=billing_project,
            ),
            [arguments + [f"--parameter=sample_id:INT64:{i}"] for i in sample_ids],
        )


@query.command(
    help="""Run a full backfill on the destination table for the query.
       Using this command will:
        - Create the table if it doesn't exist and run a full backfill.
        - Run a full backfill if the table exists and is empty.
        - Raise an exception if the table exists and has data, or if the table exists and the schema doesn't match the query.
       It supports `query.sql` files that use the is_init() pattern.
       To run in parallel per sample_id, include a @sample_id parameter in the query.

       Examples:
       - For init.sql files: ./bqetl query initialize telemetry_derived.ssl_ratios_v1
       - For query.sql files and parallel run: ./bqetl query initialize sql/moz-fx-data-shared-prod/telemetry_derived/clients_first_seen_v2/query.sql
       """,
)
@click.argument("name")
@sql_dir_option
@project_id_option()
@billing_project_option()
@click.option(
    "--dry_run/--no_dry_run",
    "--dry-run/--no-dry-run",
    help="Dry run the initialization",
)
@parallelism_option(default=DEFAULT_INIT_PARALLELISM)
@click.option(
    "--skip-existing",
    "--skip_existing",
    help="Skip initialization for existing artifacts, "
    "otherwise initialization is run for empty tables.",
    default=False,
    is_flag=True,
)
@click.option(
    "--force/--noforce",
    help="Run the initialization even if the destination table contains data.",
    default=False,
)
@click.pass_context
def initialize(
    ctx,
    name,
    sql_dir,
    project_id,
    billing_project,
    dry_run,
    parallelism,
    skip_existing,
    force,
):
    """Create the destination table for the provided query."""
    if not is_authenticated():
        click.echo("Authentication required for creating tables.", err=True)
        sys.exit(1)

    if Path(name).exists():
        # allow name to be a path
        query_files = [Path(name)]
    else:
        file_regex = re.compile(
            r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+(_v[0-9]+)?)/"
            r"(?:query\.sql|init\.sql|materialized_view\.sql)$"
        )
        query_files = paths_matching_name_pattern(
            name, sql_dir, project_id, file_regex=file_regex
        )

    if not query_files:
        click.echo(
            f"Couldn't find directory matching `{name}`. Failed to initialize query.",
            err=True,
        )
        sys.exit(1)

    def _initialize(query_file):
        project, dataset, destination_table = extract_from_query_path(query_file)
        client = bigquery.Client(project=project)
        full_table_id = f"{project}.{dataset}.{destination_table}"
        table = None

        sql_content = query_file.read_text()
        materialized_views = list(
            map(
                Path,
                glob(f"{query_file.parent}/**/materialized_view.sql", recursive=True),
            )
        )

        # check if the provided file can be initialized and whether existing ones should be skipped
        if "is_init()" in sql_content:
            try:
                table = client.get_table(full_table_id)
                if skip_existing:
                    # table exists; skip initialization
                    return
                if not force and table.num_rows > 0:
                    raise click.ClickException(
                        f"Table {full_table_id} already exists and contains data. The initialization process is terminated."
                        " Use --force to overwrite the existing destination table."
                    )
            except NotFound:
                # continue with creating the table
                pass
        elif len(materialized_views) == 0:
            return

        try:
            # Enable initialization from query.sql files
            # Create the table by deploying the schema and metadata, then run the init.
            # This does not currently verify the accuracy of the schema or that it
            # matches the query.
            if "is_init()" in sql_content:
                if not table:
                    ctx.invoke(
                        update,
                        name=full_table_id,
                        sql_dir=sql_dir,
                        project_id=project,
                        update_downstream=False,
                        is_init=True,
                    )

                    ctx.invoke(
                        deploy,
                        name=full_table_id,
                        sql_dir=sql_dir,
                        project_id=project,
                        force=True,
                        respect_dryrun_skip=False,
                    )

                arguments = [
                    "query",
                    "--use_legacy_sql=false",
                    "--format=none",
                    "--append_table",
                    "--noreplace",
                ]
                if dry_run:
                    arguments += ["--dry_run"]

                if "@sample_id" in sql_content:
                    sample_ids = list(range(0, 100))

                    _initialize_in_parallel(
                        project=project,
                        table=full_table_id,
                        dataset=dataset,
                        query_file=query_file,
                        arguments=arguments,
                        parallelism=parallelism,
                        sample_ids=sample_ids,
                        addl_templates={
                            "is_init": lambda: True,
                        },
                        billing_project=billing_project,
                    )
                else:
                    _run_query(
                        query_files=[query_file],
                        project_id=project,
                        public_project_id=None,
                        destination_table=full_table_id,
                        dataset_id=dataset,
                        query_arguments=arguments,
                        addl_templates={
                            "is_init": lambda: True,
                        },
                        billing_project=billing_project,
                    )
            else:
                for file in materialized_views:
                    with open(file) as init_file_stream:
                        init_sql = init_file_stream.read()
                        job_config = bigquery.QueryJobConfig(
                            dry_run=dry_run,
                            default_dataset=f"{project}.{dataset}",
                        )

                        # only deploy materialized view if it doesn't exist
                        # TODO: https://github.com/mozilla/bigquery-etl/issues/5804
                        try:
                            materialized_view_table = client.get_table(full_table_id)

                            # Best-effort check, don't fail if there's an error
                            try:
                                has_changes = materialized_view_has_changes(
                                    materialized_view_table.mview_query, init_sql
                                )
                            except Exception as e:
                                change_str = f"failed to compare changes: {e}"
                            else:
                                change_str = (
                                    "sql changed" if has_changes else "sql not changed"
                                )
                            click.echo(
                                f"Skipping materialized view {full_table_id}, already exists, {change_str}"
                            )
                        except NotFound:
                            job = client.query(init_sql, job_config=job_config)

                            if not dry_run:
                                job.result()
        except Exception:
            print_exc()
            return query_file

    with ThreadPool(parallelism) as pool:
        failed_initializations = [r for r in pool.map(_initialize, query_files) if r]

    if len(failed_initializations) > 0:
        click.echo("The following tables could not be deployed:", err=True)
        for failed_deploy in failed_initializations:
            click.echo(failed_deploy, err=True)
        sys.exit(1)


def materialized_view_has_changes(deployed_sql: str, file_sql: str) -> bool:
    """Return true if the sql in the materialized view file doesn't match the deployed sql."""
    file_sql_formatted = sqlparse.format(
        re.sub(
            r"CREATE+(?:\s+OR\s+REPLACE)?\s+MATERIALIZED\s+VIEW.*?AS",
            "",
            file_sql,
            flags=re.DOTALL,
        ),
        strip_comments=True,
        strip_whitespace=True,
    )

    deployed_sql_formatted = sqlparse.format(
        deployed_sql,
        strip_comments=True,
        strip_whitespace=True,
    )
    return file_sql_formatted != deployed_sql_formatted


@query.command(
    help="""Render a query Jinja template.

    Examples:

    ./bqetl query render telemetry_derived.ssl_ratios_v1 \\
      --output-dir=/tmp
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@sql_dir_option
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to. "
    + "If not specified, rendered queries are printed to console.",
    type=click.Path(file_okay=False),
    required=False,
)
@parallelism_option()
def render(name, sql_dir, output_dir, parallelism):
    """Render a query Jinja template."""
    if name is None:
        name = "*.*"

    query_files = paths_matching_name_pattern(name, sql_dir, project_id=None)
    resolved_sql_dir = Path(sql_dir).resolve()

    with Pool(parallelism) as p:
        p.map(partial(_render_query, output_dir, resolved_sql_dir), query_files)


def _render_query(output_dir, resolved_sql_dir, query_file):
    table_name = query_file.parent.name
    dataset_id = query_file.parent.parent.name
    project_id = query_file.parent.parent.parent.name

    jinja_params = {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_name": table_name,
    }

    rendered_sql = (
        render_template(
            query_file.name,
            template_folder=query_file.parent,
            templates_dir="",
            format=False,
            **jinja_params,
        )
        + "\n"
    )

    if not any(s in str(query_file) for s in skip_format()):
        rendered_sql = reformat(rendered_sql, trailing_newline=True)

    if output_dir:
        output_file = output_dir / query_file.resolve().relative_to(resolved_sql_dir)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(rendered_sql)
    else:
        click.echo(query_file)
        click.echo(rendered_sql)


def _parse_partition_setting(partition_date):
    params = partition_date.split(":")
    if len(params) != 3:
        return None

    # Check date format
    try:
        datetime.datetime.strptime(params[2], "%Y-%m-%d").date()
    except ValueError:
        return None

    # Check column name
    if re.match(r"^\w+$", params[0]):
        return {params[0]: params[2]}


def _validate_partition_date(ctx, param, partition_date):
    """Process the CLI parameter check_date and set the parameter for BigQuery."""
    # Will be None if launched from Airflow.  Also ctx.args is not populated at this stage.
    if partition_date:
        parsed = _parse_partition_setting(partition_date)
        if parsed is None:
            raise click.BadParameter("Format must be <column-name>::<yyyy-mm-dd>")
        return parsed
    return None


def _parse_check_output(output: str) -> str:
    output = output.replace("\n", " ")
    if "ETL Data Check Failed:" in output:
        return f"ETL Data Check Failed:{output.split('ETL Data Check Failed:')[1]}"
    return output


@query.group(help="Commands for managing query schemas.")
def schema():
    """Create the CLI group for the query schema command."""
    pass


@schema.command(
    help="""
    Update the query schema based on the destination table schema and the query schema.
    If no schema.yaml file exists for a query, one will be created.

    Examples:

    ./bqetl query schema update telemetry_derived.clients_daily_v6

    # Update schema including downstream dependencies (requires GCP)
    ./bqetl query schema update telemetry_derived.clients_daily_v6 --update-downstream
    """,
)
@click.argument("name", nargs=-1)
@sql_dir_option
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project ID",
    default=ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod"),
    callback=is_valid_project,
)
@click.option(
    "--update-downstream",
    "--update_downstream",
    help="Update downstream dependencies. GCP authentication required.",
    default=False,
    is_flag=True,
)
@click.option(
    "--tmp-dataset",
    "--tmp_dataset",
    help="GCP datasets for creating updated tables temporarily.",
    default="tmp",
)
@use_cloud_function_option
@respect_dryrun_skip_option(default=True)
@parallelism_option()
@click.option(
    "--is-init",
    "--is_init",
    help="Indicates whether the `is_init()` condition should be set to true of false.",
    is_flag=True,
    default=False,
)
def update(
    name,
    sql_dir,
    project_id,
    update_downstream,
    tmp_dataset,
    use_cloud_function,
    respect_dryrun_skip,
    parallelism,
    is_init,
):
    """CLI command for generating the query schema."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    query_files = paths_matching_name_pattern(
        name, sql_dir, project_id, files=["query.sql"]
    )
    # skip updating schemas that are not to be deployed
    query_files = [
        query_file
        for query_file in query_files
        if str(query_file)
        not in ConfigLoader.get("schema", "deploy", "skip", fallback=[])
    ]
    dependency_graph = get_dependency_graph([sql_dir], without_views=True)
    manager = multiprocessing.Manager()
    tmp_tables = manager.dict({})

    # order query files to make sure derived_from dependencies are resolved
    query_file_graph = {}
    for query_file in query_files:
        query_file_graph[query_file] = []
        try:
            metadata = Metadata.of_query_file(str(query_file))
            if metadata and metadata.schema and metadata.schema.derived_from:
                for derived_from in metadata.schema.derived_from:
                    parent_queries = [
                        query
                        for query in paths_matching_name_pattern(
                            ".".join(derived_from.table), sql_dir, project_id
                        )
                    ]

                    if len(parent_queries) > 0:
                        query_file_graph[query_file].append(parent_queries[0])

        except FileNotFoundError:
            query_file_graph[query_file] = []

    credentials = get_credentials()
    id_token = get_id_token(credentials=credentials)

    ts = ParallelTopologicalSorter(
        query_file_graph, parallelism=parallelism, with_follow_up=update_downstream
    )
    ts.map(
        partial(
            _update_query_schema_with_downstream,
            sql_dir,
            project_id,
            tmp_dataset,
            dependency_graph,
            tmp_tables,
            use_cloud_function,
            respect_dryrun_skip,
            update_downstream,
            is_init=is_init,
            credentials=credentials,
            id_token=id_token,
        )
    )

    if len(tmp_tables) > 0:
        client = bigquery.Client()
        # delete temporary tables
        for _, table in tmp_tables.items():
            client.delete_table(table, not_found_ok=True)


def _update_query_schema_with_downstream(
    sql_dir,
    project_id,
    tmp_dataset,
    dependency_graph,
    tmp_tables={},
    use_cloud_function=True,
    respect_dryrun_skip=True,
    update_downstream=False,
    query_file=None,
    follow_up_queue=None,
    is_init=False,
    credentials=None,
    id_token=None,
):
    try:
        changed = _update_query_schema(
            query_file,
            sql_dir,
            project_id,
            tmp_dataset,
            tmp_tables,
            use_cloud_function,
            respect_dryrun_skip,
            is_init,
            credentials,
            id_token,
        )

        if update_downstream:
            # update downstream dependencies
            if changed:
                if not is_authenticated():
                    click.echo(
                        "Cannot update downstream dependencies."
                        "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
                        "and check that the project is set correctly."
                    )
                    sys.exit(1)

                project, dataset, table = extract_from_query_path(query_file)
                identifier = f"{project}.{dataset}.{table}"
                tmp_identifier = f"{project}.{tmp_dataset}.{table}_{random_str(12)}"

                # create temporary table with updated schema
                if identifier not in tmp_tables:
                    schema = Schema.from_schema_file(query_file.parent / SCHEMA_FILE)
                    schema.deploy(tmp_identifier)
                    tmp_tables[identifier] = tmp_identifier

                # get downstream dependencies that will be updated in the next iteration
                dependencies = [
                    p
                    for k, refs in dependency_graph.items()
                    for p in paths_matching_name_pattern(
                        k, sql_dir, project_id, files=("query.sql",)
                    )
                    if identifier in refs
                ]

                for d in dependencies:
                    click.echo(f"Update downstream dependency schema for {d}")
                    if follow_up_queue:
                        follow_up_queue.put(d)
    except Exception:
        print_exc()


def _update_query_schema(
    query_file,
    sql_dir,
    project_id,
    tmp_dataset,
    tmp_tables={},
    use_cloud_function=True,
    respect_dryrun_skip=True,
    is_init=False,
    credentials=None,
    id_token=None,
):
    """
    Update the schema of a specific query file.

    Return True if the schema changed, False if it is unchanged.
    """
    if respect_dryrun_skip and str(query_file) in DryRun.skipped_files():
        click.echo(f"{query_file} dry runs are skipped. Cannot update schemas.")
        return

    tmp_tables = copy.deepcopy(tmp_tables)
    query_file_path = Path(query_file)
    existing_schema_path = query_file_path.parent / SCHEMA_FILE
    project_name, dataset_name, table_name = extract_from_query_path(query_file_path)

    try:
        metadata = Metadata.of_query_file(str(query_file_path))
    except FileNotFoundError:
        metadata = None
        click.echo(f"No metadata defined for {query_file_path}")

    # pull in updates from parent schemas
    if metadata and metadata.schema and metadata.schema.derived_from:
        for derived_from in metadata.schema.derived_from:
            parent_queries = [
                query
                for query in paths_matching_name_pattern(
                    ".".join(derived_from.table), sql_dir, project_id
                )
            ]

            if len(parent_queries) == 0:
                click.echo(
                    f"derived_from query {derived_from.table} does not exist.",
                    err=True,
                )
            else:
                parent_schema = Schema.from_schema_file(
                    parent_queries[0].parent / SCHEMA_FILE
                )
                parent_project, parent_dataset, parent_table = extract_from_query_path(
                    parent_queries[0]
                )
                parent_identifier = f"{parent_project}.{parent_dataset}.{parent_table}"

                if parent_identifier not in tmp_tables:
                    tmp_parent_identifier = (
                        f"{parent_project}.{tmp_dataset}.{parent_table}_"
                        + random_str(12)
                    )
                    parent_schema.deploy(tmp_parent_identifier)
                    tmp_tables[parent_identifier] = tmp_parent_identifier

                if existing_schema_path.is_file():
                    existing_schema = Schema.from_schema_file(existing_schema_path)
                else:
                    existing_schema = Schema.empty()

                existing_schema.merge(parent_schema, exclude=derived_from.exclude)

                # use temporary table
                tmp_identifier = (
                    f"{project_name}.{tmp_dataset}.{table_name}_{random_str(12)}"
                )
                existing_schema.deploy(tmp_identifier)
                tmp_tables[f"{project_name}.{dataset_name}.{table_name}"] = (
                    tmp_identifier
                )
                existing_schema.to_yaml_file(existing_schema_path)

    # replace temporary table references
    sql_content = render_template(
        query_file_path.name,
        template_folder=str(query_file_path.parent),
        templates_dir="",
        format=False,
        **{"is_init": lambda: is_init},
    )

    for orig_table, tmp_table in tmp_tables.items():
        table_parts = orig_table.split(".")
        for i in range(len(table_parts)):
            if ".".join(table_parts[i:]) in sql_content:
                sql_content = sql_content.replace(".".join(table_parts[i:]), tmp_table)
                break

    query_schema = None
    try:
        query_schema = Schema.from_query_file(
            query_file_path,
            content=sql_content,
            use_cloud_function=use_cloud_function,
            respect_skip=respect_dryrun_skip,
            sql_dir=sql_dir,
            credentials=credentials,
            id_token=id_token,
        )
    except Exception:
        if not existing_schema_path.exists():
            click.echo(
                click.style(
                    f"Cannot automatically update {query_file_path}. "
                    f"Please update {query_file_path / SCHEMA_FILE} manually.",
                    fg="red",
                ),
                err=True,
            )
            return

    # update bigquery metadata
    try:
        client = bigquery.Client(credentials=credentials)
        table = client.get_table(f"{project_name}.{dataset_name}.{table_name}")
        metadata_file_path = query_file_path.parent / METADATA_FILE

        if (
            table.time_partitioning
            and metadata
            and (
                metadata.bigquery is None or metadata.bigquery.time_partitioning is None
            )
        ):
            metadata.set_bigquery_partitioning(
                field=table.time_partitioning.field,
                partition_type=table.time_partitioning.type_.lower(),
                required=table.time_partitioning.require_partition_filter,
                expiration_days=(
                    table.time_partitioning.expiration_ms / 86400000.0
                    if table.time_partitioning.expiration_ms
                    else None
                ),
            )
            click.echo(f"Partitioning metadata added to {metadata_file_path}")

        if (
            table.clustering_fields
            and metadata
            and (metadata.bigquery is None or metadata.bigquery.clustering is None)
        ):
            metadata.set_bigquery_clustering(table.clustering_fields)
            click.echo(f"Clustering metadata added to {metadata_file_path}")

        if metadata:
            metadata.write(metadata_file_path)
    except NotFound:
        click.echo(
            f"Destination table {project_name}.{dataset_name}.{table_name} "
            "does not exist in BigQuery. Run bqetl query schema deploy "
            "<dataset>.<table> to create the destination table."
        )
    except FileNotFoundError:
        click.echo(
            f"No metadata file for {project_name}.{dataset_name}.{table_name}."
            " Skip schema update."
        )
        return

    partitioned_by = None
    try:
        metadata = Metadata.of_query_file(query_file_path)

        if metadata.bigquery and metadata.bigquery.time_partitioning:
            partitioned_by = metadata.bigquery.time_partitioning.field
    except FileNotFoundError:
        pass

    table_schema = Schema.for_table(
        project_name,
        dataset_name,
        table_name,
        partitioned_by=partitioned_by,
        use_cloud_function=use_cloud_function,
        respect_skip=respect_dryrun_skip,
        credentials=credentials,
        id_token=id_token,
    )

    changed = True

    if existing_schema_path.is_file():
        existing_schema = Schema.from_schema_file(existing_schema_path)
        old_schema = copy.deepcopy(existing_schema)
        if table_schema:
            existing_schema.merge(table_schema)

        if query_schema:
            existing_schema.merge(query_schema)
        existing_schema.to_yaml_file(existing_schema_path)
        changed = not existing_schema.equal(old_schema)
    else:
        query_schema.merge(table_schema)
        query_schema.to_yaml_file(existing_schema_path)

    click.echo(f"Schema {existing_schema_path} updated.")
    return changed


@schema.command(
    help="""Deploy the query schema.

    Examples:

    ./bqetl query schema deploy telemetry_derived.clients_daily_v6
    """,
)
@click.argument("name", nargs=-1)
@sql_dir_option
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project ID",
    default=ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod"),
    callback=is_valid_project,
)
@click.option(
    "--force/--noforce",
    help="Deploy the schema file without validating that it matches the query",
    default=False,
)
@use_cloud_function_option
@respect_dryrun_skip_option(default=True)
@click.option(
    "--skip-existing",
    "--skip_existing",
    help="Skip updating existing tables. "
    + "This option ensures that only new tables get deployed.",
    default=False,
    is_flag=True,
)
@click.option(
    "--skip-external-data",
    "--skip_external_data",
    help="Skip publishing external data, such as Google Sheets.",
    default=False,
    is_flag=True,
)
@click.option(
    "--destination_table",
    "--destination-table",
    required=False,
    help=(
        "Destination table name results are written to. "
        + "If not set, determines destination table based on query.  "
        + "Must be fully qualified (project.dataset.table)."
    ),
)
@parallelism_option()
@click.pass_context
def deploy(
    ctx,
    name,
    sql_dir,
    project_id,
    force,
    use_cloud_function,
    respect_dryrun_skip,
    skip_existing,
    skip_external_data,
    destination_table,
    parallelism,
):
    """CLI command for deploying destination table schemas."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    query_files = paths_matching_name_pattern(
        name, sql_dir, project_id, ["query.*", "script.sql"]
    )

    metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id, ["metadata.yaml"]
    )

    if not query_files and not metadata_files:
        # run SQL generators if no matching query has been found
        ctx.invoke(
            generate_all,
            output_dir=ctx.obj["TMP_DIR"],
            ignore=["derived_view_schemas", "stable_views"],
        )
        query_files = paths_matching_name_pattern(
            name, ctx.obj["TMP_DIR"], project_id, ["query.*", "script.sql"]
        )
        metadata_files = paths_matching_name_pattern(
            name, ctx.obj["TMP_DIR"], project_id, ["metadata.yaml"]
        )
        if not query_files and not metadata_files:
            raise click.ClickException(f"No queries matching `{name}` were found.")

    credentials = get_credentials()
    id_token = get_id_token(credentials=credentials)

    query_file_paths = [query_file.parent for query_file in query_files]
    metadata_files_without_query_file = [
        metadata_file
        for metadata_file in metadata_files
        if metadata_file.parent not in query_file_paths
        and not any(
            file.suffix == ".sql" or file.name == "query.py"
            for file in metadata_file.parent.iterdir()
            if file.is_file()
        )
    ]

    _deploy = partial(
        deploy_table,
        destination_table=destination_table,
        force=force,
        use_cloud_function=use_cloud_function,
        skip_existing=skip_existing,
        skip_external_data=skip_external_data,
        respect_dryrun_skip=respect_dryrun_skip,
        sql_dir=sql_dir,
        credentials=credentials,
        id_token=id_token,
    )

    failed_deploys, skipped_deploys, external_deploys = [], [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        future_to_query = {
            executor.submit(_deploy, artifact_file): artifact_file
            for artifact_file in query_files + metadata_files_without_query_file
            if str(artifact_file)
            not in ConfigLoader.get("schema", "deploy", "skip", fallback=[])
        }
        for future in futures.as_completed(future_to_query):
            artifact_file = future_to_query[future]
            try:
                future.result()
            except SkippedDeployException as e:
                print(f"Skipped deploy for {artifact_file}: ({e})")
                skipped_deploys.append(artifact_file)
            except FailedDeployException as e:
                print(f"Failed deploy for {artifact_file}: ({e})")
                failed_deploys.append(artifact_file)
            except SkippedExternalDataException as e:
                print(f"Skipping deploy for external data table {artifact_file}: ({e})")
                external_deploys.append(artifact_file)
            else:
                print(f"{artifact_file} successfully deployed!")

    if skipped_deploys:
        click.echo("The following deploys were skipped:")
        for skipped_deploy in skipped_deploys:
            click.echo(skipped_deploy)

    if external_deploys:
        click.echo("The following deploys of external data tables were skipped:")
        for external_deploy in external_deploys:
            click.echo(external_deploy)

    if failed_deploys:
        click.echo("The following tables could not be deployed:")
        for failed_deploy in failed_deploys:
            click.echo(failed_deploy)
        sys.exit(1)


def _validate_schema_from_path(
    query_file_path,
    use_cloud_function=True,
    respect_dryrun_skip=True,
    credentials=None,
    id_token=None,
):
    """Dry Runs and validates a query schema from its path."""
    return (
        DryRun(
            query_file_path,
            use_cloud_function=use_cloud_function,
            respect_skip=respect_dryrun_skip,
            credentials=credentials,
            id_token=id_token,
        ).validate_schema(),
        query_file_path,
    )


@schema.command(
    help="""Validate the query schema

    Examples:

    ./bqetl query schema validate telemetry_derived.clients_daily_v6
    """,
    name="validate",
)
@click.argument("name")
@sql_dir_option
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project ID",
    default=ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod"),
    callback=is_valid_project,
)
@use_cloud_function_option
@respect_dryrun_skip_option(default=True)
@click.pass_context
def validate_schema(
    ctx, name, sql_dir, project_id, use_cloud_function, respect_dryrun_skip
):
    """Validate the defined query schema with the query and the destination table."""
    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    if query_files == []:
        # run SQL generators if no matching query has been found
        ctx.invoke(
            generate_all,
            output_dir=ctx.obj["TMP_DIR"],
            ignore=["derived_view_schemas", "stable_views"],
        )
        query_files = paths_matching_name_pattern(name, ctx.obj["TMP_DIR"], project_id)
        if query_files == []:
            raise click.ClickException(f"No queries matching `{name}` were found.")

    credentials = get_credentials()
    id_token = get_id_token(credentials=credentials)

    _validate_schema = partial(
        _validate_schema_from_path,
        use_cloud_function=use_cloud_function,
        respect_dryrun_skip=respect_dryrun_skip,
        credentials=credentials,
        id_token=id_token,
    )

    with Pool(8) as p:
        result = p.map(_validate_schema, query_files, chunksize=1)

    all_valid = True

    for is_valid, query_file_path in result:
        if is_valid is False:
            if all_valid:
                click.echo("\nSchemas for the following queries are invalid:")
            all_valid = False
            click.echo(query_file_path)

    if not all_valid:
        sys.exit(1)
    else:
        click.echo("\nAll schemas are valid.")
