"""bigquery-etl CLI query command."""

import copy
import datetime
import logging
import multiprocessing
import os
import re
import string
import subprocess
import sys
import tempfile
import typing
from datetime import date, timedelta
from functools import partial
from multiprocessing.pool import Pool, ThreadPool
from pathlib import Path
from tempfile import NamedTemporaryFile
from timeit import default_timer
from traceback import print_exc

import click
import yaml
from dateutil.rrule import MONTHLY, rrule
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, PreconditionFailed

from ..backfill.utils import QUALIFIED_TABLE_NAME_RE, qualified_table_name_matching
from ..cli.format import format
from ..cli.utils import (
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
from ..dryrun import DryRun
from ..format_sql.format import skip_format
from ..format_sql.formatter import reformat
from ..metadata import validate_metadata
from ..metadata.parse_metadata import (
    METADATA_FILE,
    BigQueryMetadata,
    ClusteringMetadata,
    DatasetMetadata,
    ExternalDataFormat,
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
DEFAULT_PARALLELISM = 10


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
    "--init",
    "-i",
    help="Create an init.sql file to initialize the table",
    default=False,
    is_flag=True,
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
def create(ctx, name, sql_dir, project_id, owner, init, dag, no_schedule):
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
        # create a directory for the corresponding view
        derived_path = path / project_id / dataset / (name + version)
        derived_path.mkdir(parents=True)

        view_path = path / project_id / dataset.replace("_derived", "") / name
        view_path.mkdir(parents=True)
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

    if view_path:
        click.echo(f"Created corresponding view in {view_path}")
        view_file = view_path / "view.sql"
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

    # optionally create init.sql
    if init:
        init_file = derived_path / "init.sql"
        init_file.write_text(
            reformat(
                f"""
                -- SQL for initializing the query destination table.
                CREATE OR REPLACE TABLE
                  `{ConfigLoader.get('default', 'project', fallback="moz-fx-data-shared-prod")}.{dataset}.{name}{version}`
                AS SELECT * FROM table"""
            )
            + "\n"
        )

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

    dags_to_be_generated = set()

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
            dags_to_be_generated.add(dag)
        else:
            dags = get_dags(None, sql_dir.parent / "dags.yaml", sql_dir=sql_dir)
            if metadata.scheduling == {}:
                click.echo(f"No scheduling information for: {query_file}", err=True)
                sys.exit(1)
            else:
                dags_to_be_generated.add(metadata.scheduling["dag_name"])

    # re-run DAG generation for the affected DAG
    for d in dags_to_be_generated:
        existing_dag = dags.dag_by_name(d)
        logging.info(f"Running DAG generation for {existing_dag.name}")
        output_dir = sql_dir.parent / "dags"
        dags.dag_to_airflow(output_dir, existing_dag)


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
@click.option("--cost", help="Include information about query costs", is_flag=True)
@click.option(
    "--last_updated",
    help="Include timestamps when destination tables were last updated",
    is_flag=True,
)
@click.pass_context
def info(ctx, name, sql_dir, project_id, cost, last_updated):
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

        if cost or last_updated:
            if not is_authenticated():
                click.echo(
                    "Authentication to GCP required for "
                    "accessing cost and last_updated."
                )
            else:
                client = bigquery.Client()
                end_date = date.today().strftime("%Y-%m-%d")
                start_date = (date.today() - timedelta(7)).strftime("%Y-%m-%d")
                result = client.query(
                    f"""
                    SELECT
                        SUM(cost_usd) AS cost,
                        MAX(creation_time) AS last_updated
                    FROM `moz-fx-data-shared-prod.monitoring_derived.bigquery_etl_scheduled_queries_cost_v1`
                    WHERE submission_date BETWEEN '{start_date}' AND '{end_date}'
                        AND dataset = '{dataset}'
                        AND table = '{table}'
                """  # noqa E501
                ).result()

                if result.total_rows == 0:
                    if last_updated:
                        click.echo("last_updated: never")
                    if cost:
                        click.echo("Cost over the last 7 days: none")

                for row in result:
                    if last_updated:
                        click.echo(f"  last_updated: {row.last_updated}")
                    if cost:
                        click.echo(
                            f"  Cost over the last 7 days: {round(row.cost, 2)} USD"
                        )
        click.echo("")


def _backfill_query(
    query_file_path,
    project_id,
    date_partition_parameter,
    exclude,
    max_rows,
    dry_run,
    no_partition,
    args,
    partitioning_type,
    backfill_date,
    destination_table,
):
    """Run a query backfill for a specific date."""
    project, dataset, table = extract_from_query_path(query_file_path)

    match partitioning_type:
        case PartitionType.DAY:
            partition = backfill_date.strftime("%Y%m%d")
        case PartitionType.MONTH:
            partition = backfill_date.strftime("%Y%m")
        case _:
            raise ValueError(f"Unsupported partitioning type: {partitioning_type}")

    backfill_date = backfill_date.strftime("%Y-%m-%d")
    if backfill_date not in exclude:
        if destination_table is None:
            destination_table = f"{project}.{dataset}.{table}"

        if not no_partition:
            destination_table = f"{destination_table}${partition}"

        if not QUALIFIED_TABLE_NAME_RE.match(destination_table):
            click.echo(
                "Destination table must be named like:" + " <project>.<dataset>.<table>"
            )
            sys.exit(1)

        click.echo(
            f"Run backfill for {destination_table} "
            f"with @{date_partition_parameter}={backfill_date}"
        )

        arguments = [
            "query",
            f"--parameter={date_partition_parameter}:DATE:{backfill_date}",
            "--use_legacy_sql=false",
            "--replace",
            f"--max_rows={max_rows}",
            f"--project_id={project_id}",
            "--format=none",
        ] + args
        if dry_run:
            arguments += ["--dry_run"]

        _run_query(
            [query_file_path],
            project_id=project_id,
            dataset_id=dataset,
            destination_table=destination_table,
            public_project_id=ConfigLoader.get(
                "default", "public_project", fallback="mozilla-public-data"
            ),
            query_arguments=arguments,
        )

    else:
        click.echo(
            f"Skip {query_file_path} with @{date_partition_parameter}={backfill_date}"
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
    "--no_partition",
    "--no-partition",
    is_flag=True,
    default=False,
    help="Disable writing results to a partition. Overwrites entire destination table.",
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
@click.pass_context
def backfill(
    ctx,
    name,
    sql_dir,
    project_id,
    start_date,
    end_date,
    exclude,
    dry_run,
    max_rows,
    parallelism,
    no_partition,
    destination_table,
):
    """Run a backfill."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
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

    dates = [start_date + timedelta(i) for i in range((end_date - start_date).days + 1)]

    for query_file in query_files:
        query_file_path = Path(query_file)

        depends_on_past = False
        date_partition_parameter = "submission_date"

        try:
            metadata = Metadata.of_query_file(str(query_file_path))
            depends_on_past = metadata.scheduling.get(
                "depends_on_past", depends_on_past
            )
            date_partition_parameter = metadata.scheduling.get(
                "date_partition_parameter", date_partition_parameter
            )

            # For backwards compatibility assume partitioning type is day
            # in case metadata is missing
            if metadata.bigquery:
                partitioning_type = metadata.bigquery.time_partitioning.type
            else:
                partitioning_type = PartitionType.DAY
                click.echo(
                    "Bigquery partitioning type not set. Using PartitionType.DAY"
                )

            match partitioning_type:
                case PartitionType.DAY:
                    dates = [
                        start_date + timedelta(i)
                        for i in range((end_date - start_date).days + 1)
                    ]
                case PartitionType.MONTH:
                    dates = list(
                        rrule(
                            freq=MONTHLY,
                            dtstart=start_date.replace(day=1),
                            until=end_date,
                        )
                    )
                    # Dates in excluded must be the first day of the month to match `dates`
                    exclude = [
                        date.fromisoformat(day).replace(day=1).strftime("%Y-%m-%d")
                        for day in exclude
                    ]
                case _:
                    raise ValueError(
                        f"Unsupported partitioning type: {partitioning_type}"
                    )

        except FileNotFoundError:
            click.echo(f"No metadata defined for {query_file_path}")

        if depends_on_past and exclude != []:
            click.echo(
                f"Warning: depends_on_past = True for {query_file_path} but the"
                f"following dates will be excluded from the backfill: {exclude}"
            )

        client = bigquery.Client(project=project_id)
        try:
            project, dataset, table = extract_from_query_path(query_file_path)
            client.get_table(f"{project}.{dataset}.{table}")
        except NotFound:
            ctx.invoke(initialize, name=query_file, dry_run=dry_run)

        backfill_query = partial(
            _backfill_query,
            query_file_path,
            project_id,
            date_partition_parameter,
            exclude,
            max_rows,
            dry_run,
            no_partition,
            ctx.args,
            partitioning_type,
            destination_table=destination_table,
        )

        if not depends_on_past:
            # run backfill for dates in parallel if depends_on_past is false
            with Pool(parallelism) as p:
                result = p.map(backfill_query, dates, chunksize=1)
            if not all(result):
                sys.exit(1)
        else:
            # if data depends on previous runs, then execute backfill sequentially
            for backfill_date in dates:
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
    public_project_id,
    destination_table,
    dataset_id,
):
    """Run a query."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
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

    _run_query(
        query_files,
        project_id,
        public_project_id,
        destination_table,
        dataset_id,
        ctx.args,
    )


def _run_query(
    query_files,
    project_id,
    public_project_id,
    destination_table,
    dataset_id,
    query_arguments,
    addl_templates: typing.Optional[dict] = None,
    mapped_values=None,
    parallelized=None,
):
    client = bigquery.Client(project_id)
    """Run a query."""
    if dataset_id is not None:
        # dataset ID was parsed by argparse but needs to be passed as parameter
        # when running the query
        query_arguments.append("--dataset_id={}".format(dataset_id))

    if project_id is not None:
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
            logging.warning("No metadata.yaml found for {}", query_file)

        if not use_public_table and destination_table is not None:
            # destination table was parsed by argparse, however if it wasn't modified to
            # point to a public table it needs to be passed as parameter for the query

            if re.match(QUALIFIED_TABLE_NAME_RE, destination_table):
                project, dataset, table = qualified_table_name_matching(
                    destination_table
                )
                destination_table = "{}:{}.{}".format(project, dataset, table)

            query_arguments.append("--destination_table={}".format(destination_table))

        if bool(list(filter(lambda x: x.startswith("--parameter"), query_arguments))):
            # need to do this as parameters are not supported with legacy sql
            query_arguments.append("--use_legacy_sql=False")

        # this assumed query command should always be passed inside query_arguments
        if "query" not in query_arguments:
            query_arguments = ["query"] + query_arguments

        # write rendered query to a temporary file;
        # query string cannot be passed directly to bq as SQL comments will be interpreted as CLI arguments
        with tempfile.NamedTemporaryFile(mode="w+") as query_stream:
            query_stream.write(
                render_template(
                    query_file.name,
                    template_folder=str(query_file.parent),
                    templates_dir="",
                    format=False,
                    **addl_templates,
                )
            )
            query_stream.seek(0)

            if mapped_values is None or parallelized is False:
                # Run the query as shell command so that passed parameters can be used as is.
                subprocess.check_call(["bq"] + query_arguments, stdin=query_stream)
            else:
                query_content = query_stream.read()
                query = query_content.format(
                    mapped_values=mapped_values,
                )
                job = client.query(
                    query=query,
                    job_config=bigquery.QueryJobConfig(
                        use_query_cache=False,
                        use_legacy_sql=False,
                    ),
                )
                job.result()


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
):
    """Validate queries by dry running, formatting and checking scheduling configs."""
    if name is None:
        name = "*.*"

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    dataset_dirs = set()
    for query in query_files:
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

        validate_metadata.validate(query.parent)
        dataset_dirs.add(query.parent.parent)

    if no_dryrun:
        click.echo("Dry run skipped for query files.")

    for dataset_dir in dataset_dirs:
        validate_metadata.validate_datasets(dataset_dir)


def _initialize_in_parallel(
    project,
    table,
    dataset,
    query_file,
    arguments,
    parallelism,
    mapped_values,
    parallelized,
    addl_templates,
):
    with ThreadPool(parallelism) as pool:
        start = default_timer()
        # Process all subsets in the query in parallel (eg. all sample_ids).
        pool.map(
            partial(
                _run_query,
                [query_file],
                project,
                None,
                table,
                dataset,
                arguments,
                addl_templates,
                parallelized=parallelized,
            ),
            mapped_values,
        )
        print(f"Job completed in {default_timer() - start}")


@query.command(
    help="""Create and initialize the destination table for the query.
    Only for queries that have an `init.sql` file.

    Examples:

    ./bqetl query initialize telemetry_derived.ssl_ratios_v1
    """,
)
@click.argument("name")
@sql_dir_option
@project_id_option()
@click.option(
    "--dry_run/--no_dry_run",
    "--dry-run/--no-dry-run",
    help="Dry run the initialization",
)
@click.pass_context
def initialize(ctx, name, sql_dir, project_id, dry_run):
    """Create the destination table for the provided query."""
    client = bigquery.Client()
    parallelized = False
    mapped_values = list(range(0, 1))

    if not is_authenticated():
        click.echo("Authentication required for creating tables.", err=True)
        sys.exit(1)

    if Path(name).exists():
        # allow name to be a path
        query_files = [Path(name)]
    else:
        query_files = paths_matching_name_pattern(name, sql_dir, project_id)

    if not query_files:
        click.echo(
            f"Couldn't find directory matching `{name}`. Failed to initialize query.",
            err=True,
        )
        sys.exit(1)

    for query_file in query_files:
        sql_content = query_file.read_text()

        # Enable initialization from query.sql files
        # Create the table by deploying the schema and metadata, then run the init.
        # This does not currently verify the accuracy of the schema or that it
        # matches the query.
        if "is_init()" in sql_content:
            project = query_file.parent.parent.parent.name
            dataset = query_file.parent.parent.name
            destination_table = query_file.parent.name
            full_table_id = f"{project}.{dataset}.{destination_table}"

            try:
                client.get_table(full_table_id)
                raise PreconditionFailed(
                    f"Table {full_table_id} already exists. The initialization process is terminated."
                )
            except NotFound:
                ctx.invoke(deploy, name=full_table_id, force=True)

            query_file_path = Path(query_file)
            metadata = Metadata.of_query_file(query_file_path)
            if metadata and metadata.initialization:
                ini = metadata.initialization.get("from_sample_id", None)
                end = metadata.initialization.get("to_sample_id", None)
                # Running in parallel only when mapped_values are set in the query and metadata.
                if (
                    ini is not None
                    and end is not None
                    and "mapped_values" in sql_content
                ):
                    mapped_values = [
                        f"AND sample_id = {i}" for i in list(range(ini, end))
                    ]
                    parallelized = True

            arguments = [
                "query",
                "--use_legacy_sql=false",
                "--replace",
                "--format=none",
            ]
            if dry_run:
                arguments += ["--dry_run"]

            _initialize_in_parallel(
                project=project,
                table=destination_table,
                dataset=dataset,
                query_file=query_file,
                arguments=arguments,
                parallelism=DEFAULT_PARALLELISM,
                mapped_values=mapped_values,
                parallelized=parallelized,
                addl_templates={
                    "is_init": lambda: True,
                },
            )
        else:
            init_files = Path(query_file.parent).rglob("init.sql")

            for init_file in init_files:
                project = init_file.parent.parent.parent.name

                with open(init_file) as init_file_stream:
                    init_sql = init_file_stream.read()
                    dataset = Path(init_file).parent.parent.name
                    destination_table = query_file.parent.name
                    job_config = bigquery.QueryJobConfig(
                        dry_run=dry_run,
                        default_dataset=f"{project}.{dataset}",
                        destination=f"{project}.{dataset}.{destination_table}",
                    )

                    if "CREATE MATERIALIZED VIEW" in init_sql:
                        click.echo(f"Create materialized view for {init_file}")
                        # existing materialized view have to be deleted before re-creation
                        view_name = query_file.parent.name
                        client.delete_table(
                            f"{project}.{dataset}.{view_name}", not_found_ok=True
                        )
                    else:
                        click.echo(f"Create destination table for {init_file}")

                    job = client.query(init_sql, job_config=job_config)

                    if not dry_run:
                        job.result()


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
def render(name, sql_dir, output_dir):
    """Render a query Jinja template."""
    if name is None:
        name = "*.*"

    query_files = paths_matching_name_pattern(name, sql_dir, project_id=None)
    resolved_sql_dir = Path(sql_dir).resolve()
    for query_file in query_files:
        table_name = query_file.parent.name
        dataset_id = query_file.parent.parent.name
        project_id = query_file.parent.parent.parent.name

        jinja_params = {
            **{
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_name": table_name,
            },
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
            output_file = output_dir / query_file.resolve().relative_to(
                resolved_sql_dir
            )
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
@click.argument("name")
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
@parallelism_option
def update(
    name,
    sql_dir,
    project_id,
    update_downstream,
    tmp_dataset,
    use_cloud_function,
    respect_dryrun_skip,
    parallelism,
):
    """CLI command for generating the query schema."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    query_files = paths_matching_name_pattern(
        name, sql_dir, project_id, files=["query.sql"]
    )
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
        )

        if update_downstream:
            # update downstream dependencies
            if changed:
                if not is_authenticated():
                    click.echo(
                        "Cannot update downstream dependencies."
                        "Authentication to GCP required. Run `gcloud auth login` "
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
                tmp_tables[
                    f"{project_name}.{dataset_name}.{table_name}"
                ] = tmp_identifier
                existing_schema.to_yaml_file(existing_schema_path)

    # replace temporary table references
    sql_content = render_template(
        query_file_path.name,
        template_folder=str(query_file_path.parent),
        templates_dir="",
        format=False,
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
        client = bigquery.Client()
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
                expiration_days=table.time_partitioning.expiration_ms / 86400000.0
                if table.time_partitioning.expiration_ms
                else None,
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
        partitioned_by,
        use_cloud_function=use_cloud_function,
        respect_skip=respect_dryrun_skip,
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
@click.argument("name")
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
@parallelism_option
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
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    client = bigquery.Client()

    query_files = paths_matching_name_pattern(name, sql_dir, project_id, ["query.*"])
    if not query_files:
        # run SQL generators if no matching query has been found
        ctx.invoke(
            generate_all,
            output_dir=ctx.obj["TMP_DIR"],
            ignore=["derived_view_schemas", "stable_views"],
        )
        query_files = paths_matching_name_pattern(
            name, ctx.obj["TMP_DIR"], project_id, ["query.*"]
        )

    def _deploy(query_file):
        if respect_dryrun_skip and str(query_file) in DryRun.skipped_files():
            click.echo(f"{query_file} dry runs are skipped. Cannot validate schemas.")
            return

        query_file_path = Path(query_file)
        existing_schema_path = query_file_path.parent / SCHEMA_FILE

        if not existing_schema_path.is_file():
            click.echo(f"No schema file found for {query_file}")
            return

        try:
            table_name = query_file_path.parent.name
            dataset_name = query_file_path.parent.parent.name
            project_name = query_file_path.parent.parent.parent.name

            if destination_table:
                full_table_id = destination_table
            else:
                full_table_id = f"{project_name}.{dataset_name}.{table_name}"

            existing_schema = Schema.from_schema_file(existing_schema_path)

            if not force and str(query_file_path).endswith("query.sql"):
                query_schema = Schema.from_query_file(
                    query_file_path,
                    use_cloud_function=use_cloud_function,
                    respect_skip=respect_dryrun_skip,
                )
                if not existing_schema.equal(query_schema):
                    click.echo(
                        f"Query {query_file_path} does not match "
                        f"schema in {existing_schema_path}. "
                        f"To update the local schema file, "
                        f"run `./bqetl query schema update "
                        f"{dataset_name}.{table_name}`",
                        err=True,
                    )
                    sys.exit(1)

            with NamedTemporaryFile(suffix=".json") as tmp_schema_file:
                existing_schema.to_json_file(Path(tmp_schema_file.name))
                bigquery_schema = client.schema_from_json(tmp_schema_file.name)

            try:
                table = client.get_table(full_table_id)
            except NotFound:
                table = bigquery.Table(full_table_id)

            table.schema = bigquery_schema
            _attach_metadata(query_file_path, table)

            if not table.created:
                client.create_table(table)
                click.echo(f"Destination table {full_table_id} created.")
            elif not skip_existing:
                client.update_table(
                    table,
                    [
                        "schema",
                        "friendly_name",
                        "description",
                        "time_partitioning",
                        "clustering_fields",
                        "labels",
                    ],
                )
                click.echo(f"Schema (and metadata) updated for {full_table_id}.")
        except Exception:
            print_exc()
            return query_file

    with ThreadPool(parallelism) as pool:
        failed_deploys = [r for r in pool.map(_deploy, query_files) if r]

    if not skip_external_data:
        failed_external_deploys = _deploy_external_data(
            name, sql_dir, project_id, skip_existing
        )
        failed_deploys += failed_external_deploys

    if len(failed_deploys) > 0:
        click.echo("The following tables could not be deployed:")
        for failed_deploy in failed_deploys:
            click.echo(failed_deploy)
        sys.exit(1)

    click.echo("All tables have been deployed.")


def _attach_metadata(query_file_path: Path, table: bigquery.Table) -> None:
    """Add metadata from query file's metadata.yaml to table object."""
    try:
        metadata = Metadata.of_query_file(query_file_path)
    except FileNotFoundError:
        return

    table.description = metadata.description
    table.friendly_name = metadata.friendly_name

    if metadata.bigquery and metadata.bigquery.time_partitioning:
        table.time_partitioning = bigquery.TimePartitioning(
            metadata.bigquery.time_partitioning.type.bigquery_type,
            field=metadata.bigquery.time_partitioning.field,
            require_partition_filter=(
                metadata.bigquery.time_partitioning.require_partition_filter
            ),
            expiration_ms=metadata.bigquery.time_partitioning.expiration_ms,
        )

    if metadata.bigquery and metadata.bigquery.clustering:
        table.clustering_fields = metadata.bigquery.clustering.fields

    # BigQuery only allows for string type labels with specific requirements to be published:
    # https://cloud.google.com/bigquery/docs/labels-intro#requirements
    if metadata.labels:
        table.labels = {
            key: value
            for key, value in metadata.labels.items()
            if isinstance(value, str)
        }


def _deploy_external_data(
    name,
    sql_dir,
    project_id,
    skip_existing,
) -> list:
    """Publish external data tables."""
    # whether a table should be created from external data is defined in the metadata
    metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id, ["metadata.yaml"]
    )
    client = bigquery.Client()
    failed_deploys = []
    for metadata_file_path in metadata_files:
        metadata = Metadata.from_file(metadata_file_path)
        if not metadata.external_data:
            # skip all tables that are not created from external data
            continue

        existing_schema_path = metadata_file_path.parent / SCHEMA_FILE

        if not existing_schema_path.is_file():
            # tables created from external data must specify a schema
            click.echo(f"No schema file found for {metadata_file_path}")
            continue

        try:
            table_name = metadata_file_path.parent.name
            dataset_name = metadata_file_path.parent.parent.name
            project_name = metadata_file_path.parent.parent.parent.name
            full_table_id = f"{project_name}.{dataset_name}.{table_name}"

            existing_schema = Schema.from_schema_file(existing_schema_path)

            try:
                table = client.get_table(full_table_id)
            except NotFound:
                table = bigquery.Table(full_table_id)

            with NamedTemporaryFile(suffix=".json") as tmp_schema_file:
                existing_schema.to_json_file(Path(tmp_schema_file.name))
                bigquery_schema = client.schema_from_json(tmp_schema_file.name)

            table.schema = bigquery_schema
            _attach_metadata(metadata_file_path, table)

            if not table.created:
                if metadata.external_data.format in (
                    ExternalDataFormat.GOOGLE_SHEETS,
                    ExternalDataFormat.CSV,
                ):
                    external_config = bigquery.ExternalConfig(
                        metadata.external_data.format.value.upper()
                    )
                    external_config.source_uris = metadata.external_data.source_uris
                    external_config.ignore_unknown_values = True
                    external_config.autodetect = False

                    for key, v in metadata.external_data.options.items():
                        setattr(external_config.options, key, v)

                    table.external_data_configuration = external_config
                    table = client.create_table(table)
                    click.echo(f"Destination table {full_table_id} created.")

                else:
                    click.echo(
                        f"External data format {metadata.external_data.format} unsupported."
                    )
            elif not skip_existing:
                client.update_table(
                    table,
                    [
                        "schema",
                        "friendly_name",
                        "description",
                        "labels",
                    ],
                )
                click.echo(f"Schema (and metadata) updated for {full_table_id}.")
        except Exception:
            print_exc()
            failed_deploys.append(metadata_file_path)

    return failed_deploys


def _validate_schema_from_path(
    query_file_path, use_cloud_function=True, respect_dryrun_skip=True
):
    """Dry Runs and validates a query schema from its path."""
    return (
        DryRun(
            query_file_path,
            use_cloud_function=use_cloud_function,
            respect_skip=respect_dryrun_skip,
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

    _validate_schema = partial(
        _validate_schema_from_path,
        use_cloud_function=use_cloud_function,
        respect_dryrun_skip=respect_dryrun_skip,
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
