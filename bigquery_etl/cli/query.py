"""bigquery-etl CLI query command."""

import copy
import re
import string
import sys
from datetime import date, timedelta
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path
from tempfile import NamedTemporaryFile

import click
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..cli.format import format
from ..cli.utils import (
    is_authenticated,
    is_valid_project,
    paths_matching_name_pattern,
    project_id_option,
    respect_dryrun_skip_option,
    sql_dir_option,
    use_cloud_function_option,
)
from ..dependency import get_dependency_graph
from ..dryrun import SKIP, DryRun
from ..format_sql.formatter import reformat
from ..metadata import validate_metadata
from ..metadata.parse_metadata import (
    METADATA_FILE,
    DatasetMetadata,
    Metadata,
    BigQueryMetadata,
    PartitionMetadata,
    ClusteringMetadata,
    PartitionType,
)
from ..query_scheduling.dag_collection import DagCollection
from ..query_scheduling.generate_airflow_dags import get_dags
from ..run_query import run
from ..schema import SCHEMA_FILE, Schema
from ..util import extract_from_query_path
from ..util.common import random_str
from .dryrun import dryrun

QUERY_NAME_RE = re.compile(r"(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)")
VERSION_RE = re.compile(r"_v[0-9]+")


@click.group(help="Commands for managing queries.")
def query():
    """Create the CLI group for the query command."""
    pass


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
@project_id_option("moz-fx-data-shared-prod")
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
def create(name, sql_dir, project_id, owner, init):
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
                  `moz-fx-data-shared-prod.{dataset}.{name}{version}`
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
            print(
                f"Updated {query_file.parent / METADATA_FILE} with scheduling"
                " information. For more information about scheduling queries see: "
                "https://github.com/mozilla/bigquery-etl#scheduling-queries-in-airflow"
            )

            # update dags since new task has been added
            dags = get_dags(None, sql_dir.parent / "dags.yaml")
            dags_to_be_generated.add(dag)
        else:
            dags = get_dags(None, sql_dir.parent / "dags.yaml")
            if metadata.scheduling == {}:
                click.echo(f"No scheduling information for: {query_file}", err=True)
                sys.exit(1)
            else:
                dags_to_be_generated.add(metadata.scheduling["dag_name"])

    # re-run DAG generation for the affected DAG
    for d in dags_to_be_generated:
        existing_dag = dags.dag_by_name(d)
        print(f"Running DAG generation for {existing_dag.name}")
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
def info(name, sql_dir, project_id, cost, last_updated):
    """Return information about all or specific queries."""
    if name is None:
        name = "*.*"

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)

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
    exclude,
    max_rows,
    dry_run,
    no_partition,
    args,
    backfill_date,
):
    """Run a query backfill for a specific date."""
    project, dataset, table = extract_from_query_path(query_file_path)

    backfill_date = backfill_date.strftime("%Y-%m-%d")
    if backfill_date not in exclude:
        partition = backfill_date.replace("-", "")

        if no_partition:
            destination_table = table
        else:
            destination_table = f"{table}${partition}"

        click.echo(
            f"Run backfill for {project}.{dataset}.{destination_table} "
            f"with @submission_date={backfill_date}"
        )

        arguments = [
            "query",
            f"--parameter=submission_date:DATE:{backfill_date}",
            "--use_legacy_sql=false",
            "--replace",
            f"--max_rows={max_rows}",
            f"--project_id={project_id}",
        ] + args
        if dry_run:
            arguments += ["--dry_run"]

        run(query_file_path, dataset, destination_table, arguments)
    else:
        click.echo(f"Skip {query_file_path} with @submission_date={backfill_date}")

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
    help="Dates excluded from backfill. Date format: yyyy-mm-dd",
    default=[],
)
@click.option("--dry_run/--no_dry_run", help="Dry run the backfill")
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
):
    """Run a backfill."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    dates = [start_date + timedelta(i) for i in range((end_date - start_date).days + 1)]

    for query_file in query_files:
        query_file_path = Path(query_file)

        try:
            metadata = Metadata.of_query_file(str(query_file_path))
            depends_on_past = metadata.scheduling.get("depends_on_past", False)
        except FileNotFoundError:
            depends_on_past = False

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
            exclude,
            max_rows,
            dry_run,
            no_partition,
            ctx.args,
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
@click.pass_context
def validate(
    ctx,
    name,
    sql_dir,
    project_id,
    use_cloud_function,
    validate_schemas,
    respect_dryrun_skip,
):
    """Validate queries by dry running, formatting and checking scheduling configs."""
    if name is None:
        name = "*.*"

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    dataset_dirs = set()
    for query in query_files:
        project = query.parent.parent.parent.name
        ctx.invoke(format, paths=[str(query)])
        ctx.invoke(
            dryrun,
            paths=[str(query)],
            use_cloud_function=use_cloud_function,
            project=project,
            validate_schemas=validate_schemas,
            respect_skip=respect_dryrun_skip,
        )
        validate_metadata.validate(query.parent)
        dataset_dirs.add(query.parent.parent)

    for dataset_dir in dataset_dirs:
        validate_metadata.validate_datasets(dataset_dir)


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
    help="Dry run the initialization",
)
def initialize(name, sql_dir, project_id, dry_run):
    """Create the destination table for the provided query."""
    if not is_authenticated():
        click.echo("Authentication required for creating tables.", err=True)
        sys.exit(1)

    if Path(name).exists():
        # allow name to be a path
        query_files = [Path(name)]
    else:
        query_files = paths_matching_name_pattern(name, sql_dir, project_id)

    for query_file in query_files:
        init_files = Path(query_file.parent).rglob("init.sql")
        client = bigquery.Client()

        for init_file in init_files:
            project = init_file.parent.parent.parent.name

            with open(init_file) as init_file_stream:
                init_sql = init_file_stream.read()
                dataset = Path(init_file).parent.parent.name
                job_config = bigquery.QueryJobConfig(
                    dry_run=dry_run,
                    default_dataset=f"{project}.{dataset}",
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


@query.group(help="Commands for managing query schemas.")
def schema():
    """Create the CLI group for the query schema command."""
    pass


@schema.command(
    help="""Update the query schema based on the destination table schema and the query schema.
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
    default="moz-fx-data-shared-prod",
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
    default="analysis",
)
@use_cloud_function_option
@respect_dryrun_skip_option(default=True)
def update(
    name,
    sql_dir,
    project_id,
    update_downstream,
    tmp_dataset,
    use_cloud_function,
    respect_dryrun_skip,
):
    """CLI command for generating the query schema."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    query_files = paths_matching_name_pattern(name, sql_dir, project_id)
    dependency_graph = get_dependency_graph([sql_dir], without_views=True)
    tmp_tables = {}

    for query_file in query_files:
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
                    query_files.append(d)

    if len(tmp_tables) > 0:
        client = bigquery.Client()
        # delete temporary tables
        for _, table in tmp_tables.items():
            client.delete_table(table, not_found_ok=True)


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
    if respect_dryrun_skip and str(query_file) in SKIP:
        click.echo(f"{query_file} dry runs are skipped. Cannot update schemas.")
        return

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
                    existing_schema.merge(parent_schema, exclude=derived_from.exclude)

                    # use temporary table
                    tmp_identifier = (
                        f"{project_name}.{tmp_dataset}.{table_name}_{random_str(12)}"
                    )
                    existing_schema.deploy(tmp_identifier)
                    tmp_tables[
                        f"{project_name}.{dataset_name}.{table_name}"
                    ] = tmp_identifier

    # replace temporary table references
    sql_content = query_file_path.read_text()

    for orig_table, tmp_table in tmp_tables.items():
        table_parts = orig_table.split(".")
        for i in range(len(table_parts)):
            if ".".join(table_parts[i:]) in sql_content:
                sql_content = sql_content.replace(".".join(table_parts[i:]), tmp_table)
                break

    try:
        query_schema = Schema.from_query_file(
            query_file_path,
            content=sql_content,
            use_cloud_function=use_cloud_function,
            respect_skip=respect_dryrun_skip,
        )
    except Exception:
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
        project_name, dataset_name, table_name, partitioned_by
    )

    changed = True

    if existing_schema_path.is_file():
        existing_schema = Schema.from_schema_file(existing_schema_path)
        old_schema = copy.deepcopy(existing_schema)
        existing_schema.merge(table_schema)
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
    default="moz-fx-data-shared-prod",
    callback=is_valid_project,
)
@click.option(
    "--force/--noforce",
    help="Deploy the schema file without validating that it matches the query",
    default=False,
)
@use_cloud_function_option
@respect_dryrun_skip_option(default=True)
@click.pass_context
def deploy(
    ctx, name, sql_dir, project_id, force, use_cloud_function, respect_dryrun_skip
):
    """CLI command for deploying destination table schemas."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    client = bigquery.Client()

    query_files = paths_matching_name_pattern(name, sql_dir, project_id)

    for query_file in query_files:
        if respect_dryrun_skip and str(query_file) in SKIP:
            click.echo(f"{query_file} dry runs are skipped. Cannot validate schemas.")
            continue

        query_file_path = Path(query_file)
        existing_schema_path = query_file_path.parent / SCHEMA_FILE

        if existing_schema_path.is_file():
            click.echo(f"Deploy schema for {query_file}")

            table_name = query_file_path.parent.name
            dataset_name = query_file_path.parent.parent.name
            project_name = query_file_path.parent.parent.parent.name
            existing_schema = Schema.from_schema_file(existing_schema_path)
            full_table_id = f"{project_name}.{dataset_name}.{table_name}"

            if not force:
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

            tmp_schema_file = NamedTemporaryFile()
            existing_schema.to_json_file(Path(tmp_schema_file.name))
            bigquery_schema = client.schema_from_json(tmp_schema_file.name)

            try:
                # destination table already exists, update schema
                table = client.get_table(full_table_id)
                table.schema = bigquery_schema
                client.update_table(table, ["schema"])
                click.echo(f"Schema updated for {full_table_id}")
            except NotFound:
                # no destination table, create new table based on schema and metadata
                metadata = Metadata.of_query_file(query_file_path)
                new_table = bigquery.Table(full_table_id, schema=bigquery_schema)

                if metadata.bigquery and metadata.bigquery.time_partitioning:
                    new_table.time_partitioning = bigquery.TimePartitioning(
                        metadata.bigquery.time_partitioning.type.bigquery_type,
                        field=metadata.bigquery.time_partitioning.field,
                        require_partition_filter=(
                            metadata.bigquery.time_partitioning.require_partition_filter
                        ),
                        expiration_ms=metadata.bigquery.time_partitioning.expiration_ms,
                    )

                if metadata.bigquery and metadata.bigquery.clustering:
                    new_table.clustering_fields = metadata.bigquery.clustering.fields

                client.create_table(new_table)
                click.echo(f"Destination table {full_table_id} created.")
        else:
            click.echo(f"No schema file found for {query_file}")


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
    default="moz-fx-data-shared-prod",
    callback=is_valid_project,
)
@use_cloud_function_option
@respect_dryrun_skip_option(default=True)
def validate_schema(name, sql_dir, project_id, use_cloud_function, respect_dryrun_skip):
    """Validate the defined query schema against the query and destination table."""
    query_files = paths_matching_name_pattern(name, sql_dir, project_id)

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
