"""bigquery-etl CLI query command."""

import re
import string
import sys
from datetime import date, timedelta
from fnmatch import fnmatchcase
from pathlib import Path

import click
from google.cloud import bigquery

from ..cli.dryrun import dryrun
from ..cli.format import format
from ..cli.utils import is_authenticated, is_valid_dir, is_valid_project
from ..format_sql.formatter import reformat
from ..metadata import validate_metadata
from ..metadata.parse_metadata import METADATA_FILE, Metadata
from ..query_scheduling.generate_airflow_dags import get_dags
from ..run_query import run

QUERY_NAME_RE = re.compile(r"(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)")
SQL_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+_v[0-9]+)/"
    r"(?:query\.sql|part1\.sql|script\.sql)$"
)
VERSION_RE = re.compile(r"_v[0-9]+")


def _queries_matching_name_pattern(pattern, sql_path, project_id):
    """Return paths to queries matching the name pattern."""
    sql_path = Path(sql_path)
    if project_id is not None:
        sql_path = sql_path / project_id

    all_sql_files = Path(sql_path).rglob("*.sql")
    sql_files = []

    for sql_file in all_sql_files:
        match = SQL_FILE_RE.match(str(sql_file))
        if match:
            project = match.group(1)
            dataset = match.group(2)
            table = match.group(3)
            query_name = f"{project}.{dataset}.{table}"
            if fnmatchcase(query_name, f"*{pattern}"):
                sql_files.append(sql_file)
            elif project_id and fnmatchcase(query_name, f"{project_id}.{pattern}"):
                sql_files.append(sql_file)

    return sql_files


sql_dir_option = click.option(
    "--sql_dir",
    help="Path to directory which contains queries.",
    type=click.Path(file_okay=False),
    default="sql",
    callback=is_valid_dir,
)

project_id_option = click.option(
    "--project-id",
    "--project_id",
    help="GCP project ID",
    default=None,
)


@click.group(help="Commands for managing queries.")
def query():
    """Create the CLI group for the query command."""
    pass


@query.command(
    help="Create a new query with name "
    "<dataset>.<query_name>, for example: telemetry_derived.asn_aggregates. "
    "Use the --project_id option to change the project the query is added to; "
    "default is moz-fx-data-shared-prod",
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


@query.command(
    help="Schedule an existing query",
)
@click.argument("name")
@sql_dir_option
@project_id_option
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
    query_files = _queries_matching_name_pattern(name, sql_dir, project_id)

    if query_files == []:
        click.echo(f"Name doesn't refer to any queries: {name}", err=True)
        sys.exit(1)

    sql_dir = Path(sql_dir)

    dags = get_dags(None, sql_dir.parent / "dags.yaml")

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
    help="Get information about all or specific queries.",
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option
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

    query_files = _queries_matching_name_pattern(name, sql_dir, project_id)

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
                    FROM `moz-fx-data-shared-prod.monitoring.bigquery_etl_scheduled_queries_cost_v1`
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


@query.command(
    help="Run a backfill for a query. Additional parameters will get passed to bq.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@sql_dir_option
@project_id_option
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
@click.pass_context
def backfill(ctx, name, sql_dir, project_id, start_date, end_date, exclude, dry_run):
    """Run a backfill."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    query_files = _queries_matching_name_pattern(name, sql_dir, project_id)
    dates = [start_date + timedelta(i) for i in range((end_date - start_date).days + 1)]

    for query_file in query_files:
        query_file_path = Path(query_file)
        table = query_file_path.parent.name
        dataset = query_file_path.parent.parent.name
        project = query_file_path.parent.parent.parent.name

        for backfill_date in dates:
            backfill_date = backfill_date.strftime("%Y-%m-%d")
            if backfill_date not in exclude:
                partition = backfill_date.replace("-", "")
                click.echo(
                    f"Run backfill for {project}.{dataset}.{table}${partition} "
                    f"with @submission_date={backfill_date}"
                )

                arguments = [
                    "query",
                    "--time_partitioning_type=DAY",
                    f"--parameter=submission_date:DATE:{backfill_date}",
                    "--use_legacy_sql=false",
                    "--replace",
                ] + ctx.args
                if dry_run:
                    arguments += ["--dry_run"]

                run(query_file_path, dataset, f"{table}${partition}", arguments)
            else:
                click.echo(f"Skip {query_file} with @submission_date={backfill_date}")


@query.command(
    help="Validate a query.",
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option
@click.option(
    "--use_cloud_function",
    "--use-cloud-function",
    help=(
        "Use the Cloud Function for dry running SQL, if set to `True`. "
        "The Cloud Function can only access tables in shared-prod. "
        "If set to `False`, use active GCP credentials for the dry run."
    ),
    type=bool,
    default=True,
)
@click.pass_context
def validate(ctx, name, sql_dir, project_id, use_cloud_function):
    """Validate queries by dry running, formatting and checking scheduling configs."""
    if name is None:
        name = "*.*"

    query_files = _queries_matching_name_pattern(name, sql_dir, project_id)
    for query in query_files:
        project = query.parent.parent.parent.name
        ctx.invoke(format, path=str(query))
        ctx.invoke(
            dryrun,
            path=str(query),
            use_cloud_function=use_cloud_function,
            project=project,
        )
        validate_metadata.validate(query.parent)

    # todo: validate if new fields get added


@query.command(
    help="Create and initialize the destination table for the query.",
)
@click.argument("name")
@sql_dir_option
@project_id_option
@click.option(
    "--dry_run/--no_dry_run",
    help="Dry run the backfill",
)
def initialize(name, sql_dir, project_id, dry_run):
    """Create the destination table for the provided query."""
    if not is_authenticated():
        click.echo("Authentication required for creating tables.", err=True)
        sys.exit(1)

    query_files = _queries_matching_name_pattern(name, sql_dir, project_id)

    for query_file in query_files:
        init_files = Path(query_file.parent).rglob("init.sql")
        client = bigquery.Client()

        for init_file in init_files:
            click.echo(f"Create destination table for {init_file}")
            project = init_file.parent.parent.parent.name

            with open(init_file) as init_file_stream:
                init_sql = init_file_stream.read()
                dataset = Path(init_file).parent.parent.name
                job_config = bigquery.QueryJobConfig(
                    dry_run=dry_run,
                    default_dataset=f"{project}.{dataset}",
                )
                client.query(init_sql, job_config=job_config)
