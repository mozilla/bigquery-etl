"""bigquery-etl CLI query command."""

import click
from google.cloud import bigquery
from datetime import date, timedelta
import os
from pathlib import Path
import re
import sys
import string

from ..metadata.parse_metadata import Metadata, METADATA_FILE
from ..metadata import validate_metadata
from ..format_sql.formatter import reformat
from ..query_scheduling.generate_airflow_dags import get_dags
from ..cli.utils import is_valid_dir, is_authenticated
from ..cli.format import format
from ..cli.dryrun import dryrun
from ..run_query import run


QUERY_NAME_RE = re.compile(r"(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)")
VERSION_RE = re.compile(r"_v[0-9]+")


@click.group()
def query():
    """Create the CLI group for the query command."""
    pass


@query.command(
    help="Create a new query with name "
    "<dataset>.<query_name>, for example: telemetry_derived.asn_aggregates",
)
@click.argument("name")
@click.option(
    "--path",
    "-p",
    help="Path to directory in which query should be created",
    type=click.Path(file_okay=False),
    default="sql/",
    callback=is_valid_dir,
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
def create(name, path, owner, init):
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
    path = Path(path)

    if dataset.endswith("_derived"):
        # create a directory for the corresponding view
        derived_path = path / dataset / (name + version)
        derived_path.mkdir(parents=True)

        view_path = path / dataset.replace("_derived", "") / name
        view_path.mkdir(parents=True)
    else:
        # check if there is a corresponding derived dataset
        if (path / (dataset + "_derived")).exists():
            derived_path = path / (dataset + "_derived") / (name + version)
            derived_path.mkdir(parents=True)
            view_path = path / dataset / name
            view_path.mkdir(parents=True)

            dataset = dataset + "_derived"
        else:
            # some dataset that is not specified as _derived
            # don't automatically create views
            derived_path = path / dataset / (name + version)
            derived_path.mkdir(parents=True)

    click.echo(f"Created query in {derived_path}")

    if view_path:
        click.echo(f"Created corresponding view in {view_path}")
        view_file = view_path / "view.sql"
        view_dataset = dataset.replace("_derived", "")
        view_file.write_text(
            reformat(
                f"""CREATE OR REPLACE VIEW
                  `moz-fx-data-shared-prod.{view_dataset}.{name}`
                AS SELECT * FROM
                  `moz-fx-data-shared-prod.{dataset}.{name}{version}`"""
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


@query.command(help="Schedule an existing query",)
@click.argument("path", type=click.Path(file_okay=False), callback=is_valid_dir)
@click.option(
    "--dag",
    "-d",
    help=(
        "Name of the DAG the query should be scheduled under. "
        "To see available DAGs run `bqetl dag list`. "
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
def schedule(path, dag, depends_on_past, task_name):
    """CLI command for scheduling a query."""
    path = Path(path)
    if not os.path.isfile(path / "query.sql"):
        click.echo(f"Path doesn't refer to query: {path}", err=True)
        sys.exit(1)

    sql_dir = path.parent.parent
    dags = get_dags(sql_dir, sql_dir.parent / "dags.yaml")

    metadata = Metadata.of_sql_file(path / "query.sql")

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

        metadata.write(path / METADATA_FILE)
        print(
            f"Updated {path / METADATA_FILE} with scheduling information. "
            "For more information about scheduling queries see: "
            "https://github.com/mozilla/bigquery-etl#scheduling-queries-in-airflow"
        )

        # update dags since new task has been added
        dags = get_dags(sql_dir, sql_dir.parent / "dags.yaml")
        existing_dag = dags.dag_by_name(dag)
    else:
        if metadata.scheduling == {}:
            click.echo(f"No scheduling information for: {path}", err=True)
            sys.exit(1)
        else:
            existing_dag = dags.dag_by_name(metadata.scheduling["dag_name"])

    # re-run DAG generation for the affected DAG
    print(f"Running DAG generation for {existing_dag.name}")
    output_dir = sql_dir.parent / "dags"
    dags.dag_to_airflow(output_dir, existing_dag)


@query.command(help="Get information about all or specific queries.",)
@click.argument(
    "path", default="sql/", type=click.Path(file_okay=False), callback=is_valid_dir
)
@click.option("--cost", help="Include information about query costs", is_flag=True)
@click.option(
    "--last_updated",
    help="Include timestamps when destination tables were last updated",
    is_flag=True,
)
def info(path, cost, last_updated):
    """Return information about all or specific queries."""
    query_files = Path(path).rglob("query.sql")

    for query_file in query_files:
        query_file_path = Path(query_file)
        table = query_file_path.parent.name
        dataset = query_file_path.parent.parent.name

        try:
            metadata = Metadata.of_sql_file(query_file)
        except FileNotFoundError:
            metadata = None

        click.secho(f"{dataset}.{table}", bold=True)
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
                    FROM `monitoring.bigquery_etl_scheduled_queries_cost_v1`
                    WHERE submission_date BETWEEN '{start_date}' AND '{end_date}'
                        AND dataset = '{dataset}'
                        AND table = '{table}'
                """
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
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True,),
)
@click.argument("path", type=click.Path(file_okay=False), callback=is_valid_dir)
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
@click.option(
    "--project",
    "-p",
    help="GCP project to run backfill in",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--dry_run/--no_dry_run", help="Dry run the backfill",
)
@click.pass_context
def backfill(ctx, path, start_date, end_date, exclude, project, dry_run):
    """Run a backfill."""
    if not is_authenticated():
        click.echo("Authentication to GCP required. Run `gcloud auth login`.")
        sys.exit(1)

    query_files = Path(path).rglob("query.sql")
    dates = [start_date + timedelta(i) for i in range((end_date - start_date).days + 1)]

    for query_file in query_files:
        query_file_path = Path(query_file)
        table = query_file_path.parent.name
        dataset = query_file_path.parent.parent.name

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
                    f"--parameter=submission_date:DATE:{backfill_date}",
                    "--use_legacy_sql=false",
                    "--replace",
                ] + ctx.args
                if dry_run:
                    arguments += ["--dry_run"]

                run(query_file_path, dataset, f"{table}${partition}", arguments)
            else:
                click.echo(f"Skip {query_file} with @submission_date={backfill_date}")


@query.command(help="Validate a query.",)
@click.argument("path", default="sql/", type=click.Path(file_okay=True))
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
@click.option(
    "--project",
    help="GCP project to perform dry run in when --use_cloud_function=False",
    default="moz-fx-data-shared-prod",
)
@click.pass_context
def validate(ctx, path, use_cloud_function, project):
    """Validate queries by dry running, formatting and checking scheduling configs."""
    ctx.invoke(format, path=path)
    ctx.invoke(
        dryrun, path=path, use_cloud_function=use_cloud_function, project=project
    )
    validate_metadata.validate(path)

    # todo: validate if new fields get added


@query.command(help="Create and initialize the destination table for the query.",)
@click.argument("path", type=click.Path(file_okay=False), callback=is_valid_dir)
@click.option(
    "--project",
    "-p",
    help="GCP project to create destination table in",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--dry_run/--no_dry_run", help="Dry run the backfill",
)
def initialize(path, project, dry_run):
    """Create the destination table for the provided query."""
    if not is_authenticated():
        click.echo("Authentication required for creating tables.", err=True)
        sys.exit(1)

    init_files = Path(path).rglob("init.sql")
    client = bigquery.Client()

    for init_file in init_files:
        click.echo(f"Create destination table for {init_file}")
        with open(init_file) as init_file_stream:
            init_sql = init_file_stream.read()
            dataset = Path(init_file).parent.parent.name
            job_config = bigquery.QueryJobConfig(
                dry_run=dry_run, default_dataset=f"{project}.{dataset}",
            )
            client.query(init_sql, job_config=job_config)
