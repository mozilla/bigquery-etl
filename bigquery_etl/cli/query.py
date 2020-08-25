"""bigquery-etl CLI query command."""

import click
import os
from pathlib import Path
import re
import sys
import string

from ..metadata.parse_metadata import Metadata, METADATA_FILE
from ..format_sql.formatter import reformat
from ..query_scheduling.generate_airflow_dags import get_dags
from ..cli.utils import is_valid_dir


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

        click.echo("")
