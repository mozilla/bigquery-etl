import click
import os
from pathlib import Path
import re
import sys
import string

from bigquery_etl.metadata.parse_metadata import Metadata


QUERY_NAME_RE = re.compile(
    r"(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)(?P<ver>_v[0-9]+)?"
)


def create_query_cli_group():
    """Create the CLI group for the query command."""
    group = click.Group(name="query", help="Creating/validating/deleting/... queries")
    group.add_command(query_create_command)
    return group


@click.command(
    name="create",
    help=(
        "Create a new query with name "
        "<dataset>.<query_name>, for example: telemetry_derived.asn_aggregates"
    ),
)
@click.argument("name")
@click.option(
    "--path",
    "-p",
    help="Path to directory in which query should be created",
    default="sql/",
)
@click.option(
    "--owner",
    "-o",
    help="Owner of the query (email address)",
    default="example@mozilla.com",
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
)
def query_create_command(name, path, owner, init):
    """CLI command for creating a new query."""

    if not os.path.isdir(path):
        click.echo(f"Invalid path for adding new query: {path}", err=True)
        sys.exit(1)

    # create directory structure for query
    try:
        match = QUERY_NAME_RE.match(name)
        name = match.group("name")
        dataset = match.group("dataset")
        version = match.group("ver") or "_v1"
    except AttributeError:
        click.echo(
            "New queries must be named like:"
            + " <dataset>.<table> or <dataset>.<table>_v[n]"
        )
        sys.exit(1)

    derived_path = None
    view_path = None

    if dataset.endswith("_derived"):
        # create a directory for the corresponding view
        derived_path = Path(path) / dataset / (name + version)
        os.makedirs(derived_path)

        view_path = Path(path) / dataset.replace("_derived", "") / name
        os.makedirs(view_path)
    else:
        # check if there is a corresponding derived dataset
        if (Path(path) / (dataset + "_derived")).exists():
            derived_path = Path(path) / (dataset + "_derived") / (name + version)
            os.makedirs(derived_path)
            view_path = Path(path) / dataset / name
            os.makedirs(view_path)

            dataset = dataset + "_derived"
        else:
            # some dataset that is not specified as _derived
            # don't automatically create views
            derived_path = Path(path) / dataset / (name + version)
            os.makedirs(derived_path)

    click.echo(f"Created query in {derived_path}")

    if view_path:
        click.echo(f"Created corresponding view in {view_path}")
        view_file = view_path / "view.sql"
        view_dataset = dataset.replace("_derived", "")
        view_file.write_text(
            (
                "CREATE OR REPLACE VIEW\n"
                f"  `moz-fx-data-shared-prod.{view_dataset}.{name}`\n"
                "AS SELECT * FROM\n"
                f"  `moz-fx-data-shared-prod.{dataset}.{name}{version}`\n"
            )
        )

    # create query.sql file
    query_file = derived_path / "query.sql"
    query_file.write_text(
        (
            f"-- Query for {dataset}.{name}{version}\n"
            "-- For more information on writing queries see:\n"
            "-- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html\n"
            "SELECT * FROM table WHERE submission_date = @submission_date\n"
        )
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
            (
                "-- SQL for initializing the table query results are written to.\n"
                "CREATE OR REPLACE TABLE\n"
                f"  `moz-fx-data-shared-prod.{dataset}.{name}{version}`\n"
                "AS SELECT * FROM table\n"
            )
        )


query_cli = create_query_cli_group()
