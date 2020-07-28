"""bigquery-etl CLI query command."""

import click
import os
from pathlib import Path
import re
import sys
import string

from ..metadata.parse_metadata import Metadata
from ..format_sql.formatter import reformat


QUERY_NAME_RE = re.compile(
    r"(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)(?P<ver>_v[0-9]+)?"
)


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
def create(name, path, owner, init):
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
