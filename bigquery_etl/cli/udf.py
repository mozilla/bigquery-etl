"""bigquery-etl CLI UDF command."""

import click
from pathlib import Path
import pytest
import re
import string
import sys
import yaml

from ..cli.utils import is_valid_dir
from ..format_sql.formatter import reformat
from ..cli.format import format

UDF_NAME_RE = re.compile(r"^(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)$")


@click.group()
@click.pass_context
def udf(ctx):
    """Create the CLI group for the UDF command."""
    ctx.ensure_object(dict)
    ctx.obj["UDF_DIR"] = "udf"


@click.group()
@click.pass_context
def mozfun(ctx):
    """Create the CLI group for the mozfun command."""
    ctx.ensure_object(dict)
    ctx.obj["UDF_DIR"] = "mozfun"


@udf.command(help="Create a new UDF")
@click.argument("name")
@click.option(
    "--path",
    "-p",
    help="Path to directory in which UDF should be created. "
    + "Use default directories if not set.",
    type=click.Path(file_okay=False),
)
@click.pass_context
def create(ctx, name, path):
    """CLI command for creating a new UDF."""
    udf_dir = ctx.obj["UDF_DIR"]
    if path and is_valid_dir(path):
        udf_dir = path

    # create directory structure
    try:
        match = UDF_NAME_RE.match(name)
        name = match.group("name")
        dataset = match.group("dataset")
    except AttributeError:
        click.echo("New UDFs must be named like: <dataset>.<udf_name>")
        sys.exit(1)

    udf_dir = Path(udf_dir)
    if udf_dir.name == dataset:
        udf_path = udf_dir / name
    else:
        udf_path = udf_dir / dataset / name
    udf_path.mkdir(parents=True)

    # create SQL file with UDF definition
    udf_file = udf_path / "udf.sql"
    udf_file.write_text(
        reformat(
            f"""
            -- Definition for {dataset}.{name}
            -- For more information on writing UDFs see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
            CREATE OR REPLACE FUNCTION {dataset}.{name}()
            RETURNS BOOLEAN AS (
                TRUE
            );

            -- Tests
            SELECT assert_true({dataset}.{name}())
            """
        )
        + "\n"
    )

    # create defaul metadata.yaml
    metadata_file = udf_path / "metadata.yaml"
    metadata = {
        "friendly_name": string.capwords(dataset + " " + name.replace("_", " ")),
        "description": "Please provide a description for the UDF",
    }
    metadata_file.write_text(yaml.dump(metadata))


mozfun.add_command(create)


@udf.command(help="Get UDF information")
@click.argument("path", type=click.Path(file_okay=False), required=False)
@click.option("--usages", "-u", is_flag=True, help="Show UDF usages", default=False)
@click.option(
    "--sql_dir",
    "--sql-dir",
    type=click.Path(file_okay=False),
    callback=is_valid_dir,
    help="Path to SQL files",
    default=".",
)
@click.pass_context
def info(ctx, path, usages, sql_dir):
    """CLI command for returning information about UDFs."""
    udf_dir = ctx.obj["UDF_DIR"]
    if path and is_valid_dir(None, None, path):
        udf_dir = path

    udf_files = Path(udf_dir).rglob("udf.sql")
    for udf_file in udf_files:
        udf_file_path = Path(udf_file)
        udf_name = udf_file_path.parent.name
        udf_dataset = udf_file_path.parent.parent.name

        try:
            metadata = yaml.safe_load(open(udf_file_path.parent / "metadata.yaml"))
        except FileNotFoundError:
            metadata = None

        click.secho(f"{udf_dataset}.{udf_name}", bold=True)
        click.echo(f"path: {udf_file_path}")

        if metadata is None:
            click.echo("No metadata")
        else:
            click.echo(f"description: {metadata['description']}")

        no_usages = True
        if usages:
            # find UDF usages in SQL files
            click.echo("usages: ")
            sql_files = Path(sql_dir).rglob("*.sql")
            for sql_file in sql_files:
                if f"{udf_dataset}.{udf_name}" in sql_file.read_text():
                    no_usages = False
                    click.echo(f"  {sql_file}")

            if no_usages:
                click.echo("  No usages.")

        click.echo("")


mozfun.add_command(info)


@udf.command(
    help="Validate a UDF.",
)
@click.argument("path", type=click.Path(file_okay=False), required=False)
@click.pass_context
def validate(ctx, path):
    """Validate UDFs by formatting and running tests."""
    udf_dir = ctx.obj["UDF_DIR"]
    if path and is_valid_dir(None, None, path):
        udf_dir = path
    ctx.invoke(format, path=udf_dir)
    pytest.main([udf_dir])


mozfun.add_command(validate)

# publish

