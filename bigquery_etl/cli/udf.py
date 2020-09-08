"""bigquery-etl CLI UDF command."""

import click
from pathlib import Path
import re
import string
import sys
import yaml

from ..cli.utils import is_valid_dir
from ..format_sql.formatter import reformat

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

# publish

# info
# usages

# validate
