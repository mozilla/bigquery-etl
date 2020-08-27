"""bigquery-etl CLI UDF command."""

import click
from functools import partial

@click.group()
@click.pass_context
def udf(ctx):
    """Create the CLI group for the UDF command."""
    ctx.ensure_object(dict)
    ctx.obj['UDF_DIR'] = "udf"

@click.group()
@click.pass_context
def mozfun(ctx):
    """Create the CLI group for the mozfun command."""
    ctx.ensure_object(dict)
    ctx.obj['UDF_DIR'] = "mozfun"


@udf.command(help="Create a new UDF")
@click.pass_context
def create(ctx):
    print(f"DIR {ctx.obj['UDF_DIR']}")

mozfun.add_command(create)