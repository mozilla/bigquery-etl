import click
import os
import shutil
import yaml

from pathlib import Path

from bigquery_etl.cli.query import update
from bigquery_etl.util.common import render, write_sql
from bigquery_etl.format_sql.formatter import reformat

FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = Path(FILE_PATH).parent.parent
TEMPLATE_CONFIG = FILE_PATH / "template.yaml"


def generate_query(project, dataset, destination_table, write_dir):
    """Generate feature usage table query."""
    with open(TEMPLATE_CONFIG, "r") as f:
        render_kwargs = yaml.load(f) or {}
    write_sql(
        write_dir / project,
        f"{project}.{dataset}.{destination_table}",
        "query.sql",
        render(
            "query.sql",
            template_folder=FILE_PATH / "templates",
            **render_kwargs,
            init=False,
        ),
    )


def generate_view(project, dataset, destination_table, write_dir):
    """Generate feature usage table view."""
    view_name = destination_table.split("_v")[0]
    sql = reformat(
        f"""
        CREATE OR REPLACE `{project}.{dataset}.{view_name}` AS
        SELECT
            *
        FROM
            `{project}.{dataset}.{destination_table}`
    """
    )

    write_sql(write_dir / project, f"{project}.{dataset}.{view_name}", "view.sql", sql)


def generate_metadata(project, dataset, destination_table, write_dir):
    """Copy metadata.yaml file to destination directory."""
    shutil.copyfile(
        FILE_PATH / "templates" / "metadata.yaml",
        write_dir / project / dataset / destination_table / "metadata.yaml",
    )


@click.group(help="Commands for generating the desktop feature usage table.")
def feature_usage():
    """Create the CLI group for the feature usage table."""
    pass


@feature_usage.command("generate")
@click.option(
    "--project",
    help="Which project the queries should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--dataset",
    help="Which dataset the queries should be written to.",
    default="telemetry_derived",
)
@click.option(
    "--destination_table",
    "--destination-table",
    help="Name of the destination table.",
    default="feature_usage_v2",
)
@click.option(
    "--path",
    help="Where query directories will be searched for.",
    default="bigquery_etl/feature_usage/templates",
)
@click.option(
    "--write-dir",
    help="The location to write to. Defaults to sql/.",
    default=BASE_DIR / "sql",
)
@click.pass_context
def generate(ctx, project, dataset, destination_table, path, write_dir):
    generate_query(project, path, dataset, write_dir)
    generate_view(project, path, dataset, write_dir)
    generate_metadata(project, path, dataset, write_dir)
