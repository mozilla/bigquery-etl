import os
import shutil
from pathlib import Path

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util.common import write_sql

FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = Path(FILE_PATH).parent.parent
TEMPLATE_CONFIG = FILE_PATH / "templating.yaml"


def generate_query(project, dataset, destination_table, write_dir):
    """Generate feature usage table query."""
    with open(TEMPLATE_CONFIG, "r") as f:
        render_kwargs = yaml.safe_load(f) or {}
    env = Environment(loader=FileSystemLoader(str(FILE_PATH / "templates")))
    template = env.get_template("query.sql")

    write_sql(
        write_dir / project,
        f"{project}.{dataset}.{destination_table}",
        "query.sql",
        reformat(template.render(**render_kwargs)),
    )


def generate_view(project, dataset, destination_table, write_dir):
    """Generate feature usage table view."""
    view_name = "fenix_use_counters_v2"
    view_dataset = "fenix"

    sql = reformat(
        f"""
        CREATE OR REPLACE VIEW `{project}.{view_dataset}.{view_name}` AS
        SELECT
            *
        FROM
            `{project}.{dataset}.{destination_table}`
    """
    )

    write_sql(
        write_dir / project, f"{project}.{view_dataset}.{view_name}", "view.sql", sql
    )


def generate_metadata(project, dataset, destination_table, write_dir):
    """Copy metadata.yaml file to destination directory."""
    shutil.copyfile(
        FILE_PATH / "templates" / "metadata.yaml",
        write_dir / project / dataset / destination_table / "metadata.yaml",
    )


@click.command("generate")
@click.option(
    "--target-project",
    "--target_project",
    help="Which project the queries should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--dataset",
    help="Which dataset the queries should be written to.",
    default="fenix_derived",
)
@click.option(
    "--destination_table",
    "--destination-table",
    help="Name of the destination table.",
    default="fenix_use_counters_v2",
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@use_cloud_function_option
@click.pass_context
def generate(
    ctx, target_project, dataset, destination_table, output_dir, use_cloud_function
):
    """Generate the fenix use counters V2 table."""
    output_dir = Path(output_dir)
    generate_query(target_project, dataset, destination_table, output_dir)
    generate_view(target_project, dataset, destination_table, output_dir)
    generate_metadata(target_project, dataset, destination_table, output_dir)
