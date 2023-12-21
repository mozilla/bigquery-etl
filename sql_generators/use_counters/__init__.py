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

#FIX BELOW 
FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = Path(FILE_PATH).parent.parent
TEMPLATE_CONFIG = FILE_PATH / "templates/templating.yaml"



def generate_query(template_path, project, dataset, destination_table, write_dir):
    """Generate feature usage table query."""
    with open(template_path, "r") as f:
        render_kwargs = yaml.safe_load(f) or {}
    env = Environment(loader=FileSystemLoader(str(FILE_PATH / "templates")))
    template = env.get_template("query.sql")

    write_sql(
        write_dir / project,
        f"{project}.{dataset}.{destination_table}",
        "query.sql",
        reformat(template.render(**render_kwargs)),
    )

#    view_name = "fenix_use_counters_v2"
#    view_dataset = "fenix"
def generate_view(project, dataset, destination_table, write_dir, view_name, view_dataset):
    """Generate the view."""
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


def generate_metadata(friendly_name, project, dataset, destination_table, write_dir):
    """Copy metadata.yaml file to destination directory."""
    #Apply jinja templating to the file
    #FIX BELOW

    #FIX ABOVE 

    shutil.copyfile(
        FILE_PATH / "templates" / "metadata.yaml",
        write_dir / project / dataset / destination_table / "metadata.yaml",
    )


def generate_schema(project, dataset, destination_table, write_dir): 
    """ Copy schema.yaml file to destination directory"""
    shutil.copyfile(
        FILE_PATH / "templates" / "schema.yaml",
        write_dir / project / dataset / destination_table / "schema.yaml",
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
    generate_schema(target_project, dataset, destination_table, output_dir)
