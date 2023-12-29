"""Use counter table generation."""
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
TEMPLATE_CONFIG = FILE_PATH / "templates/templating.yaml"


def generate_query(project, dataset, destination_table, write_dir):
    """Generate use counter table query."""
    if dataset == "fenix_derived":
        query_fpath = "query_fenix.sql"
    if dataset == "firefox_desktop_derived":
        query_fpath = "query_ff_desktop.sql"
    with open(TEMPLATE_CONFIG, "r") as f:
        render_kwargs = yaml.safe_load(f) or {}
    env = Environment(loader=FileSystemLoader(str(FILE_PATH / "templates")))
    template = env.get_template(query_fpath)

    write_sql(
        write_dir / project,
        f"{project}.{dataset}.{destination_table}",
        "query.sql",
        reformat(template.render(**render_kwargs)),
    )


def generate_view(project, dataset, destination_table, write_dir):
    """Generate feature usage table view."""
    view_name = destination_table.split("_v")[0]
    view_dataset = dataset.split("_derived")[0]

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


def generate_metadata(project, dataset, destination_table, write_dir:Path):
    """Copy metadata.yaml file to destination directory."""
    if dataset == "fenix_derived":
        friendly_table_name = "Fenix Use Counters V2"
    if dataset == "firefox_desktop_derived":
        friendly_table_name = "Firefox Desktop Use Counters V2"

    env = Environment(loader=FileSystemLoader(str(FILE_PATH / "templates")))
    template = env.get_template("metadata.yaml")

    path_to_write_metadata = write_dir / project / dataset / destination_table / "metadata.yaml"
    path_to_write_metadata.write_text(template.render({'friendly_table_name': friendly_table_name}))


def generate_schema(project, dataset, destination_table, write_dir):
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
    """Generate the use counter tables for Fenix and Firefox Desktop"""
    output_dir = Path(output_dir)
    #FIRST - generate Fenix one
    generate_query(target_project, dataset, destination_table, output_dir)
    generate_view(target_project, dataset, destination_table, output_dir)
    generate_metadata(target_project, dataset, destination_table, output_dir)
    generate_schema(target_project, dataset, destination_table, output_dir)
    #NEXT - generate firefox desktop one
    generate_query(project=target_project, dataset="firefox_desktop_derived", destination_table="firefox_desktop_use_counters_v2", write_dir = output_dir)
    generate_view(project=target_project, dataset="firefox_desktop_derived", destination_table="firefox_desktop_use_counters_v2", write_dir = output_dir)
    generate_metadata(project=target_project, dataset="firefox_desktop_derived", destination_table="firefox_desktop_use_counters_v2", write_dir = output_dir)
    generate_schema(project=target_project, dataset="firefox_desktop_derived", destination_table="firefox_desktop_use_counters_v2", write_dir = output_dir)
