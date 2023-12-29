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


def generate_query(
    project: str,
    dataset: str,
    destination_table: str,
    write_dir: Path,
    query_fpath: str,
):
    """Generate use counter table query."""
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


def generate_view(project: str, dataset: str, destination_table: str, write_dir: Path):
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


def generate_metadata(
    project: str,
    dataset: str,
    destination_table: str,
    write_dir: Path,
    friendly_table_name: str,
):
    """Copy metadata.yaml file to destination directory."""
    env = Environment(loader=FileSystemLoader(str(FILE_PATH / "templates")))
    template = env.get_template("metadata.yaml")

    path_to_write_metadata = (
        write_dir / project / dataset / destination_table / "metadata.yaml"
    )
    path_to_write_metadata.write_text(
        template.render({"friendly_table_name": friendly_table_name})
    )


def generate_schema(
    project: str, dataset: str, destination_table: str, write_dir: Path
):
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
    "--output-dir",
    "--output_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@use_cloud_function_option
@click.pass_context
def generate(ctx, target_project, output_dir, use_cloud_function):
    """Generate the use counter tables for Fenix and Firefox Desktop"""
    output_dir = Path(output_dir)

    config = {
        "fenix": {
            "query_fpath": "query_fenix.sql",
            "dataset": "fenix_derived",
            "dst_tbl": "fenix_use_counters_v2",
            "friendly_nm": "Fenix Use Counters V2",
        },
        "firefox": {
            "query_fpath": "query_ff_desktop.sql",
            "dataset": "firefox_desktop_derived",
            "dst_tbl": "firefox_desktop_use_counters_v2",
            "friendly_nm": "Firefox Desktop Use Counters V2",
        },
    }

    # FIRST - generate Fenix one
    generate_query(
        target_project,
        dataset=config["fenix"]["dataset"],
        destination_table=config["fenix"]["dst_tbl"],
        write_dir=output_dir,
        query_fpath=config["fenix"]["query_fpath"],
    )
    generate_view(
        target_project,
        dataset=config["fenix"]["dataset"],
        destination_table=config["fenix"]["dst_tbl"],
        write_dir=output_dir,
    )
    generate_metadata(
        target_project,
        dataset=config["fenix"]["dataset"],
        destination_table=config["fenix"]["dst_tbl"],
        write_dir=output_dir,
        friendly_table_name=config["fenix"]["friendly_nm"],
    )
    generate_schema(
        target_project,
        dataset=config["fenix"]["dataset"],
        destination_table=config["fenix"]["dst_tbl"],
        write_dir=output_dir,
    )
    # NEXT - generate firefox desktop one
    generate_query(
        project=target_project,
        dataset=config["firefox"]["dataset"],
        destination_table="firefox_desktop_use_counters_v2",
        write_dir=output_dir,
        query_fpath=config["firefox"]["query_fpath"],
    )
    generate_view(
        project=target_project,
        dataset=config["firefox"]["dataset"],
        destination_table="firefox_desktop_use_counters_v2",
        write_dir=output_dir,
    )
    generate_metadata(
        project=target_project,
        dataset=config["firefox"]["dataset"],
        destination_table="firefox_desktop_use_counters_v2",
        write_dir=output_dir,
        friendly_table_name=config["firefox"]["friendly_nm"],
    )
    generate_schema(
        project=target_project,
        dataset=config["firefox"]["dataset"],
        destination_table="firefox_desktop_use_counters_v2",
        write_dir=output_dir,
    )
