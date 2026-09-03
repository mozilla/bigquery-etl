"""Nimbus population sizing client pool table generation."""

import os
from pathlib import Path

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = FILE_PATH.parent.parent

DESTINATION_PROJECT = "moz-fx-data-experiments"
DESTINATION_DATASET = "monitoring"
QUERY_NAME = "nimbus_sizing_clients_v1"


def generate_queries(write_dir):
    """Generate per-app nimbus sizing client pool tables."""
    with open(FILE_PATH / "apps.yaml") as f:
        config = yaml.safe_load(f)

    template_dir = FILE_PATH / "templates" / QUERY_NAME
    env = Environment(loader=FileSystemLoader(template_dir), keep_trailing_newline=True)
    sql_template = env.get_template("query.sql")
    metadata_template = env.get_template("metadata.yaml")

    for app in config["apps"]:
        table_name = f"nimbus_sizing_clients_{app['name']}_v1"
        qualified_name = f"{DESTINATION_PROJECT}.{DESTINATION_DATASET}.{table_name}"

        write_sql(
            write_dir / DESTINATION_PROJECT,
            qualified_name,
            "query.sql",
            reformat(sql_template.render(**app)),
        )

        write_path = (
            Path(write_dir) / DESTINATION_PROJECT / DESTINATION_DATASET / table_name
        )
        (write_path / "metadata.yaml").write_text(metadata_template.render(**app))


@click.command("generate")
@click.option(
    "--output-dir",
    "--output_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@use_cloud_function_option
def generate(output_dir, use_cloud_function):
    """Generate the nimbus population sizing client pool tables."""
    generate_queries(Path(output_dir))
