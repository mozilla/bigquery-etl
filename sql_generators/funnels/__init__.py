"""Funnel generation."""
import os
import re
from pathlib import Path

import cattrs
import click
import toml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql
from sql_generators.funnels.config import FunnelConfig

FILE_PATH = Path(os.path.dirname(__file__))
TEMPLATES_PATH = FILE_PATH / "templates"


def bq_normalize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def generate_funnels(target_project, path, output_dir):
    output_dir = Path(output_dir) / target_project
    path = Path(path)
    converter = cattrs.BaseConverter()

    for config_file in path.glob("*.toml"):
        config = converter.structure(toml.load(config_file), FunnelConfig)
        table_name = bq_normalize_name(config_file.stem)

        env = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
        sql_template = env.get_template("funnel.sql")

        funnel_sql = reformat(
            sql_template.render(
                funnels=config.funnels,
                steps=config.steps,
                data_sources=config.data_sources,
                dimensions=config.dimensions,
            )
        )
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{config.destination_dataset}.{table_name}_v{config.version}",
            basename="query.sql",
            sql=funnel_sql,
            skip_existing=False,
        )

        metadata_template = env.get_template("metadata.yaml")
        rendered_metadata = metadata_template.render(
            {
                "funnel_name": table_name.replace("_", " ").title(),
                "owners": config.owners,
            }
        )
        (
            output_dir
            / config.destination_dataset
            / f"{table_name}_v{config.version}"
            / "metadata.yaml"
        ).write_text(rendered_metadata + "\n")


@click.command("generate")
@click.option(
    "--target-project",
    "--target_project",
    help="Which project the queries should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--path",
    help="Where query directories will be searched for.",
    default="sql_generators/funnels/configs",
    required=False,
    type=click.Path(file_okay=False),
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@use_cloud_function_option
def generate(target_project, path, output_dir, use_cloud_function):
    """Generate the funnel queries."""
    output_dir = Path(output_dir)
    generate_funnels(target_project, path, output_dir)
