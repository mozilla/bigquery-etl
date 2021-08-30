"""Experiment monitoring materialized view generation."""
import os
import shutil
from pathlib import Path

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = Path(FILE_PATH).parent.parent


def generate_queries(project, path, write_dir):
    """Generate experiment monitoring views."""
    with open(Path(path) / "templating.yaml", "r") as f:
        template_config = yaml.safe_load(f) or {}

    for query, args in template_config["queries"].items():
        env = Environment(
            loader=FileSystemLoader(FILE_PATH / "templates" / query),
            keep_trailing_newline=True,
        )
        init_template = env.get_template("init.sql")
        metadata_template = env.get_template("metadata.yaml")

        for dataset in template_config["applications"]:
            args["destination_table"] = query
            args["dataset"] = dataset

            write_sql(
                write_dir / project,
                f"{project}.{dataset}_derived.{query}",
                "init.sql",
                reformat(init_template.render(**args)),
            )

            write_path = Path(write_dir) / project / (dataset + "_derived") / query
            (write_path / "metadata.yaml").write_text(metadata_template.render(**args))


@click.group(
    help="Commands for generating the experiment monitoring materialized views."
)
def experiment_monitoring():
    """Create the CLI group for the experiment monitoring views."""
    pass


@experiment_monitoring.command("generate")
@click.option(
    "--project",
    help="Which project the queries should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--path",
    help="Where query directories will be searched for.",
    default="bigquery_etl/experiment_monitoring/templates",
    required=False,
    type=click.Path(file_okay=False),
)
@click.option(
    "--write-dir",
    help="The location to write to. Defaults to sql/.",
    default=BASE_DIR / "sql",
    type=click.Path(file_okay=True),
)
def generate(project, path, write_dir):
    """Generate the experiment monitoring views."""
    generate_queries(project, path, write_dir)
