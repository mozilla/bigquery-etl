"""Experiment monitoring materialized view generation."""
import os
from pathlib import Path

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = Path(FILE_PATH).parent.parent


def generate_queries(project, path, write_dir):
    """Generate experiment monitoring views."""
    with open(Path(path) / "templating.yaml", "r") as f:
        template_config = yaml.safe_load(f) or {}

    for query, args in template_config["queries"].items():
        template_query_dir = FILE_PATH / "templates" / query
        env = Environment(
            loader=FileSystemLoader(template_query_dir),
            keep_trailing_newline=True,
        )
        sql_templates = list(template_query_dir.glob("*.sql"))
        sql_template_file = sql_templates[0].name
        sql_template = env.get_template(sql_template_file)
        metadata_template = env.get_template("metadata.yaml")

        args["destination_table"] = query
        args["search_metrics"] = template_config["search_metrics"]

        if args["per_app"]:
            # generate a separate query for each application dataset
            for dataset in template_config["applications"]:
                args["dataset"] = dataset

                write_sql(
                    write_dir / project,
                    f"{project}.{dataset}_derived.{query}",
                    sql_template_file,
                    reformat(sql_template.render(**args)),
                )

                write_path = Path(write_dir) / project / (dataset + "_derived") / query
                (write_path / "metadata.yaml").write_text(
                    metadata_template.render(**args)
                )
        else:
            # generate a single query that UNIONs application datasets
            # these queries are written to `telemetry`
            args["applications"] = template_config["applications"]

            write_sql(
                write_dir / project,
                f"{project}.telemetry_derived.{query}",
                sql_template_file,
                reformat(sql_template.render(**args)),
            )

            write_path = Path(write_dir) / project / "telemetry_derived" / query
            (write_path / "metadata.yaml").write_text(metadata_template.render(**args))


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
    default="sql_generators/experiment_monitoring/templates",
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
    """Generate the experiment monitoring views."""
    output_dir = Path(output_dir)
    generate_queries(target_project, path, output_dir)
