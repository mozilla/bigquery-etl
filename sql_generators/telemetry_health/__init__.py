"""Telemetry health scorecard query generation."""

import os
from pathlib import Path

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql

FILE_PATH = Path(os.path.dirname(__file__))


def generate_queries(project, path, write_dir):
    """Generate telemetry health queries."""
    with open(Path(path) / "templating.yaml", "r") as f:
        template_config = yaml.safe_load(f) or {}

    applications = template_config.get("applications", [])
    application_names = template_config.get("application_names", {})

    for query, args in template_config.get("queries", {}).items():
        template_query_dir = FILE_PATH / "templates" / query
        env = Environment(
            loader=FileSystemLoader(template_query_dir),
            keep_trailing_newline=True,
        )

        sql_template = env.get_template("query.sql")
        metadata_template = env.get_template("metadata.yaml")

        destination_dataset = args.get("destination_dataset", "monitoring_derived")

        render_args = {
            "project_id": project,
            "applications": applications,
            "application_names": application_names,
            "destination_table": query,
            **args,
        }

        write_sql(
            write_dir / project,
            f"{project}.{destination_dataset}.{query}",
            "query.sql",
            reformat(sql_template.render(**render_args)),
        )

        write_path = Path(write_dir) / project / destination_dataset / query
        (write_path / "metadata.yaml").write_text(
            metadata_template.render(**render_args)
        )

        schema_path = template_query_dir / "schema.yaml"
        if schema_path.exists():
            (write_path / "schema.yaml").write_text(schema_path.read_text())

        # Generate user-facing view (strip _derived suffix and _v1 from name)
        view_dataset = destination_dataset.replace("_derived", "")
        view_name = query.rsplit("_v", 1)[0]
        view_sql = f"""CREATE OR REPLACE VIEW
  `{project}.{view_dataset}.{view_name}`
AS
SELECT
  *
FROM
  `{project}.{destination_dataset}.{query}`
"""
        view_path = Path(write_dir) / project / view_dataset / view_name
        view_path.mkdir(parents=True, exist_ok=True)
        (view_path / "view.sql").write_text(view_sql)


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
    default="sql_generators/telemetry_health/templates",
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
    """Generate the telemetry health queries."""
    output_dir = Path(output_dir)
    generate_queries(target_project, path, output_dir)
