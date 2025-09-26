"""SQL generator for gecko trace data queries and metadata."""

import os
import shutil
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.util.common import write_sql

THIS_MODULE = Path(os.path.dirname(__file__))
TEMPLATES = THIS_MODULE / "templates"
PING_NAME = "gecko_trace"
APPLICATIONS = (
    "firefox_desktop",  # The desktop version of Firefox
    "org_mozilla_fenix_nightly",  # Nightly channel of Firefox Preview
    "org_mozilla_firefox_beta",  # Beta channel of Firefox for Android
)


def generate_derived(output_dir, target_project):
    """Generate derived table SQL queries and metadata for gecko trace data."""
    env = Environment(loader=FileSystemLoader(TEMPLATES / "derived"))

    for app_id in APPLICATIONS:
        for template_name in env.list_templates("*.sql"):
            query_template = env.get_template(template_name)
            table_name = template_name.split("/")[0]
            write_sql(
                output_dir / target_project,
                f"{app_id}_derived.{table_name}",
                "query.sql",
                query_template.render(
                    target_project=target_project,
                    app_id=app_id,
                    ping_name=PING_NAME,
                ),
            )

            # Copy metadata.yaml and schema.yaml files
            template_dir = TEMPLATES / "derived" / table_name
            output_table_dir = (
                output_dir / target_project / f"{app_id}_derived" / table_name
            )

            if (template_dir / "metadata.yaml").exists():
                with open(output_table_dir / "metadata.yaml", "w") as f:
                    rendered_metadata = env.get_template(
                        table_name + "/metadata.yaml"
                    ).render(app_id=app_id)
                    f.write(rendered_metadata)

            if (template_dir / "schema.yaml").exists():
                shutil.copyfile(
                    template_dir / "schema.yaml",
                    output_table_dir / "schema.yaml",
                )


def generate_aggregates(output_dir, target_project):
    """Generate aggregate view SQL queries for gecko trace data."""
    env = Environment(loader=FileSystemLoader(TEMPLATES / "aggregates"))

    for template_name in env.list_templates("*.sql"):
        view_template = env.get_template(template_name)
        view = template_name.split("/")[0]
        write_sql(
            output_dir / target_project,
            f"{target_project}.gecko_trace_aggregates.{view}",
            "view.sql",
            view_template.render(
                target_project=target_project,
                applications=APPLICATIONS,
                ping_name=PING_NAME,
            ),
        )

    # Copy dataset_metadata.yaml file
    dataset_output_dir = output_dir / target_project / "gecko_trace_aggregates"
    dataset_metadata_path = TEMPLATES / "aggregates" / "dataset_metadata.yaml"
    shutil.copyfile(
        dataset_metadata_path,
        dataset_output_dir / "dataset_metadata.yaml",
    )


@click.command()
@click.option(
    "--output-dir",
    help="Output directory generated SQL is written to.",
    type=click.Path(file_okay=False),
    default="sql",
    show_default=True,
)
@click.option(
    "--target-project",
    help="Which project the queries should be generated for.",
    default="moz-fx-data-shared-prod",
    show_default=True,
)
@use_cloud_function_option
def generate(output_dir, target_project, use_cloud_function):
    """Generate all gecko trace SQL queries and metadata files.

    This command generates both derived table queries and aggregate views
    for gecko trace data across all supported applications.
    """
    output_dir = Path(output_dir)
    generate_derived(output_dir, target_project)
    generate_aggregates(output_dir, target_project)


if __name__ == "__main__":
    generate()  # type: ignore
