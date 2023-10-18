"""Generate review checker events/clients/microsruvey data per app."""
import os
from enum import Enum
from pathlib import Path
import yaml
import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import render, write_sql

THIS_PATH = Path(os.path.dirname(__file__))


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    firefox_desktop = "Firefox Desktop"


@click.command()
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
@click.option(
    "--target-project",
    "--target_project",
    help="Google Cloud project ID",
    default="moz-fx-data-shared-prod",
)
@use_cloud_function_option
def generate(target_project, output_dir, use_cloud_function):
    """Generate per-app queries, views and metadata for review checker aggregates.

    The parent folders will be created if not existing and existing files will be overwritten.
    """
    with open(THIS_PATH / "templates/templating.yaml", "r") as f:
        template_config = yaml.safe_load(f)
    
    output_dir = Path(output_dir) / target_project
    for query, args in template_config["queries"].items():
        template_query_dir = THIS_PATH / "templates" / query
        env = Environment(loader=FileSystemLoader(str(THIS_PATH / "templates"/query)))
        query_template = env.get_template("query.sql")
        metadata_template = "metadata.yaml"
        view_template = env.get_template("view.sql")
        for browser in Browsers:
            print(browser)
            query_sql = reformat(
                query_template.render(
                    project_id=target_project,
                    app_value=browser.value,
                    app_name=browser.name,
                )
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{browser.name}_derived.{query}_v1",
                basename="query.sql",
                sql=query_sql,
                skip_existing=False,
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{browser.name}_derived.{query}_v1",
                basename="metadata.yaml",
                sql=render(
                    metadata_template,
                    template_folder=THIS_PATH / "templates" / query,
                    app_value=browser.value,
                    app_name=browser.name,
                    format=False,
                ),
                skip_existing=False,
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{browser.name}.{query}",
                basename="view.sql",
                sql=reformat(
                    view_template.render(
                        project_id=target_project,
                        app_name=browser.name,
                    )
                ),
                skip_existing=False,
            )
