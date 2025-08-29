"""Generate clients city seen per app.  This code is only used to initialize a derived table for each app."""

import os
from enum import Enum
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import render, write_sql

# from sql_generators.glean_usage import get_app_info

THIS_PATH = Path(os.path.dirname(__file__))
TABLE_NAME = os.path.basename(os.path.normpath(THIS_PATH))


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    # can be used to get all app_id:
    # probe_scraper_app_info = get_app_info()

    firefox_desktop = "Firefox Desktop"
    fenix = "Fenix"
    # focus_ios = "Focus iOS"
    # focus_android = "Focus Android"
    # firefox_ios = "Firefox iOS"
    # klar_ios = "Klar iOS"
    # klar_android = "Klar Android"


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
    """Generate per-app queries, schemas, and metadata for clients’ first/last seen city and subdivisions.

    Initializes the derived tables from stable sources.
    After the city/subdivision fields in the stable tables are nulled,
    this initializer is no longer applicable — ongoing updates will come from the live tables.
    """
    env = Environment(loader=FileSystemLoader(str(THIS_PATH / "templates")))
    output_dir = Path(output_dir) / target_project
    # query templates
    query_template = env.get_template("query.sql")
    # schema templates
    schema_template = "schema.yaml"
    # metadata template
    metadata_template = "metadata.yaml"

    for browser in Browsers:

        if browser.name == "fenix":

            # TODO: listing only a few app_ids for testing
            app_id_list = [
                "org_mozilla_firefox",
                "org_mozilla_fenix_nightly",
                # "org_mozilla_fennec_aurora",
                # "org_mozilla_firefox_beta",
                # "org_mozilla_fenix",
            ]

            query_sql = reformat(
                query_template.render(
                    project_id=target_project,
                    app_id_list=app_id_list,
                    app_name=browser.name,
                )
            )

        else:
            query_sql = reformat(
                query_template.render(
                    project_id=target_project,
                    app_id_list=[browser.name],
                    app_name=browser.name,
                )
            )

        # Write query SQL files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="query.sql",
            sql=query_sql,
            skip_existing=False,
        )

        # Write metadata YAML files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="metadata.yaml",
            sql=render(
                metadata_template,
                template_folder=THIS_PATH / "templates",
                app_name=browser.name,
                app_value=browser.value,
                table_name=TABLE_NAME,
                format=False,
            ),
            skip_existing=False,
        )

        # Write schema YAML files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="schema.yaml",
            sql=render(
                schema_template,
                template_folder=THIS_PATH / "templates",
                format=False,
            ),
            skip_existing=False,
        )
