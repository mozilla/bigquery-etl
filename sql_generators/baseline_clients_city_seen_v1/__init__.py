"""Generate baseline clients city seen per app."""

import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import render, write_sql
from sql_generators.glean_usage import get_app_info

THIS_PATH = Path(os.path.dirname(__file__))
TABLE_NAME = os.path.basename(os.path.normpath(THIS_PATH))
BASE_NAME = "_".join(TABLE_NAME.split("_")[:-1])


def get_app_info_map() -> Dict[str, List[str]]:
    """Build a map of app name to a list of bq_dataset_families."""
    app_info = get_app_info()

    app_info = [
        info
        for name, info in app_info.items()
        if name
        not in ConfigLoader.get("generate", "glean_usage", "skip_apps", fallback=[])
    ]

    app_info_map: Dict[str, List[str]] = defaultdict(list)

    for app in app_info:
        for app_dataset in app:
            app_name = app_dataset["app_name"]
            app_id = app_dataset["bq_dataset_family"]
            app_info_map[app_name].append(app_id)

    return app_info_map


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

    query_template = env.get_template("query.sql")
    view_template = env.get_template("view.sql")
    schema_template = "schema.yaml"
    metadata_template = "metadata.yaml"

    # can be used to get all app_id:
    app_info_map = get_app_info_map()
    # browser_app_list = ["firefox_desktop", "fenix"]
    # app_info_map = {k: app_info_map[k] for k in browser_app_list if k in app_info_map}

    # TODO: listing only a few app_ids for testing
    app_info_map = {
        "firefox_desktop": ["firefox_desktop"],
        "fenix": [
            "org_mozilla_firefox",
            "org_mozilla_fenix_nightly",
            # 'org_mozilla_firefox_beta',
            # 'org_mozilla_fenix',
            # 'org_mozilla_fennec_aurora'
        ],
    }

    for app_name, app_id_list in app_info_map.items():

        query_sql = reformat(
            query_template.render(
                project_id=target_project,
                app_id_list=app_id_list,
                app_name=app_name,
                table_name=TABLE_NAME,
            )
        )

        # Write query SQL files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}_derived.{TABLE_NAME}",
            basename="query.sql",
            sql=query_sql,
            skip_existing=False,
        )

        # Write metadata YAML files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}_derived.{TABLE_NAME}",
            basename="metadata.yaml",
            sql=render(
                metadata_template,
                template_folder=THIS_PATH / "templates",
                app_name=app_name,
                table_name=TABLE_NAME,
                format=False,
            ),
            skip_existing=False,
        )

        # Write schema YAML files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}_derived.{TABLE_NAME}",
            basename="schema.yaml",
            sql=render(
                schema_template,
                template_folder=THIS_PATH / "templates",
                format=False,
            ),
            skip_existing=False,
        )

        # Write view SQL files.
        view_sql = reformat(
            view_template.render(
                project_id=target_project,
                app_name=app_name,
                table_name=TABLE_NAME,
            )
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}.{BASE_NAME}",
            basename="view.sql",
            sql=view_sql,
            skip_existing=False,
        )
