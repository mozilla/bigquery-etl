"""Generate active users aggregates per app."""

import os
from enum import Enum
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import render, write_sql

THIS_PATH = Path(os.path.dirname(__file__))
TABLE_NAME = os.path.basename(os.path.normpath(THIS_PATH))
BASE_NAME = "_".join(TABLE_NAME.split("_")[:-1])
DATASET_FOR_UNIONED_VIEWS = "telemetry"
CHECKS_TEMPLATE_CHANNELS = {
    "firefox_ios": [
        {
            "name": "release",
            "table": "`moz-fx-data-shared-prod.org_mozilla_ios_firefox_live.baseline_v1`",
        },
        {
            "name": "beta",
            "table": "`moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_live.baseline_v1`",
        },
        {
            "name": "nightly",
            "table": "`moz-fx-data-shared-prod.org_mozilla_ios_fennec_live.baseline_v1`",
        },
    ],
    "focus_ios": [
        {"table": "`moz-fx-data-shared-prod.org_mozilla_ios_focus_live.baseline_v1`"}
    ],
    "focus_android": [
        {
            "name": "release",
            "table": "`moz-fx-data-shared-prod.org_mozilla_focus_live.baseline_v1`",
        },
        {
            "name": "beta",
            "table": "`moz-fx-data-shared-prod.org_mozilla_focus_beta_live.baseline_v1`",
        },
        {
            "name": "nightly",
            "table": "`moz-fx-data-shared-prod.org_mozilla_focus_nightly_live.baseline_v1`",
        },
    ],
    "klar_ios": [
        {"table": "`moz-fx-data-shared-prod.org_mozilla_ios_klar_live.baseline_v1`"}
    ],
    "klar_android": [
        {"table": "`moz-fx-data-shared-prod.org_mozilla_klar_live.baseline_v1`"}
    ],
}


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    fenix = "Fenix"
    focus_ios = "Focus iOS"
    focus_android = "Focus Android"
    firefox_ios = "Firefox iOS"
    klar_ios = "Klar iOS"
    klar_android = "Klar Android"


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
    """Generate per-app queries, views and metadata for active users and search counts aggregates.

    The parent folders will be created if not existing and existing files will be overwritten.
    """
    env = Environment(loader=FileSystemLoader(str(THIS_PATH / "templates")))
    output_dir = Path(output_dir) / target_project
    # templates
    mobile_query_template = env.get_template("mobile_query.sql")
    mobile_view_template = env.get_template("view.sql")
    active_users_aggregates_view_template = env.get_template("active_users_aggregates_device_view.sql")
    metadata_template = "metadata.yaml"
    mobile_schema_template = "schema.yaml"
    mobile_checks_template = env.get_template("mobile_checks.sql")
    fenix_checks_template = env.get_template("fenix_checks.sql")

    for browser in Browsers:
        query_sql = reformat(
            mobile_query_template.render(
                project_id=target_project,
                app_value=browser.value,
                app_name=browser.name,
            )
        )

        schema_template = mobile_schema_template

        if browser.name == "fenix":
            checks_sql = fenix_checks_template.render(
                project_id=target_project,
                app_value=browser.value,
                app_name=browser.name,
            )
        elif browser.name in CHECKS_TEMPLATE_CHANNELS.keys():
            checks_sql = mobile_checks_template.render(
                project_id=target_project,
                app_value=browser.value,
                app_name=browser.name,
                channels=CHECKS_TEMPLATE_CHANNELS[browser.name],
            )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="query.sql",
            sql=query_sql,
            skip_existing=False,
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="metadata.yaml",
            sql=render(
                metadata_template,
                template_folder=THIS_PATH / "templates",
                app_value=browser.value,
                app_name=browser.name,
                format=False,
            ),
            skip_existing=False,
        )

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

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="checks.sql",
            sql=checks_sql,
            skip_existing=False,
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}.{BASE_NAME}",
            basename="view.sql",
            sql=reformat(
                mobile_view_template.render(
                    project_id=target_project,
                    app_name=browser.name,
                    table_name=TABLE_NAME,
                )
            ),
            skip_existing=False,
        )

    write_sql(
        output_dir=output_dir,
        full_table_id=f"{target_project}.{DATASET_FOR_UNIONED_VIEWS}.{BASE_NAME}_mobile",
        basename="view.sql",
        sql=reformat(
            active_users_aggregates_view_template.render(
                project_id=target_project,
                dataset_id=DATASET_FOR_UNIONED_VIEWS,
                fenix_dataset=Browsers("Fenix").name,
                focus_ios_dataset=Browsers("Focus iOS").name,
                focus_android_dataset=Browsers("Focus Android").name,
                firefox_ios_dataset=Browsers("Firefox iOS").name,
                klar_ios_dataset=Browsers("Klar iOS").name,
                klar_android_dataset=Browsers("Klar Android").name
            )
        ),
        skip_existing=False,
    )
