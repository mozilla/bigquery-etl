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
TABLE_NAME_DELETION_REQUEST = "active_users_aggregates_deletion_request"


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    # The initial implementation is a prototype for some mobile browsers.
    # Desktop is excluded due to the bigger size of data.
    # Focus Android is excluded due to its higher complexity.
    fenix = "Fenix"
    focus_ios = "Focus iOS"
    firefox_ios = "Firefox iOS"
    klar_ios = "Klar iOS"


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
    """Generate per-app queries, views and metadata for active users aggregates.

    As calculated for the client_ids who have sent a deletion request ping.
    """
    env = Environment(loader=FileSystemLoader(str(THIS_PATH / "templates")))
    mobile_deletion_request_query_template = env.get_template(
        "mobile_deletion_request_query.sql"
    )
    metadata_deletion_request_template = "metadata_deletion_request.yaml"
    output_dir = Path(output_dir) / target_project

    for browser in Browsers:
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME_DELETION_REQUEST}_v1",
            basename="query.sql",
            sql=reformat(
                mobile_deletion_request_query_template.render(
                    project_id=target_project,
                    app_value=browser.value,
                    app_name=browser.name,
                )
            ),
            skip_existing=False,
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME_DELETION_REQUEST}_v1",
            basename="metadata.yaml",
            sql=render(
                metadata_deletion_request_template,
                template_folder=THIS_PATH / "templates",
                app_value=browser.value,
                app_name=browser.name,
                parameters='["end_date:DATE:{{macros.ds_add(ds, 27)}}", "start_date:DATE:{{macros.ds_add(ds, 27-28*4)}}"]',
                format=False,
            ),
            skip_existing=False,
        )
