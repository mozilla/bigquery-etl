"""Usage Reporting ETL."""

from os import path
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import is_valid_project, use_cloud_function_option
from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql
from sql_generators.glean_usage.common import get_app_info

GENERATOR_ROOT = Path(path.dirname(__file__))

HEADER = f"Generated via `{GENERATOR_ROOT.name}` SQL generator."
VERSION = "v1"

TEMPLATES_LOCATION = "templates"
CHANNEL_TEMPLATES = (
    "usage_reporting_clients_daily_v1.query.sql",
    "usage_reporting_clients_first_seen_v1.query.sql",
    "usage_reporting_clients_last_seen_v1.query.sql",
)
CHANNEL_VIEW_TEMPLATE = "channel.view.sql"
ARTIFACT_TEMPLATES = (
    "metadata.yaml",
    "schema.yaml",
)
APP_UNION_VIEW_TEMPLATE = "app_union.view.sql"


@click.command()
@click.option(
    "--target-project",
    "--target_project",
    help="GCP project ID",
    default="moz-fx-data-shared-prod",
    callback=is_valid_project,
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
@click.option(
    "--parallelism",
    "-p",
    help="Maximum number of tasks to execute concurrently",
    default=8,
)
@click.option(
    "--except",
    "-x",
    "exclude",
    help="Process all tables except for the given tables",
)
@click.option(
    "--only",
    "-o",
    help="Process only the given tables",
)
@click.option(
    "--app_name",
    "--app-name",
    help="App to generate per-app dataset metadata and union views for.",
)
@use_cloud_function_option
def generate(
    target_project, output_dir, parallelism, exclude, only, app_name, use_cloud_function
):
    """Generate usage_reporting queries and views."""
    usage_reporting_apps = ConfigLoader.get(
        "generate", "usage_reporting", "apps", fallback=[]
    )

    app_info_filtered = dict()

    for app_name, app_info in get_app_info().items():
        if app_name not in usage_reporting_apps:
            continue

        app_info_filtered[app_name] = dict()

        if len(app_info) == 1:
            app_info_filtered[app_name]["multichannel"] = app_info[0]
        else:
            for index, channel_info in enumerate(app_info):
                if (
                    channel := channel_info.get("app_channel")
                ) not in usage_reporting_apps[app_name]["channels"]:
                    continue

                app_info_filtered[app_name][f"{channel}__{index}"] = channel_info

    output_dir = Path(output_dir) / target_project

    jinja_env = Environment(loader=FileSystemLoader(str(GENERATOR_ROOT / "templates")))
    default_template_args = {
        "project_id": target_project,
        "usage_reporting_stable_table_name": "usage_reporting_v1",
        "header": HEADER,
    }

    for app_name, app_channels in app_info_filtered.items():
        app_template_args = {
            "app_name": app_name,
            **default_template_args,
        }

        for channel_template in CHANNEL_TEMPLATES:
            table_name = channel_template.split(".")[0]

            for channel_name, channel_info in app_channels.items():
                channel_dataset = channel_info["bq_dataset_family"]
                channel_args = {
                    "channel_name": channel_name.split("__")[0],
                    "channel_dataset": channel_dataset,
                    "table_name": table_name,
                    "view_name": table_name[:-3],
                    **app_template_args,
                }

                channel_table_id = (
                    f"{target_project}.{channel_dataset}_derived.{table_name}"
                )
                channel_view_id = (
                    f"{target_project}.{channel_dataset}.{table_name[:-3]}"
                )

                channel_query_template = jinja_env.get_template(channel_template)
                rendered_query = channel_query_template.render(
                    **channel_args,
                )

                write_sql(
                    output_dir=output_dir,
                    full_table_id=channel_table_id,
                    basename="query.sql",
                    sql=reformat(rendered_query),
                    skip_existing=False,
                )

                for channel_query_artifact_template in ARTIFACT_TEMPLATES:
                    _artifact_template = jinja_env.get_template(
                        f"{table_name}.{channel_query_artifact_template}"
                    )
                    rendered_artifact = _artifact_template.render(
                        **channel_args,
                        format=False,
                    )

                    write_sql(
                        output_dir=output_dir,
                        full_table_id=channel_table_id,
                        basename=channel_query_artifact_template,
                        sql=rendered_artifact,
                        skip_existing=False,
                    )

                channel_view_template = jinja_env.get_template(CHANNEL_VIEW_TEMPLATE)

                # # Do not render channel view if only a single channel exists.
                # if channel_name == "multichannel":
                #     continue

                rendered_channel_view = channel_view_template.render(
                    **channel_args,
                )

                write_sql(
                    output_dir=output_dir,
                    full_table_id=channel_view_id,
                    basename="view.sql",
                    sql=reformat(rendered_channel_view),
                    skip_existing=False,
                )

            if not channel_info.get("app_channel"):
                continue

            channels_info = [
                {
                    "channel_dataset": channel_info["bq_dataset_family"],
                    "channel_name": channel_info.get("app_channel"),
                }
                for channel_info in app_channels.values()
            ]

            app_union_view_template = jinja_env.get_template(APP_UNION_VIEW_TEMPLATE)
            rendered_app_union_view = app_union_view_template.render(
                channels_info=channels_info,
                **app_template_args,
                table_name=table_name,
                view_name=table_name[:-3],
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{app_name}.{table_name[:-3]}",
                basename="view.sql",
                sql=reformat(rendered_app_union_view),
                skip_existing=False,
            )
