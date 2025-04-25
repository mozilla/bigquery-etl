"""Usage Reporting ETL."""

from functools import partial
from os import path
from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql
from sql_generators.glean_usage.common import get_app_info

GENERATOR_ROOT = Path(path.dirname(__file__))

HEADER = f"Generated via `{GENERATOR_ROOT.name}` SQL generator."
VERSION = "v1"

TEMPLATES_LOCATION = "templates"

# TODO: can we have the templates picked up in some way automatically?
CHANNEL_TEMPLATES = (
    "usage_reporting_clients_daily_v1.query.sql.jinja",
    "usage_reporting_clients_first_seen_v1.query.sql.jinja",
    "usage_reporting_clients_last_seen_v1.query.sql.jinja",
)
CHANNEL_VIEW_TEMPLATE = "channel.view.sql.jinja"

ARTIFACT_TEMPLATES = (
    "metadata.yaml.jinja",
    "schema.yaml.jinja",
    "bigconfig.yml.jinja",
)

BIGEYE_COLLECTION = "Operational Checks"
BIGEYE_NOTIFICATION_SLACK_CHANNEL = "#de-bigeye-triage"

APP_UNION_VIEW_TEMPLATE = "app_union.view.sql.jinja"

ACTIVE_USERS_VIEW_TEMPLATE = "usage_reporting_active_users.view.sql.jinja"
COMPOSITE_ACTIVE_USERS_TEMPLATE = "composite_active_users.view.sql.jinja"

ACTIVE_USERS_AGGREGATES_TEMPLATE = (
    "usage_reporting_active_users_aggregates_v1.query.sql.jinja"
)
ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE = (
    "usage_reporting_active_users_aggregates.view.sql.jinja"
)

COMPOSITE_ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE = (
    "composite_active_users_aggregates.view.sql.jinja"
)


def get_generation_config():
    """Retrieve external configuration for this generator."""
    # TODO: maybe we should have a data structure defined for this config and
    #       do validation as part of it.
    return ConfigLoader.get("generate", "usage_reporting", "apps", fallback=[])


def get_specific_apps_app_info_from_probe_scraper(usage_reporting_apps):
    """Retrieve app_info from probe scraper app definitions \
    and return only the info of apps defined in the generator configuration.

    The app info returned includes app_name, and bq namespaces containing data \
    for specific app channels.
    """
    probe_scraper_app_info = get_app_info()

    app_info_filtered: dict = dict()

    for app_name, app_info in probe_scraper_app_info.items():
        if app_name not in usage_reporting_apps:
            continue

        # TODO: turn the generatic dict into a custom app data structure.
        app_info_filtered[app_name] = dict()

        # If channels is set to None it means data from multiple channels exists in the same table.
        # TODO: should we use "multichannel" instead of None as an indicator of this in the config file?
        if usage_reporting_apps[app_name]["channels"] is None:
            app_info_filtered[app_name]["multichannel"] = {
                "app_channel": None,
                "app_name": app_info[0]["app_name"],
                "bq_dataset_family": app_info[0]["bq_dataset_family"],
            }
            continue

        for index, channel_info in enumerate(app_info):
            if (
                # this assumes that if no channel defined for an app in probe scraper
                # then the channel is "release".
                channel := channel_info.get("app_channel", "release")
            ) not in usage_reporting_apps[app_name]["channels"]:
                continue

            app_info_filtered[app_name][f"{channel}__{index}"] = {
                "app_channel": channel,
                "app_name": channel_info["app_name"],
                "bq_dataset_family": channel_info["bq_dataset_family"],
            }

    return app_info_filtered


def render_and_write_to_file(
    jinja_env: Environment,
    output_dir: Path,
    template: str,
    template_args: Dict[str, Any],
    table_id: str,
    basename: str,
    format: bool = True,
) -> str:
    """Render a Jinja template and write it to file."""
    rendered = jinja_env.get_template(template).render(**template_args)

    write_sql(
        output_dir=output_dir,
        skip_existing=False,
        full_table_id=table_id,
        basename=basename,
        sql=reformat(rendered) if format else rendered,
    )
    return table_id


def generate_usage_reporting(target_project: str, output_dir: Path):
    """Generate usage_reporting queries and views."""
    usage_reporting_apps = get_generation_config()
    generator_apps_info = get_specific_apps_app_info_from_probe_scraper(
        usage_reporting_apps
    )

    output_dir = Path(output_dir) / target_project
    jinja_env = Environment(loader=FileSystemLoader(str(GENERATOR_ROOT / "templates")))

    render_and_write_to_file_partial = partial(
        render_and_write_to_file, jinja_env=jinja_env, output_dir=output_dir
    )

    default_template_args = {
        "header": HEADER,
        "project_id": target_project,
        "usage_reporting_stable_table_name": "usage_reporting_v1",
        "bigeye_collection": BIGEYE_COLLECTION,
        "bigeye_notification_slack_channel": BIGEYE_NOTIFICATION_SLACK_CHANNEL,
    }

    for app_name, app_channels in generator_apps_info.items():
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
                    "view_name": ("_").join(table_name.split("_")[:-1]),
                    **app_template_args,
                }

                channel_table_id = (
                    f"{target_project}.{channel_dataset}_derived.{table_name}"
                )

                render_and_write_to_file_partial(
                    template=channel_template,
                    template_args=channel_args,
                    table_id=channel_table_id,
                    basename="query.sql",
                )

                for channel_query_artifact_template in ARTIFACT_TEMPLATES:
                    render_and_write_to_file_partial(
                        template=f"{table_name}.{channel_query_artifact_template}",
                        template_args=channel_args,
                        table_id=channel_table_id,
                        basename=".".join(
                            channel_query_artifact_template.split(".")[:-1]
                        ),
                        format=False,
                    )

                # Do not render channel view if only a single channel exists.
                if channel_name == "multichannel":
                    continue

                channel_view_id = (
                    f"{target_project}.{channel_dataset}.{table_name[:-3]}"
                )

                render_and_write_to_file_partial(
                    template=CHANNEL_VIEW_TEMPLATE,
                    template_args=channel_args,
                    table_id=channel_view_id,
                    basename="view.sql",
                )

            channels_info = [
                {
                    "channel_dataset": channel_info["bq_dataset_family"],
                    "channel_name": channel_info["app_channel"],
                }
                for channel_info in app_channels.values()
            ]

            render_and_write_to_file_partial(
                template=APP_UNION_VIEW_TEMPLATE,
                template_args={
                    "channels_info": channels_info,
                    **app_template_args,
                    "table_name": table_name,
                    "view_name": table_name[:-3],
                },
                table_id=f"{target_project}.{app_name}.{table_name[:-3]}",
                basename="view.sql",
            )

        active_users_dataset_name = ACTIVE_USERS_VIEW_TEMPLATE.split(".")[0]
        render_and_write_to_file_partial(
            template=ACTIVE_USERS_VIEW_TEMPLATE,
            template_args={
                **app_template_args,
                "view_name": active_users_dataset_name,
            },
            table_id=f"{target_project}.{app_name}.{active_users_dataset_name}",
            basename="view.sql",
        )

        composite_active_users_dataset_name = COMPOSITE_ACTIVE_USERS_TEMPLATE.split(
            "."
        )[0]
        render_and_write_to_file_partial(
            template=COMPOSITE_ACTIVE_USERS_TEMPLATE,
            template_args={
                **app_template_args,
                "view_name": composite_active_users_dataset_name,
            },
            table_id=f"{target_project}.{app_name}.{composite_active_users_dataset_name}",
            basename="view.sql",
        )

        active_users_aggregates_view_name = ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE.split(
            "."
        )[0]
        render_and_write_to_file_partial(
            template=ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE,
            template_args={
                **app_template_args,
                "view_name": active_users_aggregates_view_name,
            },
            table_id=f"{target_project}.{app_name}.{active_users_aggregates_view_name}",
            basename="view.sql",
        )

        active_users_aggregates_dataset_name = ACTIVE_USERS_AGGREGATES_TEMPLATE.split(
            "."
        )[0]
        active_users_aggregates_table_id = f"{target_project}.{app_name}_derived.{active_users_aggregates_dataset_name}"

        render_and_write_to_file_partial(
            template=ACTIVE_USERS_AGGREGATES_TEMPLATE,
            template_args={
                **app_template_args,
                "view_name": active_users_aggregates_dataset_name,
            },
            table_id=active_users_aggregates_table_id,
            basename="query.sql",
        )

        for query_artifact_template in ARTIFACT_TEMPLATES:
            render_and_write_to_file_partial(
                template=f"{active_users_aggregates_dataset_name}.{query_artifact_template}",
                template_args={
                    **app_template_args,
                    "table_name": active_users_aggregates_dataset_name,
                },
                table_id=active_users_aggregates_table_id,
                basename=".".join(query_artifact_template.split(".")[:-1]),
                format=False,
            )

        # composite active users aggregates
        composite_active_users_aggregates_dataset_name = (
            COMPOSITE_ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE.split(".")[0]
        )

        render_and_write_to_file_partial(
            template=COMPOSITE_ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE,
            template_args={
                **app_template_args,
                "view_name": composite_active_users_aggregates_dataset_name,
            },
            table_id=f"{target_project}.{app_name}.{composite_active_users_aggregates_dataset_name}",
            basename="view.sql",
        )


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--output_dir", default="sql")
    args = parser.parse_args()

    generate_usage_reporting(args.project, args.output_dir)
