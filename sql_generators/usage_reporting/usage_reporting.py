"""Usage Reporting ETL."""

from os import path
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql
from sql_generators.glean_usage.common import get_app_info

GENERATOR_ROOT = Path(path.dirname(__file__))

HEADER = f"Generated via `{GENERATOR_ROOT.name}` SQL generator."
VERSION = "v1"

TEMPLATES_LOCATION = "templates"

CHANNEL_TEMPLATES = (
    "usage_reporting_clients_daily_v1.query.sql.jinja",
    "usage_reporting_clients_first_seen_v1.query.sql.jinja",
    "usage_reporting_clients_last_seen_v1.query.sql.jinja",
)
CHANNEL_VIEW_TEMPLATE = "channel.view.sql.jinja"

ARTIFACT_TEMPLATES = (
    "metadata.yaml.jinja",
    "schema.yaml.jinja",
    "bigconfig.yaml.jinja",
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

        app_info_filtered[app_name] = dict()

        # If channels is set to None it means data from multiple channels exists in the same table.
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


def generate_usage_reporting(target_project: str, output_dir: Path):
    """Generate usage_reporting queries and views."""
    usage_reporting_apps = get_generation_config()
    generator_apps_info = get_specific_apps_app_info_from_probe_scraper(
        usage_reporting_apps
    )

    output_dir = Path(output_dir) / target_project
    jinja_env = Environment(loader=FileSystemLoader(str(GENERATOR_ROOT / "templates")))

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
                        basename=".".join(
                            channel_query_artifact_template.split(".")[:-1]
                        ),
                        sql=rendered_artifact,
                        skip_existing=False,
                    )

                channel_view_template = jinja_env.get_template(CHANNEL_VIEW_TEMPLATE)

                # Do not render channel view if only a single channel exists.
                if channel_name == "multichannel":
                    continue

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

            channels_info = [
                {
                    "channel_dataset": channel_info["bq_dataset_family"],
                    "channel_name": channel_info["app_channel"],
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

        active_users_dataset_name = ACTIVE_USERS_VIEW_TEMPLATE.split(".")[0]
        active_users_view_template = jinja_env.get_template(ACTIVE_USERS_VIEW_TEMPLATE)
        rendered_active_users_view = active_users_view_template.render(
            **app_template_args,
            view_name=active_users_dataset_name,
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}.{active_users_dataset_name}",
            basename="view.sql",
            sql=reformat(rendered_active_users_view),
            skip_existing=False,
        )

        composite_active_users_dataset_name = COMPOSITE_ACTIVE_USERS_TEMPLATE.split(
            "."
        )[0]
        composite_active_users_view_template = jinja_env.get_template(
            COMPOSITE_ACTIVE_USERS_TEMPLATE
        )
        rendered_composite_active_users_view = (
            composite_active_users_view_template.render(
                **app_template_args,
                view_name=composite_active_users_dataset_name,
            )
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}.{composite_active_users_dataset_name}",
            basename="view.sql",
            sql=reformat(rendered_composite_active_users_view),
            skip_existing=False,
        )

        active_users_aggregates_dataset_name = (
            ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE.split(".")[0]
        )
        active_users_aggregates_view_template = jinja_env.get_template(
            ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE
        )
        rendered_active_users_aggregates_view = (
            active_users_aggregates_view_template.render(
                **app_template_args,
                view_name=active_users_aggregates_dataset_name,
            )
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}.{active_users_aggregates_dataset_name}",
            basename="view.sql",
            sql=reformat(rendered_active_users_aggregates_view),
            skip_existing=False,
        )

        active_users_aggregates_dataset_name = ACTIVE_USERS_AGGREGATES_TEMPLATE.split(
            "."
        )[0]
        active_users_aggregates_template = jinja_env.get_template(
            ACTIVE_USERS_AGGREGATES_TEMPLATE
        )
        rendered_active_users_aggregates = active_users_aggregates_template.render(
            **app_template_args,
            view_name=active_users_aggregates_dataset_name,
        )

        active_users_aggregates_table_id = f"{target_project}.{app_name}_derived.{active_users_aggregates_dataset_name}"

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}_derived.{active_users_aggregates_dataset_name}",
            basename="query.sql",
            sql=reformat(rendered_active_users_aggregates),
            skip_existing=False,
        )

        for query_artifact_template in ARTIFACT_TEMPLATES:
            _artifact_template = jinja_env.get_template(
                f"{active_users_aggregates_dataset_name}.{query_artifact_template}"
            )
            rendered_artifact = _artifact_template.render(
                **channel_args,
                format=False,
            )

            write_sql(
                output_dir=output_dir,
                full_table_id=active_users_aggregates_table_id,
                basename=".".join(query_artifact_template.split(".")[:-1]),
                sql=rendered_artifact,
                skip_existing=False,
            )

        # composite active users aggregates
        composite_active_users_aggregates_dataset_name = (
            COMPOSITE_ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE.split(".")[0]
        )
        composite_active_users_aggregates_view_template = jinja_env.get_template(
            COMPOSITE_ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE
        )
        rendered_composite_active_users_aggregates_view = (
            composite_active_users_aggregates_view_template.render(
                **app_template_args,
                view_name=composite_active_users_aggregates_dataset_name,
            )
        )

        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{app_name}.{composite_active_users_aggregates_dataset_name}",
            basename="view.sql",
            sql=reformat(rendered_composite_active_users_aggregates_view),
            skip_existing=False,
        )


# TODO: resolve this later, getting an import error right now when trying to run the script directly.
# if __name__ == "__main__":
#     from argparse import ArgumentParser

#     parser = ArgumentParser(description=__doc__)
#     parser.add_argument("--project", default="moz-fx-data-shared-prod")
#     parser.add_argument("--dataset", default="sql")
#     args = parser.parse_args()

#     generate_usage_reporting(args.project, args.output_dir)
