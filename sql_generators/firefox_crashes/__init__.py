from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema import Schema
from bigquery_etl.util.common import write_sql
from sql_generators.glean_usage.common import get_app_info

CRASH_TABLES = [
    ("moz-fx-data-shared-prod", "firefox_desktop", "crash"),
    ("moz-fx-data-shared-prod", "firefox_crashreporter", "crash"),
    ("moz-fx-data-shared-prod", "fenix", "crash"),
    ("moz-fx-data-shared-prod", "focus_android", "crash"),
    ("moz-fx-data-shared-prod", "klar_android", "crash"),
]

# Apps that get a `<app_name>.crash_live` view over their per-app-id live tables,
# mapped to the subject used in the generated description.
CRASH_LIVE_APPS = {
    "firefox_desktop": "`firefox-desktop`",
    "firefox_crashreporter": "`firefox-crashreporter`",
    "fenix": "the Fenix (Firefox for Android) app ids",
    "focus_android": "the Focus for Android app ids",
    "klar_android": "the Klar for Android app ids",
}

# Fenix determines the channel from the app id and build rather than the
# static channel in the app listings, matching the `<app_name>.crash` views.
FENIX_CHANNEL_EXPRESSION = """\
mozfun.norm.fenix_app_info("{bq_dataset_family}", client_info.app_build).channel \
AS normalized_channel,"""

# Deprecated apps
EXCLUDED_DATASET_FAMILIES = {
    "org_mozilla_fenix_nightly",
    "org_mozilla_fennec_aurora",
}


def _generate_crash_live_views(
    target_project, output_dir, use_cloud_function, env, app_info
):
    """Generate `<app_name>.crash_live` views over the per-app-id live tables."""
    for dataset, description_subject in CRASH_LIVE_APPS.items():
        apps = app_info.get(dataset)
        if not apps:
            raise ValueError(f"No app listings found for {dataset}")

        included = [
            app
            for app in apps
            if app["bq_dataset_family"] not in EXCLUDED_DATASET_FAMILIES
        ]
        if not included:
            raise ValueError(f"All app ids of {dataset} are excluded")

        schemas = {}
        for app in included:
            live_table = f"{target_project}.{app['bq_dataset_family']}_live.crash_v1"
            project, live_dataset, table = live_table.split(".")
            schema = Schema.for_table(
                project=project,
                dataset=live_dataset,
                table=table,
                partitioned_by="submission_timestamp",
                use_cloud_function=use_cloud_function,
            )
            # Schema.for_table returns an empty schema for any failure, including
            # authentication and dry run errors.
            if len(schema.schema["fields"]) == 0:
                raise ValueError(
                    f"Could not get schema for {live_table} from dry run, "
                    f"possible authentication issue."
                )
            schemas[live_table] = (app, schema)

        combined_schema = Schema.empty()
        for _, schema in schemas.values():
            combined_schema.merge(schema)

        # Only the union views distinguish the app id, and the channel comes from
        # the app listings there because the live tables of a single app id all
        # carry the same channel.
        multiple_app_ids = len(schemas) > 1

        sources = []
        for live_table, (app, schema) in schemas.items():
            if not multiple_app_ids:
                channel_expression = ""
            elif dataset == "fenix":
                channel_expression = FENIX_CHANNEL_EXPRESSION.format(
                    bq_dataset_family=app["bq_dataset_family"]
                )
            elif app["app_channel"]:
                channel_expression = f"\"{app['app_channel']}\" AS normalized_channel,"
            else:
                channel_expression = "normalized_channel,"

            # normalized_channel is selected explicitly above when the view
            # overrides it, so drop it from the field list to avoid duplicates.
            fields_to_remove = ["normalized_channel"] if channel_expression else None

            sources.append(
                {
                    "table": live_table,
                    "normalized_app_id": (
                        app["bq_dataset_family"] if multiple_app_ids else None
                    ),
                    "channel_expression": channel_expression,
                    # Structs are only unnested for unions, where the column
                    # order of each branch has to line up. A single source can
                    # select its records as-is.
                    "fields": schema.generate_compatible_select_expression(
                        combined_schema,
                        fields_to_remove=fields_to_remove,
                        unnest_structs=multiple_app_ids,
                    ),
                }
            )

        view = env.get_template("crash_live.view.sql").render(
            project_id=target_project,
            dataset=dataset,
            sources=sources,
        )
        full_table_id = f"{target_project}.{dataset}.crash_live"
        write_sql(
            Path(output_dir) / target_project,
            full_table_id,
            "view.sql",
            reformat(view),
        )
        write_sql(
            Path(output_dir) / target_project,
            full_table_id,
            "metadata.yaml",
            env.get_template("crash_live.metadata.yaml").render(
                dataset=dataset,
                description_subject=description_subject,
            ),
            # checked-in metadata can override the workgroup access if it exists
            skip_existing=True,
        )


@click.command("generate")
@click.option(
    "--target-project",
    "--target_project",
    help="Which project the queries should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@use_cloud_function_option
def generate(target_project, output_dir, use_cloud_function):
    schemas = {
        f"{project}.{dataset}.{table}": Schema.for_table(
            project=project,
            dataset=dataset,
            table=table,
            partitioned_by="submission_timestamp",
            use_cloud_function=use_cloud_function,
        )
        for project, dataset, table in CRASH_TABLES
    }

    combined_schema = Schema.empty()
    for table_name, schema in schemas.items():
        if len(schema.schema["fields"]) == 0:
            raise ValueError(
                f"Could not get schema for {table_name} from dry run, "
                f"possible authentication issue."
            )
        combined_schema.merge(schema)

    fields_per_table = {
        table_name: schema.generate_compatible_select_expression(
            combined_schema,
            unnest_structs=False,
        )
        for table_name, schema in schemas.items()
    }

    template_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(template_dir))

    template = env.get_template("firefox_crashes.query.sql")
    query = template.render(tables=fields_per_table, project_id=target_project)

    write_sql(
        Path(output_dir) / target_project,
        f"{target_project}.telemetry_derived.firefox_crashes_v1",
        "query.sql",
        reformat(query),
    )

    combined_schema.to_yaml_file(
        Path(output_dir)
        / target_project
        / "telemetry_derived"
        / "firefox_crashes_v1"
        / "schema.yaml"
    )

    _generate_crash_live_views(
        target_project, output_dir, use_cloud_function, env, get_app_info()
    )


if __name__ == "__main__":
    generate()
