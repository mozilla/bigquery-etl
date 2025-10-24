"""GLEAN Usage."""

from functools import cache, partial
from pathlib import Path

import click
from pathos.multiprocessing import ProcessingPool

from bigquery_etl.cli.utils import (
    is_valid_project,
    table_matches_patterns,
    use_cloud_function_option,
)
from bigquery_etl.config import ConfigLoader
from bigquery_etl.dryrun import get_id_token
from sql_generators.glean_usage import (
    baseline_clients_daily,
    baseline_clients_first_seen,
    baseline_clients_last_seen,
    clients_last_seen_joined,
    event_error_monitoring,
    event_flow_monitoring,
    event_monitoring_live,
    events_stream,
    events_unnested,
    glean_app_ping_views,
    metrics_clients_daily,
    metrics_clients_last_seen,
)
from sql_generators.glean_usage.common import get_app_info, list_tables

# list of methods for generating queries
GLEAN_TABLES = [
    glean_app_ping_views.GleanAppPingViews(),
    baseline_clients_daily.BaselineClientsDailyTable(),
    baseline_clients_first_seen.BaselineClientsFirstSeenTable(),
    baseline_clients_last_seen.BaselineClientsLastSeenTable(),
    events_unnested.EventsUnnestedTable(),
    metrics_clients_daily.MetricsClientsDaily(),
    metrics_clients_last_seen.MetricsClientsLastSeen(),
    clients_last_seen_joined.ClientsLastSeenJoined(),
    event_monitoring_live.EventMonitoringLive(),
    event_error_monitoring.EventErrorMonitoring(),
    event_flow_monitoring.EventFlowMonitoring(),
    events_stream.EventsStreamTable(),
]


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
    """Generate per-app_id queries and views, and per-app dataset metadata and union views.

    Note that a file won't be generated if a corresponding file is already present
    in the target directory, which allows manual overrides of generated files by
    checking them into the sql/ tree of the default branch of the repository.
    """
    table_filter = partial(table_matches_patterns, "*", False)

    if only:
        table_filter = partial(table_matches_patterns, only, False)
    elif exclude:
        table_filter = partial(table_matches_patterns, exclude, True)

    output_dir = Path(output_dir) / target_project

    # per app specific datasets
    app_info = get_app_info()
    if app_name:
        app_info = {name: info for name, info in app_info.items() if name == app_name}

    app_info = [
        info
        for name, info in app_info.items()
        if name
        not in ConfigLoader.get("generate", "glean_usage", "skip_apps", fallback=[])
    ]

    app_channel_datasets = set(
        app_channel["bq_dataset_family"] + "_stable"
        for app_channels in app_info
        for app_channel in app_channels
    )

    @cache
    def get_tables(table_name="baseline_v1"):
        tables = list_tables(
            project_id=target_project,
            only_tables=[only] if only else None,
            table_filter=table_filter,
            table_name=table_name,
        )

        return [
            table for table in tables if table.split(".")[1] in app_channel_datasets
        ]

    id_token = get_id_token()

    # Prepare parameters so that generation of all Glean datasets can be done in parallel

    # Parameters to generate per-app_id datasets consist of the function to be called
    # and baseline tables
    generate_per_app_id = [
        (
            partial(
                table.generate_per_app_id,
                target_project,
                output_dir=output_dir,
                use_cloud_function=use_cloud_function,
                app_info=app_info,
                parallelism=parallelism,
                id_token=id_token,
            ),
            base_table,
        )
        for table in GLEAN_TABLES
        for base_table in get_tables(table_name=table.base_table_name)
    ]

    base_tables = {}
    unique_base_table_names = {table.base_table_name for table in GLEAN_TABLES}
    for table_name in unique_base_table_names:
        base_tables[table_name] = get_tables(table_name=table_name)

    def all_base_tables_exist(app_info, table_name="baseline_v1"):
        """Check if baseline tables exist for all app datasets."""
        # Extract dataset names from table names (format: project.dataset.table)
        existing_datasets = {table.split(".")[1] for table in base_tables[table_name]}

        # Check if all app datasets have corresponding tables
        if isinstance(app_info, dict):
            required_datasets = {f"{app_info['bq_dataset_family']}_stable"}
        else:
            required_datasets = {
                f"{app['bq_dataset_family']}_stable" for app in app_info
            }

        return all(dataset in existing_datasets for dataset in required_datasets)

    # Parameters to generate per-app datasets consist of the function to be called
    # and app_info
    generate_per_app = [
        (
            partial(
                table.generate_per_app,
                target_project,
                output_dir=output_dir,
                use_cloud_function=use_cloud_function,
                parallelism=parallelism,
                id_token=id_token,
                all_base_tables_exist=(
                    all_base_tables_exist(info, table_name=table.base_table_name)
                    if hasattr(table, "per_app_requires_all_base_tables")
                    and table.per_app_requires_all_base_tables
                    else None
                ),
            ),
            info,
        )
        for info in app_info
        for table in GLEAN_TABLES
    ]

    # Parameters to generate datasets that union all app datasets
    generate_across_apps = [
        (
            partial(
                table.generate_across_apps,
                target_project,
                output_dir=output_dir,
                use_cloud_function=use_cloud_function,
                parallelism=parallelism,
            ),
            app_info,
        )
        for table in GLEAN_TABLES
    ]

    with ProcessingPool(parallelism) as pool:
        pool.map(
            lambda f: f[0](f[1]),
            generate_per_app_id + generate_per_app + generate_across_apps,
        )
