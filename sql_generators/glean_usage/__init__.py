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
from sql_generators.glean_usage import (
    baseline_clients_daily,
    baseline_clients_first_seen,
    baseline_clients_last_seen,
    clients_last_seen_joined,
    event_error_monitoring,
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
    events_stream.EventsStreamTable(),
]

# * mlhackweek_search was an experiment that we don't want to generate tables
# for
# * regrets_reporter currently refers to two applications, skip the glean
# one to avoid confusion: https://github.com/mozilla/bigquery-etl/issues/2499
SKIP_APPS = ["mlhackweek_search", "regrets_reporter", "regrets_reporter_ucs"]


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

    @cache
    def get_tables(table_name="baseline_v1"):
        baseline_tables = list_tables(
            project_id=target_project,
            only_tables=[only] if only else None,
            table_filter=table_filter,
            table_name=table_name,
        )

        # filter out skipped apps
        return [
            baseline_table
            for baseline_table in baseline_tables
            if baseline_table.split(".")[1]
            not in [f"{skipped_app}_stable" for skipped_app in SKIP_APPS]
        ]

    output_dir = Path(output_dir) / target_project

    # per app specific datasets
    app_info = get_app_info()
    if app_name:
        app_info = {name: info for name, info in app_info.items() if name == app_name}

    app_info = [info for name, info in app_info.items() if name not in SKIP_APPS]

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
            ),
            baseline_table,
        )
        for table in GLEAN_TABLES
        for baseline_table in get_tables(table_name=table.base_table_name)
    ]

    # Parameters to generate per-app datasets consist of the function to be called
    # and app_info
    generate_per_app = [
        (
            partial(
                table.generate_per_app,
                target_project,
                output_dir=output_dir,
                use_cloud_function=use_cloud_function,
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
