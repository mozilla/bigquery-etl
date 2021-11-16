"""bigquery-etl CLI glean_usage command."""
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path
import click

from ..cli.utils import (
    is_valid_project,
    table_matches_patterns,
)
from ..glean_usage import (
    glean_app_ping_views,
    baseline_clients_daily,
    baseline_clients_first_seen,
    baseline_clients_last_seen,
    events_unnested,
    metrics_clients_daily,
    metrics_clients_last_seen,
    clients_last_seen_joined,
)
from ..glean_usage.common import list_baseline_tables, get_app_info

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
]
SKIP_APPS = ["mlhackweek_search", "regrets_reporter"]


@click.group(
    help="Commands for managing ETL about usage of Glean apps. "
    "(baseline_clients_daily, etc.)"
)
def glean_usage():
    """Create the CLI group for the glean_usage command."""
    pass


@glean_usage.command()
@click.option(
    "--project-id",
    "--project_id",
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
    help="Generate per-app_id queries+views and per-app dataset metadata and union views.",
)
def generate(project_id, output_dir, parallelism, exclude, only, app_name):
    """Generate per-appId queries, views along, per-app dataset metadata and union views."""
    table_filter = partial(table_matches_patterns, "*", False)

    if only:
        table_filter = partial(table_matches_patterns, only, False)
    elif exclude:
        table_filter = partial(table_matches_patterns, exclude, True)

    baseline_tables = list_baseline_tables(
        project_id=project_id,
        only_tables=[only] if only else None,
        table_filter=table_filter,
    )
    # filter out skipped apps
    baseline_tables = [
        baseline_table
        for baseline_table in baseline_tables
        if baseline_table.split(".")[1]
        not in [f"{skipped_app}_stable" for skipped_app in SKIP_APPS]
    ]

    output_dir = Path(output_dir) / project_id

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
                project_id,
                output_dir=output_dir,
            ),
            baseline_table,
        )
        for baseline_table in baseline_tables
        for table in GLEAN_TABLES
    ]

    # Parameters to generate per-app datasets consist of the function to be called
    # and app_info
    generate_per_app = [
        (
            partial(table.generate_per_app, project_id, output_dir=output_dir),
            info,
        )
        for info in app_info
        for table in GLEAN_TABLES
    ]

    with Pool(parallelism) as pool:
        pool.starmap(run_generate, generate_per_app_id + generate_per_app)


def run_generate(func, params):
    """Use in `generate()` for generating glean datasets in parallel."""
    func(params)
