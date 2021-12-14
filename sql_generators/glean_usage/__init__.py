"""bigquery-etl CLI glean_usage command."""
import click
import os
import sys
from functools import partial
from pathlib import Path
from pathos.multiprocessing import ProcessingPool

from bigquery_etl.cli.utils import is_valid_project, table_matches_patterns

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

import baseline_clients_daily  # noqa: E402
import baseline_clients_first_seen  # noqa: E402
import baseline_clients_last_seen  # noqa: E402
import clients_last_seen_joined  # noqa: E402
import events_unnested  # noqa: E402
import glean_app_ping_views  # noqa: E402
import metrics_clients_daily  # noqa: E402
import metrics_clients_last_seen  # noqa: E402
from common import get_app_info, list_baseline_tables  # noqa: E402

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

# * mlhackweek_search was an experiment that we don't want to generate tables
# for
# * regrets_reporter currently refers to two applications, skip the glean
# one to avoid confusion: https://github.com/mozilla/bigquery-etl/issues/2499
SKIP_APPS = ["mlhackweek_search", "regrets_reporter"]


@click.command()
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

    with ProcessingPool(parallelism) as pool:
        pool.map(lambda f: f[0](f[1]), generate_per_app_id + generate_per_app)
