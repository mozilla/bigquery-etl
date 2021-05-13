"""bigquery-etl CLI glean_usage command"""
from functools import partial
from multiprocessing.pool import ThreadPool
import click

from ..cli.utils import (
    is_valid_project,
    table_matches_patterns,
)
from ..glean_usage import (
    baseline_clients_daily,
    baseline_clients_first_seen,
    baseline_clients_last_seen,
)
from ..glean_usage.common import list_baseline_tables, get_app_info

# list of methods for generating queries
GLEAN_TABLES = [
    baseline_clients_daily.BaselineClientsDailyTable(),
    baseline_clients_first_seen.BaselineClientsFirstSeenTable(),
    baseline_clients_last_seen.BaselineClientsLastSeenTable(),
]


@click.group(help="Commands for managing Glean usage.")
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
def generate(project_id, output_dir, parallelism, exclude, only):
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

    for table in GLEAN_TABLES:
        with ThreadPool(parallelism) as pool:
            pool.map(
                partial(
                    table.generate_per_app_id,
                    project_id,
                    output_dir=output_dir,
                ),
                baseline_tables,
            )

        # per app specific datasets
        app_info = get_app_info().values()

        with ThreadPool(parallelism) as pool:
            pool.map(
                partial(table.generate_per_app, project_id, output_dir=output_dir),
                app_info,
            )
