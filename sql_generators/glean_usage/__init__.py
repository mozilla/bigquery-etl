"""GLEAN Usage."""
from functools import partial
from pathlib import Path

import click
import requests
from pathos.multiprocessing import ProcessingPool

from bigquery_etl.cli.utils import (
    is_valid_project,
    table_matches_patterns,
    use_cloud_function_option,
)
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
from sql_generators.glean_usage import (
    baseline_clients_daily,
    baseline_clients_first_seen,
    baseline_clients_last_seen,
    clients_last_seen_joined,
    event_monitoring_live,
    events_unnested,
    glean_app_ping_views,
    metrics_clients_daily,
    metrics_clients_last_seen,
)

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
]

# regrets_reporter currently refers to two applications, skip the glean
# one to avoid confusion: https://github.com/mozilla/bigquery-etl/issues/2499
SKIP_APPS = ["regrets_reporter", "regrets_reporter_ucs"]
APP_LISTINGS_URL = "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"


def get_app_info():
    """Return a list of applications from the probeinfo API."""
    resp = requests.get(APP_LISTINGS_URL)
    resp.raise_for_status()
    apps_json = resp.json()
    app_info = {}

    for app_variant_info in apps_json:
        app_name = app_variant_info["app_name"]
        if app_variant_info.get("deprecated", False):
            print(f"{app_name} is deprecated")
            continue
        if app_name in SKIP_APPS:
            print(f"{app_name} is skipped")
            continue
        elif app_name not in app_info:
            app_info[app_name] = []
        app_info[app_name].append(app_variant_info)

    return app_info


def _contains_glob(patterns):
    return any({"*", "?", "["}.intersection(pattern) for pattern in patterns)


def _extract_dataset_from_glob(pattern):
    # Assumes globs are in <dataset>.<table> form without a project specified.
    return pattern.split(".", 1)[0]


def list_baseline_tables(project_id, only_tables, table_filter):
    """Return names of all matching baseline tables in shared-prod."""
    prod_baseline_tables = [
        s.stable_table
        for s in get_stable_table_schemas()
        if s.schema_id == "moz://mozilla.org/schemas/glean/ping/1"
        and s.bq_table == "baseline_v1"
    ]
    prod_datasets_with_baseline = [t.split(".")[0] for t in prod_baseline_tables]
    stable_datasets = prod_datasets_with_baseline
    if only_tables and not _contains_glob(only_tables):
        # skip list calls when only_tables exists and contains no globs
        return [
            f"{project_id}.{t}"
            for t in only_tables
            if table_filter(t) and t in prod_baseline_tables
        ]
    if only_tables and not _contains_glob(
        _extract_dataset_from_glob(t) for t in only_tables
    ):
        stable_datasets = {_extract_dataset_from_glob(t) for t in only_tables}
        stable_datasets = {
            d
            for d in stable_datasets
            if d.endswith("_stable") and d in prod_datasets_with_baseline
        }
    return [
        f"{project_id}.{d}.baseline_v1"
        for d in stable_datasets
        if table_filter(f"{d}.baseline_v1")
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

    app_info = get_app_info()
    if app_name:
        app_info = {name: info for name, info in app_info.items() if name == app_name}

    non_deprecated_app_id_datasets = [
        f"{variant['bq_dataset_family']}_stable"
        for name, variants in app_info.items()
        for variant in variants
    ]

    all_baseline_tables = list_baseline_tables(
        project_id=target_project,
        only_tables=[only] if only else None,
        table_filter=table_filter,
    )

    non_deprecated_baseline_app_id_tables = [
        baseline_table
        for baseline_table in all_baseline_tables
        if baseline_table.split(".")[1] in non_deprecated_app_id_datasets
    ]

    output_dir = Path(output_dir) / target_project

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
            ),
            baseline_table,
        )
        for baseline_table in non_deprecated_baseline_app_id_tables
        for table in GLEAN_TABLES
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
        for name, info in app_info.items()
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
