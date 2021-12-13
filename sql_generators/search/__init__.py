"""
Generate mobile search clients_daily query.

Creates a combined CTE for metrics and baseline for Android and iOS Glean
apps, then print query to stdout

To update query file:
python -m bigquery_etl.search.mobile_search_clients_daily \
> sql/moz-fx-data-shared-prod/\
search_derived/mobile_search_clients_daily_v1/query.sql
"""
import click
from pathlib import Path
from typing import List

from jinja2 import Environment, FileSystemLoader

from bigquery_etl.format_sql.formatter import reformat

# fmt: off
APP_CHANNEL_TUPLES = [
    ("org_mozilla_fenix",           "Firefox Preview",  "beta",      "android"),  # noqa E241 E501
    ("org_mozilla_fenix_nightly",   "Firefox Preview",  "nightly",   "android"),  # noqa E241 E501
    ("org_mozilla_fennec_aurora",   "Fenix",            "nightly",   "android"),  # noqa E241 E501
    ("org_mozilla_firefox_beta",    "Fenix",            "beta",      "android"),  # noqa E241 E501
    ("org_mozilla_firefox",         "Fenix",            "release",   "android"),  # noqa E241 E501
    ("org_mozilla_ios_firefox",     "Fennec",           "release",   "ios"),  # noqa E241 E501
    ("org_mozilla_ios_firefoxbeta", "Fennec",           "beta",      "ios"),  # noqa E241 E501
    ("org_mozilla_ios_fennec",      "Fennec",           "nightly",   "ios"),  # noqa E241 E501
]
# fmt: on


def union_statements(statements: List[str]):
    """Join a list of strings together by UNION ALL."""
    return "\nUNION ALL\n".join(statements)


@click.command()
def generate():
    """Generate mobile search clients daily query and print to stdout."""
    base_dir = Path(__file__).parent

    env = Environment(loader=FileSystemLoader(base_dir / "templates"))

    android_query_template = env.get_template("fenix_metrics.template.sql")
    ios_query_template = env.get_template("ios_metrics.template.sql")

    queries = [
        android_query_template.render(
            namespace=app_channel[0], app_name=app_channel[1], channel=app_channel[2]
        )
        if app_channel[3] == "android"
        else ios_query_template.render(
            namespace=app_channel[0], app_name=app_channel[1], channel=app_channel[2]
        )
        for app_channel in APP_CHANNEL_TUPLES
    ]

    search_query_template = env.get_template("mobile_search_clients_daily.template.sql")

    fenix_combined_baseline = union_statements(
        [
            f"SELECT * FROM baseline_{namespace}"
            for namespace, _, _, platform in APP_CHANNEL_TUPLES
            if platform == "android"
        ]
    )
    fenix_combined_metrics = union_statements(
        [
            f"SELECT * FROM metrics_{namespace}"
            for namespace, _, _, platform in APP_CHANNEL_TUPLES
            if platform == "android"
        ]
    )
    ios_combined_metrics = union_statements(
        [
            f"SELECT * FROM metrics_{namespace}"
            for namespace, _, _, platform in APP_CHANNEL_TUPLES
            if platform == "ios"
        ]
    )

    search_query = search_query_template.render(
        baseline_and_metrics_by_namespace="\n".join(queries),
        fenix_baseline=fenix_combined_baseline,
        fenix_metrics=fenix_combined_metrics,
        ios_metrics=ios_combined_metrics,
    )

    print(reformat(search_query))
