"""
Generate mobile search clients_daily query.

Create a combined CTE for metrics and baseline for Android and iOS Glean apps,
then print the query to a file in the output directory.
"""

from pathlib import Path
from typing import List

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import write_sql, render

# fmt: off
FIREFOX_ANDROID_TUPLES = [
    ("org_mozilla_fenix",           "Firefox Preview",  "beta"),  # noqa E241 E501
    ("org_mozilla_fenix_nightly",   "Firefox Preview",  "nightly"),  # noqa E241 E501
    ("org_mozilla_fennec_aurora",   "Fenix",            "nightly"),  # noqa E241 E501
    ("org_mozilla_firefox_beta",    "Fenix",            "beta"),  # noqa E241 E501
    ("org_mozilla_firefox",         "Fenix",            "release"),  # noqa E241 E501
]

DATASET = "search_derived"

TABLE_NAME = "mobile_search_clients_daily_v2"

FIREFOX_IOS_TUPLES = [
    ("org_mozilla_ios_firefox",     "Fennec", "release"),  # noqa E241 E501
    ("org_mozilla_ios_firefoxbeta", "Fennec", "beta"),  # noqa E241 E501
    ("org_mozilla_ios_fennec",      "Fennec", "nightly"),  # noqa E241 E501
]

FOCUS_ANDROID_TUPLES = [
    ("org_mozilla_focus",           "Focus Android Glean",    "release"),  # noqa E241 E501
    ("org_mozilla_focus_beta",      "Focus Android Glean",    "beta"),  # noqa E241 E501
    ("org_mozilla_focus_nightly",   "Focus Android Glean",    "nightly"),  # noqa E241 E501
]

KLAR_ANDROID_TUPLES = [
    ("org_mozilla_klar",            "Klar Android Glean",     "release"),  # noqa E241 E501
]

FOCUS_iOS_TUPLES = [
    ("org_mozilla_ios_focus",           "Focus iOS Glean",    "release"), # noqa E241 E501
]

KLAR_iOS_TUPLES = [
    ("org_mozilla_ios_klar",            "Klar iOS Glean",     "release"),  # noqa E241 E501
]
# fmt: on


def union_statements(statements: List[str]):
    """Join a list of strings together by UNION ALL."""
    return "\nUNION ALL\n".join(statements)


@click.command()
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
@click.option(
    "--target-project",
    "--target_project",
    help="GCP project ID",
    default="moz-fx-data-shared-prod",
)
@use_cloud_function_option
def generate(output_dir, target_project, use_cloud_function):
    """Generate mobile search clients daily query and print to stdout."""

    base_dir = Path(__file__).parent

    output_dir = Path(output_dir) / target_project

    env = Environment(loader=FileSystemLoader(base_dir / "templates"))

    android_query_template = env.get_template("fenix.template.sql")
    ios_query_template = env.get_template("ios.template.sql")
    android_focus_template = env.get_template("android_focus.template.sql")
    android_klar_template = env.get_template("android_klar.template.sql")
    ios_focus_template = env.get_template("ios_focus.template.sql")
    ios_klar_template = env.get_template("ios_klar.template.sql")
    metadata_template = "mobile_search_clients_engines_sources_daily.metadata.yaml"

    firefox_android_queries = [
        android_query_template.render(
            namespace=namespace, app_name=app_name, channel=channel
        )
        for namespace, app_name, channel in FIREFOX_ANDROID_TUPLES
    ]

    firefox_ios_queries = [
        ios_query_template.render(
            namespace=namespace, app_name=app_name, channel=channel
        )
        for namespace, app_name, channel in FIREFOX_IOS_TUPLES
    ]

    focus_android_queries = [
        android_focus_template.render(
            namespace=namespace, app_name=app_name, channel=channel
        )
        for namespace, app_name, channel in FOCUS_ANDROID_TUPLES
    ]

    klar_android_queries = [
        android_klar_template.render(
            namespace=namespace, app_name=app_name, channel=channel
        )
        for namespace, app_name, channel in KLAR_ANDROID_TUPLES
    ]

    focus_ios_queries = [
        ios_focus_template.render(
            namespace=namespace, app_name=app_name, channel=channel
        )
        for namespace, app_name, channel in FOCUS_iOS_TUPLES
    ]

    klar_ios_queries = [
        ios_klar_template.render(
            namespace=namespace, app_name=app_name, channel=channel
        )
        for namespace, app_name, channel in KLAR_iOS_TUPLES
    ]

    queries = (
        firefox_android_queries
        + firefox_ios_queries
        + focus_android_queries
        + klar_android_queries
        + focus_ios_queries
        + klar_ios_queries
    )

    search_query_template = env.get_template("mobile_search_clients_daily.template.sql")


    fenix_combined_baseline = union_statements(
        [
            f"SELECT * FROM baseline_{namespace}"
            for namespace, _, _ in FIREFOX_ANDROID_TUPLES
        ]
    )
    
    ios_combined_baseline = union_statements(
        [f"SELECT * FROM baseline_{namespace}" for namespace, _, _ in FIREFOX_IOS_TUPLES]
    )
    android_focus_combined_baseline = union_statements(
        [
            f"SELECT * FROM baseline_{namespace}"
            for namespace, _, _ in FOCUS_ANDROID_TUPLES
        ]
    )
    android_klar_combined_baseline = union_statements(
        [
            f"SELECT * FROM baseline_{namespace}"
            for namespace, _, _ in KLAR_ANDROID_TUPLES
        ]
    )

    ios_focus_combined_baseline = union_statements(
        [f"SELECT * FROM baseline_{namespace}" for namespace, _, _ in FOCUS_iOS_TUPLES]
    )
    ios_klar_combined_baseline = union_statements(
        [f"SELECT * FROM baseline_{namespace}" for namespace, _, _ in KLAR_iOS_TUPLES]
    )

    search_query = search_query_template.render(
        baseline_by_namespace="\n".join(queries),
        fenix_baseline=fenix_combined_baseline,
        ios_metrics=ios_combined_baseline,
        android_focus_metrics=android_focus_combined_baseline,
        android_klar_metrics=android_klar_combined_baseline,
        ios_focus_metrics=ios_focus_combined_baseline,
        ios_klar_metrics=ios_klar_combined_baseline,
    )

    write_sql(
        output_dir=output_dir,
        full_table_id=f"{target_project}.{DATASET}.{TABLE_NAME}",
        basename="query.sql",
        sql=reformat(search_query),
        skip_existing=False,
    )

    write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{DATASET}.{TABLE_NAME}",
            basename="metadata.yaml",
            sql=render(
                metadata_template,
                template_folder=base_dir / "templates",
                format=False,
            ),
            skip_existing=False,
    )

