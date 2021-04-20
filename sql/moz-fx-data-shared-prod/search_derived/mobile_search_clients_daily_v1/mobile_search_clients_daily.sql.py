#!/usr/bin/env python3
"""
Generate mobile search clients_daily query by creating a combined CTE for
metrics and baseline for all Firefox Android apps, then print query to stdout
"""
import os
from typing import List

# fmt: off
ANDROID_APP_TUPLES = [
    ("org_mozilla_fenix",           "Firefox Preview",  "beta",      "android"),
    ("org_mozilla_fenix_nightly",   "Firefox Preview",  "nightly",   "android"),
    ("org_mozilla_fennec_aurora",   "Fenix",            "nightly",   "android"),
    ("org_mozilla_firefox_beta",    "Fenix",            "beta",      "android"),
    ("org_mozilla_firefox",         "Fenix",            "release",   "android"),
    ("org_mozilla_ios_firefox",     "Firefox iOS",      "release",   "ios"),
    ("org_mozilla_ios_firefoxbeta", "Firefox iOS",      "beta",      "ios"),
    ("org_mozilla_ios_fennec",      "Firefox iOS",      "nightly",   "ios"),
]
# fmt: on


def union_statements(statements: List[str]):
    """Join a list of strings together by UNION ALL"""
    return "\nUNION ALL\n".join(statements)


def main():
    base_dir = os.path.dirname(__file__)

    with open(os.path.join(base_dir, "fenix_metrics.template.sql")) as f:
        android_query_template = f.read()

    with open(os.path.join(base_dir, "ios_metrics.template.sql")) as f:
        ios_query_template = f.read()

    queries = [
        android_query_template.format(
            namespace=app_channel[0], app_name=app_channel[1], channel=app_channel[2]
        )
        if app_channel[3] == "android"
        else ios_query_template.format(
            namespace=app_channel[0], app_name=app_channel[1], channel=app_channel[2]
        )
        for app_channel in ANDROID_APP_TUPLES
    ]

    with open(os.path.join(base_dir, "mobile_search_clients_daily.template.sql")) as f:
        search_query_template = f.read()

    fenix_combined_baseline = union_statements(
        [
            f"SELECT * FROM baseline_{namespace}"
            for namespace, _, _, platform in ANDROID_APP_TUPLES
            if platform == "android"
        ]
    )
    fenix_combined_metrics = union_statements(
        [
            f"SELECT * FROM metrics_{namespace}"
            for namespace, _, _, platform in ANDROID_APP_TUPLES
            if platform == "android"
        ]
    )
    ios_combined_metrics = union_statements(
        [
            f"SELECT * FROM metrics_{namespace}"
            for namespace, _, _, platform in ANDROID_APP_TUPLES
            if platform == "ios"
        ]
    )

    search_query = search_query_template.format(
        baseline_and_metrics_by_namespace="\n".join(queries),
        fenix_baseline=fenix_combined_baseline,
        fenix_metrics=fenix_combined_metrics,
        ios_metrics=ios_combined_metrics,
    )

    print(search_query)


if __name__ == "__main__":
    main()
