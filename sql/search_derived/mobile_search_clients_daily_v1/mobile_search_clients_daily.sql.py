#!/usr/bin/env python3
"""
Generate mobile search clients_daily query by creating a combined CTE for
metrics and baseline for all Firefox Android apps, then print query to stdout
"""
import os

APP_CHANNEL_TUPLES = [
    ("org_mozilla_fenix",           "Firefox Preview",  "beta"),
    ("org_mozilla_fenix_nightly",   "Firefox Preview",  "nightly"),
    ("org_mozilla_fennec_aurora",   "Fenix",            "nightly"),
    ("org_mozilla_firefox_beta",    "Fenix",            "beta"),
    ("org_mozilla_firefox",         "Fenix",            "release"),
]


def main():
    base_dir = os.path.dirname(__file__)

    with open(os.path.join(base_dir, "fenix_metrics.template.sql")) as f:
        metrics_query_template = f.read()

    metrics_queries = [
        metrics_query_template.format(
            namespace=app_channel[0], app_name=app_channel[1], channel=app_channel[2]
        ) for app_channel in APP_CHANNEL_TUPLES
    ]

    with open(os.path.join(base_dir, "mobile_search_clients_daily.template.sql")) as f:
        search_query_template = f.read()

    combined_baseline = "\nUNION ALL\n".join(
        [f" SELECT * FROM baseline_{namespace}" for namespace, _, _ in APP_CHANNEL_TUPLES]
    )
    combined_metrics = "\nUNION ALL\n".join(
        [f"SELECT * FROM metrics_{namespace}" for namespace, _, _ in APP_CHANNEL_TUPLES]
    )

    search_query = search_query_template.format(
        baseline_and_metrics_by_namespace="\n".join(metrics_queries),
        fenix_baseline=combined_baseline,
        fenix_metrics=combined_metrics,
    )

    print(search_query)


if __name__ == "__main__":
    main()
