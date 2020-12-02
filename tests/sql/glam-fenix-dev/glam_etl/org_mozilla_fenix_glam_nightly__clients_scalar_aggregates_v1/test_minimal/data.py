"""Testing data for query."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent

UUID = "df735f02-efe5-4b07-b212-583bb99ba241"
SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

LATEST_VERSION = [dict(channel="nightly", latest_version=84)]

# NOTE: what happens when channel = "*"?
CLIENTS_SCALAR_AGGREGATES = [
    {
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "nightly",
        "scalar_aggregates": [],
    }
]

CLIENTS_DAILY_SCALAR_AGGREGATES = [
    {
        "submission_date": SUBMISSION_DATE,
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "nightly",
        "scalar_aggregates": [
            {
                "metric": "metrics_has_recent_pwas",
                "metric_type": "boolean",
                "key": "",
                "agg_type": "false",
                "value": 1.0,
            },
            {
                "metric": "places_manager_write_query_count",
                "metric_type": "counter",
                "key": "",
                "agg_type": "count",
                "value": 3.0,
            },
            {
                "metric": "places_manager_write_query_count",
                "metric_type": "counter",
                "key": "",
                "agg_type": "sum",
                "value": 48.0,
            },
        ],
    }
]

EXPECT = [
    {
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "nightly",
        "scalar_aggregates": [
            {
                "metric": "metrics_has_recent_pwas",
                "metric_type": "boolean",
                "key": "",
                "agg_type": "false",
                "value": 1.0,
            },
            {
                "metric": "places_manager_write_query_count",
                "metric_type": "counter",
                "key": "",
                "agg_type": "count",
                "value": 3.0,
            },
            {
                "metric": "places_manager_write_query_count",
                "metric_type": "counter",
                "key": "",
                "agg_type": "sum",
                "value": 48.0,
            },
            # extra field from averages
            {
                "metric": "places_manager_write_query_count",
                "metric_type": "counter",
                "key": "",
                "agg_type": "avg",
                "value": 16.0,
            },
        ],
    }
]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__latest_versions_v1.yaml",
        LATEST_VERSION,
    ),
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1.yaml",
        CLIENTS_SCALAR_AGGREGATES,
    ),
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__"
        "view_clients_daily_scalar_aggregates_v1.yaml",
        CLIENTS_DAILY_SCALAR_AGGREGATES,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
