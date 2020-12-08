"""Testing data for query."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent

UUID = "df735f02-efe5-4b07-b212-583bb99ba241"
SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

LATEST_VERSION = [dict(channel="nightly", latest_version=84)]

CLIENTS_HISTOGRAM_AGGREGATES = [
    {
        "sample_id": 1,
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "nightly",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [
                    {"key": "0", "value": 0},
                    {"key": "1", "value": 0},
                    {"key": "2", "value": 1},
                ],
            }
        ],
    }
]

CLIENTS_DAILY_HISTOGRAM_AGGREGATES = [
    {
        "submission_date": SUBMISSION_DATE,
        "sample_id": 1,
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "nightly",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [{"key": "0", "value": 1}, {"key": "1", "value": 0}],
            }
        ],
    }
]

EXPECT = [
    {
        "sample_id": 1,
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "nightly",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [
                    {"key": "0", "value": 1},
                    {"key": "1", "value": 0},
                    {"key": "2", "value": 1},
                ],
            }
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
        f"{prefix}.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1.yaml",
        CLIENTS_HISTOGRAM_AGGREGATES,
    ),
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__"
        "view_clients_daily_histogram_aggregates_v1.yaml",
        CLIENTS_DAILY_HISTOGRAM_AGGREGATES,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
