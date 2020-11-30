"""Testing data for query."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent

UUID = "df735f02-efe5-4b07-b212-583bb99ba241"
SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

# NOTE: what happens when channel = "*"?
HISTOGRAM_BUCKET_COUNTS = [
    {
        "agg_type": "summed_histogram",
        "app_build_id": "*",
        "app_version": 84,
        "channel": "*",
        "key": "",
        "metric": "network_tcp_connection",
        "metric_type": "timing_distribution",
        "os": "*",
        "ping_type": "*",
        "range_max": 3,
        "record": {"key": "1", "value": 1.0},
    },
    {
        "agg_type": "summed_histogram",
        "app_build_id": "*",
        "app_version": 84,
        "channel": "*",
        "key": "",
        "metric": "network_tcp_connection",
        "metric_type": "timing_distribution",
        "os": "*",
        "ping_type": "*",
        "range_max": 3,
        "record": {"key": "2", "value": 0.0},
    },
]

EXPECT = [
    {
        "agg_type": "histogram",
        "aggregates": [
            {"key": "0", "value": 0.125},
            {"key": "1", "value": 0.625},
            {"key": "2", "value": 0.125},
            {"key": "3", "value": 0.125},
        ],
        "app_build_id": "*",
        "app_version": 84,
        "channel": "*",
        "client_agg_type": "summed_histogram",
        "key": "",
        "metric": "network_tcp_connection",
        "metric_type": "timing_distribution",
        "os": "*",
        "ping_type": "*",
        "total_users": 1,
    }
]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__histogram_bucket_counts_v1.yaml",
        HISTOGRAM_BUCKET_COUNTS,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
