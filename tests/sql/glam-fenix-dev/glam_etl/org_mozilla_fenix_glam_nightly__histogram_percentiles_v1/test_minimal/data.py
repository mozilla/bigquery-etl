"""Testing data for query."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent

SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

HISTOGRAM_PROBE_COUNTS = [
    {
        "agg_type": "histogram",
        "aggregates": [
            # 101 buckets with uniform proportion, 0-100 inclusive.
            {"key": str(key), "value": 1 / 101}
            for key in range(101)
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

EXPECT = [
    {
        "agg_type": "percentiles",
        "aggregates": [
            {"key": "5", "value": 5.0},
            {"key": "25", "value": 25.0},
            {"key": "50", "value": 50.0},
            {"key": "75", "value": 75.0},
            {"key": "95", "value": 95.0},
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
        f"{prefix}.org_mozilla_fenix_glam_nightly__histogram_probe_counts_v1.yaml",
        HISTOGRAM_PROBE_COUNTS,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
