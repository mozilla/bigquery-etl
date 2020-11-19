"""Testing data for query."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent

SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

SCALAR_PERCENTILES = [
    {
        "agg_type": "percentiles",
        "aggregates": [
            {"key": "5", "value": 4.0},
            {"key": "25", "value": 4.0},
            {"key": "50", "value": 4.0},
            {"key": "75", "value": 8.0},
            {"key": "95", "value": 8.0},
        ],
        "app_build_id": "*",
        "app_version": 84,
        "channel": "*",
        "client_agg_type": "count",
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "total_users": 16,
    }
]

EXPECT = [
    {
        "build_id": "*",
        "channel": "*",
        "client_agg_type": "count",
        "metric": "places_manager_write_query_count",
        "metric_key": "",
        "metric_type": "counter",
        "os": "*",
        "percentiles": '{"5":4,"25":4,"50":4,"75":8,"95":8}',
        "ping_type": "*",
        "total_users": 16,
        "version": 84,
    }
]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__view_probe_counts_v1.yaml",
        SCALAR_PERCENTILES,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
