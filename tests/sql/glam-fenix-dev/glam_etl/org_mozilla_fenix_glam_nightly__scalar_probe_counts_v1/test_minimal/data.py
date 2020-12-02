"""Testing data for query."""
from pathlib import Path

import yaml

ROOT = Path(__file__).parent

SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

SCALAR_BUCKET_COUNTS = [
    {
        "agg_type": "histogram",
        "app_build_id": "*",
        "app_version": 84,
        "bucket": "2.00",
        "bucket_count": 3,
        "channel": "*",
        "client_agg_type": "count",
        "count": 1,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 3.0,
        "range_min": 0.0,
    }
]

EXPECT = [
    {
        "agg_type": "histogram",
        "aggregates": [
            {"key": "1.00", "value": 0.166_666_666_666_666_66},
            {"key": "2.00", "value": 0.666_666_666_666_666_6},
            {"key": "4.00", "value": 0.166_666_666_666_666_66},
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
        "total_users": 1,
    }
]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__scalar_bucket_counts_v1.yaml",
        SCALAR_BUCKET_COUNTS,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
