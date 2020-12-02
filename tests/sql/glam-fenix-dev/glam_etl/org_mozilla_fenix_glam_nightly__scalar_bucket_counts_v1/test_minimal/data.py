"""Testing data for query."""
from pathlib import Path
from uuid import uuid4

import yaml

ROOT = Path(__file__).parent

SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

# Other tests: non * fields
CLIENTS_SCALAR_AGGREGATES = [
    {
        "client_id": str(uuid4()),
        "ping_type": "*",
        "os": "*",
        "app_version": 84,
        "app_build_id": "*",
        "channel": "*",
        "scalar_aggregates": [
            {
                "metric": "places_manager_write_query_count",
                "metric_type": "counter",
                "key": "",
                "agg_type": "count",
                "value": 4.0,
            },
        ],
    },
    {
        "client_id": str(uuid4()),
        "ping_type": "*",
        "os": "*",
        "app_version": 84,
        "app_build_id": "*",
        "channel": "*",
        "scalar_aggregates": [
            {
                "metric": "places_manager_write_query_count",
                "metric_type": "counter",
                "key": "",
                "agg_type": "count",
                "value": 8.0,
            },
        ],
    },
]

EXPECT = [
    {
        "agg_type": "histogram",
        "app_build_id": "*",
        "app_version": 84,
        "bucket": "4.00",
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "count",
        "count": 1,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 3.0,
        "range_min": 2.0,
    },
    {
        "agg_type": "histogram",
        "app_build_id": "*",
        "app_version": 84,
        "bucket": "8.00",
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "count",
        "count": 1,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 3.0,
        "range_min": 2.0,
    }
]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1.yaml",
        CLIENTS_SCALAR_AGGREGATES,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
