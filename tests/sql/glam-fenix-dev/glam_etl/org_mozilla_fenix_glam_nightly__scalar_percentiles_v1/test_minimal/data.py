"""Testing data for query."""
from pathlib import Path
from uuid import uuid4

import yaml

ROOT = Path(__file__).parent

SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

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
            }
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
            }
        ],
    },
]

# TODO: the total user count is very wrong
EXPECT = [
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
        "total_users": 2,
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
