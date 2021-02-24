"""Testing data for query."""
from itertools import product
from pathlib import Path
from uuid import uuid4

import yaml

ROOT = Path(__file__).parent

SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"
OS = "Android"
PING_TYPE = "metrics"

# Testing precondition: ping_type, os, and app_build_id must not be "*". See
# models.py under the scalar_bucket_counts parameters to see that sets fields
# are used in the static combinations. If these are set to "*", then they will
# be double counted...
CLIENTS_SCALAR_AGGREGATES = [
    {
        "client_id": str(uuid4()),
        "ping_type": PING_TYPE,
        "os": OS,
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
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
        "ping_type": PING_TYPE,
        "os": OS,
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
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

# we must generate the set of combinations. Each one of these have the same
# values though.

BASE_ROW = {
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
}

# Didn't intend to code golf. This enumerates all of the "static combinations"
# by taking the cross product of all values. Each of these can take on a value
# from each of the clients above. Since each attribute combination has a single
# client, we do not have to change the "count" in the base row.
EXPECT = [
    {**BASE_ROW, **dict(zip(["bucket", "ping_type", "os", "app_build_id"], values))}
    for values in product(
        ["4.00", "8.00"], *zip([PING_TYPE, OS, APP_BUILD_ID], ["*"] * 3)
    )
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
