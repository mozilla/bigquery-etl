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


# NOTE: each client contributes a total of 1 to the final aggregate
CLIENTS_HISTOGRAM_AGGREGATES = [
    {
        "sample_id": 1,
        "client_id": str(uuid4()),
        "ping_type": PING_TYPE,
        "os": OS,
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "*",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [
                    {"key": "1", "value": 1},
                    {"key": "2", "value": 0},
                ],
            }
        ],
    },
    {
        "sample_id": 1,
        "client_id": str(uuid4()),
        "ping_type": PING_TYPE,
        "os": OS,
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "*",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [
                    {"key": "1", "value": 1},
                    {"key": "3", "value": 1},
                ],
            }
        ],
    },
]

BASE_ROW = {
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
}

EXPECT = [
    {**BASE_ROW, **dict(zip(["record", "ping_type", "os", "app_build_id"], values))}
    for values in product(
        [
            {"key": "1", "value": 1.5},
            {"key": "2", "value": 0.0},
            {"key": "3", "value": 0.5},
        ],
        *zip([PING_TYPE, OS, APP_BUILD_ID], ["*"] * 3),
    )
]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1.yaml",
        CLIENTS_HISTOGRAM_AGGREGATES,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
