"""Testing data for query."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent

UUID = "df735f02-efe5-4b07-b212-583bb99ba241"
SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

# NOTE: what happens when channel = "*"?
CLIENTS_HISTOGRAM_AGGREGATES = [
    {
        "sample_id": 1,
        "client_id": UUID,
        "ping_type": "*",
        "os": "*",
        "app_version": 84,
        "app_build_id": "*",
        "channel": "*",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [
                    {"key": "112863206", "value": 1},
                    {"key": "123078199", "value": 0},
                ],
            }
        ],
    }
]

EXPECT = [
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
        "range_max": 123_078_199,
        "record": {"key": "112863206", "value": 1.0},
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
        "range_max": 123_078_199,
        "record": {"key": "123078199", "value": 0.0},
    },
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
