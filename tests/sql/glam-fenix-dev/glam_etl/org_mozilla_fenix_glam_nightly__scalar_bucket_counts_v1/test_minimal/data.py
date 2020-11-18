import yaml
from pathlib import Path
from uuid import uuid4

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
        "ping_type": "*",
        "os": "*",
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

# TODO: why are the range_min and range_max set at these values?
EXPECT = [
    {
        "agg_type": "histogram",
        "app_build_id": "*",
        "app_version": 84,
        "bucket": "16.00",
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "count",
        "count": 1,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 5.0,
        "range_min": 0.0,
    },
    {
        "agg_type": "histogram",
        "app_build_id": "*",
        "app_version": 84,
        "bucket": "32.00",
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "count",
        "count": 1,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 5.0,
        "range_min": 0.0,
    },
    {
        "agg_type": "histogram",
        "app_build_id": "*",
        "app_version": 84,
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "avg",
        "count": 2,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 5.0,
        "range_min": 0.0,
    },
    {
        "agg_type": "histogram",
        "app_build_id": "2020100100",
        "app_version": 84,
        "bucket": "16.00",
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "count",
        "count": 1,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 5.0,
        "range_min": 0.0,
    },
    {
        "agg_type": "histogram",
        "app_build_id": "2020100100",
        "app_version": 84,
        "bucket": "32.00",
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "count",
        "count": 1,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 5.0,
        "range_min": 0.0,
    },
    {
        "agg_type": "histogram",
        "app_build_id": "2020100100",
        "app_version": 84,
        "bucket_count": 100,
        "channel": "*",
        "client_agg_type": "avg",
        "count": 2,
        "key": "",
        "metric": "places_manager_write_query_count",
        "metric_type": "counter",
        "os": "*",
        "ping_type": "*",
        "range_max": 5.0,
        "range_min": 0.0,
    },
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
