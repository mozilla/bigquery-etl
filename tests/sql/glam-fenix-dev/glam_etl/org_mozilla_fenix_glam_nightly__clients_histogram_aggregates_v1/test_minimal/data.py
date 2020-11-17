import yaml
import uuid
from pathlib import Path

ROOT = Path(__file__).parent

UUID = str(uuid.uuid4())

LATEST_VERSION = [dict(channel="nightly", latest_version=83)]

CLIENTS_HISTOGRAM_AGGREGATES = [
    {
        "sample_id": 1,
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": "2020111117",
        "channel": "*",
        "histogram_aggregates": [],
    }
]

CLIENTS_DAILY_HISTOGRAM_AGGREGATES = [
    {
        "sample_id": 1,
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": "2020111117",
        "channel": "*",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [
                    {"key": "112863206", "value": "1"},
                    {"key": "123078199", "value": "0"},
                ],
            },
        ],
    }
]

EXPECT = [
    {
        "sample_id": 1,
        "client_id": UUID,
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": "2020111117",
        "channel": "*",
        "histogram_aggregates": [
            {
                "metric": "network_tcp_connection",
                "metric_type": "timing_distribution",
                "key": "",
                "agg_type": "summed_histogram",
                "value": [
                    {"key": "112863206", "value": "1"},
                    {"key": "123078199", "value": "0"},
                ],
            },
        ],
    }
]

prefix = "glam_etl"
with (ROOT / f"{prefix}.org_mozilla_fenix_glam_nightly__latest_versions_v1.yaml").open(
    "w"
) as fp:
    yaml.dump(LATEST_VERSION, fp)
with (
    ROOT
    / f"{prefix}.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1.yaml"
).open("w") as fp:
    yaml.dump(CLIENTS_HISTOGRAM_AGGREGATES, fp)
with (
    ROOT
    / f"{prefix}.org_mozilla_fenix__clients_daily_histogram_aggregates_metrics_v1.yaml"
).open("w") as fp:
    yaml.dump(CLIENTS_DAILY_HISTOGRAM_AGGREGATES, fp)
with (ROOT / "expect.yaml").open("w") as fp:
    yaml.dump(EXPECT, fp)
