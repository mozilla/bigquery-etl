"""Testing data for query."""

from pathlib import Path
from uuid import uuid4

import yaml

ROOT = Path(__file__).parent

SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

# needs > 5 clients to be considered a version
CLIENTS_DAILY_SCALAR_AGGREGATES = [
    {
        "submission_date": SUBMISSION_DATE,
        "client_id": str(uuid4()),
        "ping_type": "metrics",
        "os": "Android",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "nightly",
        "scalar_aggregates": [],
    }
    for _ in range(6)
]

EXPECT = [dict(channel="nightly", latest_version=84)]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__"
        "view_clients_daily_scalar_aggregates_v1.yaml",
        CLIENTS_DAILY_SCALAR_AGGREGATES,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
