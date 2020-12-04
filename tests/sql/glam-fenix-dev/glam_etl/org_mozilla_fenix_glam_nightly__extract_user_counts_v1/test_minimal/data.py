"""Testing data for query."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent

UUID = "df735f02-efe5-4b07-b212-583bb99ba241"
SUBMISSION_DATE = "2020-10-01"
APP_BUILD_ID = "2020100100"

VIEW_USER_COUNTS = [
    {
        "ping_type": "*",
        "os": "*",
        "app_version": 84,
        "app_build_id": APP_BUILD_ID,
        "channel": "*",
        "total_users": 44444,
    }
]

EXPECT = [
    {
        "app_build_id": "2020100100",
        "app_version": 84,
        "build_date": "2020-10-01 00:00:00",
        "channel": "*",
        "os": "*",
        "ping_type": "*",
        "total_users": 44444,
    }
]

prefix = "glam_etl"
tables = [
    (
        f"{prefix}.org_mozilla_fenix_glam_nightly__view_user_counts_v1.yaml",
        VIEW_USER_COUNTS,
    ),
    ("expect.yaml", EXPECT),
]
for name, data in tables:
    with (ROOT / name).open("w") as fp:
        yaml.dump(data, fp)
