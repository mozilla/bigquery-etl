"""Code for setting up the the tests."""
import math
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import yaml

ROOT = Path(__file__).parent

EPOCH = datetime(2014, 12, 28)
START_DATE = datetime(2020, 6, 1)

HISTORY_DAYS = 14
# testing when we hit Firefox 100, soonish
START_VERSION = 100 + HISTORY_DAYS // 2


def app_build(start_date):
    """Generate an app build id per Fenix specifications."""
    return f"{(start_date-EPOCH).days*24<<3:010d}"


def input_row(submission_offset, build_offset, version_offset=0):
    """Generate a single row in a glean metrics ping that is being referenced."""
    return {
        "submission_timestamp": (START_DATE - timedelta(submission_offset)).isoformat(),
        "client_info": {"app_build": app_build(START_DATE - timedelta(build_offset))},
        "metrics": {
            "string": {"geckoview_version": f"{START_VERSION-version_offset}.0.0"}
        },
    }


def main(test_name):
    """Generate assets for each test."""
    assert (ROOT / test_name).is_dir(), f"{ROOT / test_name} not name of test"
    # We use the deprecated nightly product since it is not being filtered
    with (ROOT / test_name / "org_mozilla_fenix_nightly.metrics.yaml").open("w") as fp:
        # Needs some data out of the period for the cold start of the window
        # function. We'll also include dates in the future. There is a new
        # version every day.
        rows = [input_row(i, i, i) for i in range(-10, HISTORY_DAYS + 2)]
        yaml.dump(sorted(rows, key=lambda x: x["client_info"]["app_build"]) * 6, fp)
    # bad rows, versions less than 100 put before and after the 100 mark. The
    # one for fenix will probably get filtered out because of the channel norm
    # udf.
    bad_rows = [
        input_row(HISTORY_DAYS // 2 + 2, 30, 1337),
        input_row(HISTORY_DAYS // 2 - 2, 30, 1337),
    ]

    with (ROOT / test_name / "org_mozilla_fenix.metrics.yaml").open("w") as fp:
        yaml.dump(bad_rows, fp)
    with (ROOT / test_name / "org_mozilla_fennec_aurora.metrics.yaml").open("w") as fp:
        yaml.dump(bad_rows, fp)

    for dataset in [
        "org_mozilla_fenix_nightly",
        "org_mozilla_fenix",
        "org_mozilla_fennec_aurora",
    ]:
        shutil.copyfile(
            ROOT / "metrics.schema.json",
            ROOT / test_name / f"{dataset}.metrics.schema.json",
        )

    init_rows = []
    for hour in range(HISTORY_DAYS * 24 + 1):
        # new version every day
        version_offset = math.ceil(hour / 24)
        init_rows += [
            {
                "build_hour": (START_DATE - timedelta(hours=hour)).isoformat(),
                "geckoview_major_version": START_VERSION - version_offset,
                "n_pings": 6,
            }
        ]
    with (ROOT / test_name / "expect.yaml").open("w") as fp:
        yaml.dump(init_rows, fp)


if __name__ == "__main__":
    main("test_aggregation")
