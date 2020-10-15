"""Code for setting up the the tests."""
import math
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import yaml

ROOT = Path(__file__).parent

EPOCH = datetime(2014, 12, 28)
START_DATE = datetime(2020, 6, 1)

HISTORY_DAYS = 30


def app_build(start_date):
    return f"{(start_date-EPOCH).days*24<<3:010d}"


def input_row(submission_offset, build_offset, version_offset=0):
    return {
        "submission_timestamp": (START_DATE - timedelta(submission_offset)).isoformat(),
        "client_info": {"app_build": app_build(START_DATE - timedelta(build_offset))},
        "metrics": {"string": {"geckoview_version": f"{80-version_offset}.0.0"}},
    }


def main(test_name):
    assert (ROOT / test_name).is_dir(), f"{ROOT / test_name} not name of test"
    with (ROOT / test_name / "org_mozilla_fenix_nightly.metrics.yaml").open("w") as fp:
        # this does not generate any complicated cases where there might be
        # discontinuities in the data.
        rows = []
        # needs some data out of the period for the cold start of the window function
        # We'll also include dates in the future
        for i in range(-10, HISTORY_DAYS + 2):
            # new version every day
            rows += [input_row(i, i, i)]
        yaml.dump(
            sorted(rows, key=lambda x: x["client_info"]["app_build"]) * 6,
            fp,
        )
    # bad rows
    with (ROOT / test_name / "org_mozilla_fenix.metrics.yaml").open("w") as fp:
        yaml.dump([input_row(15, 30, 10)], fp)
    with (ROOT / test_name / "org_mozilla_fennec_aurora.metrics.yaml").open("w") as fp:
        yaml.dump([input_row(15, 30, 10)], fp)
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
                "geckoview_version": f"{80-version_offset}.0.0",
                "n_builds": 6,
            }
        ]
    if test_name == "test_init":
        with (ROOT / test_name / "expect.yaml").open("w") as fp:
            yaml.dump(init_rows, fp)
    elif test_name == "test_aggregation":
        with (
            ROOT / test_name / "org_mozilla_fenix_derived.geckoview_version_v1.yaml"
        ).open("w") as fp:
            yaml.dump(init_rows, fp)

        # we move a few days in the future to end up at 2020-06-07
        new_rows = []
        for hour in range(-6 * 24, 0):
            version_offset = math.ceil(hour / 24)
            new_rows += [
                {
                    "build_hour": (START_DATE - timedelta(hours=hour)).isoformat(),
                    "geckoview_version": f"{80-version_offset}.0.0",
                    "n_builds": 6,
                }
            ]
        with (ROOT / test_name / "expect.yaml").open("w") as fp:
            yaml.dump(new_rows + init_rows, fp)


if __name__ == "__main__":
    main("test_init")
    main("test_aggregation")
