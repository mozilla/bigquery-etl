"""Code for setting up the the tests."""
from pathlib import Path
from datetime import datetime, timedelta
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


def main():
    with (ROOT / "org_mozilla_fenix_nightly.metrics.yaml").open("w") as fp:
        # this does not generate any complicated cases where there might be
        # discontinuities in the data.
        rows = []
        for i in range(3):
            for j in range(3):
                # higher values of j should lead to an offset in version
                rows += [input_row(i, j, int(j > 1))]
        yaml.dump(
            sorted(rows, key=lambda x: x["client_info"]["app_build"]) * 5,
            fp,
        )
    with (ROOT / "expect.yaml").open("w") as fp:
        row = []
        # this is inclusive
        for hour in range(HISTORY_DAYS * 24):
            row += [
                {
                    "build_hour": (START_DATE + timedelta(hours=hour)).isoformat(),
                    "geckoview_version": "80.0.0",
                    "n_builds": 0,
                }
            ]
        yaml.dump(row, fp)


if __name__ == "__main__":
    main()
