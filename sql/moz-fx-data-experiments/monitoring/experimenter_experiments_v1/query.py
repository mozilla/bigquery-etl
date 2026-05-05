#!/usr/bin/env python3

"""Import experiments from Experimenter via the Experimenter API."""

import datetime
import json
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

import attr
import cattrs
import pytz
import requests
from google.cloud import bigquery

from bigquery_etl.schema import SCHEMA_FILE, Schema

# for nimbus experiments
EXPERIMENTER_API_URL_V8 = (
    "https://experimenter.services.mozilla.com/api/v8/experiments/"
)

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-experiments")
parser.add_argument("--destination_dataset", default="monitoring")
parser.add_argument("--destination_table", default="experimenter_experiments_v1")
parser.add_argument("--sql_dir", default="sql/")
parser.add_argument("--dry_run", action="store_true")


@attr.s(auto_attribs=True)
class Branch:
    """Defines a branch."""

    slug: str
    ratio: int
    features: dict | None


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class Outcome:
    """Defines an Outcome."""

    slug: str


@attr.s(auto_attribs=True, kw_only=True, slots=True, frozen=True)
class Segment:
    """Defines a Segment."""

    slug: str


@attr.s(auto_attribs=True)
class Experiment:
    """Defines an Experiment."""

    experimenter_slug: str | None
    normandy_slug: str | None
    type: str
    status: str | None
    branches: list[Branch]
    start_date: datetime.datetime | None
    end_date: datetime.datetime | None
    enrollment_end_date: datetime.datetime | None
    proposed_enrollment: int | None
    reference_branch: str | None
    is_high_population: bool
    app_name: str
    app_id: str
    channel: str
    channels: list[str]
    targeting: str
    targeted_percent: float
    namespace: str | None
    feature_ids: list[str]
    is_rollout: bool
    outcomes: list[str]
    segments: list[str]
    randomization_unit: str
    is_enrollment_paused: bool
    is_firefox_labs_opt_in: bool


@attr.s(auto_attribs=True)
class NimbusExperiment:
    """Represents a v8 Nimbus experiment from Experimenter."""

    slug: str  # Normandy slug
    startDate: datetime.datetime | None
    endDate: datetime.datetime | None
    enrollmentEndDate: datetime.datetime | None
    proposedEnrollment: int
    branches: list[Branch]
    referenceBranch: str | None
    appName: str
    appId: str
    channel: str
    channels: list[str]
    targeting: str
    bucketConfig: dict
    featureIds: list[str]
    isRollout: bool
    outcomes: list[Outcome] | None = None
    segments: list[Segment] | None = None
    isEnrollmentPaused: bool | None = None
    isFirefoxLabsOptIn: bool = False

    @classmethod
    def from_dict(cls, d) -> "NimbusExperiment":
        """Load an experiment from dict."""
        converter = cattrs.BaseConverter()
        converter.register_structure_hook(
            datetime.datetime,
            lambda num, _: datetime.datetime.fromisoformat(
                num.replace("Z", "+00:00")
            ).astimezone(pytz.utc),
        )
        converter.register_structure_hook(
            Branch,
            lambda b, _: Branch(
                slug=b["slug"], ratio=b["ratio"], features=b["features"]
            ),
        )
        return converter.structure(d, cls)

    def to_experiment(self) -> "Experiment":
        """Convert to Experiment."""
        return Experiment(
            normandy_slug=self.slug,
            experimenter_slug=None,
            type="v6",
            status=(
                "Live"
                if (
                    self.endDate is None
                    or (
                        self.endDate
                        and self.endDate > pytz.utc.localize(datetime.datetime.now())
                    )
                )
                else "Complete"
            ),
            start_date=self.startDate,
            end_date=self.endDate,
            enrollment_end_date=self.enrollmentEndDate,
            proposed_enrollment=self.proposedEnrollment,
            reference_branch=self.referenceBranch,
            is_high_population=False,
            branches=self.branches,
            app_name=self.appName,
            app_id=self.appId,
            channel=self.channel,
            channels=self.channels,
            targeting=self.targeting,
            targeted_percent=self.bucketConfig["count"] / self.bucketConfig["total"],
            namespace=self.bucketConfig["namespace"],
            feature_ids=self.featureIds,
            is_rollout=self.isRollout,
            outcomes=[o.slug for o in self.outcomes] if self.outcomes else [],
            segments=[s.slug for s in self.segments] if self.segments else [],
            randomization_unit=self.bucketConfig["randomizationUnit"],
            is_firefox_labs_opt_in=self.isFirefoxLabsOptIn,
            is_enrollment_paused=self.isEnrollmentPaused,
        )


def fetch(url):
    """Fetch a url."""
    for _ in range(2):
        try:
            return requests.get(
                url,
                timeout=30,
                headers={"user-agent": "https://github.com/mozilla/bigquery-etl"},
            ).json()
        except Exception as e:
            last_exception = e
            time.sleep(1)
    raise last_exception


def get_experiments() -> list[Experiment]:
    """Fetch experiments from Experimenter."""
    nimbus_experiments_json = fetch(EXPERIMENTER_API_URL_V8)
    nimbus_experiments = []

    for experiment in nimbus_experiments_json:
        try:
            nimbus_experiments.append(
                NimbusExperiment.from_dict(experiment).to_experiment()
            )
        except Exception as e:
            print(f"Cannot import experiment: {experiment}: {e}")

    return nimbus_experiments


def main():
    """Run."""
    args = parser.parse_args()
    experiments = get_experiments()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    schema = Schema.from_schema_file(Path(__file__).parent / SCHEMA_FILE)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    )
    job_config.schema = schema.to_bigquery_schema()

    converter = cattrs.BaseConverter()
    converter.register_unstructure_hook(
        datetime.datetime, lambda d: datetime.datetime.strftime(d, format="%Y-%m-%d")
    )

    blob = converter.unstructure(experiments)
    if args.dry_run:
        print(json.dumps(blob))
        sys.exit(0)

    client = bigquery.Client(args.project)
    client.load_table_from_json(blob, destination_table, job_config=job_config).result()
    print(f"Loaded {len(experiments)} experiments")


if __name__ == "__main__":
    main()
