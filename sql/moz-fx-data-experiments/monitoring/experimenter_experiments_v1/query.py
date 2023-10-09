#!/usr/bin/env python3

"""Import experiments from Experimenter via the Experimenter API."""

import datetime
import json
import sys
import time
from argparse import ArgumentParser
from typing import List, Optional

import attr
import cattrs
import pytz
import requests
from google.cloud import bigquery

EXPERIMENTER_API_URL_V1 = (
    "https://experimenter.services.mozilla.com/api/v1/experiments/"
)

# for nimbus experiments
EXPERIMENTER_API_URL_V6 = (
    "https://experimenter.services.mozilla.com/api/v6/experiments/"
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


@attr.s(auto_attribs=True)
class Experiment:
    """Defines an Experiment."""

    experimenter_slug: Optional[str]
    normandy_slug: Optional[str]
    type: str
    status: Optional[str]
    branches: List[Branch]
    start_date: Optional[datetime.datetime]
    end_date: Optional[datetime.datetime]
    enrollment_end_date: Optional[datetime.datetime]
    proposed_enrollment: Optional[int]
    reference_branch: Optional[str]
    is_high_population: bool
    app_name: str
    app_id: str
    channel: str
    targeting: str
    targeted_percent: float
    namespace: Optional[str]


def _coerce_none_to_zero(x: Optional[int]) -> int:
    return 0 if x is None else x


@attr.s(auto_attribs=True)
class Variant:
    """Defines a Variant."""

    is_control: bool
    slug: str
    ratio: int
    value: str


@attr.s(auto_attribs=True)
class ExperimentV1:
    """Experimenter v1 experiment."""

    slug: str  # experimenter slug
    type: str
    status: str
    start_date: Optional[datetime.datetime]
    end_date: Optional[datetime.datetime]
    variants: List[Variant]
    proposed_enrollment: Optional[int] = attr.ib(converter=_coerce_none_to_zero)
    firefox_channel: str
    population_percent: float
    normandy_slug: Optional[str] = None
    is_high_population: Optional[bool] = None

    @staticmethod
    def _unix_millis_to_datetime(num: Optional[float]) -> Optional[datetime.datetime]:
        """Convert unix milliseconds to datetime."""
        if num is None:
            return None
        return datetime.datetime.fromtimestamp(num / 1e3, pytz.utc)

    @classmethod
    def from_dict(cls, d) -> "ExperimentV1":
        """Load an experiment from dict."""
        converter = cattrs.BaseConverter()
        converter.register_structure_hook(
            datetime.datetime,
            lambda num, _: cls._unix_millis_to_datetime(num),
        )
        return converter.structure(d, cls)

    def to_experiment(self) -> "Experiment":
        """Convert to Experiment."""
        branches = [
            Branch(slug=variant.slug, ratio=variant.ratio) for variant in self.variants
        ]
        control_slug = None

        control_slugs = [
            variant.slug for variant in self.variants if variant.is_control
        ]
        if len(control_slugs) == 1:
            control_slug = control_slugs[0]

        return Experiment(
            normandy_slug=self.normandy_slug,
            experimenter_slug=self.slug,
            type=self.type,
            status=self.status,
            start_date=self.start_date,
            end_date=self.end_date,
            enrollment_end_date=None,
            branches=branches,
            proposed_enrollment=self.proposed_enrollment,
            reference_branch=control_slug,
            is_high_population=self.is_high_population or False,
            app_name="firefox_desktop",
            app_id="firefox-desktop",
            channel=self.firefox_channel.lower(),
            targeting="",
            targeted_percent=float(self.population_percent) / 100.0,
            namespace=None,
        )


@attr.s(auto_attribs=True)
class ExperimentV6:
    """Represents a v6 experiment from Experimenter."""

    slug: str  # Normandy slug
    startDate: Optional[datetime.datetime]
    endDate: Optional[datetime.datetime]
    enrollmentEndDate: Optional[datetime.datetime]
    proposedEnrollment: int
    branches: List[Branch]
    referenceBranch: Optional[str]
    appName: str
    appId: str
    channel: str
    targeting: str
    bucketConfig: dict

    @classmethod
    def from_dict(cls, d) -> "ExperimentV6":
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
            lambda b, _: Branch(slug=b["slug"], ratio=b["ratio"]),
        )
        return converter.structure(d, cls)

    def to_experiment(self) -> "Experiment":
        """Convert to Experiment."""
        return Experiment(
            normandy_slug=self.slug,
            experimenter_slug=None,
            type="v6",
            status="Live"
            if (
                self.endDate is None
                or (
                    self.endDate
                    and self.endDate > pytz.utc.localize(datetime.datetime.now())
                )
            )
            else "Complete",
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
            targeting=self.targeting,
            targeted_percent=self.bucketConfig["count"] / self.bucketConfig["total"],
            namespace=self.bucketConfig["namespace"],
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


def get_experiments() -> List[Experiment]:
    """Fetch experiments from Experimenter."""
    legacy_experiments_json = fetch(EXPERIMENTER_API_URL_V1)
    legacy_experiments = []

    for experiment in legacy_experiments_json:
        if experiment["type"] != "rapid":
            try:
                legacy_experiments.append(
                    ExperimentV1.from_dict(experiment).to_experiment()
                )
            except Exception as e:
                print(f"Cannot import experiment: {experiment}: {e}")

    nimbus_experiments_json = fetch(EXPERIMENTER_API_URL_V6)
    nimbus_experiments = []

    for experiment in nimbus_experiments_json:
        try:
            nimbus_experiments.append(
                ExperimentV6.from_dict(experiment).to_experiment()
            )
        except Exception as e:
            print(f"Cannot import experiment: {experiment}: {e}")

    return nimbus_experiments + legacy_experiments


def main():
    """Run."""
    args = parser.parse_args()
    experiments = get_experiments()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    bq_schema = (
        bigquery.SchemaField("experimenter_slug", "STRING"),
        bigquery.SchemaField("normandy_slug", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("enrollment_end_date", "DATE"),
        bigquery.SchemaField("proposed_enrollment", "INTEGER"),
        bigquery.SchemaField("reference_branch", "STRING"),
        bigquery.SchemaField("is_high_population", "BOOL"),
        bigquery.SchemaField(
            "branches",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("slug", "STRING"),
                bigquery.SchemaField("ratio", "INTEGER"),
            ],
        ),
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("app_name", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("targeting", "STRING"),
        bigquery.SchemaField("targeted_percent", "FLOAT"),
        bigquery.SchemaField("namespace", "STRING"),
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    )
    job_config.schema = bq_schema

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
