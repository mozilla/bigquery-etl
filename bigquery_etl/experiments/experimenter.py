"""Client for fetching experiments from the Experimenter API."""

import datetime
import logging
import time

import attr
import cattrs
import pytz
import requests
from metric_config_parser import experiment as parser_experiment

logger = logging.getLogger(__name__)

# for nimbus experiments
EXPERIMENTER_API_URL_V8 = (
    "https://experimenter.services.mozilla.com/api/v8/experiments/"
)

USER_AGENT = "https://github.com/mozilla/bigquery-etl"


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
    """Defines an Experiment. Fields match the experimenter_experiments_v1 columns."""

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
    is_enrollment_paused: bool | None
    is_firefox_labs_opt_in: bool


def _status(end_date: datetime.datetime | None) -> str:
    """Return the experiment status implied by its end date."""
    is_live = end_date is None or end_date > datetime.datetime.now(datetime.UTC)
    return "Live" if is_live else "Complete"


def _channel(channel: str) -> parser_experiment.Channel | None:
    """Coerce a raw channel string to a metric-config-parser Channel, if valid."""
    return (
        parser_experiment.Channel(channel)
        if parser_experiment.Channel.has_value(channel)
        else None
    )


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
        """Convert to the row written to experimenter_experiments_v1."""
        return Experiment(
            normandy_slug=self.slug,
            experimenter_slug=None,
            type="v6",
            status=_status(self.endDate),
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

    def to_metric_config_experiment(self) -> parser_experiment.Experiment:
        """Convert to the metric-config-parser Experiment used to resolve analysis configs."""
        return parser_experiment.Experiment(
            experimenter_slug=None,
            normandy_slug=self.slug,
            type="v6",
            status=_status(self.endDate),
            branches=[
                parser_experiment.Branch(slug=b.slug, ratio=b.ratio)
                for b in self.branches
            ],
            start_date=self.startDate,
            end_date=self.endDate,
            enrollment_end_date=self.enrollmentEndDate,
            proposed_enrollment=self.proposedEnrollment,
            reference_branch=self.referenceBranch,
            is_high_population=False,
            app_name=self.appName,
            app_id=self.appId,
            bucket_config=parser_experiment.BucketConfig(
                randomization_unit=self.bucketConfig["randomizationUnit"],
                namespace=self.bucketConfig["namespace"],
                start=self.bucketConfig["start"],
                count=self.bucketConfig["count"],
                total=self.bucketConfig["total"],
            ),
            is_enrollment_paused=self.isEnrollmentPaused,
            outcomes=[o.slug for o in self.outcomes] if self.outcomes else [],
            segments=[s.slug for s in self.segments] if self.segments else [],
            is_rollout=self.isRollout,
            channel=_channel(self.channel),
        )


def fetch(url):
    """Fetch a url."""
    last_exception = None
    for _ in range(2):
        try:
            return requests.get(
                url,
                timeout=30,
                headers={"user-agent": USER_AGENT},
            ).json()
        except Exception as e:
            last_exception = e
            time.sleep(1)
    raise last_exception


def get_nimbus_experiments() -> list[NimbusExperiment]:
    """Fetch and parse NimbusExperiment records from Experimenter, skipping bad ones."""
    nimbus_experiments_json = fetch(EXPERIMENTER_API_URL_V8)
    nimbus_experiments = []

    for experiment in nimbus_experiments_json:
        try:
            nimbus_experiments.append(NimbusExperiment.from_dict(experiment))
        except Exception as e:
            logger.warning(f"Cannot parse experiment: {experiment}: {e}")

    return nimbus_experiments


def get_experiments() -> list[Experiment]:
    """Fetch experiments from Experimenter as experimenter_experiments_v1 rows."""
    experiments = []

    for nimbus_experiment in get_nimbus_experiments():
        try:
            experiments.append(nimbus_experiment.to_experiment())
        except Exception as e:
            logger.warning(f"Cannot convert experiment {nimbus_experiment.slug}: {e}")

    return experiments
