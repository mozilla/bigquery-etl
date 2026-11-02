#!/usr/bin/env python3

"""Resolve jetstream analysis and opmon monitoring configs for Experimenter experiments."""

import copy
import datetime
import hashlib
import importlib.metadata
import json
import logging
import re
import sys
from argparse import ArgumentParser
from pathlib import Path

import attr
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from jinja2 import UndefinedError
from metric_config_parser.analysis import AnalysisSpec
from metric_config_parser.config import Config, ConfigCollection
from metric_config_parser.errors import (
    ConfigException,
    DefinitionNotFound,
    InvalidConfigurationException,
    UnexpectedKeyConfigurationException,
)
from metric_config_parser.metric import AnalysisPeriod
from metric_config_parser.metric import Metric as ParserMetric
from metric_config_parser.monitoring import MonitoringSpec

from bigquery_etl.experiments import NimbusExperiment, get_nimbus_experiments
from bigquery_etl.metrics import MetricHubConfigLoader
from bigquery_etl.schema import SCHEMA_FILE, Schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-experiments")
parser.add_argument("--destination_dataset", default="monitoring")
parser.add_argument("--destination_table", default="experiment_metric_configs_v1")
parser.add_argument("--dry_run", action="store_true")
parser.add_argument(
    "--force-refresh",
    "--force_refresh",
    action="store_true",
    help="Re-resolve every experiment instead of reusing previous rows.",
)

# A bad metric-hub config, not a bug; matches jetstream/cli.py's catch list.
RECOVERABLE_RESOLUTION_ERRORS = (
    ValueError,
    ConfigException,
    InvalidConfigurationException,
    DefinitionNotFound,
    UnexpectedKeyConfigurationException,
    UndefinedError,
    RuntimeError,
)

STATS_DATASET = "mozanalysis"
DEFAULT_MONITORING_PLATFORM = "firefox_desktop"
# Experimenter data can still change for a while after an experiment ends, so
# resolve it on every run for the first 30 days, weekly until 90 days, then
# only when first_updated changes.
RECENTLY_ENDED_WINDOW = datetime.timedelta(days=30)
WEEKLY_REFRESH_WINDOW = datetime.timedelta(days=90)
MAX_ROW_AGE = datetime.timedelta(days=7)
# A bad config fails the same way every time, but failing to load metric-hub or an
# unexpected error may be transient or a bug, so rows with these prefixes are retried.
AS_OF_ERROR_PREFIX = "Cannot load metric-hub as of the resolution timestamp: "
UNEXPECTED_ERROR_PREFIX = "Unexpected error resolving config: "
RETRIED_ERROR_PREFIXES = (AS_OF_ERROR_PREFIX, UNEXPECTED_ERROR_PREFIX)
# Bump when a change to the resolution logic alters rows without changing the schema,
# so that reused rows are re-resolved.
RESOLVER_VERSION = 1


def _bq_normalize_name(name: str) -> str:
    """Match jetstream.bq_normalize_name, so results table names line up."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def get_first_updated_by_normalized_slug(
    client: bigquery.Client, project: str
) -> dict[str, datetime.datetime]:
    """Return each experiment's `enrollments_<slug>` table's `last_updated` label.

    Mirrors jetstream's BigQueryClient.experiment_table_first_updated, run in
    bulk since this job resolves every experiment daily.
    """
    rows = list(client.query(f"""
        SELECT
          table_name,
          REGEXP_EXTRACT_ALL(
            option_value, r'STRUCT\\("last_updated", "([^"]+)"\\)'
          ) AS last_updated
        FROM `{project}.{STATS_DATASET}.INFORMATION_SCHEMA.TABLE_OPTIONS`
        WHERE option_name = 'labels' AND STARTS_WITH(table_name, 'enrollments_')
        """).result())

    first_updated = {}
    for row in rows:
        if not row.last_updated:
            continue
        normalized_slug = row.table_name[len("enrollments_") :]
        first_updated[normalized_slug] = min(
            datetime.datetime.fromtimestamp(int(ts), tz=datetime.UTC)
            for ts in row.last_updated
        )

    logger.info(
        f"Found last_updated labels for {len(first_updated)} of {len(rows)} "
        "enrollments tables"
    )
    if not first_updated:
        logger.warning(
            "No last_updated labels found on enrollments tables, so every experiment "
            "will be resolved against metric-hub HEAD"
        )
    return first_updated


def _schema_structure(fields) -> list:
    return [
        [field.name, field.field_type, field.mode, _schema_structure(field.fields)]
        for field in fields
    ]


def compute_fingerprint(schema_fields: list[bigquery.SchemaField]) -> str:
    """Hash what rows are resolved with, to re-resolve them when it changes.

    Covers the schema structure (not descriptions), the metric-config-parser version
    and RESOLVER_VERSION.
    """
    payload = json.dumps(
        [
            _schema_structure(schema_fields),
            importlib.metadata.version("mozilla-metric-config-parser"),
            RESOLVER_VERSION,
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


@attr.s(auto_attribs=True)
class PreviousRow:
    """A row from the previous run; `blob` is the row as loaded into BigQuery."""

    computed_at: datetime.datetime
    config_as_of: datetime.datetime | None
    resolver_fingerprint: str | None
    needs_retry: bool
    blob: dict
    monitoring_config_as_of: datetime.datetime | None = None


def _to_jsonable(value):
    """Convert dates in a BigQuery record to what load_table_from_json accepts."""
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def read_previous_rows(client: bigquery.Client, table: str) -> dict[str, PreviousRow]:
    """Return the previous run's rows by slug, or none if they can't be reused."""
    try:
        rows = list(
            client.query(
                "SELECT normandy_slug, computed_at, config_as_of, resolver_fingerprint,"
                " metric_config, monitoring_config, monitoring_config_as_of"
                f" FROM `{table}`"
            ).result()
        )
    except (NotFound, BadRequest) as e:
        logger.warning(
            f"Cannot reuse previous rows from `{table}`, as it is missing or lacks the "
            f"config_as_of, resolver_fingerprint or monitoring_config columns: {e}"
        )
        return {}

    return {
        row.normandy_slug: PreviousRow(
            computed_at=row.computed_at,
            config_as_of=row.config_as_of,
            resolver_fingerprint=row.resolver_fingerprint,
            needs_retry=any(
                (config["resolution_error"] or "").startswith(RETRIED_ERROR_PREFIXES)
                for config in (row.metric_config, row.monitoring_config)
                if config is not None
            ),
            blob=_to_jsonable(
                {
                    "normandy_slug": row.normandy_slug,
                    "computed_at": row.computed_at,
                    "config_as_of": row.config_as_of,
                    "resolver_fingerprint": row.resolver_fingerprint,
                    "metric_config": row.metric_config,
                    "monitoring_config": row.monitoring_config,
                    "monitoring_config_as_of": row.monitoring_config_as_of,
                }
            ),
            monitoring_config_as_of=row.monitoring_config_as_of,
        )
        for row in rows
        if row.metric_config is not None or row.monitoring_config is not None
    }


def can_reuse_row(
    previous: PreviousRow | None,
    nimbus_experiment: NimbusExperiment,
    first_updated: datetime.datetime | None,
    now: datetime.datetime,
    fingerprint: str,
    force_refresh: bool = False,
    has_jetstream: bool = True,
    monitoring_as_of: datetime.datetime | None = None,
    has_monitoring: bool = False,
) -> bool:
    """Whether resolving the experiment now would give the same row as before.

    `first_updated` and `monitoring_as_of` are the timestamps each config would be
    resolved as of, and None where it is not resolved or uses HEAD.
    """
    if force_refresh or previous is None or (has_jetstream and first_updated is None):
        return False
    if has_monitoring and monitoring_as_of is None:
        return False
    if previous.config_as_of != first_updated or previous.needs_retry:
        return False
    if previous.monitoring_config_as_of != monitoring_as_of:
        return False
    if previous.resolver_fingerprint != fingerprint:
        return False
    end_date = nimbus_experiment.endDate
    if end_date is None or now - end_date < RECENTLY_ENDED_WINDOW:
        return False
    return (
        now - end_date >= WEEKLY_REFRESH_WINDOW
        or now - previous.computed_at < MAX_ROW_AGE
    )


@attr.s(auto_attribs=True)
class Statistic:
    """A statistical treatment applied to a metric, and the periods it runs for."""

    name: str
    analysis_periods: list[str]


@attr.s(auto_attribs=True)
class Metric:
    """A metric resolved for an experiment, deduplicated across analysis periods."""

    name: str
    friendly_name: str | None
    description: str | None
    bigger_is_better: bool
    type: str
    category: str | None
    level: str | None
    owner: list[str]
    deprecated: bool
    analysis_bases: list[str]
    statistics: list[Statistic]


@attr.s(auto_attribs=True)
class UnresolvedMetric:
    """A metric reference that failed to resolve."""

    name: str
    analysis_period: str
    error: str


@attr.s(auto_attribs=True)
class Segment:
    """A segment applied to an experiment's analysis."""

    name: str
    friendly_name: str | None
    description: str | None


@attr.s(auto_attribs=True)
class ExposureSignal:
    """An exposure signal applied to an experiment's analysis."""

    name: str
    friendly_name: str | None
    description: str | None
    window_start: str | None
    window_end: str | None


@attr.s(auto_attribs=True)
class Overrides:
    """Values the external config overrides relative to Experimenter."""

    reference_branch: str | None
    start_date: str | None
    end_date: str | None
    enrollment_period: int | None


@attr.s(auto_attribs=True)
class MetricConfig:
    """Resolved jetstream analysis configuration for one experiment."""

    has_external_config: bool = False
    external_config_url: str | None = None
    external_config_last_modified: str | None = None
    has_external_config_overrides: bool | None = None
    skip: bool | None = None
    is_private: bool | None = None
    analysis_unit: str | None = None
    enrollments_query_type: str | None = None
    sample_size: int | None = None
    overrides: Overrides | None = None
    segments: list[Segment] = attr.Factory(list)
    exposure_signal: ExposureSignal | None = None
    metrics: list[Metric] = attr.Factory(list)
    unresolved_metrics: list[UnresolvedMetric] = attr.Factory(list)
    unresolved_outcomes: list[str] = attr.Factory(list)
    resolution_error: str | None = None


@attr.s(auto_attribs=True)
class MonitoringMetric:
    """A metric monitored by opmon, deduplicated across statistics."""

    name: str
    friendly_name: str | None
    description: str | None
    type: str
    category: str | None
    owner: list[str]
    deprecated: bool
    data_source: str | None
    statistics: list[str]


@attr.s(auto_attribs=True)
class MonitoringDimension:
    """A dimension opmon breaks the monitored population down by."""

    name: str
    friendly_name: str | None
    description: str | None


@attr.s(auto_attribs=True)
class MonitoringAlert:
    """An alert opmon raises on monitored metrics."""

    name: str
    type: str
    friendly_name: str | None
    description: str | None
    metrics: list[str]


@attr.s(auto_attribs=True)
class MonitoringConfig:
    """Resolved opmon monitoring configuration for one experiment or rollout."""

    has_external_config: bool = False
    external_config_url: str | None = None
    external_config_last_modified: str | None = None
    platform: str | None = None
    xaxis: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    reference_branch: str | None = None
    skip: bool | None = None
    skip_default_metrics: bool | None = None
    population_data_source: str | None = None
    monitor_entire_population: bool | None = None
    branches: list[str] = attr.Factory(list)
    group_by_dimension: str | None = None
    metrics: list[MonitoringMetric] = attr.Factory(list)
    dimensions: list[MonitoringDimension] = attr.Factory(list)
    alerts: list[MonitoringAlert] = attr.Factory(list)
    metric_groups: list[str] = attr.Factory(list)
    resolution_error: str | None = None


@attr.s(auto_attribs=True)
class Row:
    """One row written to experiment_metric_configs_v1."""

    normandy_slug: str
    computed_at: str
    config_as_of: str | None
    resolver_fingerprint: str
    metric_config: MetricConfig | None = None
    monitoring_config: MonitoringConfig | None = None
    monitoring_config_as_of: str | None = None


def _find_external_config(slug: str, configs: ConfigCollection) -> Config | None:
    """Return the experiment's own metric-hub/jetstream config, if one exists."""
    for config in configs.configs:
        if config.slug == slug and isinstance(config.spec, AnalysisSpec):
            return config
    return None


def _override(resolved_value, raw_value):
    """Return resolved_value if it overrides raw_value, else None."""
    return resolved_value if resolved_value != raw_value else None


def resolve_metric_config(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection
) -> MetricConfig:
    """Resolve one experiment's analysis config, capturing per-metric failures."""
    slug = nimbus_experiment.slug
    parser_experiment = nimbus_experiment.to_metric_config_experiment()

    external_config = _find_external_config(slug, configs)

    spec = AnalysisSpec.default_for_experiment(parser_experiment, configs)
    if external_config is not None:
        spec.merge(copy.deepcopy(external_config.spec))

    # External configs can declare outcomes beyond Experimenter's own list.
    outcome_slugs = list(parser_experiment.outcomes)
    for outcome_slug in spec.experiment.outcomes:
        if outcome_slug not in outcome_slugs:
            outcome_slugs.append(outcome_slug)

    unresolved_outcomes = []
    for outcome_slug in outcome_slugs:
        outcome = configs.spec_for_outcome(outcome_slug, parser_experiment.app_name)
        if outcome is not None:
            spec.merge_outcome(outcome)
            spec.merge_parameters(outcome.parameters)
        else:
            unresolved_outcomes.append(outcome_slug)

    resolved_experiment = spec.experiment.resolve(spec, parser_experiment, configs)

    # Accumulated per (metric name, statistic name) while walking analysis
    # periods, then converted to Metric/Statistic instances below.
    periods_by_key: dict[tuple[str, str], list[str]] = {}
    metric_by_name: dict[str, ParserMetric] = {}
    unresolved_metrics = []
    for period in AnalysisPeriod:
        for ref in getattr(spec.metrics, period.table_suffix):
            try:
                summaries = ref.resolve(spec, resolved_experiment, configs)
            except RECOVERABLE_RESOLUTION_ERRORS as e:
                unresolved_metrics.append(
                    UnresolvedMetric(
                        name=ref.name, analysis_period=period.value, error=str(e)
                    )
                )
                continue

            for summary in summaries:
                metric_by_name[summary.metric.name] = summary.metric
                key = (summary.metric.name, summary.statistic.name)
                periods_by_key.setdefault(key, [])
                if period.value not in periods_by_key[key]:
                    periods_by_key[key].append(period.value)

    statistics_by_metric: dict[str, list[Statistic]] = {}
    for (metric_name, statistic_name), analysis_periods in periods_by_key.items():
        statistics_by_metric.setdefault(metric_name, []).append(
            Statistic(name=statistic_name, analysis_periods=analysis_periods)
        )

    metrics = [
        Metric(
            name=metric.name,
            friendly_name=metric.friendly_name,
            description=metric.description,
            bigger_is_better=metric.bigger_is_better,
            type=metric.type,
            category=metric.category,
            level=metric.level.value if metric.level else None,
            owner=metric.owner or [],
            deprecated=metric.deprecated,
            analysis_bases=[basis.value for basis in metric.analysis_bases],
            statistics=statistics_by_metric[metric.name],
        )
        for metric in metric_by_name.values()
    ]

    overrides = None
    if resolved_experiment.has_external_config_overrides():
        raw_experiment = resolved_experiment.experiment
        overridden_start_date = _override(
            resolved_experiment.start_date, raw_experiment.start_date
        )
        overridden_end_date = _override(
            resolved_experiment.end_date, raw_experiment.end_date
        )
        overrides = Overrides(
            reference_branch=_override(
                resolved_experiment.reference_branch, raw_experiment.reference_branch
            ),
            start_date=(
                overridden_start_date.date().isoformat()
                if overridden_start_date
                else None
            ),
            end_date=(
                overridden_end_date.date().isoformat() if overridden_end_date else None
            ),
            enrollment_period=_override(
                resolved_experiment.enrollment_period,
                raw_experiment.proposed_enrollment,
            ),
        )

    exposure_signal = None
    if resolved_experiment.exposure_signal is not None:
        signal = resolved_experiment.exposure_signal
        exposure_signal = ExposureSignal(
            name=signal.name,
            friendly_name=signal.friendly_name,
            description=signal.description,
            window_start=(
                str(signal.window_start) if signal.window_start is not None else None
            ),
            window_end=(
                str(signal.window_end) if signal.window_end is not None else None
            ),
        )

    return MetricConfig(
        has_external_config=external_config is not None,
        external_config_url=(
            f"{ConfigCollection.repo_url}/blob/main/jetstream/{slug}.toml"
            if external_config is not None
            else None
        ),
        external_config_last_modified=(
            external_config.last_modified.isoformat()
            if external_config is not None
            else None
        ),
        has_external_config_overrides=resolved_experiment.has_external_config_overrides(),
        skip=resolved_experiment.skip,
        is_private=resolved_experiment.is_private,
        analysis_unit=(
            resolved_experiment.analysis_unit.value
            if resolved_experiment.analysis_unit
            else None
        ),
        enrollments_query_type=resolved_experiment.enrollments_query_type,
        sample_size=resolved_experiment.sample_size,
        overrides=overrides,
        segments=[
            Segment(
                name=segment.name,
                friendly_name=segment.friendly_name,
                description=segment.description,
            )
            for segment in resolved_experiment.segments
        ],
        exposure_signal=exposure_signal,
        metrics=metrics,
        unresolved_metrics=unresolved_metrics,
        unresolved_outcomes=unresolved_outcomes,
        resolution_error=None,
    )


def _error_config(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection, message: str
) -> MetricConfig:
    return MetricConfig(
        has_external_config=(
            _find_external_config(nimbus_experiment.slug, configs) is not None
        ),
        resolution_error=message,
    )


def _resolve_or_error(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection
) -> MetricConfig:
    """Resolve one experiment's config. A bad config never fails the run."""
    try:
        return resolve_metric_config(nimbus_experiment, configs)
    except RECOVERABLE_RESOLUTION_ERRORS as e:
        logger.warning(
            f"Cannot resolve metric config for {nimbus_experiment.slug}: {e}"
        )
        return _error_config(nimbus_experiment, configs, str(e))
    except Exception as e:
        # don't fail the run on an unexpected error either, but mark the row to be retried
        logger.warning(
            f"Unexpected error resolving metric config for {nimbus_experiment.slug}: {e}"
        )
        return _error_config(
            nimbus_experiment, configs, f"{UNEXPECTED_ERROR_PREFIX}{e}"
        )


def _find_monitoring_config(slug: str, configs: ConfigCollection) -> Config | None:
    """Return the slug's own metric-hub/opmon config, if one exists."""
    for config in configs.configs:
        if config.slug == slug and isinstance(config.spec, MonitoringSpec):
            return config
    return None


def is_monitored_rollout(nimbus_experiment: NimbusExperiment) -> bool:
    """Whether opmon treats the experiment as a rollout, which includes single-branch ones."""
    return nimbus_experiment.isRollout or len(nimbus_experiment.branches) == 1


def monitoring_as_of(
    nimbus_experiment: NimbusExperiment,
    now: datetime.datetime,
    external_config: Config | None = None,
) -> datetime.datetime | None:
    """Return the last day opmon monitors an ended experiment, or None while it is live.

    Like opmon, the slug's opmon config end_date overrides Experimenter's endDate.
    """
    end_date = nimbus_experiment.endDate
    if external_config is not None and external_config.spec.project.end_date:
        end_date = datetime.datetime.strptime(
            external_config.spec.project.end_date, "%Y-%m-%d"
        ).replace(tzinfo=datetime.UTC)
    if end_date is None or end_date + datetime.timedelta(days=1) > now:
        return None
    return end_date + datetime.timedelta(days=1)


def _date(value: datetime.datetime | None) -> str | None:
    return value.date().isoformat() if value else None


def resolve_monitoring_config(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection
) -> MonitoringConfig | None:
    """Resolve one experiment's opmon config the way `opmon run` does.

    None if opmon would not monitor it: not a rollout, and no opmon config in `configs`.
    """
    slug = nimbus_experiment.slug
    experiment = nimbus_experiment.to_metric_config_experiment()
    external_config = _find_monitoring_config(slug, configs)
    if external_config is None and not is_monitored_rollout(nimbus_experiment):
        return None

    platform = (
        (external_config.spec.project.platform if external_config else None)
        or experiment.app_name
        or DEFAULT_MONITORING_PLATFORM
    )
    definitions = configs.get_platform_definitions(platform)
    if definitions is None:
        raise ValueError(f"Invalid platform {platform}")

    spec = MonitoringSpec.from_definition_spec(copy.deepcopy(definitions))
    if external_config is None or not external_config.spec.project.skip_default_metrics:
        spec.merge(configs.get_platform_defaults(platform))
        if (
            is_monitored_rollout(nimbus_experiment)
            and platform == DEFAULT_MONITORING_PLATFORM
        ):
            spec.merge(configs.get_platform_defaults("rollout"))
    if external_config is not None:
        spec.merge(external_config.spec)

    resolved = spec.resolve(experiment, configs)
    project = resolved.project

    statistics_by_metric: dict[str, list[str]] = {}
    metric_by_name: dict[str, ParserMetric] = {}
    for summary in resolved.metrics:
        metric_by_name[summary.metric.name] = summary.metric
        names = statistics_by_metric.setdefault(summary.metric.name, [])
        if summary.statistic.name not in names:
            names.append(summary.statistic.name)

    return MonitoringConfig(
        has_external_config=external_config is not None,
        external_config_url=(
            f"{ConfigCollection.repo_url}/blob/main/opmon/{slug}.toml"
            if external_config is not None
            else None
        ),
        external_config_last_modified=(
            external_config.last_modified.isoformat()
            if external_config is not None
            else None
        ),
        platform=platform,
        xaxis=project.xaxis.value,
        start_date=_date(project.start_date),
        end_date=_date(project.end_date),
        reference_branch=project.reference_branch,
        skip=project.skip,
        skip_default_metrics=project.skip_default_metrics,
        population_data_source=(
            project.population.data_source.name
            if project.population.data_source
            else None
        ),
        monitor_entire_population=project.population.monitor_entire_population,
        branches=project.population.branches,
        group_by_dimension=(
            project.population.group_by_dimension.name
            if project.population.group_by_dimension
            else None
        ),
        metrics=[
            MonitoringMetric(
                name=metric.name,
                friendly_name=metric.friendly_name,
                description=metric.description,
                type=metric.type,
                category=metric.category,
                owner=metric.owner or [],
                deprecated=metric.deprecated,
                data_source=metric.data_source.name if metric.data_source else None,
                statistics=statistics_by_metric[metric.name],
            )
            for metric in metric_by_name.values()
        ],
        dimensions=[
            MonitoringDimension(
                name=dimension.name,
                friendly_name=dimension.friendly_name,
                description=dimension.description,
            )
            for dimension in resolved.dimensions
        ],
        alerts=[
            MonitoringAlert(
                name=alert.name,
                type=alert.type.value,
                friendly_name=alert.friendly_name,
                description=alert.description,
                metrics=sorted({summary.metric.name for summary in alert.metrics}),
            )
            for alert in resolved.alerts
        ],
        metric_groups=[group.name for group in project.metric_groups],
    )


def _error_monitoring_config(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection, message: str
) -> MonitoringConfig:
    return MonitoringConfig(
        has_external_config=(
            _find_monitoring_config(nimbus_experiment.slug, configs) is not None
        ),
        resolution_error=message,
    )


def _resolve_monitoring_or_error(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection
) -> MonitoringConfig | None:
    """Resolve one experiment's monitoring config. A bad config never fails the run."""
    try:
        return resolve_monitoring_config(nimbus_experiment, configs)
    except RECOVERABLE_RESOLUTION_ERRORS as e:
        logger.warning(
            f"Cannot resolve monitoring config for {nimbus_experiment.slug}: {e}"
        )
        return _error_monitoring_config(nimbus_experiment, configs, str(e))
    except Exception as e:
        logger.warning(
            f"Unexpected error resolving monitoring config for {nimbus_experiment.slug}: {e}"
        )
        return _error_monitoring_config(
            nimbus_experiment, configs, f"{UNEXPECTED_ERROR_PREFIX}{e}"
        )


def _commit_for(commits: list, timestamp: datetime.datetime):
    """Pick the commit ConfigCollection.as_of does: newest not after timestamp."""
    return next((c for c in commits if c.committed_datetime <= timestamp), commits[-1])


def _resolve_by_commit(
    items: list[tuple[NimbusExperiment, datetime.datetime | None]],
    configs: ConfigCollection,
    resolve,
    error_config,
    store,
) -> int:
    """Resolve each (experiment, timestamp) against `configs` as of its timestamp.

    A None timestamp resolves against HEAD. Returns the number of metric-hub commits used.
    """

    def as_of(timestamp):
        try:
            return configs.as_of(timestamp), None
        except Exception as e:
            logger.warning(f"Cannot load metric-hub as of {timestamp}: {e}")
            return None, e

    # as_of is slow, so call it once per metric-hub commit, not per experiment
    commits: list = []
    commit_index: dict[str, int] = {}
    members_by_commit: dict[str, list] = {}
    for nimbus_experiment, timestamp in items:
        if timestamp is None:
            store(nimbus_experiment, resolve(nimbus_experiment, configs))
        else:
            if not commits:
                commits = list(configs.repos[0].repo.iter_commits("HEAD"))
                commit_index = {commit.hexsha: i for i, commit in enumerate(commits)}
            sha = _commit_for(commits, timestamp).hexsha
            members_by_commit.setdefault(sha, []).append((nimbus_experiment, timestamp))

    for sha, members in members_by_commit.items():
        experiment_configs, error = as_of(members[0][1])
        # members start from the same commit, so they share a failure. as_of also
        # moves to a newer commit if configs fail to parse, the same way for every
        # member; landing on an older one means the prediction was wrong
        shared = error is not None or all(
            commit_index.get(repo.commit_hash, len(commits)) <= commit_index[sha]
            for repo in experiment_configs.repos
        )
        for i, (nimbus_experiment, timestamp) in enumerate(members):
            if i > 0 and not shared:
                experiment_configs, error = as_of(timestamp)
            store(
                nimbus_experiment,
                (
                    error_config(
                        nimbus_experiment, configs, f"{AS_OF_ERROR_PREFIX}{error}"
                    )
                    if error is not None
                    else resolve(nimbus_experiment, experiment_configs)
                ),
            )
    return len(members_by_commit)


def get_metric_configs(
    nimbus_experiments: list[NimbusExperiment],
    configs: ConfigCollection,
    first_updated_by_normalized_slug: dict[str, datetime.datetime],
    previous_rows: dict[str, PreviousRow],
    now: datetime.datetime,
    fingerprint: str,
    force_refresh: bool = False,
    monitoring_configs: ConfigCollection | None = None,
) -> list[dict]:
    """Return a row per experiment, reusing previous rows that are still valid.

    Rollouts get no jetstream config, as jetstream does not analyze them. Rollouts as
    opmon defines them, and slugs with an opmon config, get a monitoring config.
    """
    rows: dict[str, dict] = {}
    # (config_as_of, monitoring_config_as_of) for each row to resolve
    to_resolve: dict[str, tuple] = {}
    jetstream_items = []
    monitoring_items = []
    for nimbus_experiment in nimbus_experiments:
        slug = nimbus_experiment.slug
        has_jetstream = not nimbus_experiment.isRollout
        first_updated = (
            first_updated_by_normalized_slug.get(_bq_normalize_name(slug))
            if has_jetstream
            else None
        )
        external_monitoring_config = (
            _find_monitoring_config(slug, monitoring_configs)
            if monitoring_configs is not None
            else None
        )
        has_monitoring = monitoring_configs is not None and (
            is_monitored_rollout(nimbus_experiment)
            or external_monitoring_config is not None
        )
        monitoring_timestamp = (
            monitoring_as_of(nimbus_experiment, now, external_monitoring_config)
            if has_monitoring
            else None
        )
        previous = previous_rows.get(slug)
        if can_reuse_row(
            previous,
            nimbus_experiment,
            first_updated,
            now,
            fingerprint,
            force_refresh,
            has_jetstream=has_jetstream,
            monitoring_as_of=monitoring_timestamp,
            has_monitoring=has_monitoring,
        ):
            rows[slug] = previous.blob
            continue
        to_resolve[slug] = (first_updated, monitoring_timestamp)
        if has_jetstream:
            jetstream_items.append((nimbus_experiment, first_updated))
        if has_monitoring:
            monitoring_items.append((nimbus_experiment, monitoring_timestamp))

    metric_configs: dict[str, MetricConfig] = {}
    commit_count = _resolve_by_commit(
        jetstream_items,
        configs,
        _resolve_or_error,
        _error_config,
        lambda nimbus_experiment, config: metric_configs.update(
            {nimbus_experiment.slug: config}
        ),
    )
    resolved_monitoring_configs: dict[str, MonitoringConfig] = {}
    monitoring_commit_count = (
        _resolve_by_commit(
            monitoring_items,
            monitoring_configs,
            _resolve_monitoring_or_error,
            _error_monitoring_config,
            lambda nimbus_experiment, config: resolved_monitoring_configs.update(
                {nimbus_experiment.slug: config}
            ),
        )
        if monitoring_items
        else 0
    )

    def isoformat(timestamp):
        return timestamp.isoformat() if timestamp else None

    for slug, (first_updated, monitoring_timestamp) in to_resolve.items():
        rows[slug] = attr.asdict(
            Row(
                normandy_slug=slug,
                computed_at=now.isoformat(),
                config_as_of=isoformat(first_updated),
                resolver_fingerprint=fingerprint,
                metric_config=metric_configs.get(slug),
                monitoring_config=resolved_monitoring_configs.get(slug),
                monitoring_config_as_of=isoformat(monitoring_timestamp),
            )
        )

    logger.info(
        f"Resolved {len(to_resolve)} rows ({len(jetstream_items)} metric configs "
        f"across {commit_count} metric-hub commits, {len(monitoring_items)} monitoring "
        f"configs across {monitoring_commit_count} metric-hub commits), "
        f"reused {len(nimbus_experiments) - len(to_resolve)}"
    )
    return [rows[nimbus_experiment.slug] for nimbus_experiment in nimbus_experiments]


def main():
    """Run."""
    args = parser.parse_args()
    nimbus_experiments = get_nimbus_experiments()
    configs = MetricHubConfigLoader.experiment_configs()
    client = bigquery.Client(args.project)
    first_updated_by_normalized_slug = get_first_updated_by_normalized_slug(
        client, args.project
    )

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )
    previous_rows = (
        {} if args.force_refresh else read_previous_rows(client, destination_table)
    )
    schema = Schema.from_schema_file(Path(__file__).parent / SCHEMA_FILE)
    blob = get_metric_configs(
        nimbus_experiments,
        configs,
        first_updated_by_normalized_slug,
        previous_rows,
        datetime.datetime.now(datetime.UTC),
        compute_fingerprint(schema.to_bigquery_schema()),
        args.force_refresh,
        MetricHubConfigLoader.monitoring_configs(),
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    )
    job_config.schema = schema.to_bigquery_schema()

    if args.dry_run:
        print(json.dumps(blob))
        sys.exit(0)

    client.load_table_from_json(blob, destination_table, job_config=job_config).result()
    logger.info(f"Loaded {len(blob)} experiment metric configs")


if __name__ == "__main__":
    main()
