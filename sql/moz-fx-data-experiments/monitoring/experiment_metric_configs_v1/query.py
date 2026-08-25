#!/usr/bin/env python3

"""Resolve jetstream/metric-hub analysis configs for Experimenter experiments."""

import copy
import datetime
import json
import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

import attr
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
class Row:
    """One row written to experiment_metric_configs_v1."""

    normandy_slug: str
    computed_at: str
    metric_config: MetricConfig


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


def get_metric_configs(
    nimbus_experiments: list[NimbusExperiment], configs: ConfigCollection
) -> list[Row]:
    """Resolve metric config for each experiment. A bad config never fails the run."""
    computed_at = datetime.datetime.now(datetime.UTC).isoformat()
    rows = []

    for nimbus_experiment in nimbus_experiments:
        try:
            metric_config = resolve_metric_config(nimbus_experiment, configs)
        except Exception as e:
            # don't fail if there is any error resolving the metric config,
            # attach the error to the row and proceed
            logger.warning(
                f"Cannot resolve metric config for {nimbus_experiment.slug}: {e}"
            )
            metric_config = MetricConfig(
                has_external_config=(
                    _find_external_config(nimbus_experiment.slug, configs) is not None
                ),
                resolution_error=str(e),
            )

        rows.append(
            Row(
                normandy_slug=nimbus_experiment.slug,
                computed_at=computed_at,
                metric_config=metric_config,
            )
        )

    return rows


def main():
    """Run."""
    args = parser.parse_args()
    nimbus_experiments = get_nimbus_experiments()
    configs = MetricHubConfigLoader.experiment_configs()
    rows = get_metric_configs(nimbus_experiments, configs)

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    schema = Schema.from_schema_file(Path(__file__).parent / SCHEMA_FILE)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    )
    job_config.schema = schema.to_bigquery_schema()

    blob = [attr.asdict(row) for row in rows]

    if args.dry_run:
        print(json.dumps(blob))
        sys.exit(0)

    client = bigquery.Client(args.project)
    client.load_table_from_json(blob, destination_table, job_config=job_config).result()
    logger.info(f"Loaded {len(blob)} experiment metric configs")


if __name__ == "__main__":
    main()
