#!/usr/bin/env python3

"""Resolve jetstream/metric-hub analysis configs for Experimenter experiments."""

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
# Experimenter data can still change for a while after an experiment ends, so
# resolve it on every run for the first 30 days, weekly until 90 days, then
# only when first_updated changes.
RECENTLY_ENDED_WINDOW = datetime.timedelta(days=30)
WEEKLY_REFRESH_WINDOW = datetime.timedelta(days=90)
MAX_ROW_AGE = datetime.timedelta(days=7)


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

    Covers the schema structure (not descriptions) and the metric-config-parser version.
    """
    payload = json.dumps(
        [
            _schema_structure(schema_fields),
            importlib.metadata.version("mozilla-metric-config-parser"),
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


@attr.s(auto_attribs=True)
class PreviousRow:
    """A row from the previous run; `blob` is the row as loaded into BigQuery."""

    computed_at: datetime.datetime
    config_as_of: datetime.datetime | None
    resolver_fingerprint: str | None
    has_resolution_error: bool
    blob: dict


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
                f" metric_config FROM `{table}`"
            ).result()
        )
    except (NotFound, BadRequest) as e:
        logger.warning(
            f"Cannot reuse previous rows from `{table}`, as it is missing or lacks the "
            f"config_as_of or resolver_fingerprint column: {e}"
        )
        return {}

    return {
        row.normandy_slug: PreviousRow(
            computed_at=row.computed_at,
            config_as_of=row.config_as_of,
            resolver_fingerprint=row.resolver_fingerprint,
            has_resolution_error=row.metric_config["resolution_error"] is not None,
            blob=_to_jsonable(
                {
                    "normandy_slug": row.normandy_slug,
                    "computed_at": row.computed_at,
                    "config_as_of": row.config_as_of,
                    "resolver_fingerprint": row.resolver_fingerprint,
                    "metric_config": row.metric_config,
                }
            ),
        )
        for row in rows
        if row.metric_config is not None
    }


def can_reuse_row(
    previous: PreviousRow | None,
    nimbus_experiment: NimbusExperiment,
    first_updated: datetime.datetime | None,
    now: datetime.datetime,
    fingerprint: str,
    force_refresh: bool = False,
) -> bool:
    """Whether resolving the experiment now would give the same row as before."""
    if force_refresh or previous is None or first_updated is None:
        return False
    if previous.config_as_of != first_updated or previous.has_resolution_error:
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
class Row:
    """One row written to experiment_metric_configs_v1."""

    normandy_slug: str
    computed_at: str
    config_as_of: str | None
    resolver_fingerprint: str
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


def _error_config(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection, error: Exception
) -> MetricConfig:
    return MetricConfig(
        has_external_config=(
            _find_external_config(nimbus_experiment.slug, configs) is not None
        ),
        resolution_error=str(error),
    )


def _resolve_or_error(
    nimbus_experiment: NimbusExperiment, configs: ConfigCollection
) -> MetricConfig:
    """Resolve one experiment's config. A bad config never fails the run."""
    try:
        return resolve_metric_config(nimbus_experiment, configs)
    except Exception as e:
        # don't fail if there is any error resolving the metric config,
        # attach the error to the row and proceed
        logger.warning(
            f"Cannot resolve metric config for {nimbus_experiment.slug}: {e}"
        )
        return _error_config(nimbus_experiment, configs, e)


def _commit_for(commits: list, timestamp: datetime.datetime):
    """Pick the commit ConfigCollection.as_of does: newest not after timestamp."""
    return next((c for c in commits if c.committed_datetime <= timestamp), commits[-1])


def get_metric_configs(
    nimbus_experiments: list[NimbusExperiment],
    configs: ConfigCollection,
    first_updated_by_normalized_slug: dict[str, datetime.datetime],
    previous_rows: dict[str, PreviousRow],
    now: datetime.datetime,
    fingerprint: str,
    force_refresh: bool = False,
) -> list[dict]:
    """Return a row per experiment, reusing previous rows that are still valid."""
    # jetstream does not analyze rollouts
    nimbus_experiments = [e for e in nimbus_experiments if not e.isRollout]
    blobs: dict[str, dict] = {}
    to_resolve = []
    for nimbus_experiment in nimbus_experiments:
        first_updated = first_updated_by_normalized_slug.get(
            _bq_normalize_name(nimbus_experiment.slug)
        )
        previous = previous_rows.get(nimbus_experiment.slug)
        if can_reuse_row(
            previous, nimbus_experiment, first_updated, now, fingerprint, force_refresh
        ):
            blobs[nimbus_experiment.slug] = previous.blob
        else:
            to_resolve.append((nimbus_experiment, first_updated))

    def add_row(nimbus_experiment, first_updated, metric_config):
        blobs[nimbus_experiment.slug] = attr.asdict(
            Row(
                normandy_slug=nimbus_experiment.slug,
                computed_at=now.isoformat(),
                config_as_of=first_updated.isoformat() if first_updated else None,
                resolver_fingerprint=fingerprint,
                metric_config=metric_config,
            )
        )

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
    for nimbus_experiment, first_updated in to_resolve:
        if first_updated is None:
            add_row(
                nimbus_experiment,
                None,
                _resolve_or_error(nimbus_experiment, configs),
            )
        else:
            if not commits:
                commits = list(configs.repos[0].repo.iter_commits("HEAD"))
                commit_index = {commit.hexsha: i for i, commit in enumerate(commits)}
            sha = _commit_for(commits, first_updated).hexsha
            members_by_commit.setdefault(sha, []).append(
                (nimbus_experiment, first_updated)
            )

    for sha, members in members_by_commit.items():
        experiment_configs, error = as_of(members[0][1])
        # members start from the same commit, so they share a failure. as_of also
        # moves to a newer commit if configs fail to parse, the same way for every
        # member; landing on an older one means the prediction was wrong
        shared = error is not None or all(
            commit_index.get(repo.commit_hash, len(commits)) <= commit_index[sha]
            for repo in experiment_configs.repos
        )
        for i, (nimbus_experiment, first_updated) in enumerate(members):
            if i > 0 and not shared:
                experiment_configs, error = as_of(first_updated)
            add_row(
                nimbus_experiment,
                first_updated,
                (
                    _error_config(nimbus_experiment, configs, error)
                    if error is not None
                    else _resolve_or_error(nimbus_experiment, experiment_configs)
                ),
            )

    logger.info(
        f"Resolved {len(to_resolve)} experiments across {len(members_by_commit)} "
        f"metric-hub commits, reused {len(nimbus_experiments) - len(to_resolve)}"
    )
    return [blobs[nimbus_experiment.slug] for nimbus_experiment in nimbus_experiments]


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
