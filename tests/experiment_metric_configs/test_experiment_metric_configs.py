"""Tests for the per-metric resolution logic in experiment_metric_configs_v1/query.py."""

import datetime
import importlib.util
from pathlib import Path

import pytest
import pytz
from metric_config_parser.config import LocalConfigCollection

from bigquery_etl.experiments.experimenter import Branch, NimbusExperiment, Outcome

QUERY_DIR = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "moz-fx-data-experiments"
    / "monitoring"
    / "experiment_metric_configs_v1"
)
QUERY_PY = QUERY_DIR / "query.py"
FIXTURE_DIR = Path(__file__).parent / "metric_hub_fixture"

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not QUERY_PY.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)


def load_query_module():
    # Load the module dynamically since the path contains hyphens.
    spec = importlib.util.spec_from_file_location(
        "experiment_metric_configs_query", QUERY_PY
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def query():
    return load_query_module()


@pytest.fixture(scope="module")
def configs():
    return LocalConfigCollection.from_local_path(str(FIXTURE_DIR))


def make_nimbus_experiment(slug, outcomes=None, end_date=None):
    return NimbusExperiment(
        slug=slug,
        startDate=pytz.utc.localize(datetime.datetime(2024, 1, 1)),
        endDate=end_date,
        enrollmentEndDate=None,
        proposedEnrollment=7,
        branches=[Branch(slug="control", ratio=1, features=None)],
        referenceBranch="control",
        appName="firefox_desktop",
        appId="firefox-desktop",
        channel="release",
        channels=["release"],
        targeting="true",
        bucketConfig={
            "namespace": "ns",
            "randomizationUnit": "normandy_id",
            "start": 0,
            "count": 100,
            "total": 10000,
        },
        featureIds=[],
        isRollout=False,
        outcomes=outcomes,
    )


class TestResolveMetricConfig:
    def test_partial_metric_failure_keeps_good_metrics(self, query, configs):
        # defaults/firefox_desktop.toml has good_metric and bad_metric (no
        # statistical treatment, so it fails to resolve).
        ne = make_nimbus_experiment("no-external-config-experiment")
        metric_config = query.resolve_metric_config(ne, configs)

        assert metric_config.has_external_config is False
        assert metric_config.resolution_error is None

        metric_names = {metric.name for metric in metric_config.metrics}
        assert "good_metric" in metric_names
        assert "bad_metric" not in metric_names

        unresolved_names = {m.name for m in metric_config.unresolved_metrics}
        assert unresolved_names == {"bad_metric"}

    def test_external_config_overrides(self, query, configs):
        # override-experiment.toml sets [experiment] end_date and enrollment_period.
        ne = make_nimbus_experiment(
            "override-experiment",
            end_date=pytz.utc.localize(datetime.datetime(2024, 8, 1)),
        )
        metric_config = query.resolve_metric_config(ne, configs)

        assert metric_config.has_external_config is True
        assert metric_config.has_external_config_overrides is True
        assert metric_config.overrides.end_date == "2024-06-01"
        assert metric_config.overrides.enrollment_period == 21
        assert metric_config.overrides.reference_branch is None
        assert metric_config.overrides.start_date is None

    def test_unresolved_outcome(self, query, configs):
        ne = make_nimbus_experiment(
            "outcome-missing-experiment",
            outcomes=[Outcome(slug="nonexistent-outcome")],
        )
        metric_config = query.resolve_metric_config(ne, configs)
        assert metric_config.unresolved_outcomes == ["nonexistent-outcome"]

    def test_known_outcome_resolves(self, query, configs):
        # known_outcome contributes no metrics; this only covers the
        # merge_outcome/merge_parameters success path.
        ne = make_nimbus_experiment(
            "outcome-present-experiment",
            outcomes=[Outcome(slug="known_outcome")],
        )
        metric_config = query.resolve_metric_config(ne, configs)
        assert metric_config.unresolved_outcomes == []

    def test_whole_experiment_resolution_failure(self, query, configs):
        # segment-failure-experiment.toml references a missing segment, which
        # fails outside the per-metric try/except.
        ne = make_nimbus_experiment("segment-failure-experiment")
        with pytest.raises(query.RECOVERABLE_RESOLUTION_ERRORS):
            query.resolve_metric_config(ne, configs)


class TestGetMetricConfigs:
    def test_bad_config_does_not_fail_the_run(self, query, configs):
        experiments = [
            make_nimbus_experiment("segment-failure-experiment"),
            make_nimbus_experiment("no-external-config-experiment"),
        ]
        rows = query.get_metric_configs(experiments, configs)

        by_slug = {row.normandy_slug: row for row in rows}
        assert (
            by_slug["segment-failure-experiment"].metric_config.resolution_error
            is not None
        )
        assert (
            by_slug["no-external-config-experiment"].metric_config.resolution_error
            is None
        )
