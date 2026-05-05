"""Tests for sampled_metrics_v1 query.py."""

import importlib.util
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import pytest

# Import the query module from its file path.
_repo_root = Path(__file__).resolve().parent.parent.parent
_query_path = (
    _repo_root
    / "sql/moz-fx-data-shared-prod/telemetry_derived/sampled_metrics_v1/query.py"
)

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not _query_path.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)

# Load the module dynamically since the path contains hyphens
spec = importlib.util.spec_from_file_location("sampled_metrics_query", _query_path)
assert spec is not None and spec.loader is not None
query_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query_mod)

parse_channel = query_mod.parse_channel
parse_min_version = query_mod.parse_min_version
parse_max_version = query_mod.parse_max_version
is_active = query_mod.is_active
get_sampled_metrics_from_api = query_mod.get_sampled_metrics_from_api
get_current_state = query_mod.get_current_state
compute_diff = query_mod.compute_diff


# -- Fixtures: mock API data --------------------------------------------------

FUTURE_DATE = (date.today() + timedelta(days=30)).isoformat()
PAST_DATE = (date.today() - timedelta(days=30)).isoformat()


def _make_experiment(
    slug="test-sampling-rollout",
    is_rollout=True,
    app_name="firefox_desktop",
    start_date="2025-04-30",
    end_date=None,
    targeting="(browserSettings.update.channel == \"release\") && (version|versionCompare('138.!') >= 0)",
    bucket_count=1000,
    bucket_total=10000,
    metrics_enabled=None,
    feature_ids=None,
    branches=None,
):
    """Build a mock experiment dict matching the Experimenter API shape."""
    if metrics_enabled is None:
        metrics_enabled = {
            "timing_distribution.paint_build_displaylist_time": False,
            "memory_distribution.heap_allocated": False,
        }
    if feature_ids is None:
        feature_ids = ["gleanInternalSdk"]
    if branches is None:
        branches = [
            {
                "slug": "control",
                "ratio": 1,
                "features": [
                    {
                        "featureId": "gleanInternalSdk",
                        "value": {
                            "gleanMetricConfiguration": {
                                "metrics_enabled": metrics_enabled,
                            }
                        },
                    }
                ],
            }
        ]
    return {
        "slug": slug,
        "isRollout": is_rollout,
        "appName": app_name,
        "startDate": start_date,
        "endDate": end_date,
        "targeting": targeting,
        "featureIds": feature_ids,
        "bucketConfig": {
            "count": bucket_count,
            "total": bucket_total,
            "randomizationUnit": "normandy_id",
            "namespace": "test-namespace",
            "start": 0,
        },
        "branches": branches,
    }


# -- Tests: parse_channel -----------------------------------------------------


class TestParseChannel:
    def test_equality_operator(self):
        targeting = "(browserSettings.update.channel == \"release\") && (version|versionCompare('138.!') >= 0)"
        assert parse_channel(targeting) == "release"

    def test_beta_channel(self):
        targeting = '(browserSettings.update.channel == "beta") && (something)'
        assert parse_channel(targeting) == "beta"

    def test_in_operator(self):
        targeting = "(browserSettings.update.channel in [\"release\"]) && (version|versionCompare('136.!') >= 0)"
        assert parse_channel(targeting) == "release"

    def test_no_channel(self):
        targeting = "(version|versionCompare('138.!') >= 0)"
        assert parse_channel(targeting) is None

    def test_empty_string(self):
        assert parse_channel("") is None


# -- Tests: parse_min_version -------------------------------------------------


class TestParseMinVersion:
    def test_standard_version(self):
        targeting = "(version|versionCompare('138.!') >= 0)"
        assert parse_min_version(targeting) == "138.!"

    def test_patch_version(self):
        targeting = "(version|versionCompare('105.0.2') >= 0)"
        assert parse_min_version(targeting) == "105.0.2"

    def test_version_with_upper_bound(self):
        targeting = "(version|versionCompare('120.*') < 0) && (version|versionCompare('120.!') >= 0)"
        assert parse_min_version(targeting) == "120.!"

    def test_no_version(self):
        targeting = '(browserSettings.update.channel == "release")'
        assert parse_min_version(targeting) is None

    def test_empty_string(self):
        assert parse_min_version("") is None


# -- Tests: parse_max_version -------------------------------------------------


class TestParseMaxVersion:
    def test_standard_version(self):
        targeting = "(version|versionCompare('140.*') < 0)"
        assert parse_max_version(targeting) == "140.*"

    def test_patch_version(self):
        targeting = "(version|versionCompare('131.0.3') < 0)"
        assert parse_max_version(targeting) == "131.0.3"

    def test_with_min_version(self):
        targeting = "(version|versionCompare('130.!') >= 0) && (version|versionCompare('140.*') < 0)"
        assert parse_max_version(targeting) == "140.*"

    def test_no_version(self):
        targeting = '(browserSettings.update.channel == "release")'
        assert parse_max_version(targeting) is None

    def test_empty_string(self):
        assert parse_max_version("") is None


# -- Tests: is_active ---------------------------------------------------------


class TestIsActive:
    def test_no_end_date(self):
        assert is_active({"endDate": None}) is True

    def test_future_end_date(self):
        assert is_active({"endDate": FUTURE_DATE}) is True

    def test_past_end_date(self):
        assert is_active({"endDate": PAST_DATE}) is False

    def test_invalid_end_date(self):
        assert is_active({"endDate": "not-a-date"}) is False

    def test_missing_end_date_key(self):
        assert is_active({}) is True


# -- Tests: get_sampled_metrics_from_api ---------------------------------------


class TestGetSampledMetricsFromApi:
    @mock.patch.object(query_mod, "fetch")
    def test_basic_extraction(self, mock_fetch):
        mock_fetch.return_value = [
            _make_experiment(end_date=FUTURE_DATE),
        ]
        rows = get_sampled_metrics_from_api()

        assert len(rows) == 2
        assert all(r["experimenter_slug"] == "test-sampling-rollout" for r in rows)
        assert all(r["sample_rate"] == 0.9 for r in rows)
        assert all(r["channel"] == "release" for r in rows)
        assert all(r["min_version"] == "138.!" for r in rows)
        assert all(r["max_version"] is None for r in rows)
        assert all(r["app_name"] == "firefox_desktop" for r in rows)
        assert all(r["is_rollout"] is True for r in rows)

        types = {r["metric_type"] for r in rows}
        names = {r["metric_name"] for r in rows}
        assert types == {"timing_distribution", "memory_distribution"}
        assert names == {"paint_build_displaylist_time", "heap_allocated"}

    @mock.patch.object(query_mod, "fetch")
    def test_filters_non_glean_experiments(self, mock_fetch):
        mock_fetch.return_value = [
            _make_experiment(
                feature_ids=["someOtherFeature"],
                end_date=FUTURE_DATE,
            ),
        ]
        rows = get_sampled_metrics_from_api()
        assert rows == []

    @mock.patch.object(query_mod, "fetch")
    def test_filters_ended_experiments(self, mock_fetch):
        mock_fetch.return_value = [
            _make_experiment(end_date=PAST_DATE),
        ]
        rows = get_sampled_metrics_from_api()
        assert rows == []

    @mock.patch.object(query_mod, "fetch")
    def test_only_false_metrics_included(self, mock_fetch):
        mock_fetch.return_value = [
            _make_experiment(
                end_date=FUTURE_DATE,
                metrics_enabled={
                    "counter.sampled_metric": False,
                    "counter.unsampled_metric": True,
                },
            ),
        ]
        rows = get_sampled_metrics_from_api()
        assert len(rows) == 1
        assert rows[0]["metric_name"] == "sampled_metric"

    @mock.patch.object(query_mod, "fetch")
    def test_unions_metrics_across_branches(self, mock_fetch):
        branches = [
            {
                "slug": "control",
                "ratio": 1,
                "features": [
                    {
                        "featureId": "gleanInternalSdk",
                        "value": {
                            "gleanMetricConfiguration": {
                                "metrics_enabled": {
                                    "counter.metric_a": False,
                                }
                            }
                        },
                    }
                ],
            },
            {
                "slug": "treatment",
                "ratio": 1,
                "features": [
                    {
                        "featureId": "gleanInternalSdk",
                        "value": {
                            "gleanMetricConfiguration": {
                                "metrics_enabled": {
                                    "counter.metric_b": False,
                                }
                            }
                        },
                    }
                ],
            },
        ]
        mock_fetch.return_value = [
            _make_experiment(
                end_date=FUTURE_DATE,
                branches=branches,
            ),
        ]
        rows = get_sampled_metrics_from_api()
        names = {r["metric_name"] for r in rows}
        assert names == {"metric_a", "metric_b"}

    @mock.patch.object(query_mod, "fetch")
    def test_metric_without_dot(self, mock_fetch):
        mock_fetch.return_value = [
            _make_experiment(
                end_date=FUTURE_DATE,
                metrics_enabled={"nodot": False},
            ),
        ]
        rows = get_sampled_metrics_from_api()
        assert len(rows) == 1
        assert rows[0]["metric_type"] is None
        assert rows[0]["metric_name"] == "nodot"

    @mock.patch.object(query_mod, "fetch")
    def test_no_active_experiments(self, mock_fetch):
        mock_fetch.return_value = []
        rows = get_sampled_metrics_from_api()
        assert rows == []


# -- Tests: compute_diff ------------------------------------------------------


class TestComputeDiff:
    def test_new_metric_inserted(self):
        api_rows = [
            {
                "start_date": "2025-04-30",
                "experimenter_slug": "rollout-1",
                "is_rollout": True,
                "app_name": "firefox_desktop",
                "channel": "release",
                "min_version": "138.!",
                "max_version": None,
                "end_date": None,
                "metric_type": "counter",
                "metric_name": "new_metric",
                "sample_rate": 0.1,
            }
        ]
        current_state = {}

        result = compute_diff(api_rows, current_state)
        assert len(result) == 1
        assert result[0]["metric_name"] == "new_metric"
        assert result[0]["sample_rate"] == 0.1

    def test_unchanged_metric_not_inserted(self):
        api_rows = [
            {
                "start_date": "2025-04-30",
                "experimenter_slug": "rollout-1",
                "is_rollout": True,
                "app_name": "firefox_desktop",
                "channel": "release",
                "min_version": "138.!",
                "max_version": None,
                "end_date": None,
                "metric_type": "counter",
                "metric_name": "stable_metric",
                "sample_rate": 0.1,
            }
        ]
        current_state = {
            ("counter", "stable_metric", "release", "firefox_desktop"): 0.1,
        }

        result = compute_diff(api_rows, current_state)
        assert result == []

    def test_changed_rate_inserted(self):
        api_rows = [
            {
                "start_date": "2025-04-30",
                "experimenter_slug": "rollout-1",
                "is_rollout": True,
                "app_name": "firefox_desktop",
                "channel": "release",
                "min_version": "138.!",
                "max_version": None,
                "end_date": None,
                "metric_type": "counter",
                "metric_name": "changed_metric",
                "sample_rate": 0.5,
            }
        ]
        current_state = {
            ("counter", "changed_metric", "release", "firefox_desktop"): 0.1,
        }

        result = compute_diff(api_rows, current_state)
        assert len(result) == 1
        assert result[0]["sample_rate"] == 0.5

    def test_removed_metric_gets_100_percent(self):
        api_rows = []
        current_state = {
            ("counter", "removed_metric", "release", "firefox_desktop"): 0.1,
        }

        result = compute_diff(api_rows, current_state)
        assert len(result) == 1
        assert result[0]["metric_name"] == "removed_metric"
        assert result[0]["sample_rate"] == 1.0
        assert result[0]["experimenter_slug"] is None
        assert result[0]["is_rollout"] is None
        assert result[0]["min_version"] is None
        assert result[0]["max_version"] is None
        assert result[0]["end_date"] is None
        assert result[0]["channel"] == "release"
        assert result[0]["app_name"] == "firefox_desktop"

    def test_already_at_100_not_reinserted(self):
        api_rows = []
        current_state = {
            ("counter", "already_full", "release", "firefox_desktop"): 1.0,
        }

        result = compute_diff(api_rows, current_state)
        assert result == []

    def test_multiple_experiments_picks_most_recent(self):
        api_rows = [
            {
                "start_date": "2025-03-01",
                "experimenter_slug": "older-experiment",
                "is_rollout": True,
                "app_name": "firefox_desktop",
                "channel": "release",
                "min_version": "136.!",
                "max_version": None,
                "end_date": None,
                "metric_type": "counter",
                "metric_name": "shared_metric",
                "sample_rate": 0.5,
            },
            {
                "start_date": "2025-06-01",
                "experimenter_slug": "newer-experiment",
                "is_rollout": False,
                "app_name": "firefox_desktop",
                "channel": "release",
                "min_version": "140.!",
                "max_version": None,
                "end_date": None,
                "metric_type": "counter",
                "metric_name": "shared_metric",
                "sample_rate": 0.2,
            },
        ]
        current_state = {}

        result = compute_diff(api_rows, current_state)
        assert len(result) == 1
        assert result[0]["experimenter_slug"] == "newer-experiment"
        assert result[0]["sample_rate"] == 0.2

    def test_mixed_new_changed_removed(self):
        api_rows = [
            {
                "start_date": "2025-04-30",
                "experimenter_slug": "rollout-1",
                "is_rollout": True,
                "app_name": "firefox_desktop",
                "channel": "release",
                "min_version": "138.!",
                "max_version": None,
                "end_date": None,
                "metric_type": "counter",
                "metric_name": "new_metric",
                "sample_rate": 0.1,
            },
            {
                "start_date": "2025-04-30",
                "experimenter_slug": "rollout-1",
                "is_rollout": True,
                "app_name": "firefox_desktop",
                "channel": "release",
                "min_version": "138.!",
                "max_version": None,
                "end_date": None,
                "metric_type": "counter",
                "metric_name": "stable_metric",
                "sample_rate": 0.1,
            },
        ]
        current_state = {
            ("counter", "stable_metric", "release", "firefox_desktop"): 0.1,
            ("counter", "removed_metric", "release", "firefox_desktop"): 0.1,
        }

        result = compute_diff(api_rows, current_state)
        slugs = {r["metric_name"] for r in result}
        assert slugs == {"new_metric", "removed_metric"}

        removed = [r for r in result if r["metric_name"] == "removed_metric"][0]
        assert removed["sample_rate"] == 1.0

    def test_empty_api_and_empty_state(self):
        result = compute_diff([], {})
        assert result == []


# -- Tests: get_current_state (mocked BQ) -------------------------------------


class TestGetCurrentState:
    def test_returns_latest_per_metric(self):
        mock_client = mock.Mock()
        mock_rows = [
            {
                "metric_type": "counter",
                "metric_name": "my_metric",
                "channel": "release",
                "app_name": "firefox_desktop",
                "sample_rate": 0.1,
            },
            {
                "metric_type": "timing_distribution",
                "metric_name": "page_load",
                "channel": "beta",
                "app_name": "firefox_desktop",
                "sample_rate": 0.5,
            },
        ]
        mock_client.query.return_value.result.return_value = mock_rows

        state = get_current_state(mock_client, "project.dataset.table")

        assert state == {
            ("counter", "my_metric", "release", "firefox_desktop"): 0.1,
            ("timing_distribution", "page_load", "beta", "firefox_desktop"): 0.5,
        }
        mock_client.query.assert_called_once()

    def test_empty_table(self):
        mock_client = mock.Mock()
        mock_client.query.return_value.result.return_value = []

        state = get_current_state(mock_client, "project.dataset.table")
        assert state == {}
