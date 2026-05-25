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
parse_os = query_mod.parse_os
is_active = query_mod.is_active
get_sampled_metrics_from_api = query_mod.get_sampled_metrics_from_api
get_current_state = query_mod.get_current_state
compute_diff = query_mod.compute_diff

# Mapping used by the get_metric_type stub installed for API tests.
# Keys are the snake_cased Glean ID (dots in the canonical name replaced
# with underscores), matching what query.py looks up.
_DEFAULT_METRIC_TYPES = {
    "paint_build_displaylist_time": "timing_distribution",
    "memory_heap_allocated": "memory_distribution",
    "fog_ipc_buffer_sizes": "memory_distribution",
    "sampled_category_sampled_metric": "counter",
    "sampled_category_unsampled_metric": "counter",
    "metrics_metric_a": "counter",
    "metrics_metric_b": "counter",
}


# -- Fixtures: mock API data --------------------------------------------------

FUTURE_DATE = (date.today() + timedelta(days=30)).isoformat()
PAST_DATE = (date.today() - timedelta(days=30)).isoformat()


def _make_experiment(
    slug="test-sampling-rollout",
    is_rollout=True,
    app_name="firefox_desktop",
    start_date="2025-04-30",
    end_date=None,
    targeting="(browserSettings.update.channel == \"release\") && (os.isWindows) && (version|versionCompare('138.!') >= 0)",
    bucket_count=1000,
    bucket_total=10000,
    metrics_enabled=None,
    feature_ids=None,
    branches=None,
):
    """Build a mock experiment dict matching the Experimenter API shape."""
    if metrics_enabled is None:
        metrics_enabled = {
            "paint.build_displaylist_time": False,
            "memory.heap_allocated": False,
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


# -- Tests: parse_os ----------------------------------------------------------


class TestParseOs:
    def test_windows(self):
        assert parse_os(
            '(browserSettings.update.channel == "release") && (os.isWindows)'
        ) == ["Windows"]

    def test_windows_in_compound_expression(self):
        targeting = "((experiment.slug in activeRollouts) || ((os.isWindows) && (version|versionCompare('105.!') >= 0)))"
        assert parse_os(targeting) == ["Windows"]

    def test_mac(self):
        assert parse_os("(os.isMac)") == ["Mac"]

    def test_linux(self):
        assert parse_os("(os.isLinux)") == ["Linux"]

    def test_no_os(self):
        assert parse_os('(browserSettings.update.channel == "release")') == []

    def test_empty_string(self):
        assert parse_os("") == []

    def test_multi_os_returns_all_detected(self):
        """A rollout that targets multiple OSes returns each of them."""
        assert set(parse_os("(os.isWindows || os.isMac)")) == {"Windows", "Mac"}

    def test_all_three_oses(self):
        assert set(parse_os("(os.isWindows || os.isMac || os.isLinux)")) == {
            "Windows",
            "Mac",
            "Linux",
        }

    def test_negation_raises(self):
        with pytest.raises(ValueError, match="negation"):
            parse_os("(!os.isWindows)")

    def test_negation_with_space_raises(self):
        with pytest.raises(ValueError, match="negation"):
            parse_os("(! os.isWindows)")

    def test_unknown_predicate_raises(self):
        with pytest.raises(ValueError, match="os.isAndroid"):
            parse_os("(os.isAndroid)")

    def test_unknown_mixed_with_known_raises(self):
        with pytest.raises(ValueError, match="os.isIOS"):
            parse_os("(os.isWindows || os.isIOS)")

    def test_os_windows_version_property_does_not_raise(self):
        """os.windowsVersion is a property comparison, not an OS selector."""
        assert parse_os("(os.windowsVersion >= 6.1)") == []

    def test_real_rollout_shape_with_property_and_predicate(self):
        """Real rollouts mix os.isWindows with os.windowsVersion — must parse cleanly."""
        targeting = (
            "((currentDate|date - profileAgeCreated|date) / 86400000 >= 28 "
            "&& os.isWindows && os.windowsVersion >= 6.1)"
        )
        assert parse_os(targeting) == ["Windows"]


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
    @pytest.fixture(autouse=True)
    def _stub_metric_type_lookup(self, monkeypatch):
        """Avoid hitting the Glean dictionary; resolve from a fixed map."""
        monkeypatch.setattr(query_mod, "_glean_metric_types_by_app", {})

        def fake_get_metric_type(metric_name, app_name):
            if metric_name not in _DEFAULT_METRIC_TYPES:
                raise ValueError(
                    f"Metric '{metric_name}' not found in Glean dictionary "
                    f"for app '{app_name}'"
                )
            return _DEFAULT_METRIC_TYPES[metric_name]

        monkeypatch.setattr(query_mod, "get_metric_type", fake_get_metric_type)

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
        assert all(r["os"] == "Windows" for r in rows)
        assert all(r["min_version"] == "138.!" for r in rows)
        assert all(r["max_version"] is None for r in rows)
        assert all(r["app_name"] == "firefox_desktop" for r in rows)
        assert all(r["is_rollout"] is True for r in rows)

        types = {r["metric_type"] for r in rows}
        names = {r["metric_name"] for r in rows}
        assert types == {"timing_distribution", "memory_distribution"}
        assert names == {"paint_build_displaylist_time", "memory_heap_allocated"}

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
                    "sampled_category.sampled_metric": False,
                    "sampled_category.unsampled_metric": True,
                },
            ),
        ]
        rows = get_sampled_metrics_from_api()
        assert len(rows) == 1
        assert rows[0]["metric_name"] == "sampled_category_sampled_metric"
        assert rows[0]["metric_type"] == "counter"

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
                                    "metrics.metric_a": False,
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
                                    "metrics.metric_b": False,
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
        assert names == {"metrics_metric_a", "metrics_metric_b"}

    @mock.patch.object(query_mod, "fetch")
    def test_multi_dot_category_id(self, mock_fetch):
        """Glean categories can themselves contain dots (e.g. 'fog.ipc')."""
        mock_fetch.return_value = [
            _make_experiment(
                end_date=FUTURE_DATE,
                metrics_enabled={"fog.ipc.buffer_sizes": False},
            ),
        ]
        rows = get_sampled_metrics_from_api()
        assert len(rows) == 1
        assert rows[0]["metric_name"] == "fog_ipc_buffer_sizes"
        assert rows[0]["metric_type"] == "memory_distribution"

    @mock.patch.object(query_mod, "fetch")
    def test_multi_os_fans_out_one_row_per_os(self, mock_fetch):
        """Multi-OS targeting emits one row per (metric, os)."""
        targeting = (
            '(browserSettings.update.channel == "release") '
            "&& (os.isWindows || os.isMac) "
            "&& (version|versionCompare('138.!') >= 0)"
        )
        mock_fetch.return_value = [
            _make_experiment(
                end_date=FUTURE_DATE,
                targeting=targeting,
                metrics_enabled={"paint.build_displaylist_time": False},
            ),
        ]
        rows = get_sampled_metrics_from_api()
        assert len(rows) == 2
        oses = {r["os"] for r in rows}
        assert oses == {"Windows", "Mac"}
        # The non-OS fields are identical across the fanned-out rows.
        assert all(r["metric_name"] == "paint_build_displaylist_time" for r in rows)
        assert all(r["metric_type"] == "timing_distribution" for r in rows)

    @mock.patch.object(query_mod, "fetch")
    def test_no_os_targeting_emits_null_os(self, mock_fetch):
        """Targeting with no os.is* predicate falls back to a single os=NULL row."""
        targeting = '(browserSettings.update.channel == "release")'
        mock_fetch.return_value = [
            _make_experiment(
                end_date=FUTURE_DATE,
                targeting=targeting,
                metrics_enabled={"paint.build_displaylist_time": False},
            ),
        ]
        rows = get_sampled_metrics_from_api()
        assert len(rows) == 1
        assert rows[0]["os"] is None

    @mock.patch.object(query_mod, "fetch")
    def test_unknown_metric_raises(self, mock_fetch):
        mock_fetch.return_value = [
            _make_experiment(
                end_date=FUTURE_DATE,
                metrics_enabled={"unknown.metric": False},
            ),
        ]
        with pytest.raises(ValueError, match="unknown_metric"):
            get_sampled_metrics_from_api()

    @mock.patch.object(query_mod, "fetch")
    def test_no_active_experiments(self, mock_fetch):
        mock_fetch.return_value = []
        rows = get_sampled_metrics_from_api()
        assert rows == []


class TestGetMetricType:
    """Direct coverage of the Glean dictionary lookup helper."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self, monkeypatch):
        monkeypatch.setattr(query_mod, "_glean_metric_types_by_app", {})

    @mock.patch.object(query_mod, "fetch")
    def test_fetches_dictionary_and_resolves(self, mock_fetch):
        mock_fetch.return_value = {
            "metrics": [
                {"name": "paint.build_displaylist_time", "type": "timing_distribution"},
                {"name": "memory.heap_allocated", "type": "memory_distribution"},
            ]
        }
        assert (
            query_mod.get_metric_type("paint_build_displaylist_time", "firefox_desktop")
            == "timing_distribution"
        )
        assert (
            query_mod.get_metric_type("memory_heap_allocated", "firefox_desktop")
            == "memory_distribution"
        )
        # Second lookup for the same app reuses the cached dictionary.
        assert mock_fetch.call_count == 1

    @mock.patch.object(query_mod, "fetch")
    def test_unknown_metric_raises(self, mock_fetch):
        mock_fetch.return_value = {"metrics": []}
        with pytest.raises(ValueError, match="missing_metric"):
            query_mod.get_metric_type("missing_metric", "firefox_desktop")


# -- Tests: compute_diff ------------------------------------------------------


def _api_row(metric_name, sample_rate, **overrides):
    """Default api_row dict used in compute_diff tests."""
    row = {
        "start_date": "2025-04-30",
        "experimenter_slug": "rollout-1",
        "is_rollout": True,
        "app_name": "firefox_desktop",
        "channel": "release",
        "os": "Windows",
        "min_version": "138.!",
        "max_version": None,
        "end_date": None,
        "metric_type": "counter",
        "metric_name": metric_name,
        "sample_rate": sample_rate,
    }
    row.update(overrides)
    return row


class TestComputeDiff:
    def test_new_metric_inserted(self):
        result = compute_diff([_api_row("new_metric", 0.1)], {})
        assert len(result) == 1
        assert result[0]["metric_name"] == "new_metric"
        assert result[0]["sample_rate"] == 0.1

    def test_unchanged_metric_not_inserted(self):
        current_state = {
            ("counter", "stable_metric", "release", "firefox_desktop", "Windows"): 0.1,
        }
        result = compute_diff([_api_row("stable_metric", 0.1)], current_state)
        assert result == []

    def test_changed_rate_inserted(self):
        current_state = {
            ("counter", "changed_metric", "release", "firefox_desktop", "Windows"): 0.1,
        }
        result = compute_diff([_api_row("changed_metric", 0.5)], current_state)
        assert len(result) == 1
        assert result[0]["sample_rate"] == 0.5

    def test_same_metric_different_os_is_distinct(self):
        """A new OS for the same (metric_type, metric_name, channel, app_name)
        is a different key — the Windows row must not be treated as a state
        change of the Mac row."""
        current_state = {
            ("counter", "m", "release", "firefox_desktop", "Mac"): 0.1,
        }
        result = compute_diff([_api_row("m", 0.1)], current_state)
        # Mac row is a tombstone (no longer in api_state), Windows row is new.
        assert len(result) == 2
        oses = {r["os"] for r in result}
        assert oses == {"Windows", "Mac"}

    def test_removed_metric_gets_100_percent(self):
        current_state = {
            ("counter", "removed_metric", "release", "firefox_desktop", "Windows"): 0.1,
        }
        result = compute_diff([], current_state)
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
        assert result[0]["os"] == "Windows"

    def test_already_at_100_not_reinserted(self):
        current_state = {
            ("counter", "already_full", "release", "firefox_desktop", "Windows"): 1.0,
        }
        result = compute_diff([], current_state)
        assert result == []

    def test_multiple_experiments_picks_most_recent(self):
        api_rows = [
            _api_row(
                "shared_metric",
                0.5,
                start_date="2025-03-01",
                experimenter_slug="older-experiment",
                min_version="136.!",
            ),
            _api_row(
                "shared_metric",
                0.2,
                start_date="2025-06-01",
                experimenter_slug="newer-experiment",
                is_rollout=False,
                min_version="140.!",
            ),
        ]
        result = compute_diff(api_rows, {})
        assert len(result) == 1
        assert result[0]["experimenter_slug"] == "newer-experiment"
        assert result[0]["sample_rate"] == 0.2

    def test_mixed_new_changed_removed(self):
        api_rows = [
            _api_row("new_metric", 0.1),
            _api_row("stable_metric", 0.1),
        ]
        current_state = {
            ("counter", "stable_metric", "release", "firefox_desktop", "Windows"): 0.1,
            ("counter", "removed_metric", "release", "firefox_desktop", "Windows"): 0.1,
        }
        result = compute_diff(api_rows, current_state)
        names = {r["metric_name"] for r in result}
        assert names == {"new_metric", "removed_metric"}
        removed = [r for r in result if r["metric_name"] == "removed_metric"][0]
        assert removed["sample_rate"] == 1.0

    def test_empty_api_and_empty_state(self):
        assert compute_diff([], {}) == []


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
                "os": "Windows",
                "sample_rate": 0.1,
            },
            {
                "metric_type": "timing_distribution",
                "metric_name": "page_load",
                "channel": "beta",
                "app_name": "firefox_desktop",
                "os": "Mac",
                "sample_rate": 0.5,
            },
        ]
        mock_client.query.return_value.result.return_value = mock_rows

        state = get_current_state(mock_client, "project.dataset.table")

        assert state == {
            ("counter", "my_metric", "release", "firefox_desktop", "Windows"): 0.1,
            (
                "timing_distribution",
                "page_load",
                "beta",
                "firefox_desktop",
                "Mac",
            ): 0.5,
        }
        mock_client.query.assert_called_once()
        rendered_query = mock_client.query.call_args.args[0]
        assert (
            "PARTITION BY metric_type, metric_name, channel, app_name, os"
            in rendered_query
        )

    def test_empty_table(self):
        mock_client = mock.Mock()
        mock_client.query.return_value.result.return_value = []

        state = get_current_state(mock_client, "project.dataset.table")
        assert state == {}
