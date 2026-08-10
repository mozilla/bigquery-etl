"""Tests for probe_definitions_v1 query.py (Glean sensitivity signals)."""

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# Import the query module from its file path.
_repo_root = Path(__file__).resolve().parent.parent.parent
_query_dir = (
    _repo_root
    / "sql/moz-fx-data-shared-prod/data_governance_metadata_derived/probe_definitions_v1"
)
_query_path = _query_dir / "query.py"

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not _query_path.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)

spec = importlib.util.spec_from_file_location("probe_definitions_query", _query_path)
assert spec is not None and spec.loader is not None
query_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query_mod)


# A Glean Dictionary metric, shaped like the real per-ping response: tags and
# pings are top level, there is no "metadata" key, and data_sensitivity is absent.
AD_CLICKS = {
    "name": "browser.search.ad_clicks",
    "description": "Records clicks of adverts on SERP pages.",
    "type": "labeled_counter",
    "tags": ["Search"],
    "pings": ["baseline", "metrics"],
}
DICTIONARY = {"metrics": [AD_CLICKS]}

# probe-info keys metrics by dotted name; the last history entry is current.
PROBE_INFO = {
    "browser.search.ad_clicks": {
        "history": [
            {"data_sensitivity": ["technical"], "send_in_pings": ["metrics"]},
            {
                "data_sensitivity": ["interaction"],
                "send_in_pings": ["baseline", "metrics"],
            },
        ]
    }
}

REPOSITORIES = [
    {
        "name": "firefox-android-release",
        "dependencies": ["org.mozilla.components:lib-gecko"],
    },
    {"name": "fenix", "dependencies": ["org.mozilla.components:lib-gecko"]},
    {"name": "gecko", "library_names": ["org.mozilla.components:lib-gecko"]},
    {"name": "firefox-ios-release"},
]

APP_LISTINGS = [
    {"app_name": "fenix", "v1_name": "fenix", "app_channel": "nightly"},
    {
        "app_name": "fenix",
        "v1_name": "firefox-android-release",
        "app_channel": "release",
    },
    {"app_name": "firefox_ios", "v1_name": "firefox-ios-beta", "app_channel": "beta"},
    {
        "app_name": "firefox_ios",
        "v1_name": "firefox-ios-release",
        "app_channel": "release",
    },
    {"app_name": "firefox_desktop", "v1_name": "firefox-desktop"},
    # One app_name, one release listing per platform.
    {"app_name": "mozilla_vpn", "v1_name": "mozilla-vpn", "app_channel": "release"},
    {
        "app_name": "mozilla_vpn",
        "v1_name": "mozilla-vpn-android",
        "app_channel": "release",
    },
]

CATALOG = {
    "/glean/repositories": REPOSITORIES,
    "/glean/app-listings": APP_LISTINGS,
}

FENIX_METRICS = "/glean/firefox-android-release/metrics"


class FakeResponse:
    """Minimal stand-in for a requests Response."""

    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        """Return the canned payload."""
        return self.payload

    def raise_for_status(self):
        """Raise for 4xx/5xx, like requests does."""
        if self.status_code >= 400:
            raise query_mod.requests.HTTPError(f"status {self.status_code}")


def stub_get(responses, urls=None):
    """Build a requests.get stub routing on URL fragment.

    Each value is a JSON payload or an exception to raise. An unmatched URL 404s,
    which is how a slug with no probe-info entry behaves. Pass ``urls`` to record
    the requested URLs.
    """

    def _get(url, **kwargs):
        if urls is not None:
            urls.append(url)
        for fragment, payload in responses.items():
            if fragment in url:
                if isinstance(payload, Exception):
                    raise payload
                return FakeResponse(payload)
        return FakeResponse({}, status_code=404)

    return _get


@pytest.fixture(autouse=True)
def clear_probe_info_caches():
    """Drop the probe-info caches so one test's stub cannot leak into the next."""

    def _clear():
        query_mod.fetch_probe_info.cache_clear()
        query_mod._fetch_metrics_map.cache_clear()
        query_mod._CATALOG_CACHE.clear()

    _clear()
    yield
    _clear()


class TestGleanFieldMerge:
    def test_merges_probe_info_and_dictionary_fields(self):
        with patch.object(
            query_mod.requests,
            "get",
            stub_get(
                {**CATALOG, "/pings/": DICTIONARY, FENIX_METRICS: PROBE_INFO},
            ),
        ):
            probes = query_mod.fetch_glean_probes("fenix.baseline")

        assert len(probes) == 1
        probe = probes[0]
        assert probe["probe_name"] == "browser.search.ad_clicks"
        assert probe["probe_type"] == "labeled_counter"
        # Latest history entry wins.
        assert probe["data_sensitivity"] == ["interaction"]
        assert probe["send_in_pings"] == ["baseline", "metrics"]
        assert probe["tags"] == ["Search"]

    def test_tags_come_from_the_dictionary_top_level_key(self):
        metric = dict(AD_CLICKS, metadata={"tags": ["WrongSource"]})
        with patch.object(
            query_mod.requests,
            "get",
            stub_get({**CATALOG, "/pings/": {"metrics": [metric]}}),
        ):
            probes = query_mod.fetch_glean_probes("fenix.baseline")

        assert probes[0]["tags"] == ["Search"]

    def test_send_in_pings_falls_back_to_dictionary_pings(self):
        with patch.object(
            query_mod.requests,
            "get",
            stub_get({**CATALOG, "/pings/": DICTIONARY}),
        ):
            probes = query_mod.fetch_glean_probes("fenix.baseline")

        assert probes[0]["send_in_pings"] == ["baseline", "metrics"]
        assert probes[0]["data_sensitivity"] == []

    def test_unreachable_metrics_file_raises(self):
        # A 404 is a data gap and yields empty fields, but an outage must not be
        # turned into one: the partition would be written with the whole app's
        # sensitivity silently missing.
        with patch.object(
            query_mod.requests,
            "get",
            stub_get(
                {
                    **CATALOG,
                    "/pings/": DICTIONARY,
                    FENIX_METRICS: query_mod.requests.ConnectionError("boom"),
                },
            ),
        ):
            with pytest.raises(
                RuntimeError, match="probe-info firefox-android-release"
            ):
                query_mod.fetch_glean_probes("fenix.baseline")


class TestLibraryMetrics:
    """Most metrics are defined in a library, not in the app's own metrics file."""

    def test_library_definitions_fill_gaps(self):
        metric = {"name": "a11y.backplate", "type": "boolean", "pings": ["metrics"]}
        library = {"a11y.backplate": {"history": [{"data_sensitivity": ["technical"]}]}}
        urls = []
        with patch.object(
            query_mod.requests,
            "get",
            stub_get(
                {
                    **CATALOG,
                    "/pings/": {"metrics": [metric]},
                    "/glean/gecko/metrics": library,
                },
                urls=urls,
            ),
        ):
            probes = query_mod.fetch_glean_probes("fenix.metrics")

        # Resolved the dependency's library name back to the gecko repository.
        assert any(u.endswith("/glean/gecko/metrics") for u in urls)
        assert probes[0]["data_sensitivity"] == ["technical"]

    def test_app_definition_wins_over_library(self):
        library = {
            "browser.search.ad_clicks": {
                "history": [{"data_sensitivity": ["stored_content"]}]
            }
        }
        with patch.object(
            query_mod.requests,
            "get",
            stub_get(
                {
                    **CATALOG,
                    "/pings/": DICTIONARY,
                    FENIX_METRICS: PROBE_INFO,
                    "/glean/gecko/metrics": library,
                },
            ),
        ):
            probes = query_mod.fetch_glean_probes("fenix.baseline")

        assert probes[0]["data_sensitivity"] == ["interaction"]

    def test_transitive_dependencies_are_followed(self):
        # probe-info publishes a flat graph today; this guards the traversal if
        # a library ever declares dependencies of its own.
        repositories = [
            {"name": "app", "dependencies": ["lib-one"]},
            {"name": "one", "library_names": ["lib-one"], "dependencies": ["lib-two"]},
            {"name": "two", "library_names": ["lib-two"]},
        ]
        with patch.object(
            query_mod.requests,
            "get",
            stub_get(
                {
                    "/glean/repositories": repositories,
                    "/glean/app-listings": APP_LISTINGS,
                }
            ),
        ):
            assert query_mod._metric_sources("app") == ("app", "one", "two")

    def test_dependency_cycle_does_not_hang(self):
        repositories = [
            {"name": "a", "library_names": ["lib-a"], "dependencies": ["lib-b"]},
            {"name": "b", "library_names": ["lib-b"], "dependencies": ["lib-a"]},
        ]
        with patch.object(
            query_mod.requests,
            "get",
            stub_get(
                {
                    "/glean/repositories": repositories,
                    "/glean/app-listings": APP_LISTINGS,
                }
            ),
        ):
            assert query_mod._metric_sources("a") == ("a", "b")


class TestAppSlugResolution:
    """A Dictionary app name is not a probe-info repository name."""

    def test_release_channel_is_read_before_other_channels(self):
        # Regression guard: fenix is a repository, so a name-based lookup would
        # resolve, but to the nightly channel. Release must be read first so its
        # definitions win, and both are read so neither channel's are lost.
        with patch.object(query_mod.requests, "get", stub_get(CATALOG)):
            slugs = query_mod._repository_slugs("fenix")

        assert slugs[0] == "firefox-android-release"
        assert "fenix" in slugs

    def test_app_with_no_repository_of_its_own_name_resolves(self):
        # Regression guard: firefox_ios -> firefox-ios 404s, which left
        # data_sensitivity empty for the whole app.
        urls = []
        with patch.object(
            query_mod.requests,
            "get",
            stub_get(
                {
                    **CATALOG,
                    "/pings/": DICTIONARY,
                    "/glean/firefox-ios-release/metrics": PROBE_INFO,
                },
                urls,
            ),
        ):
            probes = query_mod.fetch_glean_probes("firefox_ios.baseline")

        assert any(u.endswith("/glean/firefox-ios-release/metrics") for u in urls)
        assert not any(u.endswith("/glean/firefox-ios/metrics") for u in urls)
        assert probes[0]["data_sensitivity"] == ["interaction"]

    def test_every_platform_listing_is_read(self):
        # mozilla_vpn has one release listing per platform, and metrics can be
        # unique to one of them, so dropping any loses their sensitivity.
        with patch.object(query_mod.requests, "get", stub_get(CATALOG)):
            assert query_mod._repository_slugs("mozilla_vpn") == (
                "mozilla-vpn",
                "mozilla-vpn-android",
            )

    def test_absent_app_channel_means_release(self):
        with patch.object(query_mod.requests, "get", stub_get(CATALOG)):
            assert query_mod._repository_slugs("firefox_desktop") == (
                "firefox-desktop",
            )

    def test_unlisted_app_falls_back_to_the_dashed_name(self):
        with patch.object(query_mod.requests, "get", stub_get(CATALOG)):
            assert query_mod._repository_slugs("mystery_app") == ("mystery-app",)


class TestCatalogFailure:
    def test_catalog_failure_raises_and_is_retried(self):
        # Degrading instead of raising would write a partition whose
        # data_sensitivity is silently incomplete, and cache that result for the
        # app, so later pings never retry the catalog.
        calls = []

        def _get(url, **kwargs):
            if "/glean/repositories" in url:
                calls.append(url)
                if len(calls) == 1:
                    raise query_mod.requests.ConnectionError("boom")
                return FakeResponse(REPOSITORIES)
            if "/glean/app-listings" in url:
                return FakeResponse(APP_LISTINGS)
            return FakeResponse({}, status_code=404)

        with patch.object(query_mod.requests, "get", _get):
            with pytest.raises(RuntimeError, match="probe-info repositories"):
                query_mod._metric_sources("fenix")
            # Not remembered, so the retry sees the recovered catalog.
            assert query_mod._metric_sources("fenix") == (
                "firefox-android-release",
                "fenix",
                "gecko",
            )

        assert len(calls) == 2


class TestSchema:
    def test_schema_yaml_matches_the_load_job_schema(self):
        # The load job schema and the deployed schema must agree, including the
        # three fields declared after the partition column.
        declared = yaml.safe_load((_query_dir / "schema.yaml").read_text())["fields"]
        assert [(f["name"], f["type"], f["mode"]) for f in declared] == [
            (f.name, f.field_type, f.mode) for f in query_mod._PROBE_DEFINITIONS_SCHEMA
        ]
