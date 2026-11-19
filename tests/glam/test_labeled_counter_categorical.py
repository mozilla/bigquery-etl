"""Tests for processing static labeled_counters as categorical histograms.

A labeled_counter with a predefined `labels` list (per the Glean Dictionary) is
the Glean replacement for a Legacy Telemetry categorical histogram. These live in
the histogram pipeline; dynamic (open-ended) labeled_counters stay in the scalar
pipeline.
"""

import sys
from unittest import mock

import pytest
import requests

from bigquery_etl.glam import clients_daily_histogram_aggregates as histograms
from bigquery_etl.glam import clients_daily_scalar_aggregates as scalars
from bigquery_etl.glam import utils


@pytest.fixture(autouse=True)
def no_retry_sleep(monkeypatch):
    """Keep the retry backoff from actually sleeping during tests."""
    monkeypatch.setattr(utils.time, "sleep", lambda _: None)


def _resp(payload=None, status_code=200):
    """Build a stand-in for a requests.Response returning `payload` from json()."""
    resp = mock.Mock()
    resp.status_code = status_code
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


class TestGleanDictionaryLookup:
    def setup_method(self):
        utils._glean_metric_metadata_cache.clear()

    def test_static_labeled_counter_detected(self):
        with mock.patch.object(
            utils.requests,
            "get",
            return_value=_resp(
                {"type": "labeled_counter", "labels": ["not_used", "succeeded"]}
            ),
        ) as get:
            assert utils.is_static_labeled_counter(
                "org_mozilla_fenix", "dns_trr_http3_0rtt_state"
            )
        # Fenix maps to the "fenix" dictionary app; file name == BQ column form.
        url = get.call_args.args[0]
        assert "/fenix/metrics/data_dns_trr_http3_0rtt_state.json" in url

    def test_dynamic_labeled_counter_not_static(self):
        with mock.patch.object(
            utils.requests,
            "get",
            return_value=_resp({"type": "labeled_counter", "labels": None}),
        ):
            assert not utils.is_static_labeled_counter("org_mozilla_fenix", "dynamic")

    def test_non_labeled_counter_not_static(self):
        with mock.patch.object(
            utils.requests,
            "get",
            return_value=_resp({"type": "timing_distribution", "labels": []}),
        ):
            assert not utils.is_static_labeled_counter("org_mozilla_fenix", "perf_load")

    def test_absent_metric_404_is_not_static(self):
        # A genuine 404 is the only "expected" miss -> not static.
        with mock.patch.object(
            utils.requests, "get", return_value=_resp(status_code=404)
        ) as get:
            assert not utils.is_static_labeled_counter("org_mozilla_fenix", "missing")
        get.assert_called_once()

    def test_network_error_propagates(self):
        # A transient failure must fail loudly, not be treated as "not static".
        with mock.patch.object(
            utils.requests,
            "get",
            side_effect=requests.exceptions.ConnectionError("boom"),
        ):
            with pytest.raises(requests.exceptions.ConnectionError):
                utils.is_static_labeled_counter("org_mozilla_fenix", "x")

    def test_non_404_http_error_propagates(self):
        resp = _resp(status_code=500)
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        with mock.patch.object(utils.requests, "get", return_value=resp):
            with pytest.raises(requests.exceptions.HTTPError):
                utils.is_static_labeled_counter("org_mozilla_fenix", "x")

    def test_unknown_product_skips_request(self):
        with mock.patch.object(utils.requests, "get") as get:
            assert not utils.is_static_labeled_counter("not_a_product", "x")
        get.assert_not_called()

    def test_result_is_cached(self):
        with mock.patch.object(
            utils.requests,
            "get",
            return_value=_resp({"type": "labeled_counter", "labels": ["a"]}),
        ) as get:
            utils.is_static_labeled_counter("org_mozilla_fenix", "m")
            utils.is_static_labeled_counter("org_mozilla_fenix", "m")
        get.assert_called_once()


SCHEMA = [
    {
        "name": "metrics",
        "fields": [
            {
                "name": "labeled_counter",
                "fields": [
                    {"name": "dns_trr_http3_0rtt_state"},  # static
                    {"name": "some_dynamic_counter"},  # dynamic
                ],
            },
            {"name": "counter", "fields": [{"name": "plain_counter"}]},
            {"name": "timing_distribution", "fields": [{"name": "perf_load"}]},
        ],
    }
]


def _fake_is_static(product, probe_name):
    return probe_name == "dns_trr_http3_0rtt_state"


class TestHistogramRouting:
    def test_static_labeled_counter_routed_to_histogram(self, monkeypatch):
        monkeypatch.setattr(histograms, "is_static_labeled_counter", _fake_is_static)
        monkeypatch.setattr(histograms, "get_sampled_metrics", lambda *a, **k: {})
        metrics, _ = histograms.get_distribution_metrics(
            SCHEMA, product="org_mozilla_fenix"
        )
        assert metrics["labeled_counter"] == ["dns_trr_http3_0rtt_state"]
        assert metrics["timing_distribution"] == ["perf_load"]

    def test_categorical_snippet_uses_bare_metric_path(self):
        # The metric's key/value array IS the categorical histogram, so it is fed
        # in directly (no `.values`) as an unlabeled histogram.
        sql = histograms.get_metrics_sql(
            {"labeled_counter": ["dns_trr_http3_0rtt_state"]}
        )
        assert (
            '(\'\', "dns_trr_http3_0rtt_state", "labeled_counter", '
            "metrics.labeled_counter.dns_trr_http3_0rtt_state)" in sql["unlabeled"]
        )
        assert sql["labeled"] == ""

    def test_distribution_snippet_still_uses_values(self):
        sql = histograms.get_metrics_sql({"timing_distribution": ["perf_load"]})
        assert "metrics.timing_distribution.perf_load.values" in sql["unlabeled"]


class TestScalarExclusion:
    def test_static_labeled_counter_excluded_from_scalar(self, monkeypatch):
        monkeypatch.setattr(scalars, "is_static_labeled_counter", _fake_is_static)
        monkeypatch.setattr(scalars, "get_sampled_metrics", lambda *a, **k: {})
        result, _ = scalars.get_scalar_metrics(
            SCHEMA, "labeled", product="org_mozilla_fenix"
        )
        # Dynamic stays in scalar; static moved to the histogram pipeline.
        assert result["labeled_counter"] == ["some_dynamic_counter"]


class TestGleanDictionaryRetries:
    """A transient Dictionary failure took out a whole ping's query (2026-08-03)."""

    def setup_method(self):
        utils._glean_metric_metadata_cache.clear()

    def test_read_timeout_is_retried_then_succeeds(self):
        payload = {"type": "labeled_counter", "labels": ["a"]}
        with mock.patch.object(
            utils.requests,
            "get",
            side_effect=[
                requests.exceptions.ReadTimeout("timed out"),
                _resp(payload),
            ],
        ) as get:
            assert utils.is_static_labeled_counter("org_mozilla_fenix", "m")
        assert get.call_count == 2

    def test_retryable_status_is_retried_then_succeeds(self):
        with mock.patch.object(
            utils.requests,
            "get",
            side_effect=[
                _resp(status_code=503),
                _resp({"type": "labeled_counter", "labels": ["a"]}),
            ],
        ) as get:
            assert utils.is_static_labeled_counter("org_mozilla_fenix", "m")
        assert get.call_count == 2

    def test_persistent_timeout_still_raises(self):
        # Retries must not turn a real outage into a silent success.
        with mock.patch.object(
            utils.requests,
            "get",
            side_effect=requests.exceptions.ReadTimeout("timed out"),
        ) as get:
            with pytest.raises(requests.exceptions.ReadTimeout):
                utils.is_static_labeled_counter("org_mozilla_fenix", "m")
        assert get.call_count == utils.GLEAN_DICTIONARY_RETRIES + 1

    def test_retries_are_reported(self, capsys):
        with mock.patch.object(
            utils.requests,
            "get",
            side_effect=[
                requests.exceptions.ReadTimeout("timed out"),
                _resp(status_code=503),
                _resp({"type": "labeled_counter", "labels": ["a"]}),
            ],
        ):
            utils.is_static_labeled_counter("org_mozilla_fenix", "m")
        err = capsys.readouterr().err
        assert "after ReadTimeout (attempt 1/5)" in err
        assert "after HTTP 503 (attempt 2/5)" in err

    def test_404_is_not_retried(self):
        with mock.patch.object(
            utils.requests, "get", return_value=_resp(status_code=404)
        ) as get:
            assert not utils.is_static_labeled_counter("org_mozilla_fenix", "m")
        get.assert_called_once()

    def test_timeout_is_generous(self):
        with mock.patch.object(
            utils.requests, "get", return_value=_resp(status_code=404)
        ) as get:
            utils.is_static_labeled_counter("org_mozilla_fenix", "m")
        assert get.call_args.kwargs["timeout"] == utils.GLEAN_DICTIONARY_TIMEOUT


class TestEmitQuery:
    def test_writes_query_to_output_dir(self, tmp_path):
        out = tmp_path / "some_ping"
        utils.emit_query("SELECT 1", str(out), "d.some_ping_v1")
        assert (out / "query.sql").read_text() == "SELECT 1\n"

    def test_no_probes_removes_stale_directory(self, tmp_path):
        out = tmp_path / "some_ping"
        out.mkdir()
        (out / "query.sql").write_text("SELECT 1\n")
        utils.emit_query(None, str(out), "d.some_ping_v1")
        assert not out.exists()

    def test_no_probes_without_existing_directory(self, tmp_path):
        out = tmp_path / "some_ping"
        utils.emit_query(None, str(out), "d.some_ping_v1")
        assert not out.exists()

    def test_failed_removal_propagates(self, tmp_path):
        out = tmp_path / "some_ping"
        out.mkdir()
        (out / "query.sql").write_text("SELECT 1\n")
        with mock.patch.object(
            utils.shutil, "rmtree", side_effect=OSError("permission denied")
        ):
            with pytest.raises(OSError):
                utils.emit_query(None, str(out), "d.some_ping_v1")

    def test_stdout_mode_writes_sql(self, capsys):
        utils.emit_query("SELECT 1", None, "d.some_ping_v1")
        assert capsys.readouterr().out.strip() == "SELECT 1"

    def test_stdout_mode_marks_empty_query_when_no_probes(self, capsys):
        utils.emit_query(None, None, "d.some_ping_v1")
        captured = capsys.readouterr()
        assert captured.out.strip() == "-- Empty query: no probes found!"
        assert "no probes found" in captured.err


EMPTY_SCHEMA: list = []


class TestGeneratorOutputContract:
    """exit 0 means handled; any non-zero exit means something broke."""

    def setup_method(self):
        utils._glean_metric_metadata_cache.clear()

    def _run(self, module, tmp_path, monkeypatch, schema, out_name="out"):
        out = tmp_path / out_name
        monkeypatch.setattr(module, "get_schema", lambda *a, **k: schema)
        monkeypatch.setattr(module, "get_sampled_metrics", lambda *a, **k: {})
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "prog",
                "--source-table",
                "org_mozilla_fenix_stable.metrics_v1",
                "--product",
                "org_mozilla_fenix",
                "--output-dir",
                str(out),
            ],
        )
        module.main()
        return out

    @pytest.mark.parametrize("module", [histograms, scalars])
    def test_no_probes_leaves_no_directory(self, module, tmp_path, monkeypatch):
        out = self._run(module, tmp_path, monkeypatch, EMPTY_SCHEMA)
        assert not out.exists()

    @pytest.mark.parametrize("module", [histograms, scalars])
    def test_probes_produce_query_file(self, module, tmp_path, monkeypatch):
        monkeypatch.setattr(module, "is_static_labeled_counter", _fake_is_static)
        out = self._run(module, tmp_path, monkeypatch, SCHEMA)
        assert (out / "query.sql").read_text().startswith("-- Query generated by:")

    @pytest.mark.parametrize("module", [histograms, scalars])
    def test_dictionary_failure_propagates(self, module, tmp_path, monkeypatch):
        # The regression: this used to exit 1, indistinguishable from "no probes",
        # so the caller deleted the query and the Airflow task stayed green.
        def boom(*args, **kwargs):
            raise requests.exceptions.ReadTimeout("timed out")

        monkeypatch.setattr(module, "is_static_labeled_counter", boom)
        with pytest.raises(requests.exceptions.ReadTimeout):
            self._run(module, tmp_path, monkeypatch, SCHEMA)
        assert not (tmp_path / "out").exists()
