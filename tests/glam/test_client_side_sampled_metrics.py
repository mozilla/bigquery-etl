"""Tests for bigquery_etl.glam.client_side_sampled_metrics."""

from unittest import mock

import pytest

from bigquery_etl.glam import client_side_sampled_metrics


class TestGet:
    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_groups_rows_by_metric_type(self, mock_run):
        mock_run.return_value = [
            {
                "metric_type": "timing_distribution",
                "metric_name": "paint_build_displaylist_time",
                "sample_rate": 0.1,
            },
            {
                "metric_type": "timing_distribution",
                "metric_name": "wr_rasterize_glyphs_time",
                "sample_rate": 0.1,
            },
            {
                "metric_type": "memory_distribution",
                "metric_name": "fog_ipc_buffer_sizes",
                "sample_rate": 0.1,
            },
        ]
        result = client_side_sampled_metrics.get(product="firefox_desktop")
        assert result == {
            "timing_distribution": [
                "paint_build_displaylist_time",
                "wr_rasterize_glyphs_time",
            ],
            "memory_distribution": ["fog_ipc_buffer_sizes"],
        }

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_unexpected_sample_rate_raises(self, mock_run):
        mock_run.return_value = [
            {
                "metric_type": "timing_distribution",
                "metric_name": "paint_build_displaylist_time",
                "sample_rate": 0.5,
            },
        ]
        with pytest.raises(RuntimeError, match="paint_build_displaylist_time"):
            client_side_sampled_metrics.get(product="firefox_desktop")

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_tombstones_are_filtered_at_sql_layer(self, mock_run):
        mock_run.return_value = []
        client_side_sampled_metrics.get(product="firefox_desktop")
        rendered_query = mock_run.call_args.args[0]
        assert "experimenter_slug IS NOT NULL" in rendered_query

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_partition_matches_upstream_state_key(self, mock_run):
        """Mirror the producer's (metric_type, metric_name, channel, app_name, os)
        state key so the latest-per-group invariant holds even if a caller
        skips `product` and the app/channel filters aren't applied."""
        mock_run.return_value = []
        client_side_sampled_metrics.get(product="firefox_desktop")
        rendered_query = mock_run.call_args.args[0]
        assert (
            "PARTITION BY metric_type, metric_name, channel, app_name, os"
            in rendered_query
        )

    def test_empty_metric_types_short_circuits(self):
        assert client_side_sampled_metrics.get(metric_types=[]) == {}

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_product_filter_in_query(self, mock_run):
        """firefox_desktop maps to app_name=firefox_desktop in the WHERE."""
        mock_run.return_value = []
        client_side_sampled_metrics.get(product="firefox_desktop")
        rendered_query = mock_run.call_args.args[0]
        assert "app_name = 'firefox_desktop'" in rendered_query

    @pytest.mark.parametrize(
        "product",
        [
            "org_mozilla_fenix",
            "org_mozilla_fenix_nightly",
            "org_mozilla_firefox",
            "org_mozilla_firefox_beta",
            "org_mozilla_fennec_aurora",
        ],
    )
    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_fenix_variants_translate_to_fenix_app_name(self, mock_run, product):
        """All Fenix variants share Experimenter's appName 'fenix'."""
        mock_run.return_value = []
        client_side_sampled_metrics.get(product=product)
        rendered_query = mock_run.call_args.args[0]
        assert "app_name = 'fenix'" in rendered_query

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_metric_types_and_product_combined(self, mock_run):
        mock_run.return_value = []
        client_side_sampled_metrics.get(
            metric_types=["counter"], product="org_mozilla_fenix"
        )
        rendered_query = mock_run.call_args.args[0]
        assert "metric_type IN ('counter')" in rendered_query
        assert "app_name = 'fenix'" in rendered_query
        assert " AND " in rendered_query

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_unknown_product_raises(self, mock_run):
        """Typos / new products surface as ValueError instead of silently
        returning no sampled metrics (which would emit them as unsampled)."""
        with pytest.raises(ValueError, match="firefox-desktop"):
            client_side_sampled_metrics.get(product="firefox-desktop")
        mock_run.assert_not_called()

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_no_product_omits_app_filter(self, mock_run):
        mock_run.return_value = []
        client_side_sampled_metrics.get(metric_types=["counter"])
        rendered_query = mock_run.call_args.args[0]
        assert "app_name =" not in rendered_query
