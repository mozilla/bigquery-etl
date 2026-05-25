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
    def test_partitions_by_os_so_stale_tombstones_dont_mask_newer_os(self, mock_run):
        """A stale os=NULL tombstone must not outrank a newer os=Windows
        row for the same metric. Adding `os` to PARTITION BY keeps the
        two in separate partitions, so the tombstone only dominates its
        own (os=NULL) partition and gets dropped by the slug-IS-NOT-NULL
        filter — leaving the os=Windows row intact in the output.
        """
        mock_run.return_value = []
        client_side_sampled_metrics.get(product="firefox_desktop")
        rendered_query = mock_run.call_args.args[0]
        assert "PARTITION BY metric_type, metric_name, os" in rendered_query

    def test_empty_metric_types_short_circuits(self):
        assert client_side_sampled_metrics.get(metric_types=[]) == {}

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_product_filter_in_query(self, mock_run):
        """firefox_desktop maps to app_name=firefox_desktop in the WHERE."""
        mock_run.return_value = []
        client_side_sampled_metrics.get(product="firefox_desktop")
        rendered_query = mock_run.call_args.args[0]
        assert "app_name = 'firefox_desktop'" in rendered_query

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_fenix_product_translates_to_fenix_app_name(self, mock_run):
        """org_mozilla_fenix maps to Experimenter's appName 'fenix'."""
        mock_run.return_value = []
        client_side_sampled_metrics.get(product="org_mozilla_fenix")
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
    def test_unknown_product_returns_empty_without_querying(self, mock_run):
        """Unmapped products short-circuit so we don't read another app's rows."""
        result = client_side_sampled_metrics.get(product="org_mozilla_fennec_aurora")
        assert result == {}
        mock_run.assert_not_called()

    @mock.patch.object(client_side_sampled_metrics, "_run_bq_query")
    def test_no_product_omits_app_filter(self, mock_run):
        mock_run.return_value = []
        client_side_sampled_metrics.get(metric_types=["counter"])
        rendered_query = mock_run.call_args.args[0]
        assert "app_name" not in rendered_query
