import datetime
from unittest.mock import ANY, MagicMock

import pytest
from google.cloud import bigquery, monitoring
from google.cloud.exceptions import NotFound

from bigquery_etl.monitoring import export_metrics


class TestExportMetrics:
    @pytest.fixture
    def test_table(self):
        return bigquery.TableReference(
            bigquery.DatasetReference("project", "dataset"), "table"
        )

    # Obfuscated real data
    @pytest.fixture
    def test_data(self):
        return [
            monitoring.TimeSeries(
                {
                    "metric": {
                        "type": "pubsub.googleapis.com/subscription/oldest_unacked_message_age",  # noqa E502
                        "labels": {"test_key": "test_value"},
                    },
                    "resource": {
                        "type": "pubsub_subscription",
                        "labels": {
                            "project_id": "test-project-1",
                            "subscription_id": "subscription",
                        },
                    },
                    "metric_kind": "GAUGE",
                    "value_type": "DOUBLE",
                    "points": [
                        {
                            "interval": {
                                "start_time": {"seconds": 1609512900},
                                "end_time": {"seconds": 1609512900},
                            },
                            "value": {"double_value": 2.0},
                        },
                        {
                            "interval": {
                                "start_time": {"seconds": 1609512600},
                                "end_time": {"seconds": 1609512600},
                            },
                            "value": {"double_value": 1.0},
                        },
                    ],
                }
            ),
            monitoring.TimeSeries(
                {
                    "metric": {
                        "type": "pubsub.googleapis.com/subscription/oldest_unacked_message_age",  # noqa E502
                    },
                    "resource": {
                        "type": "pubsub_subscription",
                        "labels": {
                            "project_id": "test-project-2",
                            "subscription_id": "subscription",
                        },
                    },
                    "metric_kind": "GAUGE",
                    "value_type": "DOUBLE",
                    "points": [
                        {
                            "interval": {
                                "start_time": {"seconds": 1609513200},
                                "end_time": {"seconds": 1609513200},
                            },
                            "value": {"double_value": 4.0},
                        },
                        {
                            "interval": {
                                "start_time": {"seconds": 1609512900},
                                "end_time": {"seconds": 1609512900},
                            },
                            "value": {"double_value": 3.0},
                        },
                    ],
                }
            ),
        ]

    def test_get_time_series_parsing(self, test_data):
        """Get time series should correctly transform TimeSeries objects to dict."""
        mock_client = MagicMock()
        mock_client.list_time_series.return_value = test_data
        results = export_metrics.get_time_series(
            mock_client,
            "project-1",
            "",
            monitoring.Aggregation(),
            datetime.datetime(2021, 1, 1),
            datetime.datetime(2020, 1, 2),
        )
        expected = [
            {
                "timestamp": datetime.datetime(
                    2021, 1, 1, 14, 50, tzinfo=datetime.timezone.utc
                ),
                "value": 1,
                "project_id": "test-project-1",
                "subscription_id": "subscription",
                "test_key": "test_value",
            },
            {
                "timestamp": datetime.datetime(
                    2021, 1, 1, 14, 55, tzinfo=datetime.timezone.utc
                ),
                "value": 2,
                "project_id": "test-project-1",
                "subscription_id": "subscription",
                "test_key": "test_value",
            },
            {
                "timestamp": datetime.datetime(
                    2021, 1, 1, 14, 55, tzinfo=datetime.timezone.utc
                ),
                "value": 3,
                "project_id": "test-project-2",
                "subscription_id": "subscription",
            },
            {
                "timestamp": datetime.datetime(
                    2021, 1, 1, 15, 0, tzinfo=datetime.timezone.utc
                ),
                "value": 4,
                "project_id": "test-project-2",
                "subscription_id": "subscription",
            },
        ]
        assert sorted(results, key=lambda x: (x["timestamp"], x["value"])) == expected

    def test_write_to_bq_overwrite_config(self, test_table):
        """Write to bq should use correct config when overwriting data."""
        mock_client = MagicMock()
        export_metrics.write_to_bq(mock_client, test_table, [], overwrite=True)

        job_config = mock_client.load_table_from_json.call_args.kwargs["job_config"]

        assert (
            job_config.create_disposition == bigquery.CreateDisposition.CREATE_IF_NEEDED
        )
        assert job_config.schema_update_options is None
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE

    def test_write_to_bq_append_config(self, test_table):
        """Write to bq should use correct config when appending data."""
        mock_client = MagicMock()
        export_metrics.write_to_bq(mock_client, test_table, [], overwrite=False)

        job_config = mock_client.load_table_from_json.call_args.kwargs["job_config"]

        assert (
            job_config.create_disposition
        ), bigquery.CreateDisposition.CREATE_IF_NEEDED
        assert (
            job_config.schema_update_options
        ), bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        assert job_config.write_disposition, bigquery.WriteDisposition.WRITE_APPEND

    def test_write_to_bq_stringify_timestamp(self):
        """Write to bq should convert timestamp values to strings."""
        mock_client = MagicMock()

        data_points = [
            {
                "timestamp": datetime.datetime(
                    2021, 1, 1, 14, 50, tzinfo=datetime.timezone.utc
                ),
                "value": 1,
            },
            {
                "timestamp": datetime.datetime(
                    2021, 1, 1, 14, 55, tzinfo=datetime.timezone.utc
                ),
                "value": 2,
            },
        ]
        expected = [
            {
                "timestamp": "2021-01-01 14:50:00+00:00",
                "value": 1,
            },
            {
                "timestamp": "2021-01-01 14:55:00+00:00",
                "value": 2,
            },
        ]

        export_metrics.write_to_bq(
            mock_client,
            bigquery.TableReference(
                bigquery.DatasetReference("project", "dataset"), "table"
            ),
            data_points,
            overwrite=False,
        )

        mock_client.load_table_from_json.assert_called_once_with(
            expected,
            ANY,
            job_config=ANY,
        )
        with pytest.raises(AssertionError):
            mock_client.load_table_from_json.assert_called_once_with(
                data_points,
                ANY,
                job_config=ANY,
            )

    def test_filter_existing_data_table_not_found(self, test_table):
        """Filter should return all data when target table is not found."""
        mock_client = MagicMock()
        mock_query = MagicMock()
        mock_query.result.side_effect = NotFound("")
        mock_client.query.return_value = mock_query

        data_points = [
            {"timestamp": datetime.datetime(2021, 1, 1, 1), "value": 1},
            {"timestamp": datetime.datetime(2021, 1, 1, 2), "value": 2},
            {"timestamp": datetime.datetime(2021, 1, 1, 3), "value": 3},
        ]
        results = export_metrics.filter_existing_data(
            data_points,
            mock_client,
            test_table,
            start_time=datetime.datetime(2021, 1, 1, 0),
            end_time=datetime.datetime(2021, 1, 1, 10),
        )
        assert results == data_points

    def test_filter_existing_data_no_existing(self, test_table):
        """Filter should return all data when there is no existing data."""
        mock_client = MagicMock()
        mock_query = MagicMock()
        mock_query.result.return_value = []
        mock_client.query.return_value = mock_query

        data_points = [
            {"timestamp": datetime.datetime(2021, 1, 1, 1), "value": 1},
            {"timestamp": datetime.datetime(2021, 1, 1, 2), "value": 2},
            {"timestamp": datetime.datetime(2021, 1, 1, 3), "value": 3},
        ]
        results = export_metrics.filter_existing_data(
            data_points,
            mock_client,
            test_table,
            start_time=datetime.datetime(2021, 1, 1, 0),
            end_time=datetime.datetime(2021, 1, 1, 10),
        )
        assert results == data_points

    def test_filter_existing_data_some_existing(self, test_table):
        """Filter should remove data points that have a matching timestamp."""

        def mock_row(timestamp):
            row = MagicMock()
            row.timestamp = timestamp
            return row

        mock_client = MagicMock()
        mock_query = MagicMock()
        mock_query.result.return_value = [
            mock_row(datetime.datetime(2021, 1, 1, 2)),
            mock_row(datetime.datetime(2021, 1, 1, 3)),
            mock_row(datetime.datetime(2021, 1, 1, 4)),
        ]
        mock_client.query.return_value = mock_query

        data_points = [
            {"timestamp": datetime.datetime(2021, 1, 1, 1), "value": 1},
            {"timestamp": datetime.datetime(2021, 1, 1, 2), "value": 2},
            {"timestamp": datetime.datetime(2021, 1, 1, 3), "value": 3},
        ]
        expected = [
            {"timestamp": datetime.datetime(2021, 1, 1, 1), "value": 1},
        ]

        results = export_metrics.filter_existing_data(
            data_points,
            mock_client,
            test_table,
            start_time=datetime.datetime(2021, 1, 1, 0),
            end_time=datetime.datetime(2021, 1, 1, 10),
        )
        assert results == expected
