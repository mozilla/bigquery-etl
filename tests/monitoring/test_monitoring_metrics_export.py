import datetime
from unittest.mock import MagicMock, patch

from google.cloud import monitoring

from bigquery_etl.monitoring import export_metrics

# Obfuscated real data
TEST_DATA = [
    monitoring.TimeSeries({
        "metric": {
            "type": "pubsub.googleapis.com/subscription/oldest_unacked_message_age",
            "labels": [
                {
                    "key": "test",
                    "value": "test"
                }
            ]
        },
        "resource": {
            "type": "pubsub_subscription",
            "labels": [
                {
                    "key": "project_id",
                    "value": "test-project-1"
                },
                {
                    "key": "subscription_id",
                    "value": "subscription"
                }
            ]
        },
        "metric_kind": "GAUGE",
        "value_type": "DOUBLE",
        "points": [
            {
                "interval": {
                    "start_time": {
                        "seconds": 1609513200
                    },
                    "end_time": {
                        "seconds": 1609513200
                    },
                },
                "value": {
                    "double_value": 1.0
                }
            },
            {
                "interval": {
                    "start_time": {
                        "seconds": 1609512900
                    },
                    "end_time": {
                        "seconds": 1609512900
                    },
                },
                "value": {
                    "double_value": 2.0
                }
            },
            {
                "interval": {
                    "start_time": {
                        "seconds": 1609512600
                    },
                    "end_time": {
                        "seconds": 1609512600
                    },
                },
                "value": {
                    "double_value": 3.0
                }
            },
        ]
    }),
    monitoring.TimeSeries({
        "metric": {
            "type": "pubsub.googleapis.com/subscription/oldest_unacked_message_age"
        },
        "resource": {
            "type": "pubsub_subscription",
            "labels": [
                {
                    "key": "project_id",
                    "value": "test-project-2"
                },
                {
                    "key": "subscription_id",
                    "value": "subscription"
                }
            ]
        },
        "metric_kind": "GAUGE",
        "value_type": "DOUBLE",
        "points": [
            {
                "interval": {
                    "start_time": {
                        "seconds": 1609513500
                    },
                    "end_time": {
                        "seconds": 1609513500
                    },
                },
                "value": {
                    "double_value": 6.0
                }
            },
            {
                "interval": {
                    "start_time": {
                        "seconds": 1609513200
                    },
                    "end_time": {
                        "seconds": 1609513200
                    },
                },
                "value": {
                    "double_value": 5.0
                }
            },
            {
                "interval": {
                    "start_time": {
                        "seconds": 1609512900
                    },
                    "end_time": {
                        "seconds": 1609512900
                    },
                },
                "value": {
                    "double_value": 4.0
                }
            },
        ]
    }),
]


class TestExportMetrics:
    def test_get_time_series_parsing(self):
        mock_client = MagicMock()
        mock_client.list_time_series.return_value = TEST_DATA
        results = export_metrics.get_time_series(
            mock_client,
            "project-1",
            "",
            monitoring.Aggregation(),
            datetime.datetime(2021, 1, 1),
            datetime.datetime(2020, 1, 2)
        )
        pass



