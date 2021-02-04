import itertools
from datetime import date
from unittest import TestCase
from unittest.mock import ANY, call, MagicMock, patch

from google.cloud.bigquery_datatransfer import enums as transfer_enums

from play_store_export import export


class TestExport(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.default_backfill_max_days = export.BACKFILL_DAYS_MAX

    def tearDown(self):
        export.BACKFILL_DAYS_MAX = self.default_backfill_max_days

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    def test_trigger_backfill_valid_date_range(self, mock_transfer_client):
        export.trigger_backfill(
            start_date=date.fromisoformat("2020-04-04"),
            end_date=date.fromisoformat("2020-04-04"),
            transfer_config_name="",
            client=mock_transfer_client,
        )

        export.trigger_backfill(
            start_date=date.fromisoformat("2020-04-04"),
            end_date=date.fromisoformat("2020-04-05"),
            transfer_config_name="",
            client=mock_transfer_client,
        )

        self.assertRaises(
            ValueError,
            export.trigger_backfill,
            start_date=date.fromisoformat("2020-04-04"),
            end_date=date.fromisoformat("2020-04-03"),
            transfer_config_name="",
            client=mock_transfer_client,
        )

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    def test_trigger_backfill_max_days(self, mock_transfer_client):
        export.trigger_backfill(
            start_date=date.fromisoformat("2020-04-04"),
            end_date=date.fromisoformat("2020-05-04"),
            transfer_config_name="",
            client=mock_transfer_client,
        )

        self.assertRaises(
            ValueError,
            export.trigger_backfill,
            start_date=date.fromisoformat("2020-04-04"),
            end_date=date.fromisoformat("2021-04-04"),
            transfer_config_name="",
            client=mock_transfer_client,
        )

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("google.cloud.bigquery_datatransfer.types")
    def test_trigger_backfill_transfer_run_args(self, mock_types, mock_transfer_client):
        mock_types.Timestamp = dict

        config_name = "config_name_1"
        start_date = date.fromisoformat("2020-04-04")

        export.trigger_backfill(
            start_date=start_date,
            end_date=date.fromisoformat("2020-05-04"),
            transfer_config_name="config_name_1",
            client=mock_transfer_client,
        )

        mock_transfer_client.start_manual_transfer_runs.assert_called_with(
            parent=config_name,
            requested_time_range={
                "start_time": {
                    "seconds": int(export.to_utc_midnight_datetime(start_date).timestamp())
                },
                "end_time": {
                    "seconds": int(export.to_utc_midnight_datetime(
                        date.fromisoformat("2020-05-05")).timestamp())
                },
            },
        )

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("time.sleep")
    def test_wait_for_transfer_success(self, mock_sleep, mock_transfer_client):
        mock_transfer_client.return_value = mock_transfer_client

        mock_transfer_client.get_transfer_run.side_effect = [
            MagicMock(state=transfer_enums.TransferState.PENDING),
            MagicMock(state=transfer_enums.TransferState.RUNNING),
            MagicMock(state=transfer_enums.TransferState.RUNNING),
            MagicMock(state=transfer_enums.TransferState.SUCCEEDED),
            MagicMock(state=transfer_enums.TransferState.SUCCEEDED),
        ]

        result = export.wait_for_transfer("transfer", timeout=10, polling_period=1)

        self.assertEqual(result, transfer_enums.TransferState.SUCCEEDED)
        self.assertEqual(mock_transfer_client.get_transfer_run.call_count, 4)

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("time.sleep")
    def test_wait_for_transfer_timeout_exact(self, mock_sleep, mock_transfer_client):
        mock_transfer_client.return_value = mock_transfer_client

        mock_transfer_client.get_transfer_run.side_effect = itertools.repeat(
            MagicMock(state=transfer_enums.TransferState.RUNNING)
        )

        result = export.wait_for_transfer("transfer", timeout=10, polling_period=1)

        self.assertEqual(result, -1)
        self.assertEqual(mock_transfer_client.get_transfer_run.call_count, 11)

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("time.sleep")
    def test_wait_for_transfer_timeout_inexact(self, mock_sleep, mock_transfer_client):
        mock_transfer_client.return_value = mock_transfer_client

        mock_transfer_client.get_transfer_run.side_effect = itertools.repeat(
            MagicMock(state=transfer_enums.TransferState.RUNNING)
        )

        result = export.wait_for_transfer("transfer", timeout=10, polling_period=3)

        self.assertEqual(result, -1)
        self.assertEqual(mock_transfer_client.get_transfer_run.call_count, 5)

    @classmethod
    def mock_transfer_run(cls, name, schedule_time):
        mock_transfer = MagicMock()
        mock_transfer.name = name
        mock_transfer.schedule_time.seconds = schedule_time
        return mock_transfer

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("play_store_export.export.trigger_backfill")
    @patch("play_store_export.export.wait_for_transfer")
    def test_export_max_days_under(self, mock_wait_for_transfer,
                                   mock_trigger_backfill, mock_transfer_client):
        export.BACKFILL_DAYS_MAX = 5

        mock_trigger_backfill.return_value = [
            self.mock_transfer_run("c", 30),
            self.mock_transfer_run("d", 40),
            self.mock_transfer_run("b", 20),
            self.mock_transfer_run("a", 10),
        ]
        mock_wait_for_transfer.return_value = transfer_enums.TransferState.SUCCEEDED

        export.start_export("project", "config", "us",
                            base_date=date(2020, 5, 5), backfill_day_count=4)

        mock_trigger_backfill.assert_called_once_with(date(2020, 5, 2), date(2020, 5, 5), ANY, ANY)
        mock_wait_for_transfer.assert_has_calls([call("a"), call("b"), call("c"), call("d")])

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("play_store_export.export.trigger_backfill")
    @patch("play_store_export.export.wait_for_transfer")
    def test_export_max_days_equal(self, mock_wait_for_transfer,
                                   mock_trigger_backfill, mock_transfer_client):
        export.BACKFILL_DAYS_MAX = 3

        mock_trigger_backfill.return_value = [
            self.mock_transfer_run("d", 40),
            self.mock_transfer_run("b", 20),
            self.mock_transfer_run("a", 10),
        ]
        mock_wait_for_transfer.return_value = transfer_enums.TransferState.SUCCEEDED

        export.start_export("project", "config", "us",
                            base_date=date(2020, 5, 5), backfill_day_count=3)

        mock_trigger_backfill.assert_called_once_with(date(2020, 5, 3), date(2020, 5, 5), ANY, ANY)
        mock_wait_for_transfer.assert_has_calls([call("a"), call("b"), call("d")])

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("play_store_export.export.trigger_backfill")
    @patch("play_store_export.export.wait_for_transfer")
    def test_export_max_days_over(self, mock_wait_for_transfer,
                                  mock_trigger_backfill, mock_transfer_client):
        export.BACKFILL_DAYS_MAX = 2

        mock_trigger_backfill.side_effect = [
            [
                self.mock_transfer_run("a", 10),
                self.mock_transfer_run("b", 20),
            ],
            [
                self.mock_transfer_run("d", 40),
                self.mock_transfer_run("c", 30),
            ],
            [
                self.mock_transfer_run("e", 50),
            ],
        ]
        mock_wait_for_transfer.return_value = transfer_enums.TransferState.SUCCEEDED

        export.start_export("project", "config", "us",
                            base_date=date(2020, 5, 5), backfill_day_count=5)

        mock_trigger_backfill.assert_has_calls([
            call(date(2020, 5, 4), date(2020, 5, 5), ANY, ANY),
            call(date(2020, 5, 2), date(2020, 5, 3), ANY, ANY),
            call(date(2020, 5, 1), date(2020, 5, 1), ANY, ANY),
        ])
        mock_wait_for_transfer.assert_has_calls(
            [call("a"), call("b"), call("c"), call("d"), call("e")]
        )

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    @patch("play_store_export.export.trigger_backfill")
    @patch("play_store_export.export.wait_for_transfer")
    def test_export_failed_transfer(self, mock_wait_for_transfer,
                                    mock_trigger_backfill, mock_transfer_client):
        mock_trigger_backfill.return_value = [
            self.mock_transfer_run("a", 10),
        ]
        mock_wait_for_transfer.return_value = transfer_enums.TransferState.FAILED

        self.assertRaises(export.DataTransferException, export.start_export,
                          "project", "config", "us",
                          base_date=date(2020, 5, 5), backfill_day_count=1)
        mock_trigger_backfill.assert_called_once_with(date(2020, 5, 5), date(2020, 5, 5), ANY, ANY)
        mock_wait_for_transfer.assert_has_calls([call("a")])
