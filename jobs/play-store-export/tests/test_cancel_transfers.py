from unittest import TestCase
from unittest.mock import call, MagicMock, patch

from play_store_export.cancel_transfers import cancel_active_transfers


class TestCancelTransfers(TestCase):
    @classmethod
    def mock_transfer_run(cls, name):
        transfer_run = MagicMock()
        transfer_run.name = name
        return transfer_run

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    def test_no_results(self, mock_transfer_client):
        mock_iterator = MagicMock(pages=[[]])

        mock_transfer_client.return_value = mock_transfer_client
        mock_transfer_client.list_transfer_runs.return_value = mock_iterator

        cancel_active_transfers("project", "config", "us")

        mock_transfer_client.delete_transfer_run.assert_not_called()

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    def test_no_pages(self, mock_transfer_client):
        mock_iterator = MagicMock(pages=[])

        mock_transfer_client.return_value = mock_transfer_client
        mock_transfer_client.list_transfer_runs.return_value = mock_iterator

        cancel_active_transfers("project", "config", "us")

        mock_transfer_client.delete_transfer_run.assert_not_called()

    @patch("google.cloud.bigquery_datatransfer.DataTransferServiceClient")
    def test_transfer_list_pagination(self, mock_transfer_client):
        mock_iterator = MagicMock(pages=[
            [self.mock_transfer_run("a"), self.mock_transfer_run("b")],
            [self.mock_transfer_run("c")],
            [self.mock_transfer_run("d")],
        ])

        mock_transfer_client.return_value = mock_transfer_client
        mock_transfer_client.location_transfer_config_path.return_value = "config_name"
        mock_transfer_client.list_transfer_runs.return_value = mock_iterator

        cancel_active_transfers("project", "config", "us")

        mock_transfer_client.delete_transfer_run.assert_has_calls(
            [call("a"), call("b"), call("c"), call("d")],
            any_order=True,
        )
        self.assertEqual(mock_transfer_client.delete_transfer_run.call_count, 4)
