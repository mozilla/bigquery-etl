from unittest import TestCase
from unittest.mock import patch

from click.testing import CliRunner
from desktop_mau import desktop_mau_dau


class TestCreateNewJob(TestCase):
    @patch("desktop_mau.desktop_mau_dau.generate_plots")
    @patch("desktop_mau.desktop_mau_dau.upload_files")
    @patch("google.cloud.storage.Client")
    def test_no_bucket_no_upload(
        self, mock_storage, mock_upload_files, mock_generate_plots
    ):
        runner = CliRunner()
        runner.invoke(
            desktop_mau_dau.main,
            ["--project", "abc"],
            catch_exceptions=False,
        )

        self.assertEqual(mock_upload_files.call_count, 0)
        self.assertEqual(mock_storage.call_count, 0)
