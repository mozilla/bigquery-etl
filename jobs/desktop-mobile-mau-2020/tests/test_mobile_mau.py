from unittest import TestCase
from unittest.mock import patch

from click.testing import CliRunner
from mobile_mau import mobile_mau


class TestCreateNewJob(TestCase):
    @patch("mobile_mau.mobile_mau.create_table_and_plot")
    @patch("mobile_mau.mobile_mau.extract_mobile_product_mau")
    @patch("mobile_mau.mobile_mau.upload_files")
    @patch("google.cloud.storage.Client")
    @patch("pathlib.Path.write_text")
    def test_template_paths(
        self,
        mock_write_text,
        mock_storage,
        mock_upload_files,
        mock_extract_mau,
        mock_create_table,
    ):
        runner = CliRunner()

        mock_create_table.return_value = ("", "")

        runner.invoke(mobile_mau.main, ["--project", "abc"], catch_exceptions=False)

        self.assertEqual(mock_write_text.call_count, 1)
        self.assertEqual(mock_upload_files.call_count, 0)
        self.assertEqual(mock_storage.call_count, 0)
