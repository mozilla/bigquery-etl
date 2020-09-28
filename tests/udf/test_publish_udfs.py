from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

from bigquery_etl.udf import parse_udf, publish_udfs

TEST_DIR = Path(__file__).parent.parent


class TestPublishUdfs:
    @mock.patch("google.cloud.bigquery.Client")
    def test_publish_udf_with_description(self, mock_client):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udf = parse_udf.RawUdf.from_file(
            udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        mock_client.query = MagicMock()
        publish_udfs.publish_udf(
            raw_udf, mock_client, "test-project", "", "", [], False
        )
        query = (
            "CREATE OR REPLACE FUNCTION udf.test_shift_28_bits_one_day(x INT64)"
            + " AS (\n  IFNULL((x << 1) & udf.test_bitmask_lowest_28(), 0)\n)"
            + 'OPTIONS(description="Shift input bits one day left and drop any bits'
            + ' beyond 28 days.");'
        )
        mock_client.query.assert_called_with(query)

    @mock.patch("google.cloud.bigquery.Client")
    def test_publish_js_udf_with_description(self, mock_client):
        udf_dir = TEST_DIR / "data" / "udf"
        raw_udf = parse_udf.RawUdf.from_file(udf_dir / "test_js_udf" / "udf.sql")
        mock_client.query = MagicMock()
        publish_udfs.publish_udf(
            raw_udf, mock_client, "test-project", "", "", [], False
        )
        query = (
            "CREATE OR REPLACE FUNCTION udf.test_js_udf(input BYTES)\nRETURNS "
            + "STRING DETERMINISTIC\nLANGUAGE js\nAS\n "
            + ' """return 1;"""\nOPTIONS(description="Some description",'
            + 'library = "gs:///script.js");'
        )
        mock_client.query.assert_called_with(query)

    def test_get_udf_dirs(self):
        udf_dirs = publish_udfs.get_udf_dirs(("data/udf",), "non-existing")
        assert udf_dirs == []

        udf_dirs = publish_udfs.get_udf_dirs(("data/udf",), "tests")
        assert "tests/data/udf" in udf_dirs

    def test_get_udf_dirs_non_mozfun(self):
        udf_dirs = publish_udfs.get_udf_dirs(("udf",), None)
        assert "moz-fx-data-shared-prod/udf" in udf_dirs

    def test_get_udf_dirs_mozfun(self):
        udf_dirs = publish_udfs.get_udf_dirs(("mozfun",), "mozfun")
        assert "mozfun" in udf_dirs

        udf_dirs = publish_udfs.get_udf_dirs(("mozfun",), None)
        assert udf_dirs == []
