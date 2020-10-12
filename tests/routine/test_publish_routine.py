from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

from bigquery_etl.routine import parse_routine, publish_routines

TEST_DIR = Path(__file__).parent.parent


class TestPublishRoutine:
    udf_dir = TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project" / "udf"

    @mock.patch("google.cloud.bigquery.Client")
    def test_publish_routine_with_description(self, mock_client):
        raw_routine = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_shift_28_bits_one_day" / "udf.sql"
        )
        mock_client.query = MagicMock()
        publish_routines.publish_routine(
            raw_routine, mock_client, "test-project", "", "", [], False
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
        raw_routine = parse_routine.RawRoutine.from_file(
            self.udf_dir / "test_js_udf" / "udf.sql"
        )
        mock_client.query = MagicMock()
        publish_routines.publish_routine(
            raw_routine, mock_client, "test-project", "", "", [], False
        )
        query = (
            "CREATE OR REPLACE FUNCTION udf.test_js_udf(input BYTES)\nRETURNS "
            + "STRING DETERMINISTIC\nLANGUAGE js\nAS\n "
            + ' """return 1;"""\nOPTIONS(description="Some description",'
            + 'library = "gs:///script.js");'
        )
        mock_client.query.assert_called_with(query)
