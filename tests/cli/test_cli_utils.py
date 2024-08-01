from pathlib import Path

import pytest
from click.exceptions import BadParameter

from bigquery_etl.cli.utils import (
    is_authenticated,
    is_valid_dir,
    is_valid_file,
    is_valid_project,
    table_matches_patterns, extract_last_group_by_from_query,
)

TEST_DIR = Path(__file__).parent.parent


class TestUtils:
    def test_is_valid_dir(self):
        with pytest.raises(BadParameter):
            assert is_valid_dir(None, None, "invalid")
        with pytest.raises(BadParameter):
            assert is_valid_dir(None, None, str(TEST_DIR / "data" / "dags.yaml"))
        assert is_valid_dir(None, None, str(TEST_DIR)) == str(TEST_DIR)

    def test_is_valid_file(self):
        with pytest.raises(BadParameter):
            assert is_valid_file(None, None, "invalid")
        with pytest.raises(BadParameter):
            assert is_valid_file(None, None, str(TEST_DIR))
        assert is_valid_file(None, None, str(TEST_DIR / "data" / "dags.yaml")) == str(
            TEST_DIR / "data" / "dags.yaml"
        )

    @pytest.mark.integration
    def test_is_authenticated(self):
        assert is_authenticated()

    def test_is_valid_project(self):
        assert is_valid_project(None, None, "mozfun")
        assert is_valid_project(None, None, "moz-fx-data-shared-prod")
        assert is_valid_project(None, None, "moz-fx-data-backfill-1")
        with pytest.raises(BadParameter):
            assert is_valid_project(None, None, "not-existing")

    def test_table_matches_patterns(self):
        assert not table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern=["telemetry_live.main_v4", "telemetry_live.event_v4"],
            invert=True,
        )
        assert not table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern="telemetry_live.main_v4",
            invert=True,
        )

        assert table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern=["telemetry_live.first_shutdown_v4", "telemetry_live.event_v4"],
            invert=True,
        )
        assert table_matches_patterns(
            table="telemetry_live.main_v4",
            pattern="telemetry_live.event_v4",
            invert=True,
        )

    def test_extract_last_group_by_from_query(self):
        assert extract_last_group_by_from_query(query='SELECT column_1 FROM test_table GROUP BY ALL') == (['ALL'])
        assert extract_last_group_by_from_query(query=
                                                '''SELECT column_1, SUM(metric_1) AS metric_1 FROM test_table
                                                 GROUP BY 1;''') == (['1'])
        assert extract_last_group_by_from_query(query='SELECT column_1 FROM test_table GROUP BY 1, 2, 3') == (
            ['1', '2', '3'])
        assert extract_last_group_by_from_query(query='SELECT column_1 FROM test_table GROUP BY 1, 2, 3') == (
        ['1', '2', '3'])
        assert extract_last_group_by_from_query(query=
                                                '''SELECT column_1, column_2 FROM test_table GROUP BY column_1, column_2
                                                 ORDER BY 1 LIMIT 100''') == (['column_1', 'column_2'])
        assert extract_last_group_by_from_query(query='SELECT column_1 FROM test_table') == ([])
        assert extract_last_group_by_from_query(query='SELECT column_1 FROM test_table;') == ([])
        assert extract_last_group_by_from_query(query='SELECT column_1 FROM test_table GROUP BY column_1') == ['column_1']
        assert extract_last_group_by_from_query(query='SELECT column_1, column_2 FROM test_table GROUP BY (column_1, column_2)') == [
            'column_1', 'column_2']
        assert extract_last_group_by_from_query(query =
                                                '''WITH cte AS (
                                                SELECT column_1 FROM test_table GROUP BY column_1
                                                )
                                                SELECT column_1 FROM cte
                                                ''') == ['column_1']
        assert extract_last_group_by_from_query(query =
                                                '''WITH cte AS (
                                                SELECT column_1 FROM test_table GROUP BY column_1
                                                ),
                                                cte2 AS (
                                                SELECT column_1, column2 FROM test_table GROUP BY column_1, column2
                                                )
                                                SELECT column_1 FROM cte2
                                                GROUP BY column_1
                                                ORDER BY 1 DESC LIMIT 1;
                                                ''') == ['column_1']
        assert extract_last_group_by_from_query(query=
                                                '''WITH cte1 AS (
                                                SELECT column_1, column3 FROM test_table GROUP BY column_1, column3
                                                ),
                                                cte3 AS (
                                                SELECT column_1, column3 FROM cte1 GROUP BY column_3
                                                )
                                                SELECT column_1 FROM cte3 LIMIT 2;
                                                ''') == ['column_3']
        assert extract_last_group_by_from_query(query =
                                            '''WITH cte1 AS (
                                            SELECT column_1 FROM test_table GROUP BY column_1
                                            ),
                                            cte2 AS (
                                            SELECT column_2 FROM test_table GROUP BY column_2
                                            ),
                                            cte3 AS (
                                            SELECT column_1 FROM cte1
                                            UNION ALL
                                            SELECT column2 FROM cte2
                                            )
                                            SELECT * FROM cte3
                                            ''') == ['column_2']

        assert extract_last_group_by_from_query(query=
                                            '''WITH cte1 AS (
                                            SELECT column_1 FROM test_table GROUP BY column_1
                                            ),
                                            cte2 AS (
                                            SELECT column_1 FROM test_table GROUP BY column_2
                                            )
                                            SELECT * FROM cte2;
                                            ''') == ['column_2']
