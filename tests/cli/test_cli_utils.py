from pathlib import Path

import pytest
from click.exceptions import BadParameter

from bigquery_etl.cli.utils import (
    is_authenticated,
    is_valid_dir,
    is_valid_file,
    is_valid_project,
    table_matches_patterns,
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
