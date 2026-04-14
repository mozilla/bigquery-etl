from datetime import timedelta

import click
import pytest

from bigquery_etl.cli.target import _parse_duration


class TestParseDuration:
    def test_days(self):
        assert _parse_duration("7d") == timedelta(days=7)

    def test_hours(self):
        assert _parse_duration("24h") == timedelta(hours=24)

    def test_weeks(self):
        assert _parse_duration("2w") == timedelta(weeks=2)

    def test_minutes(self):
        assert _parse_duration("30m") == timedelta(minutes=30)

    def test_seconds(self):
        assert _parse_duration("30s") == timedelta(seconds=30)

    def test_compound(self):
        assert _parse_duration("1d12h") == timedelta(days=1, hours=12)

    def test_invalid_format(self):
        with pytest.raises(click.BadParameter):
            _parse_duration("abc")
