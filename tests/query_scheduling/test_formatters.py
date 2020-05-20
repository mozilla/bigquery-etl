from bigquery_etl.query_scheduling.formatters import format_schedule_interval


class TestFormatters:
    def test_format_schedule_interval(self):
        assert "@daily" == format_schedule_interval("daily")
        assert "@once" == format_schedule_interval("once")
        assert "* 1 * * *" == format_schedule_interval("* 1 * * *")
        assert "foo bar" == format_schedule_interval("foo bar")
