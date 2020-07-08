from bigquery_etl.query_scheduling.utils import schedule_interval_delta


class TestUtils:
    def test_schedule_interval_delta(self):
        assert schedule_interval_delta("0 1 * * *", "0 2 * * *") == "3600s"
        assert schedule_interval_delta("0 2 * * *", "0 0 * * *") == "-7200s"
        assert schedule_interval_delta("* * * * *", "* * * * *") == "0s"
        assert schedule_interval_delta("0 1 * * *", "0 1 * * *") == "0s"
        assert schedule_interval_delta("daily", "0 1 * * *") == "3600s"
        assert schedule_interval_delta("daily", "0 0 * * *") == "0s"
