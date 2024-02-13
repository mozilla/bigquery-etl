from datetime import datetime

from pandas import to_datetime
from kpi_forecasting.metric_hub import MetricHub
from kpi_forecasting.utils import previous_period_last_date


def test_metrichub_for_dau_kpi():
    test_metric_hub = MetricHub(
        app_name="multi_product",
        slug="mobile_daily_active_users_v1",
        start_date="2024-01-01",
    )
    now = to_datetime(datetime.utcnow()).date()

    query = test_metric_hub.query()
    query_where = f"WHERE submission_date BETWEEN '2024-01-01' AND '{now}'\n GROUP BY"

    assert query_where in query
    assert "\n    AND" not in query


def test_metrichub_with_where():
    test_metric_hub = MetricHub(
        app_name="multi_product",
        slug="mobile_daily_active_users_v1",
        start_date="2024-01-01",
        where="test_condition = condition",
    )

    query = test_metric_hub.query()
    assert f"\n    AND {test_metric_hub.where}" in query


def test_metrichub_with_segments():
    test_metric_hub = MetricHub(
        app_name="multi_product",
        slug="mobile_daily_active_users_v1",
        start_date="2024-01-01",
        segments={"test_segment1": "segment1", "test_segment2": "segment2"},
    )

    query = test_metric_hub.query()
    assert "segment1 AS test_segment1,\n     segment2 AS test_segment2" in query


def test_metrichub_with_segments_and_where():
    test_metric_hub = MetricHub(
        app_name="multi_product",
        slug="mobile_daily_active_users_v1",
        start_date="2024-01-01",
        where="test_condition = condition",
        segments={"test_segment1": "segment1", "test_segment2": "segment2"},
    )

    query = test_metric_hub.query()
    assert f"\n    AND {test_metric_hub.where}" in query
    assert "segment1 AS test_segment1,\n     segment2 AS test_segment2" in query


def test_metrichub_no_end_date():
    test_metric_hub = MetricHub(
        app_name="multi_product",
        slug="mobile_daily_active_users_v1",
        start_date="2024-01-01",
    )
    now = to_datetime(datetime.utcnow()).date()

    assert test_metric_hub.end_date == now


def test_metrichub_last_complete_month():
    test_metric_hub = MetricHub(
        app_name="multi_product",
        slug="mobile_daily_active_users_v1",
        start_date="2024-01-01",
        end_date="last complete month",
    )
    now = to_datetime(datetime.utcnow())
    prev_date = previous_period_last_date("last complete month", now)

    assert test_metric_hub.end_date == to_datetime(prev_date).date()
