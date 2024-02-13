import datetime

from kpi_forecasting.utils import parse_end_date, previous_period_last_date


def test_parse_end_date_with_date():

    dt = "2024-01-01"
    parsed_date = parse_end_date(dt)

    assert dt == parsed_date


def test_parse_end_date_with_none():

    dt = None
    parsed_date = parse_end_date(dt)

    assert parsed_date.date() == datetime.datetime.utcnow().date()


def test_parse_end_date_prev_year():

    dt = "last complete year"
    parsed_date = parse_end_date(dt)

    yr = datetime.datetime.utcnow().date()

    assert parsed_date == datetime.date(yr.year - 1, 12, 31)


def test_parse_end_date_prev_month():

    dt = "last complete month"
    now = datetime.datetime(2024, 1, 1)

    prev = previous_period_last_date(dt, now)

    assert prev == datetime.date(2023, 12, 31)
