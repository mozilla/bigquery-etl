from argparse import Namespace
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from bigquery_etl.jira.events import (
    STALE_DATE_DAYS,
    JiraEventsBigQueryIntegration,
    daily_jql,
    is_stale_date,
    seed_jql,
)

DEFAULT_JQL = "project = SREIN"
TODAY = date(2026, 7, 27)


@pytest.fixture(autouse=True)
def pinned_today(monkeypatch):
    """Pin "today" so the staleness guard does not depend on the wall clock."""
    monkeypatch.setattr("bigquery_etl.jira.events.utc_today", lambda: TODAY)


def args(**overrides):
    base = {
        "destination": "proj.dataset.srein_events_v1",
        "base_jira_url": "https://jira.example.com",
        "jql": DEFAULT_JQL,
        "default_jql": DEFAULT_JQL,
        "date": "2026-07-27",
        "seed": False,
        "since": None,
        "until": None,
        "write_append": False,
        "write_truncate": False,
        "allow_stale_date": False,
    }
    return Namespace(**{**base, **overrides})


def test_daily_jql_widens_the_window_by_one_day_each_side():
    assert daily_jql(DEFAULT_JQL, date(2026, 7, 27)) == (
        '(project = SREIN) AND updated >= "2026-07-26" AND updated < "2026-07-29"'
    )


def test_seed_jql_without_bounds_is_the_bare_jql():
    assert seed_jql(DEFAULT_JQL, None, None) == "(project = SREIN)"


def test_seed_jql_with_bounds_filters_on_created():
    assert seed_jql(DEFAULT_JQL, "2025-05-01", "2025-06-01") == (
        '(project = SREIN) AND created >= "2025-05-01" AND created < "2025-06-01"'
    )


def test_seed_jql_with_only_since():
    assert seed_jql(DEFAULT_JQL, "2025-05-01", None) == (
        '(project = SREIN) AND created >= "2025-05-01"'
    )


def _run(namespace):
    """Run the integration with API and BigQuery layers mocked out."""
    api = MagicMock()
    api.get_issues.return_value = [{"issue_key": "SREIN-1"}]
    api.iter_events.return_value = iter(
        [
            {"issue_key": "SREIN-1", "event_ts": "2026-07-27T10:00:00+00:00"},
            {"issue_key": "SREIN-1", "event_ts": "2026-08-01T10:00:00+00:00"},
        ]
    )
    bq = MagicMock()
    bq.load_events.return_value = 1

    with (
        patch("bigquery_etl.jira.events.JiraClient"),
        patch("bigquery_etl.jira.events.JiraEventsAPI", return_value=api) as api_cls,
        patch("bigquery_etl.jira.events.EventsBigQueryAPI", return_value=bq),
    ):
        JiraEventsBigQueryIntegration().run(namespace)

    return api_cls, bq


def test_daily_run_filters_to_the_target_partition():
    api_cls, bq = _run(args())

    assert 'updated >= "2026-07-26"' in api_cls.call_args.args[1]

    kwargs = bq.load_events.call_args.kwargs
    assert kwargs["date_partition"] == "20260727"
    assert kwargs["write_append"] is False
    loaded = list(kwargs["events"])
    assert [e["event_ts"] for e in loaded] == ["2026-07-27T10:00:00+00:00"]


def test_seed_run_loads_every_event_into_the_base_table():
    api_cls, bq = _run(args(seed=True, date=None))

    assert api_cls.call_args.args[1] == "(project = SREIN)"

    kwargs = bq.load_events.call_args.kwargs
    assert kwargs["date_partition"] is None
    assert len(list(kwargs["events"])) == 2, "seed must not filter by date"


def test_seed_with_write_append_passes_it_through():
    api_cls, bq = _run(
        args(seed=True, date=None, since="2025-06-01", write_append=True)
    )

    assert api_cls.call_args.args[1] == '(project = SREIN) AND created >= "2025-06-01"'
    assert bq.load_events.call_args.kwargs["write_append"] is True


def test_seed_with_write_truncate_passes_write_append_false():
    _, bq = _run(args(seed=True, date=None, since="2025-06-01", write_truncate=True))
    assert bq.load_events.call_args.kwargs["write_append"] is False


def test_seed_and_date_together_is_rejected():
    with pytest.raises(ValueError, match="mutually exclusive"):
        JiraEventsBigQueryIntegration().run(args(seed=True))


def test_seed_with_custom_jql_is_rejected():
    namespace = args(seed=True, date=None, jql="project = SREIN AND labels = urgent")
    with pytest.raises(ValueError, match="custom --jql"):
        JiraEventsBigQueryIntegration().run(namespace)


def test_bounded_seed_without_disposition_is_rejected():
    namespace = args(seed=True, date=None, since="2025-06-01")
    with pytest.raises(ValueError, match="exactly one of"):
        JiraEventsBigQueryIntegration().run(namespace)


def test_bounded_seed_with_until_only_without_disposition_is_rejected():
    namespace = args(seed=True, date=None, until="2025-06-01")
    with pytest.raises(ValueError, match="exactly one of"):
        JiraEventsBigQueryIntegration().run(namespace)


def test_bounded_seed_with_both_dispositions_is_rejected():
    namespace = args(
        seed=True,
        date=None,
        since="2025-06-01",
        write_append=True,
        write_truncate=True,
    )
    with pytest.raises(ValueError, match="exactly one of"):
        JiraEventsBigQueryIntegration().run(namespace)


def test_daily_run_requires_a_date():
    with pytest.raises(ValueError, match="--date"):
        JiraEventsBigQueryIntegration().run(args(date=None))


def test_unbounded_seed_with_write_append_is_rejected():
    """An unbounded seed fetches all history; appending it duplicates the table."""
    namespace = args(seed=True, date=None, write_append=True)
    with pytest.raises(ValueError, match="unbounded --seed"):
        JiraEventsBigQueryIntegration().run(namespace)


def test_unbounded_seed_with_write_truncate_is_allowed():
    _, bq = _run(args(seed=True, date=None, write_truncate=True))
    assert bq.load_events.call_args.kwargs["write_append"] is False


@pytest.mark.parametrize(
    "days_ago,stale",
    [(0, False), (1, False), (STALE_DATE_DAYS, False), (STALE_DATE_DAYS + 1, True)],
)
def test_is_stale_date_threshold(days_ago, stale):
    assert is_stale_date(TODAY - timedelta(days=days_ago), TODAY) is stale


def test_is_stale_date_accepts_a_future_date():
    assert is_stale_date(TODAY + timedelta(days=1), TODAY) is False


def test_stale_date_is_rejected():
    stale = (TODAY - timedelta(days=STALE_DATE_DAYS + 1)).isoformat()
    with pytest.raises(ValueError) as excinfo:
        JiraEventsBigQueryIntegration().run(args(date=stale))

    message = str(excinfo.value)
    assert "incomplete fetch" in message
    assert "--seed" in message
    assert "--allow-stale-date" in message


def test_stale_date_is_allowed_with_the_override():
    stale = (TODAY - timedelta(days=30)).isoformat()
    _, bq = _run(args(date=stale, allow_stale_date=True))
    assert bq.load_events.call_args.kwargs["date_partition"] == "20260627"


def test_date_at_the_staleness_threshold_is_allowed():
    boundary = (TODAY - timedelta(days=STALE_DATE_DAYS)).isoformat()
    _, bq = _run(args(date=boundary))
    assert bq.load_events.call_args.kwargs["date_partition"] == "20260724"


@pytest.mark.parametrize(
    "override",
    [
        {"since": "2025-06-01"},
        {"until": "2025-06-01"},
        {"write_append": True},
        {"write_truncate": True},
    ],
)
def test_seed_only_arguments_are_rejected_alongside_date(override):
    with pytest.raises(ValueError, match="only apply to --seed"):
        JiraEventsBigQueryIntegration().run(args(**override))


def test_allow_stale_date_is_rejected_alongside_seed():
    namespace = args(seed=True, date=None, allow_stale_date=True)
    with pytest.raises(ValueError, match="--allow-stale-date"):
        JiraEventsBigQueryIntegration().run(namespace)
