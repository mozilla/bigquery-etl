from argparse import Namespace
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from bigquery_etl.jira.events import JiraEventsBigQueryIntegration, daily_jql, seed_jql

DEFAULT_JQL = "project = SREIN"


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
    _, bq = _run(args(seed=True, date=None, since="2025-06-01", write_append=True))
    assert bq.load_events.call_args.kwargs["write_append"] is True


def test_seed_and_date_together_is_rejected():
    with pytest.raises(ValueError, match="mutually exclusive"):
        JiraEventsBigQueryIntegration().run(args(seed=True))


def test_seed_with_custom_jql_is_rejected():
    namespace = args(seed=True, date=None, jql="project = SREIN AND labels = urgent")
    with pytest.raises(ValueError, match="custom --jql"):
        JiraEventsBigQueryIntegration().run(namespace)


def test_seed_with_since_and_default_jql_is_allowed():
    api_cls, bq = _run(args(seed=True, date=None, since="2025-06-01"))

    assert api_cls.call_args.args[1] == '(project = SREIN) AND created >= "2025-06-01"'
    assert bq.load_events.call_count == 1


def test_daily_run_requires_a_date():
    with pytest.raises(ValueError, match="--date"):
        JiraEventsBigQueryIntegration().run(args(date=None))
