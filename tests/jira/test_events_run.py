from argparse import Namespace
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from bigquery_etl.jira.events import (
    JiraEventsBigQueryIntegration,
    build_parser,
    daily_jql,
    seed_jql,
)

DEFAULT_JQL = "project = SREIN"


def args(**overrides):
    base = {
        "destination": "proj.dataset.srein_events_v1",
        "base_jira_url": "https://jira.example.com",
        "jql": DEFAULT_JQL,
        "date": "2026-07-27",
        "seed": False,
        "since": None,
        "until": None,
        "write_append": False,
        "write_truncate": False,
    }
    return Namespace(**{**base, **overrides})


def test_daily_jql_has_a_lower_bound_only():
    """No upper bound: it would be the sole cause of lossy re-runs, and buys nothing.

    The lower bound is widened by a day because JQL date literals are evaluated in
    the token owner's Jira profile timezone rather than UTC. `events_on_date` then
    filters precisely, so the surplus is discarded.
    """
    assert daily_jql(DEFAULT_JQL, date(2026, 7, 27)) == (
        '(project = SREIN) AND updated >= "2026-07-26"'
    )
    assert "updated <" not in daily_jql(DEFAULT_JQL, date(2026, 7, 27))


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


def test_an_old_date_is_accepted_and_rebuilds_its_partition_completely():
    """`updated >= D-1` has no upper bound, so re-running an old date is complete.

    Every event this table records bumps the issue's `updated`, so an issue with
    events on D always satisfies `updated >= D-1` no matter how often it has been
    touched since. That is what makes clearing an old Airflow task an ordinary
    repair rather than a silent partial overwrite.
    """
    api_cls, bq = _run(args(date="2025-07-27"))

    # Lower bound is D-1 (timezone slack); crucially there is no upper bound, so
    # issues touched again in the year since are still selected.
    assert api_cls.call_args.args[1] == '(project = SREIN) AND updated >= "2025-07-26"'
    assert bq.load_events.call_args.kwargs["date_partition"] == "20250727"


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


def test_jql_is_not_an_overridable_flag():
    """The table's scope is fixed at build_parser, not settable at run time.

    Every mode WRITE_TRUNCATEs its whole target -- the base table for --seed, one
    partition for --date -- so a narrower scope would silently drop everything it
    did not fetch. There is no value a caller could pass that is both different from
    the table's configured scope and safe, so the flag does not exist.
    """
    parser = build_parser("d", "u", "project = ABC")

    assert parser.parse_args([]).jql == "project = ABC"
    with pytest.raises(SystemExit):
        parser.parse_args(["--jql", "key = ABC-1"])


def test_build_parser_supplies_every_attribute_validate_requires():
    parsed = build_parser("d", "u", "j").parse_args([])
    required = {
        "destination",
        "base_jira_url",
        "jql",
        "date",
        "seed",
        "since",
        "until",
        "write_append",
        "write_truncate",
    }
    assert required <= set(vars(parsed))
