"""Tests for gcp_monitoring_metrics_hourly_v1 query.py."""

import datetime
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

_repo_root = Path(__file__).resolve().parent.parent.parent
_query_path = (
    _repo_root
    / "sql/moz-fx-data-shared-prod/monitoring_derived"
    / "gcp_monitoring_metrics_hourly_v1/query.py"
)

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not _query_path.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)

spec = importlib.util.spec_from_file_location(
    "gcp_monitoring_metrics_hourly_query", _query_path
)
assert spec is not None and spec.loader is not None
query_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query_mod)

UTC = datetime.timezone.utc
DAY_START = datetime.datetime(2026, 10, 31, tzinfo=UTC)

DATAFLOW = query_mod.MetricDefinition(
    name="dataflow_bytes",
    metric_type="dataflow.googleapis.com/job/user_counter",
    projects=("my-project",),
    group_by=("job_name", "metric_name"),
    label_filters={"metric_name": r".+/bytes\.v1"},
    running_total=True,
)

PUBSUB = query_mod.MetricDefinition(
    name="subscription_bytes",
    metric_type="pubsub.googleapis.com/subscription/byte_cost",
    projects=("my-project",),
    group_by=("subscription_id", "operation_type"),
)


def hour(n):
    return DAY_START + datetime.timedelta(hours=n)


def response(body, ok=True, status_code=200):
    """Stub an HTTP response from the PromQL endpoint."""
    return SimpleNamespace(
        ok=ok, status_code=status_code, text=str(body), json=lambda: body
    )


def success(result, **extra):
    return response({"status": "success", "data": {"result": result}, **extra})


def test_build_promql_running_total():
    selector = (
        '{__name__="dataflow.googleapis.com/job/user_counter", '
        'project_id="my-project", '
        'metric_name=~".+/bytes\\\\.v1"}'
    )
    assert query_mod.build_promql(DATAFLOW, "my-project") == (
        "sum by (project_id, job_name, metric_name) ("
        f"(last_over_time({selector}[3600s])"
        f" - last_over_time({selector}[6h] offset 3600s))"
        f" or last_over_time({selector}[3600s]))"
    )


def test_build_promql_without_label_filters():
    assert query_mod.build_promql(PUBSUB, "my-project") == (
        "sum by (project_id, subscription_id, operation_type) (increase({"
        '__name__="pubsub.googleapis.com/subscription/byte_cost", '
        'project_id="my-project"'
        "}[3600s]))"
    )


def test_to_rows():
    result = [
        {
            "metric": {
                "project_id": "my-project",
                "job_name": "decoder",
                "metric_name": "ns/doc_v1/bytes",
            },
            "values": [
                [hour(23).timestamp(), "0"],
                [hour(24).timestamp(), "12.5"],
            ],
        }
    ]

    assert query_mod.to_rows(DATAFLOW, "my-project", result) == [
        {
            "submission_date": "2026-10-31",
            "interval_start_time": "2026-10-31T23:00:00+00:00",
            "interval_end_time": "2026-11-01T00:00:00+00:00",
            "metric": "dataflow_bytes",
            "metric_type": "dataflow.googleapis.com/job/user_counter",
            "project_id": "my-project",
            "labels": [
                {"key": "job_name", "value": "decoder"},
                {"key": "metric_name", "value": "ns/doc_v1/bytes"},
            ],
            "value": 12.5,
        }
    ]


def test_to_rows_with_missing_labels():
    # Labels that are absent from a series are left out rather than null, and
    # project_id falls back to the queried project.
    result = [
        {"metric": {"subscription_id": "sub"}, "values": [[hour(1).timestamp(), "3"]]}
    ]

    (row,) = query_mod.to_rows(PUBSUB, "my-project", result)

    assert row["project_id"] == "my-project"
    assert row["labels"] == [{"key": "subscription_id", "value": "sub"}]


def test_fetch_day_queries_each_hour_of_the_day():
    with patch.object(query_mod, "query_range", return_value=[]) as query_range:
        query_mod.fetch_day(MagicMock(), DAY_START.date(), [DATAFLOW])

    _, project, query, start, end = query_range.call_args.args
    assert project == "my-project"
    assert query == query_mod.build_promql(DATAFLOW, "my-project")
    # Points are evaluated at the end of each hour, so the first one covers
    # 00:00 to 01:00.
    assert start == hour(1)
    assert end == hour(24)


def test_query_range_request():
    session = MagicMock()
    session.get.return_value = success([{"metric": {}, "values": []}])

    result = query_mod.query_range(session, "my-project", "up", hour(1), hour(24))

    assert result == [{"metric": {}, "values": []}]
    url = session.get.call_args.args[0]
    assert url == (
        # Every query goes through the scoping project
        "https://monitoring.googleapis.com/v1/projects/moz-fx-data-shared-prod"
        "/location/global/prometheus/api/v1/query_range"
    )
    assert session.get.call_args.kwargs["params"] == {
        "query": "up",
        "start": "2026-10-31T01:00:00+00:00",
        "end": "2026-11-01T00:00:00+00:00",
        "step": "3600s",
    }


def test_query_range_null_result():
    session = MagicMock()
    session.get.return_value = success(None)

    assert query_mod.query_range(session, "my-project", "up", hour(1), hour(24)) == []


def test_query_range_retries_warnings():
    session = MagicMock()
    session.get.side_effect = [
        success([], warnings=["execution_errors"]),
        success([{"metric": {}, "values": []}]),
    ]

    with patch.object(query_mod.time, "sleep"):
        result = query_mod.query_range(session, "my-project", "up", hour(1), hour(24))

    assert result == [{"metric": {}, "values": []}]
    assert session.get.call_count == 2


def test_query_range_gives_up_after_repeated_warnings():
    session = MagicMock()
    session.get.return_value = success([], warnings=["execution_errors"])

    with (
        patch.object(query_mod.time, "sleep") as sleep,
        pytest.raises(RuntimeError),
    ):
        query_mod.query_range(session, "my-project", "up", hour(1), hour(24))

    assert session.get.call_count == query_mod.WARNING_ATTEMPTS
    # No sleep after the last attempt.
    assert sleep.call_count == query_mod.WARNING_ATTEMPTS - 1


@pytest.mark.parametrize(
    "bad_response",
    [
        response("Permission denied", ok=False, status_code=403),
        response({"status": "error", "error": "bad query"}),
    ],
)
def test_query_range_errors(bad_response):
    session = MagicMock()
    session.get.return_value = bad_response

    with pytest.raises(RuntimeError, match="my-project"):
        query_mod.query_range(session, "my-project", "up", hour(1), hour(24))


def test_empty_result_does_not_write():
    with (
        patch.object(query_mod, "create_session"),
        patch.object(query_mod, "fetch_day", return_value=[]),
        patch.object(query_mod.bigquery, "Client") as client,
    ):
        result = CliRunner().invoke(query_mod.main, ["--date", "2026-10-31"])

    assert result.exit_code != 0
    assert "No metric data" in result.output
    client.assert_not_called()


@pytest.mark.parametrize(
    "subset",
    [["--metric", "pubsub_topic_byte_cost"], ["--project", "my-project"]],
)
def test_subset_requires_dry_run(subset):
    result = CliRunner().invoke(query_mod.main, ["--date", "2026-10-31", *subset])

    assert result.exit_code != 0
    assert "--dry-run" in result.output


def test_main_replaces_the_date_partition():
    rows = [{"submission_date": "2026-10-31", "metric": "pubsub_topic_byte_cost"}]
    with (
        patch.object(query_mod, "create_session"),
        patch.object(query_mod, "fetch_day", return_value=rows),
        patch.object(query_mod.bigquery, "Client") as bq_client,
    ):
        bq_client.return_value.query_and_wait.return_value = [
            SimpleNamespace(metric="pubsub_topic_byte_cost")
        ]
        result = CliRunner().invoke(
            query_mod.main,
            ["--date", "2026-10-31", "--destination-table", "proj.dataset.table"],
        )

    assert result.exit_code == 0, result.output
    bq_client.assert_called_once_with(project="proj")
    load = bq_client.return_value.load_table_from_json
    loaded_rows, destination = load.call_args.args
    job_config = load.call_args.kwargs["job_config"]
    assert loaded_rows == rows
    assert destination == "proj.dataset.table$20261031"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.time_partitioning.field == "submission_date"
    assert job_config.clustering_fields == ["metric", "project_id"]


def test_dry_run_with_subset():
    with (
        patch.object(query_mod, "create_session") as create_session,
        patch.object(query_mod, "fetch_day", return_value=[{}]) as fetch_day,
        patch.object(query_mod.bigquery, "Client") as bq_client,
    ):
        result = CliRunner().invoke(
            query_mod.main,
            [
                "--date",
                "2026-10-31",
                "--project",
                "moz-fx-ingestion-sink-prod",
                "--project",
                "moz-fx-dataservices-high-prod",
                "--metric",
                "pubsub_topic_byte_cost",
                "--quota-project",
                "billing-project",
                "--dry-run",
            ],
        )

    assert result.exit_code == 0, result.output
    create_session.assert_called_once_with("billing-project")
    session, date, metrics = fetch_day.call_args.args
    assert session is create_session.return_value
    assert date == DAY_START.date()
    # --project narrows each metric's own projects rather than replacing them.
    assert [(metric.name, metric.projects) for metric in metrics] == [
        ("pubsub_topic_byte_cost", ("moz-fx-ingestion-sink-prod",))
    ]
    bq_client.assert_not_called()


def test_fetch_day_stops_at_last_ingested_hour():
    now = hour(5) + datetime.timedelta(minutes=8)

    with patch.object(query_mod, "query_range", return_value=[]) as query_range:
        query_mod.fetch_day(MagicMock(), DAY_START.date(), [DATAFLOW], now=now)

    # 05:00 ended only 8 minutes ago, so the last hour loaded is 03:00 to 04:00.
    *_, start, end = query_range.call_args.args
    assert start == hour(1)
    assert end == hour(4)


def test_fetch_day_with_no_complete_hours():
    now = hour(1) + datetime.timedelta(minutes=5)

    with patch.object(query_mod, "query_range") as query_range:
        rows = query_mod.fetch_day(MagicMock(), DAY_START.date(), [DATAFLOW], now=now)

    assert rows == []
    query_range.assert_not_called()


def test_fetch_day_queries_each_metric_in_its_projects():
    def fake_query_range(session, project, query, start, end):
        return [{"metric": {}, "values": [[hour(1).timestamp(), "1"]]}]

    dataflow = query_mod.replace(DATAFLOW, projects=("project-a", "project-b"))
    pubsub = query_mod.replace(PUBSUB, projects=("project-c",))
    with patch.object(
        query_mod, "query_range", side_effect=fake_query_range
    ) as query_range:
        rows = query_mod.fetch_day(MagicMock(), DAY_START.date(), [dataflow, pubsub])

    assert query_range.call_count == 3
    # Queries run in parallel, but rows come back in metric then project order.
    assert [(row["metric"], row["project_id"]) for row in rows] == [
        ("dataflow_bytes", "project-a"),
        ("dataflow_bytes", "project-b"),
        ("subscription_bytes", "project-c"),
    ]


def test_only_dataflow_counters_are_running_totals():
    # increase() is wrong for running totals, and differencing is wrong for
    # DELTA metrics.
    assert {m.name for m in query_mod.METRICS if m.running_total} == {
        m.name
        for m in query_mod.METRICS
        if m.metric_type == "dataflow.googleapis.com/job/user_counter"
    }


def test_every_metric_has_projects():
    assert all(metric.projects for metric in query_mod.METRICS)


def test_rows_match_schema():
    result = [
        {
            "metric": {"subscription_id": "sub", "operation_type": "ack"},
            "values": [[hour(1).timestamp(), "3"]],
        }
    ]

    (row,) = query_mod.to_rows(PUBSUB, "my-project", result)

    # The load job uses SCHEMA from schema.yaml, so a renamed column would
    # otherwise only fail in production.
    fields = {field.name: field for field in query_mod.SCHEMA}
    assert set(row) == set(fields)
    label_fields = {field.name for field in fields["labels"].fields}
    assert all(set(label) == label_fields for label in row["labels"])


def test_create_session_fetches_token_up_front():
    credentials = MagicMock()

    with patch.object(
        query_mod.google.auth, "default", return_value=(credentials, "project")
    ) as default:
        query_mod.create_session("billing-project")

    assert default.call_args.kwargs["quota_project_id"] == "billing-project"
    credentials.refresh.assert_called_once()


def run_main_with_stored_metrics(stored):
    """Run main with one fetched metric and the given stored metrics."""
    rows = [{"submission_date": "2026-10-31", "metric": "pubsub_topic_byte_cost"}]
    with (
        patch.object(query_mod, "create_session"),
        patch.object(query_mod, "fetch_day", return_value=rows),
        patch.object(query_mod.bigquery, "Client") as bq_client,
    ):
        query_and_wait = bq_client.return_value.query_and_wait
        if isinstance(stored, Exception):
            query_and_wait.side_effect = stored
        else:
            query_and_wait.return_value = [
                SimpleNamespace(metric=metric) for metric in stored
            ]
        result = CliRunner().invoke(query_mod.main, ["--date", "2026-10-31"])
    return result, bq_client.return_value.load_table_from_json


def test_main_keeps_partition_when_a_stored_metric_is_missing():
    # e.g. rerunning an old date after Pub/Sub data aged out but load
    # balancer data didn't.
    result, load = run_main_with_stored_metrics(
        ["pubsub_topic_byte_cost", "dataflow_valid_submission"]
    )

    assert result.exit_code != 0
    assert "dataflow_valid_submission" in result.output
    load.assert_not_called()


def test_main_writes_new_partition():
    result, load = run_main_with_stored_metrics(query_mod.NotFound("table not found"))

    assert result.exit_code == 0, result.output
    load.assert_called_once()


def test_main_ignores_stored_metrics_that_were_removed():
    result, load = run_main_with_stored_metrics(
        ["pubsub_topic_byte_cost", "metric_no_longer_defined"]
    )

    assert result.exit_code == 0, result.output
    load.assert_called_once()
