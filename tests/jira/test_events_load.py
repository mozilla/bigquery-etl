import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from bigquery_etl.jira.events import EVENT_SCHEMA, EventsBigQueryAPI, events_on_date

DEST = "proj.dataset.srein_events_v1"


def _event(issue_key, event_ts):
    return {"issue_key": issue_key, "event_ts": event_ts, "event_type": "created"}


def test_events_on_date_keeps_only_the_target_day():
    events = [
        _event("A-1", "2026-07-26T23:59:59+00:00"),
        _event("A-2", "2026-07-27T00:00:00+00:00"),
        _event("A-3", "2026-07-27T23:59:59+00:00"),
        _event("A-4", "2026-07-28T00:00:00+00:00"),
    ]
    kept = list(events_on_date(events, date(2026, 7, 27)))
    assert [e["issue_key"] for e in kept] == ["A-2", "A-3"]


def test_events_on_date_is_lazy():
    def exploding():
        yield _event("A-1", "2026-07-27T00:00:00+00:00")
        raise AssertionError("consumed too eagerly")

    generator = events_on_date(exploding(), date(2026, 7, 27))
    assert next(generator)["issue_key"] == "A-1"


def test_events_on_date_drops_missing_timestamps():
    assert list(events_on_date([{"issue_key": "A-1"}], date(2026, 7, 27))) == []


def _run_load(**kwargs):
    """Invoke load_events with a mocked BigQuery client; return (count, mock, ndjson)."""
    captured = {}

    def fake_load(file_obj, table, job_config=None):
        captured["table"] = table
        captured["job_config"] = job_config
        captured["ndjson"] = file_obj.read().decode("utf-8")
        return MagicMock()

    client = MagicMock()
    client.load_table_from_file.side_effect = fake_load

    with patch("bigquery_etl.jira.events.bigquery.Client", return_value=client):
        count = EventsBigQueryAPI().load_events(**kwargs)

    return count, captured


def test_load_writes_ndjson_and_returns_count():
    events = [
        _event("A-1", "2026-07-27T00:00:00+00:00"),
        _event("A-2", "2026-07-27T01:00:00+00:00"),
    ]
    count, captured = _run_load(destination=DEST, events=iter(events))

    assert count == 2
    lines = captured["ndjson"].strip().split("\n")
    assert [json.loads(line) for line in lines] == events


def test_load_without_partition_targets_the_base_table():
    _, captured = _run_load(destination=DEST, events=iter([]))
    assert captured["table"] == DEST


def test_load_with_partition_uses_the_decorator():
    _, captured = _run_load(
        destination=DEST, events=iter([]), date_partition="20260727"
    )
    assert captured["table"] == f"{DEST}$20260727"


def test_load_always_write_truncates_with_the_event_schema():
    _, captured = _run_load(destination=DEST, events=iter([]))
    config = captured["job_config"]

    assert config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert config.source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    assert config.autodetect is False
    assert config.schema == EVENT_SCHEMA
    assert config.time_partitioning.field == "event_ts"
    assert config.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
    # A seed WRITE_TRUNCATEs the base table, which replaces the table
    # definition; without this the table loses its declared clustering.
    assert config.clustering_fields == ["project_key", "issue_key"]


def test_load_has_no_append_mode():
    """There is no way to leave the target holding a mix of old and new rows.

    Both callers WRITE_TRUNCATE exactly what they fetched - the base table for a
    seed, one partition for a daily run - so an append mode could only ever produce
    indistinguishable duplicates.
    """
    with pytest.raises(TypeError, match="write_append"):
        _run_load(destination=DEST, events=iter([]), write_append=True)


def test_load_of_zero_events_still_issues_a_truncating_load():
    count, captured = _run_load(
        destination=DEST, events=iter([]), date_partition="20260727"
    )

    assert count == 0
    assert captured["ndjson"] == ""
    assert captured["table"] == f"{DEST}$20260727"
    assert (
        captured["job_config"].write_disposition
        == bigquery.WriteDisposition.WRITE_TRUNCATE
    )
