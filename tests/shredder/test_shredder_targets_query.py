"""Tests for monitoring_derived.shredder_targets_v1 query.py."""

import datetime
import importlib.util
from pathlib import Path
from unittest import mock

import pytest

from bigquery_etl.shredder.config import (
    GLEAN_CLIENT_ID,
    NOT_SHREDDED,
    DeleteSource,
    DeleteTarget,
)

_query_path = (
    Path(__file__).resolve().parent.parent.parent
    / "sql/moz-fx-data-shared-prod/monitoring_derived/shredder_targets_v1/query.py"
)

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not _query_path.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)

spec = importlib.util.spec_from_file_location("shredder_targets_query", _query_path)
assert spec is not None and spec.loader is not None
query_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query_mod)

PROJECT = "moz-fx-data-shared-prod"
CHANNELS = {
    "org_mozilla_firefox": "fenix",
    "org_mozilla_fenix": "fenix",
}
FENIX_SOURCES = {
    DeleteSource(table=f"{channel}_stable.deletion_request_v1", field=GLEAN_CLIENT_ID)
    for channel in CHANNELS
}


def missing_deletions(delete_targets, associated_deletions):
    with (
        mock.patch.object(query_mod, "DELETE_TARGETS", delete_targets),
        mock.patch.object(query_mod, "find_glean_targets", return_value={}),
        mock.patch.object(
            query_mod, "get_glean_app_id_to_app_name_mapping", return_value=CHANNELS
        ),
        mock.patch.object(
            query_mod,
            "get_deletion_request_tables",
            return_value={source.table for source in FENIX_SOURCES},
        ),
        mock.patch.object(query_mod.bigquery, "Client"),
    ):
        return query_mod.get_missing_deletions(associated_deletions)


@pytest.mark.parametrize(
    "sources",
    [
        # a single source in parentheses is not a tuple
        (DeleteSource(table="fenix.deletion_request", field=GLEAN_CLIENT_ID)),
        (DeleteSource(table="fenix.deletion_request", field=GLEAN_CLIENT_ID),),
    ],
)
def test_app_view_sources_are_expanded(sources):
    """Per-app deletion request views expand to per-channel tables for single sources too."""
    target = DeleteTarget(
        table="ltv_derived.fenix_new_profile_ltv_v1", field="client_id"
    )
    result = missing_deletions(
        {target: sources},
        {f"{PROJECT}.{target.table}": set(FENIX_SOURCES)},
    )

    assert len(result) == 1
    assert result[0]["matching_sources"]
    assert sorted(s["table"] for s in result[0]["current_sources"]) == sorted(
        s.table for s in FENIX_SOURCES
    )


def test_unconfigured_table_is_reported():
    table = f"{PROJECT}.org_mozilla_firefox_derived.new_table_v1"
    result = missing_deletions({}, {table: set(FENIX_SOURCES)})

    assert len(result) == 1
    assert result[0]["table_id"] == "new_table_v1"
    assert result[0]["current_sources"] == []
    assert not result[0]["matching_sources"]


def test_find_client_id_tables_uses_run_date_and_skips_not_shredded():
    not_shredded_table = next(iter(NOT_SHREDDED))
    _, dataset, table = not_shredded_table.split(".")
    rows = [
        mock.Mock(table_schema="telemetry_derived", table_name="clients_daily_v6"),
        mock.Mock(table_schema=dataset, table_name=table),
    ]
    with mock.patch.object(query_mod.bigquery, "Client") as mock_client:
        mock_client.return_value.query_and_wait.return_value = rows
        result = query_mod.find_client_id_tables(PROJECT, datetime.date(2026, 11, 18))

    assert result == [f"{PROJECT}.telemetry_derived.clients_daily_v6"]

    _, kwargs = mock_client.return_value.query_and_wait.call_args
    assert "'2025-11-24'" not in kwargs["query"]
    assert "DATE_SUB(@run_date, INTERVAL 3 DAY)" in kwargs["query"]
    (param,) = kwargs["job_config"].query_parameters
    assert (param.name, param.value) == ("run_date", datetime.date(2026, 11, 18))
