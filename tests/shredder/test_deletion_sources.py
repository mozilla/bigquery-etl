from unittest import mock

from bigquery_etl.schema.stable_table_schema import SchemaFile
from bigquery_etl.shredder.config import (
    CLIENT_ID,
    DESKTOP_SRC,
    GLEAN_CLIENT_ID,
    DeleteSource,
)
from bigquery_etl.shredder.deletion_sources import (
    get_associated_deletions,
    get_deletion_request_tables,
)

PROJECT = "moz-fx-data-shared-prod"
CHANNELS = {
    "org_mozilla_firefox": "fenix",
    "org_mozilla_focus": "focus_android",
    "org_mozilla_tv_firefox": "firefox_fire_tv",
}
DELETION_REQUEST_TABLES = {
    "org_mozilla_firefox_stable.deletion_request_v1",
    "org_mozilla_focus_stable.deletion_request_v1",
}

FENIX_SRC = DeleteSource(
    table="org_mozilla_firefox_stable.deletion_request_v1",
    field=GLEAN_CLIENT_ID,
)
FOCUS_SRC = DeleteSource(
    table="org_mozilla_focus_stable.deletion_request_v1",
    field=GLEAN_CLIENT_ID,
)
FOCUS_ADDITIONAL_SRC = DeleteSource(
    table="org_mozilla_focus_derived.additional_deletion_requests_v1",
    field=CLIENT_ID,
)


def associated_deletions(upstream_stable_tables):
    return get_associated_deletions(
        upstream_stable_tables,
        glean_channel_names=CHANNELS,
        deletion_request_tables=DELETION_REQUEST_TABLES,
    )


def test_configured_stable_table_uses_delete_targets():
    """Stable tables in DELETE_TARGETS use the configured sources."""
    table = f"{PROJECT}.telemetry_derived.crash_aggregates_v1"
    result = associated_deletions(
        {table: {f"{PROJECT}.telemetry_stable.crash_v4"}},
    )
    assert result == {table: {DESKTOP_SRC}}


def test_glean_stable_table():
    """Glean stable tables map to their channel's deletion_request_v1 table."""
    stable_table = f"{PROJECT}.org_mozilla_firefox_stable.metrics_v1"
    derived_table = f"{PROJECT}.org_mozilla_firefox_derived.clients_daily_v1"
    result = associated_deletions(
        {stable_table: {stable_table}, derived_table: {stable_table}},
    )
    assert result == {stable_table: {FENIX_SRC}, derived_table: {FENIX_SRC}}


def test_glean_stable_table_without_deletion_request():
    """Glean channels without a deletion_request_v1 table have no sources."""
    table = f"{PROJECT}.org_mozilla_tv_firefox_derived.clients_daily_v1"
    result = associated_deletions(
        {table: {f"{PROJECT}.org_mozilla_tv_firefox_stable.events_v1"}},
    )
    assert result == {table: set()}


def test_non_glean_stable_table():
    """Unconfigured stable tables that aren't Glean have no sources."""
    table = f"{PROJECT}.pocket_derived.fire_tv_events_daily_v1"
    result = associated_deletions(
        {table: {f"{PROJECT}.pocket_stable.fire_tv_events_v1"}},
    )
    assert result == {table: set()}


def test_multiple_upstream_tables():
    """Sources from every upstream stable table are combined."""
    table = f"{PROJECT}.telemetry_derived.unified_metrics_v1"
    result = associated_deletions(
        {
            table: {
                f"{PROJECT}.telemetry_stable.crash_v4",
                f"{PROJECT}.org_mozilla_firefox_stable.baseline_v1",
                f"{PROJECT}.org_mozilla_focus_stable.baseline_v1",
                f"{PROJECT}.pocket_stable.fire_tv_events_v1",
            }
        },
    )
    assert result == {table: {DESKTOP_SRC, FENIX_SRC, FOCUS_SRC}}


def test_additional_deletion_requests():
    """additional_deletion_requests_v1 is added for its channel when it's an input table."""
    additional = f"{PROJECT}.org_mozilla_focus_derived.additional_deletion_requests_v1"
    focus_table = f"{PROJECT}.org_mozilla_focus_derived.clients_daily_v1"
    fenix_table = f"{PROJECT}.org_mozilla_firefox_derived.clients_daily_v1"
    result = associated_deletions(
        {
            additional: set(),
            focus_table: {f"{PROJECT}.org_mozilla_focus_stable.baseline_v1"},
            fenix_table: {f"{PROJECT}.org_mozilla_firefox_stable.baseline_v1"},
        },
    )
    assert result == {
        additional: set(),
        focus_table: {FOCUS_SRC, FOCUS_ADDITIONAL_SRC},
        fenix_table: {FENIX_SRC},
    }


def test_no_upstream_tables():
    table = f"{PROJECT}.telemetry_derived.external_v1"
    assert associated_deletions({table: set()}) == {table: set()}


@mock.patch("bigquery_etl.shredder.deletion_sources.get_stable_table_schemas")
def test_get_deletion_request_tables(mock_schemas):
    """deletion_request_v1 tables come from mozilla-pipeline-schemas."""

    def schema(dataset_family, table):
        return SchemaFile(
            schema=[],
            schema_id="moz://mozilla.org/schemas/glean/ping/1",
            bq_dataset_family=dataset_family,
            bq_table=table,
            document_namespace=dataset_family,
            document_type=table,
            document_version=1,
        )

    mock_schemas.return_value = [
        schema("org_mozilla_firefox", "deletion_request_v1"),
        schema("org_mozilla_firefox", "metrics_v1"),
        schema("org_mozilla_focus", "deletion_request_v1"),
        schema("telemetry", "deletion_request_v4"),
    ]

    assert get_deletion_request_tables() == DELETION_REQUEST_TABLES
