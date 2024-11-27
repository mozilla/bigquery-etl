from multiprocessing.pool import ThreadPool
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

from bigquery_etl.shredder.config import (
    CLIENT_ID,
    DELETE_TARGETS,
    GLEAN_CLIENT_ID,
    DeleteSource,
    DeleteTarget,
    _list_tables,
    find_glean_targets,
    get_glean_channel_to_app_name_mapping,
)

GLEAN_APP_LISTING = [
    {
        "app_channel": "release",
        "app_id": "org.mozilla.firefox",
        "app_name": "fenix",
        "bq_dataset_family": "org_mozilla_firefox",
    },
    {
        "app_channel": "beta",
        "app_id": "org.mozilla.firefox_beta",
        "app_name": "fenix",
        "bq_dataset_family": "org_mozilla_firefox_beta",
    },
    {
        "app_channel": "nightly",
        "app_id": "org.mozilla.fenix.nightly",
        "app_name": "fenix",
        "bq_dataset_family": "org_mozilla_fenix_nightly",
    },
    {
        "app_channel": "release",
        "app_id": "org.mozilla.focus",
        "app_name": "focus_android",
        "bq_dataset_family": "org_mozilla_focus",
    },
    {
        "app_channel": "beta",
        "app_id": "org.mozilla.focus.beta",
        "app_name": "focus_android",
        "bq_dataset_family": "org_mozilla_focus_beta",
    },
    {
        "app_channel": "beta",
        "app_id": "org.mozilla.focus.beta",
        "app_name": "focus_android",
        "bq_dataset_family": "org_mozilla_focus_beta",
    },
    {
        "app_id": "firefox.desktop",
        "app_name": "firefox_desktop",
        "bq_dataset_family": "firefox_desktop",
    },
]


class FakeClient:
    def list_datasets(self, project):
        return [
            bigquery.DatasetReference(project, f"{app}_{suffix}")
            for app in [
                "org_mozilla_firefox",
                "org_mozilla_firefox_beta",
                "org_mozilla_focus",
                "org_mozilla_focus_beta",
            ]
            for suffix in ["stable", "derived"]
        ]

    def list_tables(self, dataset_ref):
        labels = {}
        if dataset_ref.dataset_id.endswith("stable"):
            table_ids = ["metrics_v1", "deletion_request_v1", "migration_v1"]
            labels["schema_id"] = "glean_ping_1"
        elif dataset_ref.dataset_id in {
            "org_mozilla_focus_derived",
            "org_mozilla_focus_beta_derived",
        }:
            table_ids = [
                "additional_deletion_requests_v1",  # should be ignored
                "clients_daily_v1",
                "dau_v1",  # aggregated, no client_id
            ]
        elif dataset_ref.dataset_id.endswith("derived"):
            table_ids = ["clients_daily_v1"]
        else:
            raise Exception(f"unexpected dataset: {dataset_ref}")
        return [
            bigquery.table.TableListItem(
                {
                    "tableReference": bigquery.TableReference(
                        dataset_ref, table_id
                    ).to_api_repr(),
                    "labels": labels,
                }
            )
            for table_id in table_ids
        ]

    def get_table(self, table_ref):
        table = bigquery.Table(table_ref)
        table._properties[table._PROPERTY_TO_API_FIELD["type"]] = "TABLE"
        if table.dataset_id.endswith("stable"):
            table.schema = [
                bigquery.SchemaField(
                    "client_info",
                    "RECORD",
                    "NULLABLE",
                    [bigquery.SchemaField("client_id", "STRING")],
                )
            ]
        elif table.table_id in {
            "additional_deletion_requests_v1",
            "clients_daily_v1",
        } or table.dataset_id in {
            "fenix",
            "focus_android",
        }:
            table.schema = [bigquery.SchemaField("client_id", "STRING")]
        else:
            table.schema = [bigquery.SchemaField("document_id", "STRING")]
        return table


@mock.patch("bigquery_etl.shredder.config.requests")
def test_glean_targets(mock_requests):
    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    with ThreadPool(1) as pool:
        targets = find_glean_targets(pool, FakeClient())

    # convert tuples to sets because additional_deletion_requests are in
    # non-deterministic order due to multiprocessing
    # order doesn't matter in real execution
    for source, target in targets.items():
        targets[source] = set(target)

    assert targets == {
        **{
            target: {  # firefox release tables
                DeleteSource(
                    table="org_mozilla_firefox_stable.deletion_request_v1",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for target in [
                DeleteTarget(
                    table="org_mozilla_firefox_stable.metrics_v1",
                    field=("client_info.client_id",),
                    project="moz-fx-data-shared-prod",
                ),
                DeleteTarget(
                    table="org_mozilla_firefox_derived.clients_daily_v1",
                    field=("client_id",),
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
        **{
            target: {  # firefox beta tables
                DeleteSource(
                    table="org_mozilla_firefox_beta_stable.deletion_request_v1",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for target in [
                DeleteTarget(
                    table="org_mozilla_firefox_beta_stable.metrics_v1",
                    field=("client_info.client_id",),
                    project="moz-fx-data-shared-prod",
                ),
                DeleteTarget(
                    table="org_mozilla_firefox_beta_derived.clients_daily_v1",
                    field=("client_id",),
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
        **{
            target: {  # firefox combined-channel tables
                DeleteSource(
                    table="fenix.deletion_request",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for target in [
                DeleteTarget(
                    table="fenix_derived.clients_daily_v1",
                    field=("client_id",),
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
        **{
            target: {  # focus release tables
                DeleteSource(
                    table="org_mozilla_focus_stable.deletion_request_v1",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
                DeleteSource(
                    table="org_mozilla_focus_derived.additional_deletion_requests_v1",
                    field="client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for target in [
                DeleteTarget(
                    table="org_mozilla_focus_stable.metrics_v1",
                    field=("client_info.client_id",) * 2,
                    project="moz-fx-data-shared-prod",
                ),
                DeleteTarget(
                    table="org_mozilla_focus_derived.clients_daily_v1",
                    field=("client_id",) * 2,
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
        **{
            target: {  # focus beta tables
                DeleteSource(
                    table="org_mozilla_focus_beta_stable.deletion_request_v1",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
                DeleteSource(
                    table="org_mozilla_focus_beta_derived.additional_deletion_requests_v1",
                    field="client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for target in [
                DeleteTarget(
                    table="org_mozilla_focus_beta_stable.metrics_v1",
                    field=("client_info.client_id",) * 2,
                    project="moz-fx-data-shared-prod",
                ),
                DeleteTarget(
                    table="org_mozilla_focus_beta_derived.clients_daily_v1",
                    field=("client_id",) * 2,
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
        **{
            target: {  # focus combined-channel tables
                DeleteSource(
                    table="focus_android.deletion_request",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
                DeleteSource(
                    table="org_mozilla_focus_beta_derived.additional_deletion_requests_v1",
                    field="client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
                DeleteSource(
                    table="org_mozilla_focus_derived.additional_deletion_requests_v1",
                    field="client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for target in [
                DeleteTarget(
                    table="focus_android_derived.clients_daily_v1",
                    field=("client_id",) * 3,
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
    }


@mock.patch("bigquery_etl.shredder.config.requests")
def test_glean_targets_override(mock_requests):
    """Targets in GLEAN_DERIVED_OVERRIDES should override the target in find_glean_targets."""

    class TargetOverrideClient:
        def list_datasets(self, project):
            return [
                bigquery.DatasetReference(project, name)
                for name in [
                    "firefox_desktop_stable",
                    "firefox_desktop_derived",
                ]
            ]

        def list_tables(self, dataset_ref):
            labels = {}
            if dataset_ref.dataset_id == "firefox_desktop_stable":
                table_ids = ["metrics_v1", "deletion_request_v1"]
                labels["schema_id"] = "glean_ping_1"
            elif dataset_ref.dataset_id == "firefox_desktop_derived":
                table_ids = [
                    "adclick_history_v1",  # should use value from override
                    "other_table_v1",
                    "pageload_1pct_v1",  # should be ignored
                ]
            else:
                raise Exception(f"unexpected dataset: {dataset_ref}")
            return [
                bigquery.table.TableListItem(
                    {
                        "tableReference": bigquery.TableReference(
                            dataset_ref, table_id
                        ).to_api_repr(),
                        "labels": labels,
                    }
                )
                for table_id in table_ids
            ]

        def get_table(self, table_ref):
            table = bigquery.Table(table_ref)
            table._properties[table._PROPERTY_TO_API_FIELD["type"]] = "TABLE"
            if table.dataset_id.endswith("stable"):
                table.schema = [
                    bigquery.SchemaField(
                        "client_info",
                        "RECORD",
                        "NULLABLE",
                        [bigquery.SchemaField("client_id", "STRING")],
                    )
                ]
            else:
                table.schema = [bigquery.SchemaField("client_id", "STRING")]
            return table

    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    with ThreadPool(1) as pool:
        targets = find_glean_targets(pool, TargetOverrideClient())

    # convert tuples to sets because additional_deletion_requests are in
    # non-deterministic order due to multiprocessing
    # order doesn't matter in real execution
    for source, target in targets.items():
        targets[source] = set(target) if isinstance(target, tuple) else target

    desktop_deletions = DeleteSource(
        table="firefox_desktop_stable.deletion_request_v1",
        field=GLEAN_CLIENT_ID,
    )

    assert targets == {
        DeleteTarget(
            table="firefox_desktop_derived.other_table_v1", field=(CLIENT_ID,)
        ): {desktop_deletions},
        DeleteTarget(
            table="firefox_desktop_stable.metrics_v1", field=(GLEAN_CLIENT_ID,)
        ): {desktop_deletions},
        # adclick_history_v1 should not be here
    }


@mock.patch("bigquery_etl.shredder.config.requests")
def test_glean_channel_app_mapping(mock_requests):
    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    actual = get_glean_channel_to_app_name_mapping()

    expected = {
        "org_mozilla_firefox": "fenix",
        "org_mozilla_firefox_beta": "fenix",
        "org_mozilla_fenix_nightly": "fenix",
        "org_mozilla_focus": "focus_android",
        "org_mozilla_focus_beta": "focus_android",
        "firefox_desktop": "firefox_desktop",
    }

    assert actual == expected


def test_list_tables_wrapper_empty():
    """List tables wrapper should return an empty list when dataset doesn't exist."""

    class EmptyFakeClient(FakeClient):
        def list_tables(self, dataset_ref):
            raise NotFound("not found")

    tables = _list_tables(DatasetReference("project", "dataset"), EmptyFakeClient())

    assert tables == []


def test_delete_target_fields_match_sources():
    """The number of fields in the delete targets should match the number of sources."""
    for target, sources in DELETE_TARGETS.items():
        field_count = 1 if isinstance(target.field, str) else len(target.field)
        source_count = 1 if isinstance(sources, DeleteSource) else len(sources)
        assert field_count == source_count, (
            f"Invalid delete target for {target.table}: number of fields in target "
            f"(found {field_count}) must match number of sources (found {source_count})"
        )


def test_delete_source_invalid():
    """DeleteSource constructor should fail when the given table is invalid."""
    DeleteSource(
        table="dataset.deletion_request_v1",
        field="client_id",
        project="project",
    )

    with pytest.raises(ValueError):
        DeleteSource(
            table="deletion_request_v1",
            field="client_id",
            project="project",
        )

    with pytest.raises(ValueError):
        DeleteSource(
            table="project.dataset.deletion_request_v1",
            field="client_id",
            project="project",
        )


def test_delete_target_invalid():
    """DeleteTarget constructor should fail when the given table is invalid."""
    DeleteTarget(
        table="dataset.deletion_request_v1",
        field="client_id",
        project="project",
    )

    with pytest.raises(ValueError):
        DeleteTarget(
            table="deletion_request_v1",
            field="client_id",
            project="project",
        )

    with pytest.raises(ValueError):
        DeleteTarget(
            table="project.dataset.deletion_request_v1",
            field="client_id",
            project="project",
        )
