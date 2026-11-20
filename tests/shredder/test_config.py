from multiprocessing.pool import ThreadPool
from unittest import mock

import pytest
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

from bigquery_etl.cli.utils import get_glean_app_id_to_app_name_mapping
from bigquery_etl.schema.stable_table_schema import SchemaFile
from bigquery_etl.shredder.config import (
    CLIENT_ID,
    DELETE_TARGETS,
    GLEAN_CLIENT_ID,
    NOT_SHREDDED,
    DeleteSource,
    DeleteTarget,
    GleanTableInfo,
    _list_tables,
    build_glean_targets,
    field_paths,
    find_glean_targets,
)
from bigquery_etl.shredder.offline_targets import find_glean_targets_offline

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
        if dataset_ref.dataset_id.endswith("stable"):
            table_ids = [
                ("metrics_v1", {"schema_id": "glean_ping_1"}),
                ("deletion_request_v1", {"schema_id": "glean_ping_1"}),
                ("migration_v1", {"schema_id": "glean_ping_1"}),
                ("usage_reporting_v1", {"schema_id": "glean-min_ping_1"}),
                ("usage_deletion_request_v1", {"schema_id": "glean-min_ping_1"}),
            ]
        elif dataset_ref.dataset_id in {
            "org_mozilla_focus_derived",
            "org_mozilla_focus_beta_derived",
        }:
            table_ids = [
                "additional_deletion_requests_v1",  # should be ignored
                "clients_daily_v1",
                "dau_v1",  # aggregated, no client_id,
                "usage_reporting_clients_daily_v1",  # should use usage_deletion_request_v1
            ]
        elif dataset_ref.dataset_id.endswith("derived"):
            table_ids = ["clients_daily_v1"]
        else:
            raise Exception(f"unexpected dataset: {dataset_ref}")
        return [
            bigquery.table.TableListItem(
                {
                    "tableReference": bigquery.TableReference(
                        dataset_ref,
                        table_id if isinstance(table_id, str) else table_id[0],
                    ).to_api_repr(),
                    "labels": {} if isinstance(table_id, str) else table_id[1],
                }
            )
            for table_id in table_ids
        ]

    def get_table(self, table_ref):
        table = bigquery.Table(table_ref)
        table._properties[table._PROPERTY_TO_API_FIELD["type"]] = "TABLE"
        if table.table_id in {
            "usage_reporting_v1",
            "usage_deletion_request_v1",
        }:
            table.schema = [
                bigquery.SchemaField(
                    "metrics",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField(
                            "uuid",
                            "RECORD",
                            fields=[bigquery.SchemaField("usage_profile_id", "STRING")],
                        ),
                    ],
                )
            ]
        elif table.dataset_id.endswith("stable"):
            table.schema = [
                bigquery.SchemaField(
                    "client_info",
                    "RECORD",
                    fields=[bigquery.SchemaField("client_id", "STRING")],
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
        elif table.table_id in {
            "usage_reporting_clients_daily_v1",
        }:
            table.schema = [bigquery.SchemaField("usage_profile_id", "STRING")]
        else:
            table.schema = [bigquery.SchemaField("document_id", "STRING")]
        return table


@mock.patch("bigquery_etl.cli.utils.requests")
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
                # migration_v1 is only skipped for the channels in NOT_SHREDDED
                DeleteTarget(
                    table="org_mozilla_focus_stable.migration_v1",
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
                # migration_v1 is only skipped for the channels in NOT_SHREDDED
                DeleteTarget(
                    table="org_mozilla_focus_beta_stable.migration_v1",
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
        **{  # usage_reporting_v1
            DeleteTarget(
                table=f"{app_id}_stable.usage_reporting_v1",
                field=("metrics.uuid.usage_profile_id",),
                project="moz-fx-data-shared-prod",
            ): {
                DeleteSource(
                    table=f"{app_id}_stable.usage_deletion_request_v1",
                    field="metrics.uuid.usage_profile_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for app_id in [
                "org_mozilla_focus",
                "org_mozilla_focus_beta",
                "org_mozilla_firefox",
                "org_mozilla_firefox_beta",
            ]
        },
        **{  # usage_reporting_clients_daily_v1
            DeleteTarget(
                table=f"{app_id}_derived.usage_reporting_clients_daily_v1",
                field=("usage_profile_id",),
                project="moz-fx-data-shared-prod",
            ): {
                DeleteSource(
                    table=f"{app_id}_stable.usage_deletion_request_v1",
                    field="metrics.uuid.usage_profile_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            }
            for app_id in [
                "org_mozilla_focus",
                "org_mozilla_focus_beta",
            ]
        },
    }


FAKE_CLIENT_CHANNELS = [
    "org_mozilla_firefox",
    "org_mozilla_firefox_beta",
    "org_mozilla_focus",
    "org_mozilla_focus_beta",
]


def fake_client_table_infos():
    """Return the tables in FakeClient as offline metadata for build_glean_targets."""
    usage_profile_id = frozenset(
        {"metrics", "metrics.uuid", "metrics.uuid.usage_profile_id"}
    )
    stable_tables = [
        GleanTableInfo(
            dataset_id=f"{channel}_stable",
            table_id=table_id,
            labels={"schema_id": schema_id},
            field_paths=paths,
        )
        for channel in FAKE_CLIENT_CHANNELS
        for table_id, schema_id, paths in [
            ("metrics_v1", "glean_ping_1", frozenset()),
            ("deletion_request_v1", "glean_ping_1", frozenset()),
            ("migration_v1", "glean_ping_1", frozenset()),
            ("usage_reporting_v1", "glean-min_ping_1", frozenset()),
            ("usage_deletion_request_v1", "glean-min_ping_1", usage_profile_id),
        ]
    ]
    derived_tables = [
        GleanTableInfo(
            dataset_id=dataset_id,
            table_id="clients_daily_v1",
            field_paths=frozenset({"client_id"}),
        )
        for dataset_id in [
            "org_mozilla_firefox_derived",
            "org_mozilla_firefox_beta_derived",
            "fenix_derived",
            "focus_android_derived",
        ]
    ] + [
        GleanTableInfo(
            dataset_id=dataset_id,
            table_id=table_id,
            field_paths=frozenset({column}),
        )
        for dataset_id in [
            "org_mozilla_focus_derived",
            "org_mozilla_focus_beta_derived",
        ]
        for table_id, column in [
            ("additional_deletion_requests_v1", "client_id"),
            ("clients_daily_v1", "client_id"),
            ("dau_v1", "document_id"),
            ("usage_reporting_clients_daily_v1", "usage_profile_id"),
        ]
    ]
    return stable_tables, derived_tables


def as_sets(targets):
    """Convert source tuples to sets because their order depends on listing order."""
    return {
        target: set(sources) if isinstance(sources, tuple) else sources
        for target, sources in targets.items()
    }


@mock.patch("bigquery_etl.cli.utils.requests")
def test_build_glean_targets_matches_find_glean_targets(mock_requests):
    """Offline metadata gives the same targets as listing the same tables in BigQuery."""
    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    with ThreadPool(1) as pool:
        expected = find_glean_targets(pool, FakeClient())

    stable_tables, derived_tables = fake_client_table_infos()
    actual = build_glean_targets(
        stable_tables,
        derived_tables,
        app_deletion_request_views={"fenix", "focus_android"},
        channel_to_app_name=get_glean_app_id_to_app_name_mapping(),
    )

    assert as_sets(actual) == as_sets(expected)


def write_fake_client_sql_dir(sql_dir):
    """Write the tables in FakeClient as MPS schemas and a generated sql directory."""
    schema_ids = {
        "glean_ping_1": "moz://mozilla.org/schemas/glean/ping/1",
        "glean-min_ping_1": "moz://mozilla.org/schemas/glean-min/ping/1",
    }
    stable_tables, derived_tables = fake_client_table_infos()

    def to_fields(paths):
        """Build JSON schema fields from dotted paths."""
        fields: dict = {}
        for path in sorted(paths):
            *parents, name = path.split(".")
            level = fields
            for parent in parents:
                level = level[parent].setdefault("children", {})
            level.setdefault(name, {"name": name})

        def convert(level):
            return [
                {
                    "name": name,
                    "type": "RECORD" if "children" in field else "STRING",
                    "mode": "NULLABLE",
                    **(
                        {"fields": convert(field["children"])}
                        if "children" in field
                        else {}
                    ),
                }
                for name, field in level.items()
            ]

        return convert(fields)

    schemas = [
        SchemaFile(
            schema=to_fields(table.field_paths),
            schema_id=schema_ids[table.labels["schema_id"]],
            bq_dataset_family=table.dataset_id.removesuffix("_stable"),
            bq_table=table.table_id,
            document_namespace=table.dataset_id,
            document_type=table.table_id,
            document_version=1,
        )
        for table in stable_tables
    ]
    for table in derived_tables:
        table_dir = (
            sql_dir / "moz-fx-data-shared-prod" / table.dataset_id / table.table_id
        )
        table_dir.mkdir(parents=True)
        (table_dir / "query.sql").write_text("SELECT 1")
        (table_dir / "schema.yaml").write_text(
            yaml.dump({"fields": to_fields(table.field_paths)})
        )
    for app_name in ["fenix", "focus_android"]:
        view_dir = sql_dir / "moz-fx-data-shared-prod" / app_name / "deletion_request"
        view_dir.mkdir(parents=True)
        (view_dir / "view.sql").write_text("SELECT 1")
    return schemas


@mock.patch("bigquery_etl.cli.utils.requests")
def test_find_glean_targets_offline_matches_find_glean_targets(mock_requests, tmp_path):
    """MPS schemas and a sql directory give the same targets as BigQuery for the same tables."""
    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    with ThreadPool(1) as pool:
        expected = find_glean_targets(pool, FakeClient())

    schemas = write_fake_client_sql_dir(tmp_path)
    actual = find_glean_targets_offline(
        tmp_path,
        schemas=schemas,
        channel_to_app_name=get_glean_app_id_to_app_name_mapping(),
    )

    assert as_sets(actual) == as_sets(expected)


@mock.patch("bigquery_etl.cli.utils.requests")
def test_build_glean_targets_without_app_view(mock_requests):
    """Apps without a deletion_request view use the per-channel tables."""
    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    stable_tables, derived_tables = fake_client_table_infos()
    targets = build_glean_targets(
        stable_tables,
        derived_tables,
        app_deletion_request_views={"focus_android"},
        channel_to_app_name=get_glean_app_id_to_app_name_mapping(),
    )

    assert as_sets(targets)[
        DeleteTarget(table="fenix_derived.clients_daily_v1", field=(CLIENT_ID,) * 2)
    ] == {
        DeleteSource(
            table=f"{channel}_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
        )
        for channel in ["org_mozilla_firefox", "org_mozilla_firefox_beta"]
    }


def test_build_glean_targets_ignores_datasets_without_deletion_requests():
    """Derived tables are only targets in datasets with Glean deletion requests."""
    stable_tables = [
        GleanTableInfo(
            dataset_id="org_mozilla_firefox_stable",
            table_id=table_id,
            labels={"schema_id": "glean_ping_1"},
        )
        for table_id in ["metrics_v1", "deletion_request_v1"]
    ]
    derived_tables = [
        GleanTableInfo(
            dataset_id=dataset_id,
            table_id="clients_daily_v1",
            field_paths=frozenset({"client_id"}),
        )
        for dataset_id in ["org_mozilla_firefox_derived", "telemetry_derived"]
    ]

    targets = build_glean_targets(
        stable_tables,
        derived_tables,
        app_deletion_request_views=set(),
        channel_to_app_name={"org_mozilla_firefox": "fenix"},
    )

    assert {target.table for target in targets} == {
        "org_mozilla_firefox_stable.metrics_v1",
        "org_mozilla_firefox_derived.clients_daily_v1",
    }


def test_field_paths():
    schema = [
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField(
            "client_info",
            "RECORD",
            fields=[bigquery.SchemaField("client_id", "STRING")],
        ),
        bigquery.SchemaField(
            "metrics",
            "RECORD",
            fields=[
                bigquery.SchemaField(
                    "uuid",
                    "RECORD",
                    fields=[bigquery.SchemaField("usage_profile_id", "STRING")],
                )
            ],
        ),
    ]

    assert field_paths(schema) == {
        "client_id",
        "client_info",
        "client_info.client_id",
        "metrics",
        "metrics.uuid",
        "metrics.uuid.usage_profile_id",
    }


@mock.patch("bigquery_etl.cli.utils.requests")
def test_glean_targets_schema_version_and_v1_suffix(mock_requests):
    """Match any glean_ping_ schema version but only include _v1 tables.

    metrics_v1 carries the newer glean_ping_2 schema id and should still be
    picked up. metrics_v2 carries a glean schema id too but must be ignored
    because it does not have the _v1 suffix.
    """

    class SchemaVersionClient:
        def list_datasets(self, project):
            return [
                bigquery.DatasetReference(project, name)
                for name in [
                    "firefox_desktop_stable",
                    "firefox_desktop_derived",
                ]
            ]

        def list_tables(self, dataset_ref):
            if dataset_ref.dataset_id == "firefox_desktop_stable":
                table_ids = [
                    ("metrics_v1", {"schema_id": "glean_ping_2"}),
                    (
                        "metrics_v2",
                        {"schema_id": "glean_ping_2"},
                    ),  # ignored: _v2 suffix
                    ("deletion_request_v1", {"schema_id": "glean_ping_2"}),
                ]
            elif dataset_ref.dataset_id == "firefox_desktop_derived":
                table_ids = ["clients_daily_v1"]
            else:
                raise Exception(f"unexpected dataset: {dataset_ref}")
            return [
                bigquery.table.TableListItem(
                    {
                        "tableReference": bigquery.TableReference(
                            dataset_ref, table_id[0]
                        ).to_api_repr(),
                        "labels": table_id[1],
                    }
                )
                for table_id in [
                    t if isinstance(t, tuple) else (t, {}) for t in table_ids
                ]
            ]

        def get_table(self, table_ref):
            table = bigquery.Table(table_ref)
            table._properties[table._PROPERTY_TO_API_FIELD["type"]] = "TABLE"
            if table.dataset_id.endswith("stable"):
                table.schema = [
                    bigquery.SchemaField(
                        "client_info",
                        "RECORD",
                        fields=[bigquery.SchemaField("client_id", "STRING")],
                    )
                ]
            else:
                table.schema = [bigquery.SchemaField("client_id", "STRING")]
            return table

    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    with ThreadPool(1) as pool:
        targets = find_glean_targets(pool, SchemaVersionClient())

    target_tables = {target.table for target in targets}

    # metrics_v1 has the glean_ping_2 schema id and the _v1 suffix, so it's shredded
    assert "firefox_desktop_stable.metrics_v1" in target_tables
    # metrics_v2 has a glean schema id but the _v2 suffix, so it's ignored
    assert "firefox_desktop_stable.metrics_v2" not in target_tables


@mock.patch("bigquery_etl.cli.utils.requests")
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


@mock.patch("bigquery_etl.cli.utils.requests")
def test_glean_channel_app_mapping(mock_requests):
    mock_response = mock.Mock()
    mock_response.json.return_value = GLEAN_APP_LISTING
    mock_requests.get.return_value = mock_response

    actual = get_glean_app_id_to_app_name_mapping()

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


def test_not_shredded_is_not_a_delete_target():
    """NOT_SHREDDED keys are fully qualified and can't also be configured targets."""
    configured = {f"{target.project}.{target.table}" for target in DELETE_TARGETS}
    for table, reason in NOT_SHREDDED.items():
        assert len(table.split(".")) == 3, f"{table} must be project.dataset.table"
        assert reason, f"{table} needs a reason"
        assert table not in configured, f"{table} is in NOT_SHREDDED and DELETE_TARGETS"


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
