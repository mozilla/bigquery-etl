import copy
from multiprocessing.pool import ThreadPool
from pathlib import Path
from unittest import mock

import pytest
import yaml

from bigquery_etl.cli.utils import get_glean_app_id_to_app_name_mapping
from bigquery_etl.schema.stable_table_schema import SchemaFile
from bigquery_etl.shredder.config import (
    CLIENT_ID,
    GLEAN_CLIENT_ID,
    DeleteSource,
    DeleteTarget,
    find_glean_targets,
)
from bigquery_etl.shredder.offline_targets import (
    find_glean_targets_offline,
    is_table_dir,
    normalize_table_ids,
    schema_id_label,
    stable_table_infos,
)
from tests.shredder.test_config import (
    GLEAN_APP_LISTING,
    FakeClient,
    as_sets,
    fake_client_table_infos,
)

PROJECT = "moz-fx-data-shared-prod"
GLEAN_SCHEMA_ID = "moz://mozilla.org/schemas/glean/ping/1"
GLEAN_MIN_SCHEMA_ID = "moz://mozilla.org/schemas/glean-min/ping/1"
CHANNELS = {"org_mozilla_firefox": "fenix", "org_mozilla_firefox_beta": "fenix"}
CLIENT_ID_FIELD = {"name": "client_id", "type": "STRING", "mode": "NULLABLE"}
USAGE_PROFILE_ID_FIELDS = [
    {
        "name": "metrics",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {
                "name": "uuid",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "usage_profile_id", "type": "STRING", "mode": "NULLABLE"}
                ],
            }
        ],
    }
]


def stable_schema(
    dataset_family, table, schema_id=GLEAN_SCHEMA_ID, fields=(), **kwargs
):
    return SchemaFile(
        schema=list(fields),
        schema_id=schema_id,
        bq_dataset_family=dataset_family,
        bq_table=table,
        document_namespace=dataset_family,
        document_type=table,
        document_version=1,
        **kwargs,
    )


def write_table(sql_dir, dataset_id, table_id, files=("query.sql",), fields=None):
    """Write a table directory with the given files and an optional schema.yaml."""
    table_dir = Path(sql_dir, PROJECT, dataset_id, table_id)
    table_dir.mkdir(parents=True)
    for file_name in files:
        (table_dir / file_name).write_text("SELECT 1")
    if fields is not None:
        (table_dir / "schema.yaml").write_text(yaml.dump({"fields": fields}))
    return table_dir


@pytest.fixture
def schemas():
    return [
        stable_schema(channel, table)
        for channel in CHANNELS
        for table in ["deletion_request_v1", "metrics_v1"]
    ] + [
        stable_schema("org_mozilla_firefox", "sync_v1", include_client_id=False),
        stable_schema("telemetry", "main_v5", schema_id="moz://x/main/ping/4"),
    ]


def channel_sources(*channels):
    return tuple(
        DeleteSource(
            table=f"{channel}_stable.deletion_request_v1", field=GLEAN_CLIENT_ID
        )
        for channel in channels
    )


def test_schema_id_label():
    assert schema_id_label(GLEAN_SCHEMA_ID) == "glean_ping_1"
    assert schema_id_label(GLEAN_MIN_SCHEMA_ID) == "glean-min_ping_1"


def test_stable_table_infos():
    infos = {
        f"{info.dataset_id}.{info.table_id}": info
        for info in stable_table_infos(
            [
                stable_schema(
                    "org_mozilla_firefox", "sync_v1", include_client_id=False
                ),
                stable_schema(
                    "org_mozilla_firefox",
                    "usage_deletion_request_v1",
                    schema_id=GLEAN_MIN_SCHEMA_ID,
                    fields=USAGE_PROFILE_ID_FIELDS,
                ),
                stable_schema(
                    "org_mozilla_firefox", "metrics_v1", fields=[CLIENT_ID_FIELD]
                ),
            ]
        )
    }

    sync = infos["org_mozilla_firefox_stable.sync_v1"]
    assert sync.labels == {"schema_id": "glean_ping_1", "include_client_id": "false"}

    usage = infos["org_mozilla_firefox_stable.usage_deletion_request_v1"]
    assert usage.labels["schema_id"] == "glean-min_ping_1"
    assert "metrics.uuid.usage_profile_id" in usage.field_paths

    # field paths are only needed for usage_deletion_request_v1
    assert infos["org_mozilla_firefox_stable.metrics_v1"].field_paths == frozenset()


@pytest.mark.parametrize(
    "files,expected",
    [
        (["query.sql", "metadata.yaml"], True),
        (["query.py"], True),
        (["script.sql"], True),
        (["part1.sql", "part2.sql"], True),
        # written by something outside the repo
        (["metadata.yaml", "schema.yaml"], True),
        (["view.sql", "metadata.yaml"], False),
        (["materialized_view.sql", "metadata.yaml", "schema.yaml"], False),
        (["checks.sql"], False),
    ],
)
def test_is_table_dir(tmp_path, files, expected):
    for file_name in files:
        (tmp_path / file_name).write_text("")
    assert is_table_dir(tmp_path) is expected


def test_is_table_dir_external_table(tmp_path):
    """External tables aren't tables, like find_glean_targets keeping only TABLE."""
    (tmp_path / "schema.yaml").write_text("")
    (tmp_path / "metadata.yaml").write_text(
        yaml.dump({"external_data": {"format": "csv", "source_uris": ["gs://x/*"]}})
    )
    assert is_table_dir(tmp_path) is False

    (tmp_path / "metadata.yaml").write_text(yaml.dump({"friendly_name": "x"}))
    assert is_table_dir(tmp_path) is True


def test_stable_table_infos_does_not_change_schemas():
    """The cached stable table schemas are shared, so they must not be modified."""
    fields = [
        {
            "name": "metrics",
            "type": "RECORD",
            "fields": [
                {
                    "name": "uuid",
                    "type": "RECORD",
                    "fields": [{"name": "usage_profile_id", "type": "STRING"}],
                }
            ],
        }
    ]
    schema = stable_schema(
        "org_mozilla_firefox",
        "usage_deletion_request_v1",
        schema_id=GLEAN_MIN_SCHEMA_ID,
        fields=copy.deepcopy(fields),
    )

    (info,) = stable_table_infos([schema])

    assert "metrics.uuid.usage_profile_id" in info.field_paths
    assert schema.schema == fields


def test_find_glean_targets_offline(tmp_path, schemas):
    write_table(
        tmp_path, "org_mozilla_firefox_derived", "clients_v1", fields=[CLIENT_ID_FIELD]
    )
    write_table(
        tmp_path,
        "org_mozilla_firefox_derived",
        "client_info_v1",
        fields=[
            {
                "name": "client_info",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [CLIENT_ID_FIELD],
            }
        ],
    )
    write_table(tmp_path, "fenix_derived", "clients_v1", fields=[CLIENT_ID_FIELD])
    # not targets
    write_table(
        tmp_path,
        "org_mozilla_firefox_derived",
        "clients_view",
        files=["view.sql"],
        fields=[CLIENT_ID_FIELD],
    )
    write_table(tmp_path, "org_mozilla_firefox_derived", "no_schema_v1")
    write_table(tmp_path, "telemetry_derived", "clients_v1", fields=[CLIENT_ID_FIELD])

    targets = find_glean_targets_offline(
        tmp_path, schemas=schemas, channel_to_app_name=CHANNELS
    ).targets

    firefox = channel_sources("org_mozilla_firefox")
    assert targets == {
        DeleteTarget(
            table="org_mozilla_firefox_stable.metrics_v1", field=(GLEAN_CLIENT_ID,)
        ): firefox,
        DeleteTarget(
            table="org_mozilla_firefox_beta_stable.metrics_v1", field=(GLEAN_CLIENT_ID,)
        ): channel_sources("org_mozilla_firefox_beta"),
        DeleteTarget(
            table="org_mozilla_firefox_derived.clients_v1", field=(CLIENT_ID,)
        ): firefox,
        DeleteTarget(
            table="org_mozilla_firefox_derived.client_info_v1", field=(GLEAN_CLIENT_ID,)
        ): firefox,
        # no fenix.deletion_request view, so every channel's table is a source
        DeleteTarget(table="fenix_derived.clients_v1", field=(CLIENT_ID,) * 2): (
            channel_sources("org_mozilla_firefox", "org_mozilla_firefox_beta")
        ),
    }


def test_find_glean_targets_offline_app_view(tmp_path, schemas):
    write_table(tmp_path, "fenix_derived", "clients_v1", fields=[CLIENT_ID_FIELD])
    write_table(tmp_path, "fenix", "deletion_request", files=["view.sql"])

    targets = find_glean_targets_offline(
        tmp_path, schemas=schemas, channel_to_app_name=CHANNELS
    ).targets

    assert targets[
        DeleteTarget(table="fenix_derived.clients_v1", field=(CLIENT_ID,))
    ] == (
        DeleteSource(table="fenix.deletion_request", field=GLEAN_CLIENT_ID),
    )


def test_find_glean_targets_offline_schema_include(tmp_path, schemas):
    """schema.yaml includes are resolved against the given sql directory."""
    write_table(
        tmp_path, "org_mozilla_firefox_derived", "base_v1", fields=[CLIENT_ID_FIELD]
    )
    table_dir = write_table(tmp_path, "org_mozilla_firefox_derived", "copy_v1")
    (table_dir / "schema.yaml").write_text(
        "fields: !include\n"
        f"  file: sql/{PROJECT}/org_mozilla_firefox_derived/base_v1/schema.yaml\n"
        "  jmespath: fields\n"
    )

    targets = find_glean_targets_offline(
        tmp_path, schemas=schemas, channel_to_app_name=CHANNELS
    ).targets

    assert (
        DeleteTarget(table="org_mozilla_firefox_derived.copy_v1", field=(CLIENT_ID,))
        in targets
    )


def test_find_glean_targets_offline_other_project(tmp_path, schemas):
    """Only moz-fx-data-shared-prod has Glean stable tables."""
    targets = find_glean_targets_offline(
        tmp_path,
        project="moz-fx-data-marketing-prod",
        schemas=schemas,
        channel_to_app_name=CHANNELS,
    ).targets
    assert targets == {}


@pytest.fixture
def mock_dry_run():
    with (
        mock.patch("bigquery_etl.shredder.offline_targets.get_id_token"),
        mock.patch("bigquery_etl.shredder.offline_targets.DryRun") as dry_run,
    ):
        dry_run.return_value.get_schema.return_value = {"fields": [CLIENT_ID_FIELD]}
        yield dry_run


def dry_run_files(dry_run):
    return sorted(Path(call.args[0]).parent.name for call in dry_run.call_args_list)


def test_dry_run_table_without_schema(tmp_path, schemas, mock_dry_run):
    """Tables in dry_run_tables without a schema.yaml get columns from a dry run."""
    write_table(tmp_path, "org_mozilla_firefox_derived", "generated_v1")
    write_table(tmp_path, "org_mozilla_firefox_derived", "not_requested_v1")

    targets = find_glean_targets_offline(
        tmp_path,
        schemas=schemas,
        channel_to_app_name=CHANNELS,
        dry_run_tables={"org_mozilla_firefox_derived.generated_v1"},
    ).targets

    assert dry_run_files(mock_dry_run) == ["generated_v1"]
    target_tables = {target.table for target in targets}
    assert "org_mozilla_firefox_derived.generated_v1" in target_tables
    assert "org_mozilla_firefox_derived.not_requested_v1" not in target_tables


def test_dry_run_not_used_with_schema(tmp_path, schemas, mock_dry_run):
    """schema.yaml is used when it exists, and tables without query.sql aren't dry run."""
    write_table(
        tmp_path,
        "org_mozilla_firefox_derived",
        "with_schema_v1",
        fields=[CLIENT_ID_FIELD],
    )
    write_table(
        tmp_path, "org_mozilla_firefox_derived", "python_v1", files=["query.py"]
    )

    targets = find_glean_targets_offline(
        tmp_path,
        schemas=schemas,
        channel_to_app_name=CHANNELS,
        dry_run_tables={
            "org_mozilla_firefox_derived.with_schema_v1",
            "org_mozilla_firefox_derived.python_v1",
        },
    ).targets

    mock_dry_run.assert_not_called()
    assert {target.table for target in targets} >= {
        "org_mozilla_firefox_derived.with_schema_v1"
    }
    assert "org_mozilla_firefox_derived.python_v1" not in {
        target.table for target in targets
    }


@pytest.mark.parametrize(
    "get_schema,expected_reason",
    [
        (
            mock.Mock(side_effect=Exception("invalid query")),
            "dry run failed: invalid query",
        ),
        # e.g. queries on the dry run skip list
        (mock.Mock(return_value={}), "dry run returned no schema"),
    ],
)
def test_dry_run_failure(tmp_path, schemas, mock_dry_run, get_schema, expected_reason):
    """A query that can't be dry run is unresolved, not silently a non-target."""
    write_table(tmp_path, "org_mozilla_firefox_derived", "broken_v1")
    mock_dry_run.return_value.get_schema = get_schema

    result = find_glean_targets_offline(
        tmp_path,
        schemas=schemas,
        channel_to_app_name=CHANNELS,
        dry_run_tables={"org_mozilla_firefox_derived.broken_v1"},
    )

    assert "org_mozilla_firefox_derived.broken_v1" not in {
        target.table for target in result.targets
    }
    assert expected_reason in result.unresolved["org_mozilla_firefox_derived.broken_v1"]


def test_dry_run_without_id_token(tmp_path, schemas):
    """Without a dry run identity, dry run tables are unresolved and the rest resolve."""
    write_table(tmp_path, "org_mozilla_firefox_derived", "generated_v1")
    write_table(
        tmp_path, "org_mozilla_firefox_derived", "resolved_v1", fields=[CLIENT_ID_FIELD]
    )

    with (
        mock.patch(
            "bigquery_etl.shredder.offline_targets.get_id_token",
            side_effect=Exception("no credentials"),
        ),
        mock.patch("bigquery_etl.shredder.offline_targets.DryRun") as dry_run,
    ):
        result = find_glean_targets_offline(
            tmp_path,
            schemas=schemas,
            channel_to_app_name=CHANNELS,
            dry_run_tables={"org_mozilla_firefox_derived.generated_v1"},
        )

    dry_run.assert_not_called()
    assert result.unresolved == {
        "org_mozilla_firefox_derived.generated_v1": (
            "no schema.yaml, could not get dry run id token: no credentials"
        )
    }
    assert (
        DeleteTarget(
            table="org_mozilla_firefox_derived.resolved_v1", field=(CLIENT_ID,)
        )
        in result.targets
    )


def test_unresolved_tables(tmp_path, schemas, mock_dry_run):
    """Tables whose columns can't be determined are reported with the reason."""
    write_table(tmp_path, "org_mozilla_firefox_derived", "not_requested_v1")
    write_table(
        tmp_path,
        "org_mozilla_firefox_derived",
        "metadata_only_v1",
        files=["metadata.yaml"],
    )
    write_table(
        tmp_path, "org_mozilla_firefox_derived", "resolved_v1", fields=[CLIENT_ID_FIELD]
    )
    broken_dir = write_table(
        tmp_path, "org_mozilla_firefox_derived", "broken_include_v1"
    )
    (broken_dir / "schema.yaml").write_text(
        "fields: !include\n  file: sql/missing/schema.yaml\n"
    )

    result = find_glean_targets_offline(
        tmp_path,
        schemas=schemas,
        channel_to_app_name=CHANNELS,
        dry_run_tables={"org_mozilla_firefox_derived.metadata_only_v1"},
    )

    mock_dry_run.assert_not_called()
    assert set(result.unresolved) == {
        "org_mozilla_firefox_derived.not_requested_v1",
        "org_mozilla_firefox_derived.metadata_only_v1",
        "org_mozilla_firefox_derived.broken_include_v1",
    }
    unresolved = result.unresolved
    assert unresolved["org_mozilla_firefox_derived.not_requested_v1"] == (
        "no schema.yaml, not in dry_run_tables"
    )
    assert unresolved["org_mozilla_firefox_derived.metadata_only_v1"] == (
        "no schema.yaml, no query.sql to dry run"
    )
    assert unresolved["org_mozilla_firefox_derived.broken_include_v1"].startswith(
        "schema.yaml could not be loaded"
    )


def test_dry_run_when_schema_fails_to_load(tmp_path, schemas, mock_dry_run):
    """A schema.yaml that can't be loaded falls back to a dry run if requested."""
    table_dir = write_table(
        tmp_path, "org_mozilla_firefox_derived", "broken_include_v1"
    )
    (table_dir / "schema.yaml").write_text(
        "fields: !include\n  file: sql/missing/schema.yaml\n"
    )

    result = find_glean_targets_offline(
        tmp_path,
        schemas=schemas,
        channel_to_app_name=CHANNELS,
        dry_run_tables={"org_mozilla_firefox_derived.broken_include_v1"},
    )

    assert dry_run_files(mock_dry_run) == ["broken_include_v1"]
    assert result.unresolved == {}
    assert (
        DeleteTarget(
            table="org_mozilla_firefox_derived.broken_include_v1", field=(CLIENT_ID,)
        )
        in result.targets
    )


def test_dry_run_tables_fully_qualified(tmp_path, schemas, mock_dry_run):
    """dry_run_tables accepts project.dataset.table for the target project."""
    write_table(tmp_path, "org_mozilla_firefox_derived", "generated_v1")

    result = find_glean_targets_offline(
        tmp_path,
        schemas=schemas,
        channel_to_app_name=CHANNELS,
        dry_run_tables={f"{PROJECT}.org_mozilla_firefox_derived.generated_v1"},
    )

    assert dry_run_files(mock_dry_run) == ["generated_v1"]
    assert result.unresolved == {}


@pytest.mark.parametrize(
    "table_id",
    [
        "other-project.org_mozilla_firefox_derived.generated_v1",
        "generated_v1",
        "a.b.c.d",
    ],
)
def test_normalize_table_ids_rejects_other_forms(table_id):
    with pytest.raises(ValueError):
        normalize_table_ids([table_id], PROJECT)


def test_normalize_table_ids():
    assert normalize_table_ids(
        ["dataset.table_v1", f"{PROJECT}.dataset.table_v2"], PROJECT
    ) == {"dataset.table_v1", "dataset.table_v2"}


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
    ).targets

    assert as_sets(actual) == as_sets(expected)


@pytest.mark.parametrize("has_view", [True, False])
def test_find_glean_targets_offline_app_usage_view(tmp_path, schemas, has_view):
    """App-level usage tables use the app's usage_deletion_request view if it exists."""
    usage_schemas = [
        stable_schema(
            channel,
            "usage_deletion_request_v1",
            schema_id=GLEAN_MIN_SCHEMA_ID,
            fields=USAGE_PROFILE_ID_FIELDS,
        )
        for channel in CHANNELS
    ]
    write_table(
        tmp_path,
        "fenix_derived",
        "usage_clients_v1",
        fields=[{"name": "usage_profile_id", "type": "STRING", "mode": "NULLABLE"}],
    )
    if has_view:
        write_table(tmp_path, "fenix", "usage_deletion_request", files=["view.sql"])

    targets = find_glean_targets_offline(
        tmp_path, schemas=schemas + usage_schemas, channel_to_app_name=CHANNELS
    ).targets

    usage_targets = {
        target: set(sources)
        for target, sources in targets.items()
        if target.table == "fenix_derived.usage_clients_v1"
    }
    if has_view:
        expected_tables = {"fenix.usage_deletion_request"}
    else:
        expected_tables = {
            f"{channel}_stable.usage_deletion_request_v1" for channel in CHANNELS
        }
    ((target, sources),) = usage_targets.items()
    assert {source.table for source in sources} == expected_tables
    assert {source.field for source in sources} == {"metrics.uuid.usage_profile_id"}
    assert target.fields == ("usage_profile_id",) * len(expected_tables)


def test_usage_only_channel(tmp_path, schemas):
    """A channel with a usage_deletion_request ping but no deletion_request ping.

    Its derived usage_profile_id tables are usage targets, but its client_id tables
    have no deletion requests and so aren't targets.
    """
    usage_schemas = [
        stable_schema(
            "org_mozilla_usage_only",
            "usage_deletion_request_v1",
            schema_id=GLEAN_MIN_SCHEMA_ID,
            fields=USAGE_PROFILE_ID_FIELDS,
        )
    ]
    write_table(
        tmp_path,
        "org_mozilla_usage_only_derived",
        "usage_clients_v1",
        fields=[{"name": "usage_profile_id", "type": "STRING", "mode": "NULLABLE"}],
    )
    write_table(
        tmp_path,
        "org_mozilla_usage_only_derived",
        "clients_v1",
        fields=[CLIENT_ID_FIELD],
    )

    targets = find_glean_targets_offline(
        tmp_path,
        schemas=schemas + usage_schemas,
        channel_to_app_name={**CHANNELS, "org_mozilla_usage_only": "usage_only"},
    ).targets

    assert targets[
        DeleteTarget(
            table="org_mozilla_usage_only_derived.usage_clients_v1",
            field=("usage_profile_id",),
        )
    ] == (
        DeleteSource(
            table="org_mozilla_usage_only_stable.usage_deletion_request_v1",
            field="metrics.uuid.usage_profile_id",
        ),
    )
    assert "org_mozilla_usage_only_derived.clients_v1" not in {
        target.table for target in targets
    }
