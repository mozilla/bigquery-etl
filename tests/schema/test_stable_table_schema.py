import io
import json
import tarfile
from unittest import mock

import pytest

from bigquery_etl.schema import stable_table_schema
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas


def _schemas_tarball(schemas: dict[str, dict]) -> bytes:
    """Build an MPS-like tarball from {namespace/doctype/doctype.N: pipeline metadata}."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for path, pipeline_meta in schemas.items():
            for suffix, content in (
                (".schema.json", {"mozPipelineMetadata": pipeline_meta}),
                (".bq", []),
            ):
                data = json.dumps(content).encode()
                info = tarfile.TarInfo(f"mps/schemas/{path}{suffix}")
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


@pytest.fixture
def stable_table_schemas():
    tarball = _schemas_tarball(
        {
            "org-mozilla-firefox/metrics/metrics.1": {
                "bq_dataset_family": "org_mozilla_firefox",
                "bq_table": "metrics_v1",
                "include_client_id": True,
            },
            "org-mozilla-firefox/sync/sync.1": {
                "bq_dataset_family": "org_mozilla_firefox",
                "bq_table": "sync_v1",
                "include_client_id": False,
            },
            "telemetry/main/main.5": {
                "bq_dataset_family": "telemetry",
                "bq_table": "main_v5",
            },
        }
    )

    get_stable_table_schemas.cache_clear()
    with (
        mock.patch.object(
            stable_table_schema,
            "prod_schemas_uri",
            return_value="https://example.com/archive/abc.tar.gz",
        ),
        mock.patch.object(
            stable_table_schema, "get_private_cache_dir", return_value=None
        ),
        mock.patch.object(stable_table_schema.DryRun, "clear_cache"),
        mock.patch.object(
            stable_table_schema.urllib.request,
            "urlopen",
            return_value=io.BytesIO(tarball),
        ),
    ):
        yield {schema.stable_table: schema for schema in get_stable_table_schemas()}
    get_stable_table_schemas.cache_clear()


def test_pipeline_metadata_fields(stable_table_schemas):
    metrics = stable_table_schemas["org_mozilla_firefox_stable.metrics_v1"]
    assert metrics.include_client_id is True

    sync = stable_table_schemas["org_mozilla_firefox_stable.sync_v1"]
    assert sync.include_client_id is False


def test_pipeline_metadata_defaults(stable_table_schemas):
    """Schemas without include_client_id default to true, like the shredder's label default."""
    main = stable_table_schemas["telemetry_stable.main_v5"]
    assert main.include_client_id is True


def test_cache_file_is_versioned(tmp_path):
    """Pickles written before SchemaFile changed aren't loaded."""
    tarball = _schemas_tarball(
        {
            "telemetry/main/main.5": {
                "bq_dataset_family": "telemetry",
                "bq_table": "main_v5",
            },
        }
    )
    # a cache file at the unversioned path, as written by older code
    (tmp_path / "schemas_abc.pkl").write_bytes(b"not a pickle")

    get_stable_table_schemas.cache_clear()
    with (
        mock.patch.object(
            stable_table_schema,
            "prod_schemas_uri",
            return_value="https://example.com/archive/abc.tar.gz",
        ),
        mock.patch.object(
            stable_table_schema, "get_private_cache_dir", return_value=tmp_path
        ),
        mock.patch.object(stable_table_schema.DryRun, "clear_cache"),
        mock.patch.object(
            stable_table_schema.urllib.request,
            "urlopen",
            return_value=io.BytesIO(tarball),
        ) as urlopen,
    ):
        schemas = get_stable_table_schemas()
    get_stable_table_schemas.cache_clear()

    urlopen.assert_called_once()
    assert [schema.stable_table for schema in schemas] == ["telemetry_stable.main_v5"]
    assert (
        tmp_path / f"schemas_abc_v{stable_table_schema.SCHEMA_CACHE_VERSION}.pkl"
    ).exists()
