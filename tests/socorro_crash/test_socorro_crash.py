"""Tests for the schema-unwrapping logic in socorro_crash_v2/query.py.

query.py loads the raw JSON with a "natural" schema derived from schema.yaml:
the wrapped-array fields (RECORD<list: ARRAY<RECORD<element>>>) are flattened
back to the plain arrays the source JSON actually contains, and crash_date is
dropped. transform.sql then rewraps them. These tests cover that flattening.
"""

import importlib.util
from pathlib import Path

import pytest

from bigquery_etl.schema import Schema

QUERY_DIR = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "moz-fx-data-shared-prod"
    / "telemetry_derived"
    / "socorro_crash_v2"
)
QUERY_PY = QUERY_DIR / "query.py"

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not QUERY_PY.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)


def load_query_module():
    # Load the module dynamically since the path contains hyphens
    spec = importlib.util.spec_from_file_location("socorro_crash_query", QUERY_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def query():
    return load_query_module()


def field_by_name(fields, name):
    for f in fields:
        if f.name == name:
            return f
    raise KeyError(name)


class TestUnwrap:
    def test_scalar_field_unchanged(self, query):
        field = {"name": "uuid", "type": "STRING", "mode": "NULLABLE"}
        assert query._unwrap(field) == field

    def test_wrapped_scalar_array_becomes_repeated_scalar(self, query):
        # addons: RECORD<list: ARRAY<RECORD<element: STRING>>> -> REPEATED STRING
        field = {
            "name": "addons",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "list",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {"name": "element", "type": "STRING", "mode": "NULLABLE"}
                    ],
                }
            ],
        }
        assert query._unwrap(field) == {
            "name": "addons",
            "type": "STRING",
            "mode": "REPEATED",
        }

    def test_wrapped_record_array_becomes_repeated_record(self, query):
        # modules: RECORD<list: ARRAY<RECORD<element: RECORD<filename>>>>
        field = {
            "name": "modules",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "list",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "element",
                            "type": "RECORD",
                            "mode": "NULLABLE",
                            "fields": [
                                {
                                    "name": "filename",
                                    "type": "STRING",
                                    "mode": "NULLABLE",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        unwrapped = query._unwrap(field)
        assert unwrapped["name"] == "modules"
        assert unwrapped["type"] == "RECORD"
        assert unwrapped["mode"] == "REPEATED"
        assert [f["name"] for f in unwrapped["fields"]] == ["filename"]

    def test_nested_wrapped_array_is_unwrapped_recursively(self, query):
        # json_dump.threads holds threads, each with a nested wrapped frames array.
        threads = {
            "name": "threads",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "list",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "element",
                            "type": "RECORD",
                            "mode": "NULLABLE",
                            "fields": [
                                {
                                    "name": "frames",
                                    "type": "RECORD",
                                    "mode": "NULLABLE",
                                    "fields": [
                                        {
                                            "name": "list",
                                            "type": "RECORD",
                                            "mode": "REPEATED",
                                            "fields": [
                                                {
                                                    "name": "element",
                                                    "type": "RECORD",
                                                    "mode": "NULLABLE",
                                                    "fields": [
                                                        {
                                                            "name": "frame",
                                                            "type": "INTEGER",
                                                            "mode": "NULLABLE",
                                                        }
                                                    ],
                                                }
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        unwrapped = query._unwrap(threads)
        assert unwrapped["type"] == "RECORD"
        assert unwrapped["mode"] == "REPEATED"
        frames = unwrapped["fields"][0]
        assert frames["name"] == "frames"
        assert frames["type"] == "RECORD"
        # The nested frames array is itself flattened to a REPEATED record.
        assert frames["mode"] == "REPEATED"
        assert [f["name"] for f in frames["fields"]] == ["frame"]


class TestLoadSourceSchema:
    def test_drops_crash_date_and_unwraps(self, query):
        schema = query.load_source_schema()
        names = {f.name for f in schema}
        # crash_date is injected by the transform, not loaded from JSON.
        assert "crash_date" not in names
        # addons is a wrapped scalar array in the table; loaded as REPEATED STRING.
        addons = field_by_name(schema, "addons")
        assert addons.field_type == "STRING"
        assert addons.mode == "REPEATED"

    def test_matches_schema_yaml_minus_crash_date(self, query):
        # Read via Schema, not yaml.safe_load: schema.yaml uses !include-field-description
        # and the other include tags, which the plain YAML loader cannot construct.
        schema_yaml = QUERY_DIR / "schema.yaml"
        table_fields = Schema.from_schema_file(schema_yaml).schema["fields"]
        expected = {f["name"] for f in table_fields} - {"crash_date"}
        loaded = {f.name for f in query.load_source_schema()}
        assert loaded == expected

    def test_loose_int_fields_load_as_string(self, query):
        schema = query.load_source_schema()
        for name in query.LOOSE_INT_FIELDS:
            assert field_by_name(schema, name).field_type == "STRING", name

    def test_other_int_fields_still_load_as_int64(self, query):
        # Only LOOSE_INT_FIELDS are widened; the rest keep the table's type so
        # values above 2^31 (notably memory_measures) still load as integers.
        schema = query.load_source_schema()
        assert field_by_name(schema, "hang_type").field_type == "INT64"
        memory_measures = field_by_name(schema, "memory_measures")
        assert field_by_name(memory_measures.fields, "vsize").field_type == "INT64"


class TestLoosen:
    def test_loose_field_becomes_string(self, query):
        field = {"name": "install_age", "type": "INT64", "mode": "NULLABLE"}
        assert query._loosen(field) == {
            "name": "install_age",
            "type": "STRING",
            "mode": "NULLABLE",
        }

    def test_other_field_unchanged(self, query):
        field = {"name": "hang_type", "type": "INT64", "mode": "NULLABLE"}
        assert query._loosen(field) == field

    def test_transform_safe_casts_every_loose_field(self, query):
        # A field loaded as STRING must be cast back to INT64 in the transform,
        # otherwise the write fails on a type mismatch with the table.
        transform = (QUERY_DIR / "transform.sql").read_text()
        for name in query.LOOSE_INT_FIELDS:
            assert f"SAFE_CAST({name} AS INT64) AS {name}" in transform, name
