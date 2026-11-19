"""Bind each issues table's schema.yaml to the schema the integration produces.

A load job carries an explicit schema and WRITE_TRUNCATEs, so it rewrites the
table definition. Artifact deployment then rewrites it back from schema.yaml. If
the two disagree, the table's schema flip-flops on every run and nobody can tell
which side is wrong from the table alone. These tests make the two declarations
fail together instead.
"""

import importlib.util
from pathlib import Path

import pytest
import yaml

from bigquery_etl.jira.issues import build_schema

DATASET = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "moz-fx-data-shared-prod"
    / "jira_tickets_derived"
)


def load_extra_fields(table: str):
    """Import a table's query.py and return its EXTRA_FIELDS.

    Importing is safe: every query.py guards execution behind `__main__`.
    """
    path = DATASET / table / "query.py"
    spec = importlib.util.spec_from_file_location(f"{table}_query", path)
    assert spec is not None and spec.loader is not None, f"cannot import {path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, "EXTRA_FIELDS", ())


def issues_tables() -> list[str]:
    """Every table in the dataset whose query.py drives the issues integration.

    Discovered rather than listed: a hardcoded list means the next issues table
    added here silently gets no coverage, and the schema flip-flop this file
    exists to catch would just happen quietly on the new table instead.
    """
    return sorted(
        path.parent.name
        for path in DATASET.glob("*/query.py")
        if "JiraIssueBigQueryIntegration" in path.read_text()
    )


ALL_TABLES = issues_tables()

# The split falls out of whether the module defines EXTRA_FIELDS, so a table
# moves between these by editing its query.py and nothing else.
BASE_ONLY_TABLES = [t for t in ALL_TABLES if not load_extra_fields(t)]
EXTRA_FIELD_TABLES = [t for t in ALL_TABLES if load_extra_fields(t)]


def yaml_schema(table: str) -> list[tuple]:
    """Read schema.yaml as (name, type, mode) triples in declared order."""
    doc = yaml.safe_load((DATASET / table / "schema.yaml").read_text())
    return [(f["name"], f["type"], f.get("mode", "NULLABLE")) for f in doc["fields"]]


def expected_schema(extra_fields=()) -> list[tuple]:
    """Same triples, from the schema the integration would send to BigQuery."""
    return [(f.name, f.field_type, f.mode) for f in build_schema(extra_fields)]


@pytest.mark.parametrize("table", BASE_ONLY_TABLES)
def test_base_only_table_matches_the_base_schema(table):
    assert yaml_schema(table) == expected_schema()


@pytest.mark.parametrize("table", ALL_TABLES)
def test_every_table_starts_with_the_base_schema(table):
    """Extras are appended, never interleaved, so the base is always a prefix."""
    base = expected_schema()
    assert yaml_schema(table)[: len(base)] == base


@pytest.mark.parametrize("table", EXTRA_FIELD_TABLES)
def test_extra_field_table_matches_its_declared_fields(table):
    assert yaml_schema(table) == expected_schema(load_extra_fields(table))


@pytest.mark.parametrize("table", ALL_TABLES)
def test_every_table_has_updated(table):
    assert "updated" in [name for name, _, _ in yaml_schema(table)]


def test_discovery_finds_every_issues_table():
    """Guards the discovery itself: a glob that silently matches nothing passes everything."""
    assert set(ALL_TABLES) >= {
        "ffxp_epic_issues_v1",
        "iim_incident_issues_v1",
        "srein_issues_v1",
    }
    assert BASE_ONLY_TABLES and EXTRA_FIELD_TABLES
    assert set(BASE_ONLY_TABLES) & set(EXTRA_FIELD_TABLES) == set()
