"""Tests for the plausible_external loads.

Each table's `SELECT_SQL` writes into a partition of a table whose schema comes
from its `schema.yaml`, and BigQuery matches those columns *by position*. So the
projection and the schema file have to agree on names and order, and nothing
else checks that: these are `query.py` tables, so the SQL never goes through
`bqetl query validate`.

A mismatch fails loudly only when the types differ. Two adjacent columns of the
same type -- `referrer` and `referrer_source`, or any pair of the many STRING
columns -- would silently swap values instead.
"""

import importlib.util
from pathlib import Path

import pytest
import yaml
from sqlglot import exp, parse_one

_repo_root = Path(__file__).resolve().parent.parent.parent
_dataset_dir = _repo_root / "sql/moz-fx-data-shared-prod/plausible_external"

# In CI the sql/ directory may be replaced with generated SQL, which removes
# Python query files.
if not _dataset_dir.exists():
    pytest.skip(
        "plausible_external not available (sql/ replaced in CI)",
        allow_module_level=True,
    )

TABLES = ["events_v1", "sessions_v1"]


def _load_query_module(table):
    """Import a table's query.py by path."""
    path = _dataset_dir / table / "query.py"
    spec = importlib.util.spec_from_file_location(f"plausible_{table}_query", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _projected_columns(select_sql):
    """Return the column names a SELECT projects, in order.

    Resolves `SELECT * EXCEPT (...)` back through the CTE it selects from, which
    is how sessions_v1 drops its row_number helper column.
    """
    tree = parse_one(
        select_sql.format(tmp_table="project.tmp.staged"), dialect="bigquery"
    )
    select = tree if isinstance(tree, exp.Select) else tree.this

    star = next(
        (e for e in select.expressions if isinstance(e, exp.Star)),
        None,
    )
    if star is None:
        return [e.alias_or_name for e in select.expressions]

    # A star that also renames columns would defeat this resolution, so refuse
    # to guess rather than report a projection that isn't the real one.
    assert not star.args.get("rename"), "SELECT * RENAME is not supported by this test"

    excluded = {c.name for c in star.args.get("except_") or []}
    ctes = list(tree.find_all(exp.CTE))
    assert ctes, "a star projection must come from a CTE for this test to resolve it"
    return [
        e.alias_or_name
        for e in ctes[0].this.expressions
        if e.alias_or_name not in excluded
    ]


def _schema_columns(table):
    """Return the column names in a table's schema.yaml, in order."""
    schema = yaml.safe_load((_dataset_dir / table / "schema.yaml").read_text())
    return [field["name"] for field in schema["fields"]]


@pytest.mark.parametrize("table", TABLES)
def test_projection_matches_schema_order(table):
    """The SELECT must project schema.yaml's columns, by name and in order."""
    projected = _projected_columns(_load_query_module(table).SELECT_SQL)
    assert projected == _schema_columns(table), (
        f"{table}: SELECT_SQL projection does not match schema.yaml. BigQuery "
        "matches columns positionally, so this either fails the load or, for "
        "two columns of the same type, silently swaps their values."
    )


@pytest.mark.parametrize("table", TABLES)
def test_no_duplicate_projected_columns(table):
    """A repeated alias would make the positional mapping ambiguous."""
    projected = _projected_columns(_load_query_module(table).SELECT_SQL)
    assert len(projected) == len(set(projected)), f"{table}: duplicate column alias"


@pytest.mark.parametrize("table", TABLES)
def test_backfill_entrypoint_is_callable(table):
    """`bqetl query backfill` resolves a module-level callable, not __main__."""
    main = getattr(_load_query_module(table), "main", None)
    assert callable(main), (
        f"{table}: query.py must expose a module-level main() for "
        "`bqetl query backfill --query-script-entrypoint main`."
    )


def _load_module():
    """Import the shared loader."""
    spec = importlib.util.spec_from_file_location(
        "plausible_load", _dataset_dir / "load.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_destination_defaults_to_the_owned_table():
    """Without an override, a run writes its own table's dated partition."""
    load = _load_module()
    args = load.parse_args("t", ["--date", "2026-08-05"])
    assert (
        load.resolve_destination(args, "events_v1")
        == "moz-fx-data-shared-prod.plausible_external.events_v1$20260805"
    )


def test_destination_table_override_is_honoured():
    """`bqetl backfill` redirects the run into backfills_staging_derived.

    It passes the staging table as a fully qualified --destination-table, and
    the date decorator still has to be appended so each date fills its own
    partition of the staging table.
    """
    load = _load_module()
    staging = (
        "moz-fx-data-shared-prod.backfills_staging_derived"
        ".plausible_external__events_v1_2026_08_20"
    )
    args = load.parse_args(
        "t", ["--date", "2026-08-05", f"--destination-table={staging}"]
    )
    assert load.resolve_destination(args, "events_v1") == f"{staging}$20260805"


def test_destination_table_must_be_fully_qualified():
    """A bare table name would silently write to the wrong place."""
    load = _load_module()
    args = load.parse_args("t", ["--date", "2026-08-05", "--destination-table=events"])
    with pytest.raises(ValueError, match="fully qualified"):
        load.resolve_destination(args, "events_v1")
