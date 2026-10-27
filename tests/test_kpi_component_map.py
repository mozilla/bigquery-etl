"""Checks on the seed data for search_derived.kpi_component_map_v1.

search_kpi_clients_daily_v1 joins clicks to this map on result_type and date, so a
mistake here silently changes the KPI instead of failing a build.
"""

import csv
import datetime
import re
from collections import defaultdict
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parent.parent
TABLE_DIR = (
    ROOT / "sql" / "moz-fx-data-shared-prod" / "search_derived" / "kpi_component_map_v1"
)
DATA_PATH = (TABLE_DIR / "data.csv").relative_to(ROOT)
METADATA_PATH = (TABLE_DIR / "metadata.yaml").relative_to(ROOT)
UDF_PATH = ROOT / "sql" / "mozfun" / "norm" / "result_type_to_product_name" / "udf.sql"
COMPONENTS = {"suggest_sponsored", "suggest_nonsponsored", "ai_chat", "excluded"}
FAR_FUTURE = datetime.date(9999, 12, 31)


@pytest.fixture(scope="module")
def fields():
    """Return the schema fields."""
    return yaml.safe_load((TABLE_DIR / "schema.yaml").read_text())["fields"]


@pytest.fixture(scope="module")
def rows():
    """Return the rows of data.csv."""
    with open(TABLE_DIR / "data.csv", newline="") as f:
        return list(csv.DictReader(f))


def _date(value):
    return datetime.date.fromisoformat(value) if value else None


def _udf_products():
    """Return {raw name: product name} from the UDF's `IN (...)` lists."""
    udf = UDF_PATH.read_text()
    branches = re.findall(r"\bIN\s*\(([^)]*)\)\s*THEN\s+(\S+)", udf)
    assert len(branches) == len(re.findall(r"\bIN\s*\(", udf)), (
        f"Some `IN (...)` list in {UDF_PATH.relative_to(ROOT)} isn't followed by"
        " THEN; if the UDF's layout changed, update the parsing here."
    )
    products = {}
    for in_list, then in branches:
        assert then == "res" or re.fullmatch(r"'[^']*'", then), (
            f"{UDF_PATH.relative_to(ROOT)} has `THEN {then}`. This module only"
            " understands a quoted name or `res`; update the parsing here."
        )
        for name in re.findall(r"'([^']+)'", in_list):
            products[name] = name if then == "res" else then.strip("'")
    return products


def test_columns_match_schema(fields, rows):
    """Columns are in schema order, required ones are filled and values parse."""
    names = [field["name"] for field in fields]
    assert list(rows[0].keys()) == names, (
        f"The header of {DATA_PATH} must list the columns in the same order as"
        f" schema.yaml: {','.join(names)}. bqetl loads the CSV by position, so a"
        " column out of place loads into the wrong field."
    )
    for line, row in enumerate(rows, start=2):
        for field in fields:
            value = row[field["name"]]
            where = f"{DATA_PATH} line {line} ({row['result_type']}), {field['name']}"
            if field["mode"] == "REQUIRED":
                assert value, f"{where} is empty but required (see schema.yaml)."
            try:
                if field["type"] == "DATE":
                    _date(value)
                elif field["type"] == "INTEGER":
                    int(value)
            except ValueError:
                pytest.fail(
                    f"{where} is {value!r}, which isn't a valid {field['type']}"
                    " (dates are YYYY-MM-DD)."
                )


def test_components_are_known(rows):
    """Every component is one the KPI knows; a typo would drop out of every total."""
    unknown = {
        row["result_type"]: row["component"]
        for row in rows
        if row["component"] not in COMPONENTS
    }
    assert not unknown, (
        f"Unknown component in {DATA_PATH}: {unknown}. Use one of"
        f" {', '.join(sorted(COMPONENTS))}. search_kpi_clients_daily_v1 sums clicks"
        " by these exact names, so any other value (even a typo) silently drops"
        " those clicks from the KPI."
    )


def test_periods_do_not_overlap(rows):
    """A result type matches at most one row on any date, so no click counts twice."""
    periods = defaultdict(list)
    for row in rows:
        try:
            start = _date(row["effective_from"])
            end = _date(row["effective_to"]) or FAR_FUTURE
        except ValueError:
            continue  # test_columns_match_schema reports bad dates

        assert start < end, (
            f"{row['result_type']} in {DATA_PATH} has effective_to"
            f" ({row['effective_to']}) on or before effective_from"
            f" ({row['effective_from']}), so the row never applies. effective_to is"
            " exclusive: it's the first date the row no longer applies."
        )
        periods[row["result_type"]].append((start, end))
    for result_type, spans in periods.items():
        spans.sort()
        for (_, end), (next_start, _) in zip(spans, spans[1:]):
            assert end <= next_start, (
                f"{result_type} has two rows in {DATA_PATH} that both apply on"
                f" {next_start}. search_kpi_clients_daily_v1 joins on result_type"
                " and date, so its clicks would be counted twice. To change how a"
                " result type counts from some date, set the old row's"
                " effective_to to the new row's effective_from."
            )


def test_every_udf_result_type_is_mapped(rows):
    """Every raw name in result_type_to_product_name has a row in the map.

    Only the names in the UDF's `IN (...)` lists can be checked. Its `LIKE` branches
    match names nobody can list in advance; test_udf_like_branches_are_known covers
    those.
    """
    udf_names = set(_udf_products())
    assert udf_names, (
        f"Found no result type names in {UDF_PATH.relative_to(ROOT)}. This test"
        " reads the names from its `IN ('...')` lists; if the UDF's layout"
        " changed, update the parsing here."
    )
    missing = sorted(udf_names - {row["result_type"] for row in rows})
    next_version = max(int(row["definition_version"]) for row in rows) + 1
    assert not missing, (
        f"These result types are in mozfun.norm.result_type_to_product_name but"
        f" have no row in the search KPI component map: {', '.join(missing)}.\n\n"
        "Why: search_kpi_clients_daily_v1 looks up every clicked urlbar result type"
        " in this map to decide whether it counts toward the search KPI. A clicked"
        " type with no row fails its daily build.\n\n"
        f"To fix, add one row per name to {DATA_PATH}:\n"
        "  result_type          the raw name\n"
        "  product_result_type  what the UDF returns for it\n"
        "  component            suggest_sponsored, suggest_nonsponsored, ai_chat"
        " or excluded\n"
        "                       (excluded unless it's a Suggest result that"
        " replaces a search)\n"
        "  effective_from       1970-01-01\n"
        "  effective_to         leave empty\n"
        "  reason               one sentence on why it counts or doesn't\n"
        f"  definition_version   {next_version}\n\n"
        "Not sure which component? Ask the people listed under `owners:` in"
        f" {METADATA_PATH}.\n\n"
        "This only covers names in the UDF's `IN (...)` lists, not ones matched by"
        " its `LIKE` branches."
    )


def test_product_result_type_matches_udf(rows):
    """product_result_type is what result_type_to_product_name returns."""
    products = _udf_products()
    stale = [
        f"  {row['result_type']}: {row['product_result_type']} in the map,"
        f" {products[row['result_type']]} in the UDF"
        for row in rows
        if row["result_type"] in products
        and row["product_result_type"] != products[row["result_type"]]
    ]
    assert not stale, (
        f"product_result_type in {DATA_PATH} no longer matches"
        " mozfun.norm.result_type_to_product_name for these result types:\n"
        + "\n".join(stale)
        + "\n\nThe column copies the UDF's output so readers see the same label"
        " everywhere. Update it to what the UDF returns now, and bump"
        " definition_version on the changed rows as for any change to the map."
    )


def test_udf_like_branches_are_known():
    """The UDF's only LIKE branch is the one this module knows how to check."""
    patterns = set(re.findall(r"\bLIKE\s*\(?\s*'([^']+)'", UDF_PATH.read_text()))
    assert patterns == {"rust_%_opt_in"}, (
        f"{UDF_PATH.relative_to(ROOT)} now has these LIKE patterns:"
        f" {', '.join(sorted(patterns)) or 'none'}. Names matched by a pattern can't"
        " be listed, so test_every_udf_result_type_is_mapped can't check them, and"
        " a newly clicked one only fails later, in search_kpi_clients_daily_v1's"
        " unmapped-type check. Decide which component the pattern's names belong"
        f" to, add rows to {DATA_PATH} for the ones seen so far, and add a check"
        " like test_rust_opt_in_rows_are_excluded."
    )


def test_rust_opt_in_rows_are_excluded(rows):
    """Rows matched by the UDF's rust_%_opt_in branch are excluded consent prompts."""
    for row in rows:
        if not re.fullmatch(r"rust_.*_opt_in", row["result_type"]):
            continue
        assert row["component"] == "excluded", (
            f"{row['result_type']} in {DATA_PATH} is a Suggest opt-in prompt, which"
            " is a consent prompt and not a result, so its component must be"
            f" excluded, not {row['component']}."
        )
        assert row["product_result_type"] == row["result_type"][len("rust_") :], (
            f"{row['result_type']} in {DATA_PATH} has product_result_type"
            f" {row['product_result_type']}, but the UDF's rust_%_opt_in branch"
            " returns the name without its rust_ prefix:"
            f" {row['result_type'][len('rust_'):]}."
        )
