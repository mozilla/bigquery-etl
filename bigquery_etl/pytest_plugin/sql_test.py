# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""Utilities."""

import codecs
import json
import pprint
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from ..schema import SCHEMA_FILE, Schema
from ..schema.stable_table_schema import get_stable_table_schemas

QueryParameter = Union[
    bigquery.ArrayQueryParameter,
    bigquery.ScalarQueryParameter,
    bigquery.StructQueryParameter,
]


@dataclass
class Table:
    """Define info needed to create a table for a generated test."""

    name: str
    rows: List[Dict[str, Any]]
    schema: Optional[List[bigquery.SchemaField]] = None


def get_source_schema(
    full_name: str, tests_dir: Path, sql_dir: Path
) -> Optional[List[bigquery.SchemaField]]:
    """Get the schema for a test source table/view, if one can be found."""
    if "." not in full_name:
        return None
    name_parts = full_name.split(".")
    dataset_name, table_name = name_parts[-2:]
    stable_match = re.fullmatch(r"(?P<dataset_family>.+)_(?:stable|live)", dataset_name)

    schema_files = [
        tests_dir / f"{full_name}.schema.yaml",
        sql_dir.joinpath(*name_parts) / SCHEMA_FILE,
    ]
    if stable_match and dataset_name.endswith("_live"):
        # Live tables have the same schemas as their stable tables, which may have `schema.yaml` files.
        stable_name_parts = [
            *name_parts[:-2],
            f"{stable_match['dataset_family']}_stable",
            table_name,
        ]
        schema_files.append(sql_dir.joinpath(*stable_name_parts) / SCHEMA_FILE)
    for schema_file in schema_files:
        if schema_file.exists():
            return Schema.from_schema_file(schema_file).to_bigquery_schema()

    if stable_match:
        for stable_table_schema in get_stable_table_schemas():
            if (
                stable_table_schema.bq_dataset_family == stable_match["dataset_family"]
                and stable_table_schema.bq_table == table_name
            ):
                return Schema(
                    {"fields": stable_table_schema.schema}
                ).to_bigquery_schema()

    return None


@lru_cache(maxsize=1)
def bq_client() -> bigquery.Client:
    """Return a BigQuery client shared by all tests in this process.

    A client per test fetches a new GitHub OIDC token each time in CI, which
    intermittently fails with RefreshError under many parallel tests.
    """
    return bigquery.Client()


@contextmanager
def dataset(bq: bigquery.Client, dataset_id: str):
    """Context manager for creating and deleting the BigQuery dataset for a test."""
    # The dataset shouldn't already exist, but specifying `exists_ok` is still necessary here
    # just in case the BigQuery client times out waiting for the initial API call and retries,
    # except the initial API call actually does create the dataset before the retry executes.
    result = bq.create_dataset(dataset_id, exists_ok=True)
    try:
        yield result.reference
    finally:
        bq.delete_dataset(dataset_id, delete_contents=True)


def default_encoding(obj):
    """Add custom logic for serializing rows into JSON for BigQuery."""
    if isinstance(obj, date) or isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def load_table(bq: bigquery.Client, dataset: bigquery.Dataset, table: Table):
    """Load table for a test."""
    destination = dataset.table(table.name)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    if table.schema is None:
        # autodetect schema if not provided
        job_config.autodetect = True
    else:
        job_config.schema = table.schema
        # look for time_partitioning_field in provided schema
        for field in job_config.schema:
            if field.description == "time_partitioning_field":
                job_config.time_partitioning = bigquery.TimePartitioning(
                    field=field.name
                )
                break  # stop because there can only be one time partitioning field

    mem_file = BytesIO()
    for row in table.rows:
        mem_file.write(json.dumps(row, default=default_encoding).encode() + b"\n")
    mem_file.seek(0)
    job = bq.load_table_from_file(mem_file, destination, job_config=job_config)

    try:
        job.result()
    except BadRequest:
        # print the first 5 rows for debugging errors
        for row in job.errors[:5]:
            print(row)
        raise


def get_query_params(
    params: List[Dict[str, Any]],
) -> Generator[QueryParameter, None, None]:
    """Convert test parameters to BigQuery query parameters."""
    for param in params:
        if {"name", "type", "type_", "value"}.issuperset(param.keys()):
            # this is a scalar query param
            param["type_"] = param.pop("type", param.pop("type_", "STRING"))
            yield bigquery.ScalarQueryParameter(**param)
        else:
            # attempt to coerce to some type of query param
            try:
                yield bigquery.StructQueryParameter.from_api_repr(param)
            except KeyError:
                try:
                    yield bigquery.ArrayQueryParameter.from_api_repr(param)
                except KeyError:
                    # this is a different format for scalar param than above
                    yield bigquery.ScalarQueryParameter.from_api_repr(param)


def coerce_result(*elements: Any) -> Generator[Any, None, None]:
    """Recursively coerce elements to types available in json.

    Coerce date and datetime to string using isoformat.
    Coerce bigquery.Row to dict using comprehensions.
    Coerce bytes to base64 encoded strings.
    Omit dict keys named "created_at" or "generated_time".
    Omit columns with null or empty array results to simplify `expect` files.
    """
    for element in elements:
        if isinstance(element, (dict, bigquery.Row)):
            yield {
                key: (
                    list(coerce_result(*value))
                    if isinstance(value, list)
                    else next(coerce_result(value))
                )
                for key, value in element.items()
                # drop generated_time column
                if key not in ("created_at", "generated_time")
                and value is not None
                and value != []
            }
        elif isinstance(element, (date, datetime)):
            yield element.isoformat()
        elif isinstance(element, Decimal):
            yield str(element)
        elif isinstance(element, bytes):
            yield codecs.encode(element, "base64").decode().strip()
        else:
            yield element


def get_differences(expected, result, path="", sep="."):
    """Get the differences between two JSON-like python objects.

    For complicated objects, this is a big improvement over pytest -vv
    """
    differences = []

    if expected is not None and result is None:
        differences.append(("Expected exists but not Result", path))
    if expected is None and result is not None:
        differences.append(("Result exists but not Expected", path))
    if expected is None and result is None:
        return differences

    exp_is_dict, res_is_dict = isinstance(expected, dict), isinstance(result, dict)
    exp_is_list, res_is_list = isinstance(expected, list), isinstance(result, list)
    if exp_is_dict and not res_is_dict:
        differences.append(("Expected is dict but not Result", path))
    elif res_is_dict and not exp_is_dict:
        differences.append(("Result is dict but not Expected", path))
    elif not exp_is_dict and not res_is_dict:
        if exp_is_list and res_is_list:
            for i in range(max(len(expected), len(result))):
                if i >= len(result):
                    differences.append(
                        (f"Result missing element {expected[i]}", path + sep + str(i))
                    )
                elif i >= len(expected):
                    differences.append(
                        (
                            f"Result contains extra element {result[i]}",
                            path + sep + str(i),
                        )
                    )
                else:
                    differences += get_differences(
                        expected[i], result[i], path + sep + str(i)
                    )
        elif expected != result:
            differences.append((f"Expected={expected}, Result={result}", path))
    else:
        exp_keys, res_keys = set(expected.keys()), set(result.keys())
        in_exp_not_res, in_res_not_exp = exp_keys - res_keys, res_keys - exp_keys

        for k in in_exp_not_res:
            differences.append(("In Expected, not in Result", path + sep + k))
        for k in in_res_not_exp:
            differences.append(("In Result, not in Expected", path + sep + k))

        for k in exp_keys & res_keys:
            differences += get_differences(expected[k], result[k], path + sep + k)

    return differences


def print_and_test(expected, result):
    """Print objects and differences, then test equality."""
    pp = pprint.PrettyPrinter(indent=2)

    print("\nExpected:")
    pp.pprint(expected)

    print("\nActual:")
    pp.pprint(result)

    print("\nDifferences:")
    print("\n".join([" - ".join(v) for v in get_differences(expected, result)]))

    assert result == expected
