# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""Utilities."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from google.cloud import bigquery
from typing import Any, Callable, Dict, Generator, List, Optional, Union

import json
import os
import os.path
import yaml

QueryParameter = Union[
    bigquery.ArrayQueryParameter,
    bigquery.ScalarQueryParameter,
    bigquery.StructQueryParameter,
]

table_extensions = {
    "ndjson": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    "csv": bigquery.SourceFormat.CSV,
    "backup_info": bigquery.SourceFormat.DATASTORE_BACKUP,
    "export_metadata": bigquery.SourceFormat.DATASTORE_BACKUP,
    "avro": bigquery.SourceFormat.AVRO,
    "parquet": bigquery.SourceFormat.PARQUET,
    "orc": bigquery.SourceFormat.ORC,
}


@dataclass
class Table:
    """Define info needed to create a table for a generated test."""

    name: str
    source_format: str
    source_path: str
    # post_init fields
    schema: Optional[List[bigquery.SchemaField]] = None

    def __post_init__(self):
        """Fill in calculated fields if not provided."""
        if self.schema is None:
            resource_dir, resource = os.path.split(self.source_path)
            full_name, _ = resource.rsplit(".", 1)
            try:
                self.schema = [
                    bigquery.SchemaField.from_api_repr(field)
                    for field in load(resource_dir, f"{full_name}.schema")
                ]
            except FileNotFoundError:
                pass


@dataclass
class GeneratedTest:
    """Define the info needed to run a generated test."""

    expect: List[Dict[str, Any]]
    name: str
    query: str
    query_name: str
    query_params: List[Any]
    replace: Dict[str, str]
    tables: Dict[str, Table]
    # post_init fields
    dataset_id: Optional[str] = None
    modified_query: Optional[str] = None

    def __post_init__(self):
        """Fill in calculated fields if not provided."""
        if self.dataset_id is None:
            self.dataset_id = f"{self.query_name}_{self.name}"
        if "CIRCLE_BUILD_NUM" in os.environ:
            self.dataset_id += f"_{os.environ['CIRCLE_BUILD_NUM']}"
        if self.modified_query is None:
            self.modified_query = self.query
            for old, new in self.replace.items():
                self.modified_query = self.modified_query.replace(old, new)


def read(*paths: str, decoder: Optional[Callable] = None, **kwargs):
    """Read a file and apply decoder if provided."""
    with open(os.path.join(*paths), **kwargs) as f:
        return decoder(f) if decoder else f.read()


def ndjson_load(file_obj) -> List[Any]:
    """Decode newline delimited json from file_obj."""
    return [json.loads(line) for line in file_obj]


def load(resource_dir: str, *basenames: str, **search: Optional[Callable]) -> Any:
    """Read the first matching file found in resource_dir.

    Calls read on paths under resource_dir with a name sans extension in
    basenames and an extension and decoder in search.

    :param resource_dir: directory to check for files
    :param basenames: file names to look for, without an extension
    :param search: mapping of file extension to decoder
    :return: first response from read() that doesn't raise FileNotFoundError
    :raises FileNotFoundError: when all matching files raise FileNotFoundError
    """
    search = search or {
        "yaml": yaml.full_load,
        "json": json.load,
        "ndjson": ndjson_load,
    }
    not_found: List[str] = []
    for basename in basenames:
        for ext, decoder in search.items():
            try:
                return read(resource_dir, f"{basename}.{ext}", decoder=decoder)
            except FileNotFoundError:
                not_found.append(f"{basename}.{ext}")
    raise FileNotFoundError(f"[Errno 2] No such files in '{resource_dir}': {not_found}")


def get_query_params(resource_dir: str) -> Generator[QueryParameter, None, None]:
    """Attempt to load the first query params found in resource_dir."""
    try:
        params = load(resource_dir, "query_params")
    except FileNotFoundError:
        params = []
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


def generate_tests() -> Generator[GeneratedTest, None, None]:
    """Attempt to generate tests."""
    tests_dir = os.path.dirname(__file__)
    sql_dir = os.path.join(os.path.dirname(tests_dir), "target", "sql")

    # iterate over directories in tests_dir
    for query_name in next(os.walk(tests_dir))[1]:
        query_dir = os.path.join(tests_dir, query_name)

        # read query or skip
        try:
            query = read(sql_dir, f"{query_name}.sql")
        except FileNotFoundError:
            continue

        # generate a test for each directory in query_dir
        for test_name in next(os.walk(query_dir))[1]:
            resource_dir = os.path.join(query_dir, test_name)
            query_params = list(get_query_params(resource_dir))
            tables: Dict[str, Table] = {}
            replace: Dict[str, str] = {}

            # load expect or skip
            try:
                expect = load(resource_dir, "expect")
            except FileNotFoundError:
                continue

            # generate tables for files with a supported table extension
            for resource in next(os.walk(resource_dir))[2]:
                if "." not in resource:
                    continue  # tables require an extension
                table_name, extension = resource.rsplit(".", 1)
                if table_name.endswith(".schema") or table_name in (
                    "expect",
                    "query_params",
                ):
                    continue  # not a table
                print(table_name)
                if extension in table_extensions:
                    source_format = table_extensions[extension]
                    source_path = os.path.join(resource_dir, resource)
                    if "." in table_name:
                        # define replace to remove dataset from table_name in sql
                        replace[table_name] = table_name.rsplit(".", 1)[1]
                        # remove dataset from table_name
                        table_name = replace[table_name]
                    tables[table_name] = Table(table_name, source_format, source_path)

            # yield a test
            yield GeneratedTest(
                expect=expect,
                name=test_name,
                query=query,
                query_name=query_name,
                query_params=query_params,
                replace=replace,
                tables=tables,
            )


def coerce_result(*elements: Any) -> Generator[Any, None, None]:
    """Recursively coerce elements to types available in json.

    Coerce date and datetime to string using isoformat.
    Coerce bigquery.Row to dict using comprehensions.
    Omit dict keys named "generated_time".
    """
    for element in elements:
        if isinstance(element, (dict, bigquery.Row)):
            yield {
                key: list(coerce_result(*value))
                if isinstance(value, list)
                else next(coerce_result(value))
                for key, value in element.items()
                # drop generated_time column
                if key not in ("generated_time",) and value is not None
            }
        elif isinstance(element, (date, datetime)):
            yield element.isoformat()
        elif isinstance(element, Decimal):
            yield str(element)
        else:
            yield element
