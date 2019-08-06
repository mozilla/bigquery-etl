# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""Utilities."""

from bigquery_etl import parse_udf
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from typing import Any, Callable, Dict, Generator, List, Optional, Union

import json
import os
import os.path
import pytest
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

raw_udfs = parse_udf.read_udf_dirs()


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


class SqlTest(pytest.Item, pytest.File):
    """Test a SQL query."""

    def __init__(self, path, parent):
        """Initialize."""
        super().__init__(path, parent)
        self._nodeid += "::SQL"
        self.add_marker("sql")

    def runtest(self):
        """Run."""
        query_name = self.fspath.dirpath().basename
        query = read(f"{self.fspath.dirname.replace('tests', 'sql')}.sql")
        expect = load(self.fspath.strpath, "expect")

        tables: Dict[str, Table] = {}

        # generate tables for files with a supported table extension
        for resource in next(os.walk(self.fspath))[2]:
            if "." not in resource:
                continue  # tables require an extension
            table_name, extension = resource.rsplit(".", 1)
            if table_name.endswith(".schema") or table_name in (
                "expect",
                "query_params",
            ):
                continue  # not a table
            if extension in table_extensions:
                source_format = table_extensions[extension]
                source_path = os.path.join(self.fspath.strpath, resource)
                if "." in table_name:
                    # remove dataset from table_name
                    original, table_name = table_name, table_name.rsplit(".", 1)[1]
                    query = query.replace(original, table_name)
                tables[table_name] = Table(table_name, source_format, source_path)

        # rewrite all udfs as temporary
        temp_udfs = parse_udf.sub_persisent_udfs_as_temp(query)
        if temp_udfs != query:
            query = temp_udfs
            # prepend udf definitions
            query = parse_udf.prepend_udf_usage_definitions(query, raw_udfs)

        dataset_id = "_".join(self.fspath.strpath.split(os.path.sep)[-3:])
        if "CIRCLE_BUILD_NUM" in os.environ:
            dataset_id += f"_{os.environ['CIRCLE_BUILD_NUM']}"

        bq = bigquery.Client()
        with dataset(bq, dataset_id) as default_dataset:
            load_tables(bq, default_dataset, tables.values())

            # configure job
            job_config = bigquery.QueryJobConfig(
                default_dataset=default_dataset,
                destination=bigquery.TableReference(default_dataset, query_name),
                query_parameters=get_query_params(self.fspath.strpath),
                use_legacy_sql=False,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            # run query
            job = bq.query(query, job_config=job_config)
            result = list(coerce_result(*job.result()))
            result.sort(key=lambda row: json.dumps(row, sort_keys=True))
            expect.sort(key=lambda row: json.dumps(row, sort_keys=True))

            assert expect == result


@contextmanager
def dataset(bq, dataset_id):
    """Context manager for creating and deleting the BigQuery dataset for a test."""
    try:
        bq.get_dataset(dataset_id)
    except NotFound:
        bq.create_dataset(dataset_id)
    yield bq.dataset(dataset_id)
    bq.delete_dataset(dataset_id, delete_contents=True)


def load_tables(bq, dataset, tables):
    """Load tables for a test."""
    for table in tables:
        destination = f"{dataset.dataset_id}.{table.name}"
        job_config = bigquery.LoadJobConfig(
            default_dataset=dataset,
            source_format=table.source_format,
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
        with open(table.source_path, "rb") as file_obj:
            job = bq.load_table_from_file(file_obj, destination, job_config=job_config)
        try:
            job.result()
        except BadRequest:
            print(job.errors)
            raise


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
