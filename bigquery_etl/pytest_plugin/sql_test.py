# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""Utilities."""

from bigquery_etl.udf import parse_udf
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from io import BytesIO, TextIOWrapper
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import codecs
import json
import os
import os.path
import pprint
import yaml

QueryParameter = Union[
    bigquery.ArrayQueryParameter,
    bigquery.ScalarQueryParameter,
    bigquery.StructQueryParameter,
]

TABLE_EXTENSIONS = {
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
    # a tuple means read via `load(*source_path)` and format as source_format
    # a string means source_path is already in source_format
    source_path: Union[str, Tuple[str, str]]
    # post_init fields
    schema: Optional[List[bigquery.SchemaField]] = None

    def __post_init__(self):
        """Fill in calculated fields if not provided."""
        if self.schema is None:
            if isinstance(self.source_path, str):
                resource_dir, resource = os.path.split(self.source_path)
                full_name, _ = resource.rsplit(".", 1)
            else:
                resource_dir, full_name = self.source_path
            try:
                self.schema = [
                    bigquery.SchemaField.from_api_repr(field)
                    for field in load(resource_dir, f"{full_name}.schema")
                ]
            except FileNotFoundError:
                pass


class NDJsonDecodeError(Exception):
    """ndjson decode error."""

    pass


class JsonDecodeError(Exception):
    """JSON decode error."""

    pass


@contextmanager
def dataset(bq: bigquery.Client, dataset_id: str):
    """Context manager for creating and deleting the BigQuery dataset for a test."""
    try:
        bq.get_dataset(dataset_id)
    except NotFound:
        bq.create_dataset(dataset_id)
    try:
        yield bq.dataset(dataset_id)
    finally:
        bq.delete_dataset(dataset_id, delete_contents=True)


def load_tables(
    bq: bigquery.Client, dataset: bigquery.Dataset, tables: Iterable[Table]
):
    """Load tables for a test."""
    for table in tables:
        destination = dataset.table(table.name)
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

        if isinstance(table.source_path, str):
            with open(table.source_path, "rb") as file_obj:
                job = bq.load_table_from_file(
                    file_obj, destination, job_config=job_config
                )
        else:
            file_obj = BytesIO()
            for row in load(*table.source_path):
                file_obj.write(json.dumps(row).encode() + b"\n")
            file_obj.seek(0)
            job = bq.load_table_from_file(file_obj, destination, job_config=job_config)

        try:
            job.result()
        except BadRequest:
            raise


def load_views(bq: bigquery.Client, dataset: bigquery.Dataset, views: Dict[str, str]):
    """Load views for a test."""
    for table, view_query in views.items():
        view = bigquery.Table(dataset.table(table))
        view.view_query = view_query.format(
            project=dataset.project, dataset=dataset.dataset_id
        )
        bq.create_table(view)


def read(*paths: str, decoder: Optional[Callable] = None, **kwargs):
    """Read a file and apply decoder if provided."""
    with open(os.path.join(*paths), **kwargs) as f:
        return decoder(f) if decoder else f.read()


def ndjson_load(file_obj: TextIOWrapper) -> List[Any]:
    """Decode newline delimited json from file_obj."""
    res = []
    for i, line in enumerate(file_obj):
        try:
            res.append(json.loads(line))
        except json.JSONDecodeError as e:
            raise NDJsonDecodeError(
                f"Line {i+1} column {e.colno} of file {file_obj.name}, {e.msg}"
            )

    return res


def json_load(file_obj: TextIOWrapper) -> List[Any]:
    """Decode json from file_obj."""
    try:
        return json.load(file_obj)
    except json.JSONDecodeError as e:
        raise JsonDecodeError(
            f"Line {e.lineno} column {e.colno} of file {file_obj.name}, {e.msg}"
        )


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
        "json": json_load,
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
    Coerce bytes to base64 encoded strings.
    Omit dict keys named "generated_time".
    Omit columns with null results to simplify `expect` files.
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
                if key not in ("generated_time",) and value is not None
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
            for i, (exp_elem, res_elem) in enumerate(zip(expected, result)):
                differences += get_differences(exp_elem, res_elem, path + sep + str(i))
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
