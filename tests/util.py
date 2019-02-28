from dataclasses import dataclass
from datetime import date
from google.cloud import bigquery
from typing import Any, Callable, Dict, List, Optional, Set

import json
import os.path
import yaml

table_extensions = {
    "ndjson": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    "csv": bigquery.SourceFormat.CSV,
    "backup_info": bigquery.SourceFormat.DATASTORE_BACKUP,
    "export_metadata": bigquery.SourceFormat.DATASTORE_BACKUP,
    "avro": bigquery.SourceFormat.AVRO,
    "parquet": bigquery.SourceFormat.PARQUET,
    "orc": bigquery.SourceFormat.ORC,
}


@dataclass(frozen=True)
class Table:
    name: str
    source_format: str
    source_path: str

    def get_schema(self):
        resource_dir, resource = os.path.split(self.source_path)
        full_name, _ = resource.rsplit(".", 1)
        try:
            return [
                bigquery.SchemaField.from_api_repr(field)
                for field in load(resource_dir, f"{full_name}.schema")
            ]
        except FileNotFoundError:
            return None


@dataclass
class GeneratedTest:
    expect: List[Dict[str, Any]]
    name: str
    query: str
    query_name: str
    query_params: List[Any]
    replace: Dict[str, str]
    tables: Set[Table]

    def get_dataset_id(self):
        return f"{self.query_name}_{self.name}"

    def get_modified_query(self):
        query = self.query
        for old, new in self.replace.items():
            query.replace(old, new)
        return query


def read(*paths: str, decoder: Optional[Callable] = None, **kwargs):
    with open(os.path.join(*paths), **kwargs) as f:
        return decoder(f) if decoder else f.read()


def ndjson_load(file_obj):
    return [json.loads(line) for line in file_obj]


def load(resource_dir: str, basename: str, **search):
    search = search or {"yaml": yaml.load, "json": json.load, "ndjson": ndjson_load}
    for ext, decoder in search.items():
        try:
            return read(resource_dir, f"{basename}.{ext}", decoder=decoder)
        except FileNotFoundError:
            pass
    not_found = "', '".join(map(lambda ext: f"{basename}.{ext}", search))
    raise FileNotFoundError(f"[Errno 2] No such file or directory: '{not_found}'")


def get_query_params(resource_dir: str):
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


def generate_tests():
    tests_dir = os.path.dirname(__file__)
    sql_dir = os.path.join(os.path.dirname(tests_dir), "sql")

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
            query_params = get_query_params(resource_dir)
            tables: Set[Table] = set()
            replace: Dict[str, str] = {}

            # load expect or skip
            try:
                expect = load(resource_dir, "expect")
            except FileNotFoundError:
                continue

            # generate tables for files with a supported table extension
            for resource in next(os.walk(resource_dir))[2]:
                if resource in ("expect.ndjson", "query_params.ndjson"):
                    continue  # expect and query_params are not tables
                table_name, extension = resource, ""
                if "." in resource:
                    table_name, extension = resource.rsplit(".", 1)
                if extension in table_extensions:
                    source_format = table_extensions[extension]
                    source_path = os.path.join(resource_dir, resource)
                    if "." in table_name:
                        # define replace to remove dataset from table_name in sql
                        replace[table_name] = table_name.rsplit(".", 1)[1]
                        # remove dataset from table_name
                        table_name = replace[table_name]
                    tables.add(Table(table_name, source_format, source_path))

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


def coerce_result(*elements: Any):
    for element in elements:
        if isinstance(element, (dict, bigquery.Row)):
            yield {
                key: list(coerce_result(*value))
                if isinstance(value, list)
                else next(coerce_result(value))
                for key, value in element.items()
                # drop generated_time column
                if key not in ("generated_time",)
            }
        elif isinstance(element, date):
            yield element.strftime("%Y-%m-%d")
        else:
            yield element
