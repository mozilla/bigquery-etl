"""PyTest plugin for running sql tests."""

from typing import Dict
import json
import os.path

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
import pytest

from ..udf import parse_udf
from ..util.test_sql import (
    coerce_result,
    dataset,
    get_query_params,
    load,
    load_tables,
    load_views,
    read,
    Table,
    TABLE_EXTENSIONS,
    print_and_test,
)

expect_names = {f"expect.{ext}" for ext in ("yaml", "json", "ndjson")}


def pytest_configure(config):
    """Register a custom marker."""
    config.addinivalue_line("markers", "sql: mark sql tests.")


def pytest_collect_file(parent, path):
    """Collect non-python query tests."""
    if path.basename in expect_names:
        return SqlTest(path.parts()[-2], parent)


class SqlTest(pytest.Item, pytest.File):
    """Test a SQL query."""

    def __init__(self, path, parent):
        """Initialize."""
        super().__init__(path, parent)
        self._nodeid += "::SQL"
        self.add_marker("sql")

    def reportinfo(self):
        """Set report title to `{dataset}.{table}:{test}`."""
        dataset, table, test = self.fspath.strpath.split(os.path.sep)[-3:]
        return super().reportinfo()[:2] + (f"{dataset}.{table}:{test}",)

    def repr_failure(self, excinfo):
        """Skip traceback for api error."""
        if excinfo.errisinstance(BadRequest):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def _prunetraceback(self, excinfo):
        """Prune traceback to runtest method."""
        traceback = excinfo.traceback
        ntraceback = traceback.cut(path=__file__)
        excinfo.traceback = ntraceback.filter()

    def runtest(self):
        """Run."""
        query_name = self.fspath.dirpath().basename
        query = read(f"{self.fspath.dirname.replace('tests', 'sql')}/query.sql")
        expect = load(self.fspath.strpath, "expect")

        tables: Dict[str, Table] = {}
        views: Dict[str, str] = {}

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
            if extension in TABLE_EXTENSIONS or extension in ("yaml", "json"):
                if extension in TABLE_EXTENSIONS:
                    source_format = TABLE_EXTENSIONS[extension]
                    source_path = os.path.join(self.fspath.strpath, resource)
                else:
                    source_format = TABLE_EXTENSIONS["ndjson"]
                    source_path = (self.fspath.strpath, table_name)
                if "." in table_name:
                    # combine project and dataset name with table name
                    original, table_name = (
                        table_name,
                        table_name.replace(".", "_").replace("-", "_"),
                    )
                    query = query.replace(original, table_name)
                tables[table_name] = Table(table_name, source_format, source_path)
            elif extension == "sql":
                if "." in table_name:
                    # combine project and dataset name with table name
                    original, table_name = (
                        table_name,
                        table_name.replace(".", "_").replace("-", "_"),
                    )
                    query = query.replace(original, table_name)
                views[table_name] = read(self.fspath.strpath, resource)

        # rewrite all udfs as temporary
        query = parse_udf.persistent_udf_as_temp(query)

        dataset_id = "_".join(self.fspath.strpath.split(os.path.sep)[-3:])
        if "CIRCLE_BUILD_NUM" in os.environ:
            dataset_id += f"_{os.environ['CIRCLE_BUILD_NUM']}"

        bq = bigquery.Client()
        with dataset(bq, dataset_id) as default_dataset:
            load_tables(bq, default_dataset, tables.values())
            load_views(bq, default_dataset, views)

            # configure job
            res_table = bigquery.TableReference(default_dataset, query_name)

            job_config = bigquery.QueryJobConfig(
                default_dataset=default_dataset,
                destination=res_table,
                query_parameters=get_query_params(self.fspath.strpath),
                use_legacy_sql=False,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            # run query
            job = bq.query(query, job_config=job_config)
            result = list(coerce_result(*job.result()))
            result.sort(key=lambda row: json.dumps(row, sort_keys=True))
            expect.sort(key=lambda row: json.dumps(row, sort_keys=True))

            print_and_test(expect, result)
