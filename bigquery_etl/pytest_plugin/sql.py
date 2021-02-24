"""PyTest plugin for running sql tests."""

import json
import os.path
from typing import Dict

import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from ..routine import parse_routine
from .sql_test import (
    TABLE_EXTENSIONS,
    Table,
    coerce_result,
    dataset,
    get_query_params,
    load,
    load_tables,
    load_views,
    print_and_test,
    read,
)

expect_names = {f"expect.{ext}" for ext in ("yaml", "json", "ndjson")}


def pytest_configure(config):
    """Register a custom marker."""
    config.addinivalue_line("markers", "sql: mark sql tests.")


def pytest_collect_file(parent, path):
    """Collect non-python query tests."""
    if path.basename in expect_names:
        return SqlTest.from_parent(parent, fspath=path.parts()[-2])


class SqlTest(pytest.Item, pytest.File):
    """Test a SQL query."""

    def __init__(self, fspath, parent):
        """Initialize."""
        super().__init__(fspath, parent)
        self._nodeid += "::SQL"
        self.add_marker("sql")

    def reportinfo(self):
        """Set report title to `{dataset}.{table}:{test}`."""
        project, dataset, table, test = self.fspath.strpath.split(os.path.sep)[-4:]
        return super().reportinfo()[:2] + (f"{project}.{dataset}.{table}:{test}",)

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
        test_name = self.fspath.basename
        query_name = self.fspath.dirpath().basename
        dataset_name = self.fspath.dirpath().dirpath().basename
        project_dir = (
            self.fspath.dirpath().dirpath().dirpath().dirname.replace("tests", "")
        )

        init_test = False
        script_test = False

        # init tests write to dataset_query_test, instead of their
        # default name
        # We assume the init sql contains `CREATE TABLE dataset.table`
        path = self.fspath.dirname.replace("tests", "")
        if test_name == "test_init":
            init_test = True

            query = read(f"{path}/init.sql")
            original, dest_name = (
                f"{dataset_name}.{query_name}",
                f"{dataset_name}_{query_name}_{test_name}",
            )
            query = query.replace(original, dest_name)
            query_name = dest_name
        elif test_name == "test_script":
            script_test = True
            query = read(f"{path}/script.sql")
        else:
            query = read(f"{path}/query.sql")

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
                print(f"Initialized {table_name}")
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
        query = parse_routine.sub_local_routines(query, project_dir)

        dataset_id = "_".join(self.fspath.strpath.split(os.path.sep)[-3:])
        if "CIRCLE_BUILD_NUM" in os.environ:
            dataset_id += f"_{os.environ['CIRCLE_BUILD_NUM']}"

        bq = bigquery.Client()
        with dataset(bq, dataset_id) as default_dataset:
            load_tables(bq, default_dataset, tables.values())
            load_views(bq, default_dataset, views)

            # configure job
            res_table = bigquery.TableReference(default_dataset, query_name)

            if init_test or script_test:
                job_config = bigquery.QueryJobConfig(
                    default_dataset=default_dataset,
                    query_parameters=get_query_params(self.fspath.strpath),
                    use_legacy_sql=False,
                )

                bq.query(query, job_config=job_config).result()
                # Retrieve final state of table on init or script tests
                job = bq.query(
                    f"SELECT * FROM {dataset_id}.{query_name}", job_config=job_config
                )

            else:
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
