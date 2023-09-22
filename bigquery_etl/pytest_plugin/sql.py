"""PyTest plugin for running sql tests."""

import datetime
import json
import os.path
import re
from functools import partial
from multiprocessing.pool import ThreadPool

import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from ..routine import parse_routine
from ..util.common import render
from .sql_test import (
    TABLE_EXTENSIONS,
    Table,
    coerce_result,
    dataset,
    default_encoding,
    get_query_params,
    load,
    load_table,
    load_view,
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
        project_name = self.fspath.dirpath().dirpath().dirpath().basename
        project_dir = (
            self.fspath.dirpath().dirpath().dirpath().dirname.replace("tests", "")
        )

        init_test = False
        script_test = False

        # init tests write to dataset_query_test, instead of their default name
        path = self.fspath.dirname.replace("tests", "")
        if test_name == "test_init":
            init_test = True

            query = render("init.sql", template_folder=path)
            original, dest_name = (
                rf"`?(?<![._])\b({project_name}`?\.`?)?{dataset_name}`?\.`?{query_name}\b(?![._])`?",
                f"{dataset_name}_{query_name}_{test_name}",
            )
            query = re.sub(original, dest_name, query)
            query_name = dest_name
        elif test_name == "test_script":
            script_test = True
            query = render("script.sql", template_folder=path)
        else:
            query = render("query.sql", template_folder=path)

        expect = load(self.fspath.strpath, "expect")

        tables = {}
        views = {}

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
                    original_pattern = (
                        r"`?(?<![._])\b"
                        + r"`?\.`?".join(original.split("."))
                        + r"\b(?![._])`?"
                    )
                    query = re.sub(original_pattern, table_name, query)
                else:
                    original = table_name

                # second check for tablename tweaks.
                # if the tablename ends with a date then need to replace that date with '*' for the
                # query text substitution to work.
                # e.g. see moz-fx-data-marketing-prod.65789850.ga_sessions_20230214
                # A query using that table uses moz-fx-data-marketing-prod.65789850.ga_sessions_*
                # with the date appended to allow for daily processing.
                try:
                    datetime.datetime.strptime(table_name[-8:], "%Y%m%d")
                except ValueError:
                    pass
                else:
                    generic_table_name = table_name[:-8] + "*"
                    generic_original = original[:-8] + "*"
                    query = query.replace(generic_original, generic_table_name)
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

        # if we're reading an initialization function, ensure that we're not
        # using a partition filter since we rely on `select * from {table}`
        query = query.replace(
            "require_partition_filter = TRUE", "require_partition_filter = FALSE"
        )

        dataset_id = "_".join(self.fspath.strpath.split(os.path.sep)[-3:])
        if "CIRCLE_BUILD_NUM" in os.environ:
            dataset_id += f"_{os.environ['CIRCLE_BUILD_NUM']}"

        with bigquery.Client() as bq, dataset(bq, dataset_id) as default_dataset:
            with ThreadPool(8) as pool:
                pool.map(
                    partial(load_table, bq, default_dataset),
                    tables.values(),
                    chunksize=1,
                )
                pool.starmap(
                    partial(load_view, bq, default_dataset), views.items(), chunksize=1
                )

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
            result.sort(
                key=lambda row: json.dumps(
                    row, sort_keys=True, default=default_encoding
                )
            )
            # make sure we encode dates correctly
            expect = json.loads(
                json.dumps(
                    sorted(
                        expect,
                        key=lambda row: json.dumps(
                            row, sort_keys=True, default=default_encoding
                        ),
                    ),
                    default=default_encoding,
                )
            )

            print_and_test(expect, result)
