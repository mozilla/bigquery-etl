"""PyTest plugin for running sql tests."""

import datetime
import json
import os.path
import re
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path

import pytest
import yaml
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from ..routine import parse_routine
from ..util.common import render
from .sql_test import (
    Table,
    bq_client,
    coerce_result,
    dataset,
    default_encoding,
    get_query_params,
    get_source_schema,
    load_table,
    print_and_test,
)


def pytest_configure(config):
    """Register a custom marker."""
    config.addinivalue_line("markers", "sql: mark sql tests.")


def pytest_collect_file(parent, path):
    """Collect `tests/test_*.yaml` files in ETL/view directories."""
    if (
        path.dirpath().basename == "tests"
        and path.basename.startswith("test_")
        and path.ext == ".yaml"
    ):
        return SqlTest.from_parent(parent, fspath=path)


class SqlTest(pytest.Item, pytest.File):
    """Test a SQL query."""

    def __init__(self, fspath, parent):
        """Initialize."""
        super().__init__(fspath, parent)
        self._nodeid += "::SQL"
        self.add_marker("sql")

    def collect(self):
        """Collect files to test."""
        return super().collect()

    def reportinfo(self):
        """Set report title to `{project}.{dataset}.{table}:{test}`."""
        project, dataset, table = self.fspath.strpath.split(os.path.sep)[-5:-2]
        return super().reportinfo()[:2] + (
            f"{project}.{dataset}.{table}:{self.fspath.purebasename}",
        )

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
        test_file = Path(self.fspath)
        tests_dir, query_dir, dataset_dir, project_dir, sql_dir = test_file.parents[:5]
        test_name = test_file.stem
        query_name = query_dir.name

        test = yaml.safe_load(test_file.read_text())
        is_init = bool(test.get("is_init", False))
        render_kwargs = {"template_folder": query_dir, "is_init": lambda: is_init}

        if (query_dir / "query.sql").exists():
            query = render("query.sql", **render_kwargs)
        elif (query_dir / "view.sql").exists():
            query = re.sub(
                "CREATE OR REPLACE VIEW.*?AS",
                "",
                render("view.sql", **render_kwargs),
                flags=re.DOTALL,
            )
        else:
            pytest.skip(
                f"No query.sql or view.sql in {query_dir} (if it's generated, run `./bqetl generate`)"
            )

        expect = test["expect"]

        tables = {}
        for table_name, rows in test["sources"].items():
            original = table_name
            if "." in table_name:
                # combine project and dataset name with table name
                table_name = table_name.replace(".", "_").replace("-", "_")
                original_pattern = (
                    r"`?(?<![._])\b"
                    + r"`?\.`?".join(original.split("."))
                    + r"\b(?![._])`?"
                )
                query = re.sub(original_pattern, table_name, query)

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

            tables[table_name] = Table(
                table_name, rows, get_source_schema(original, tests_dir, sql_dir)
            )
            print(f"Initialized {table_name}")

        # rewrite all udfs as temporary
        query = parse_routine.sub_local_routines(query, project_dir)

        # if we're reading an initialization function, ensure that we're not
        # using a partition filter since we rely on `select * from {table}`
        query = query.replace(
            "require_partition_filter = TRUE", "require_partition_filter = FALSE"
        )

        dataset_id = f"{dataset_dir.name}_{query_name}_{test_name}"
        if "CI_RUN_ID" in os.environ:
            dataset_id += f"_{os.environ['CI_RUN_ID']}"

        bq = bq_client()
        with dataset(bq, dataset_id) as default_dataset:
            with ThreadPool(8) as pool:
                pool.map(
                    partial(load_table, bq, default_dataset),
                    tables.values(),
                    chunksize=1,
                )

            # configure job
            res_table = bigquery.TableReference(default_dataset, query_name)

            job_config = bigquery.QueryJobConfig(
                default_dataset=default_dataset,
                destination=res_table,
                query_parameters=get_query_params(test.get("parameters") or []),
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
