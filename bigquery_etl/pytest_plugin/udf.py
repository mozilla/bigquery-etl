"""PyTest plugin for running udf tests."""

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
import pytest
import re

from .sql_test import dataset

from ..udf.parse_udf import (
    UDF_DIRS,
    MOZFUN_DIR,
    UDF_FILE,
    PROCEDURE_FILE,
    parse_udf_dirs,
    GENERIC_DATASET,
)

TEST_UDF_DIRS = {"assert"}.union(UDF_DIRS).union(MOZFUN_DIR)
_parsed_udfs = None


def parsed_udfs():
    """Get cached parsed udfs."""
    global _parsed_udfs
    if _parsed_udfs is None:
        _parsed_udfs = {
            udf.filepath: udf
            for udf in parse_udf_dirs("tests/assert", *UDF_DIRS, *MOZFUN_DIR)
        }

    return _parsed_udfs


def pytest_configure(config):
    """Register a custom marker."""
    config.addinivalue_line("markers", "udf: mark udf tests.")


def pytest_collect_file(parent, path):
    """Collect non-python query tests."""
    if "tests/data" not in str(path.dirpath()):
        if path.basename in (UDF_FILE, PROCEDURE_FILE):
            if (
                path.dirpath().dirpath().basename in TEST_UDF_DIRS
                or path.dirpath().dirpath().dirpath().basename in MOZFUN_DIR
            ):
                return UdfFile.from_parent(parent, fspath=path)


class UdfFile(pytest.File):
    """UDF File."""

    def collect(self):
        """Collect."""
        self.add_marker("udf")
        self.udf = parsed_udfs()[self.name]
        for i, query in enumerate(self.udf.tests_full_sql):
            yield UdfTest.from_parent(self, name=f"{self.udf.name}#{i+1}", query=query)


class UdfTest(pytest.Item):
    """UDF Test."""

    def __init__(self, name, parent, query):
        """Initialize."""
        super().__init__(name, parent)
        self.query = query
        if "#xfail" in query:
            self.add_marker(pytest.mark.xfail(strict=True))

    def safe_name(self):
        """Get the name as a valid slug."""
        value = re.sub(r"[^\w\s_]", "", self.name.lower()).strip()
        return re.sub(r"[_\s]+", "_", value)

    def reportinfo(self):
        """Set report title to `self.name`."""
        return super().reportinfo()[:2] + (self.name,)

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
        """Run Test."""
        bq = bigquery.Client()
        with dataset(bq, self.safe_name()) as default_dataset:
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=False, default_dataset=default_dataset
            )
            job = bq.query(
                self.query.replace(GENERIC_DATASET, default_dataset.dataset_id),
                job_config=job_config,
            )
            job.result()
