"""PyTest plugin for running udf tests."""
import traceback

from .parse_udf import UDF_DIRS, parse_udf_dirs
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

import pytest

TEST_UDF_DIRS = {"assert"}.union(UDF_DIRS)
_parsed_udfs = None


def parsed_udfs():
    """Get cached parsed udfs."""
    global _parsed_udfs
    if _parsed_udfs is None:
        _parsed_udfs = {
            udf.filepath: udf for udf in parse_udf_dirs("tests/assert", *UDF_DIRS)
        }
    return _parsed_udfs


def pytest_collect_file(parent, path):
    """Collect non-python query tests."""
    if path.basename.endswith(".sql") and path.dirpath().basename in TEST_UDF_DIRS:
        return UdfFile(path, parent)


class UdfFile(pytest.File):
    """UDF File."""

    def __init__(self, path, parent):
        """Initialize."""
        super().__init__(path, parent)
        self.add_marker("udf")
        self.udf = parsed_udfs()[self.name]

    def collect(self):
        for i, query in enumerate(self.udf.tests_full_sql):
            yield UdfTest(f"{self.udf.name}#{i+1}", self, query)


class UdfTest(pytest.Item):
    """UDF Test."""

    def __init__(self, name, parent, query):
        """Initialize."""
        super().__init__(name, parent)
        self.query = query

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
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        job = bigquery.Client().query(self.query, job_config=job_config)
        job.result()
