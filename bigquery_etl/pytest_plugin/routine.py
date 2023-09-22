"""PyTest plugin for running udf tests."""

import os
import re

import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.util.common import project_dirs

from ..routine.parse_routine import (
    GENERIC_DATASET,
    PROCEDURE_FILE,
    UDF_FILE,
    parse_routines,
)
from .sql_test import dataset

_parsed_routines = None


def parsed_routines():
    """Get cached parsed routines."""
    global _parsed_routines
    if _parsed_routines is None:
        _parsed_routines = {
            routine.filepath: routine
            for project in (project_dirs() + ["tests/assert"])
            for routine in parse_routines(project)
        }

    return _parsed_routines


def pytest_configure(config):
    """Register a custom marker."""
    config.addinivalue_line("markers", "routine: mark routine tests.")


def pytest_collect_file(parent, path):
    """Collect non-python query tests."""
    if "tests/data" not in str(path.dirpath()):
        if path.basename in (UDF_FILE, PROCEDURE_FILE):
            return RoutineFile.from_parent(parent, fspath=path)


class RoutineFile(pytest.File):
    """Routine File."""

    def collect(self):
        """Collect."""
        self.add_marker("routine")
        self.routine = parsed_routines()[self.name]
        for i, query in enumerate(self.routine.tests_full_sql):
            yield RoutineTest.from_parent(
                self, name=f"{self.routine.name}#{i+1}", query=query
            )


class RoutineTest(pytest.Item):
    """Routine Test."""

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
        with bigquery.Client() as bq:
            dataset_id = self.safe_name()
            if "CIRCLE_BUILD_NUM" in os.environ:
                dataset_id += f"_{os.environ['CIRCLE_BUILD_NUM']}"
            with dataset(bq, dataset_id) as default_dataset:
                job_config = bigquery.QueryJobConfig(
                    use_legacy_sql=False, default_dataset=default_dataset
                )
                job = bq.query(
                    self.query.replace(GENERIC_DATASET, default_dataset.dataset_id),
                    job_config=job_config,
                )
                job.result()
