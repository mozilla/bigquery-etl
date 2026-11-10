"""PyTest plugin for running udf tests."""

import os
import re

import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader
from bigquery_etl.util.common import project_dirs

from ..routine.parse_routine import (
    GENERIC_DATASET,
    PROCEDURE_FILE,
    UDF_FILE,
    ParsedRoutine,
    cached_routine_listing,
    project_routine_dirs,
    read_routine_files,
    routine_tests_sql,
)
from .sql_test import dataset

_routine_index_key = pytest.StashKey["_RoutineIndex | Exception"]()


class _RoutineIndex:
    """
    Routine files in the test projects, built once per pytest session.

    Maps each routine filepath to its test project, its raw routine, and the raw
    routines its tests can use. Each routine dir is parsed once even when several
    test projects include it, and when several test projects can see a routine, the
    last one is used.
    """

    def __init__(self):
        # listings of the routine dirs, reused whenever this index parses routines
        self.listing_cache = {}
        self.routines = {}

        test_projects = ConfigLoader.get(
            "routine", "test_projects", fallback=project_dirs()
        )
        parsed_dirs = {}
        with cached_routine_listing(self.listing_cache):
            for project in test_projects:
                raw_routines = {}
                for routine_dir in project_routine_dirs(project):
                    key = str(routine_dir)
                    if key not in parsed_dirs:
                        parsed_dirs[key] = read_routine_files(routine_dir)
                    raw_routines.update(parsed_dirs[key])
                for raw_routine in raw_routines.values():
                    self.routines[raw_routine.filepath] = (
                        project,
                        raw_routine,
                        raw_routines,
                    )

    def parse(self, path):
        """Parse the routine at `path` and build its test SQL."""
        if path not in self.routines:
            raise KeyError(
                f"{path} is not in any of the routine test projects, or a routine "
                "with the same name in a later dir takes precedence"
            )
        project, raw_routine, raw_routines = self.routines[path]
        with cached_routine_listing(self.listing_cache):
            tests_full_sql = routine_tests_sql(raw_routine, raw_routines, project)
        return ParsedRoutine.from_raw(raw_routine, tests_full_sql)


def _routine_index(session):
    """Get the session's routine index, building it on first use."""
    # a failed build is stored too, so it isn't retried for every routine file
    if _routine_index_key not in session.stash:
        try:
            session.stash[_routine_index_key] = _RoutineIndex()
        except Exception as e:
            session.stash[_routine_index_key] = e
            raise
    index = session.stash[_routine_index_key]
    if isinstance(index, Exception):
        raise RuntimeError(
            "Building the routine test index failed, see the error for the first "
            f"routine file: {index!r}"
        ) from None
    return index


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
        base_path = self.parent.parent.parent.parent.parent.path
        path = str(self.path.relative_to(base_path))
        self.routine = _routine_index(self.session).parse(path)

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
        bq = bigquery.Client()
        dataset_id = self.safe_name()
        if "CI_RUN_ID" in os.environ:
            dataset_id += f"_{os.environ['CI_RUN_ID']}"
        with dataset(bq, dataset_id) as default_dataset:
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=False, default_dataset=default_dataset
            )
            job = bq.query(
                self.query.replace(GENERIC_DATASET, default_dataset.dataset_id),
                job_config=job_config,
            )
            job.result()
