"""PyTest plugin for running sql tests."""

import os

import pytest

from bigquery_etl.format_sql.formatter import reformat


def pytest_configure(config):
    """Register a custom marker."""
    config.addinivalue_line("markers", "format_sql: mark format_sql tests.")


def pytest_collect_file(parent, path):
    """Collect format_sql tests."""
    if path.basename == "expect.sql":
        print(path)
        return FormatTest.from_parent(parent, fspath=path)


class FormatTest(pytest.Item, pytest.File):
    """Test a SQL query."""

    def __init__(self, fspath, parent):
        """Initialize."""
        super().__init__(fspath, parent)
        self._nodeid += "::FORMAT_SQL"
        self.add_marker("format_sql")

    def reportinfo(self):
        """Append fspath to report title."""
        return super().reportinfo()[:2] + (self.fspath.relto(os.getcwd()),)

    def _prunetraceback(self, excinfo):
        """Prune traceback to runtest method."""
        traceback = excinfo.traceback
        ntraceback = traceback.cut(path=__file__)
        excinfo.traceback = ntraceback.filter()

    def runtest(self):
        """Run."""
        with open(self.fspath) as fp:
            expect = fp.read()
        try:
            with open(f"{self.fspath.dirname}/input.sql") as fp:
                query = fp.read()
        except FileNotFoundError:
            query = expect
        assert reformat(query) + "\n" == expect
