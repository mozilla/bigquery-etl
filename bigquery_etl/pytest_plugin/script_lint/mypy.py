"""PyTest plugin for collecting mypy tests on python scripts."""

from pytest_mypy import MypyFile, mypy_argv

from . import is_python_executable


def pytest_configure(config):
    """Add flag when running mypy.

    Fixes https://github.com/python/mypy/issues/1380
    """
    mypy_argv.append("--scripts-are-modules")


# adapted from
# https://github.com/dbader/pytest-mypy/blob/v0.7.0/src/pytest_mypy.py#L100-L111
def pytest_collect_file(parent, path):
    """Collect mypy tests."""
    if any(
        [parent.config.option.mypy, parent.config.option.mypy_ignore_missing_imports]
    ) and is_python_executable(path):
        return MypyFile.from_parent(parent=parent, fspath=path)
