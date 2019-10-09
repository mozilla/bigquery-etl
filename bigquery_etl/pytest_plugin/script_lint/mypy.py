"""PyTest plugin for collecting mypy tests on python scripts."""

from pytest_mypy import mypy_argv, MypyItem

from . import is_python_executable


def pytest_configure(config):
    """Add flag when running mypy.

    Fixes https://github.com/python/mypy/issues/1380
    """
    mypy_argv.append("--scripts-are-modules")


# adapted from
# https://github.com/dbader/pytest-mypy/blob/v0.4.1/src/pytest_mypy.py#L36-L43
def pytest_collect_file(parent, path):
    """Collect mypy tests."""
    if any(
        [parent.config.option.mypy, parent.config.option.mypy_ignore_missing_imports]
    ) and is_python_executable(path):
        return MypyItem(path, parent)
