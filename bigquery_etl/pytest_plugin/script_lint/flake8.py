"""PyTest plugin for collecting flake8 tests on python scripts."""

from pytest_flake8 import Flake8Item

from . import is_python_executable


# adapted from
# https://github.com/tholo/pytest-flake8/blob/1.0.6/pytest_flake8.py#L59-L82
def pytest_collect_file(parent, path):
    """Collect flake8 tests."""
    config = parent.config
    if config.option.flake8 and is_python_executable(path):
        flake8ignore = config._flake8ignore(path)
        if flake8ignore is not None:
            item = Flake8Item.from_parent(parent, fspath=path)
            item.flake8ignore = flake8ignore
            item.maxlength = config._flake8maxlen
            item.maxcomplexity = config._flake8maxcomplexity
            item.showshource = config._flake8showshource
            item.statistics = config._flake8statistics
            return item
