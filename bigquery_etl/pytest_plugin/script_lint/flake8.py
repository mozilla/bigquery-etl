"""PyTest plugin for collecting flake8 tests on python scripts."""

from pytest_flake8 import Flake8Item

from . import is_python_executable


# adapted from
# https://github.com/tholo/pytest-flake8/blob/1.0.4/pytest_flake8.py#L59-L72
def pytest_collect_file(parent, path):
    """Collect flake8 tests."""
    config = parent.config
    if config.option.flake8 and is_python_executable(path):
        flake8ignore = config._flake8ignore(path)
        if flake8ignore is not None:
            return Flake8Item(
                path,
                parent,
                flake8ignore=flake8ignore,
                maxlength=config._flake8maxlen,
                maxcomplexity=config._flake8maxcomplexity,
                showshource=config._flake8showshource,
                statistics=config._flake8statistics,
            )
