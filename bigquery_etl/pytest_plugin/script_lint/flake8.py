"""PyTest plugin for collecting flake8 tests on python scripts."""

from pytest_flake8 import Flake8File

from . import is_python_executable


# adapted from
# https://github.com/coherent-oss/pytest-flake8/blob/v1.3.0/pytest_flake8.py#L62-L78
def pytest_collect_file(file_path, path, parent):
    """Collect flake8 tests."""
    config = parent.config
    if config.option.flake8 and is_python_executable(path):
        flake8ignore = config._flake8ignore(path)
        if flake8ignore is not None:
            return Flake8File.from_parent(
                parent,
                path=file_path,
                flake8ignore=flake8ignore,
                maxlength=config._flake8maxlen,
                maxdoclength=config._flake8maxdoclen,
                maxcomplexity=config._flake8maxcomplexity,
                showsource=config._flake8showsource,
                statistics=config._flake8statistics,
            )
