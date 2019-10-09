"""PyTest plugin for collecting docstyle tests on python scripts."""

import re

from pytest_docstyle import Item

from . import is_python_executable


# adapted from
# https://github.com/henry0312/pytest-docstyle/blob/v1.5.0/pytest_docstyle.py#L35-L41
def pytest_collect_file(parent, path):
    """Collect docstyle tests."""
    config = parent.config
    if (
        config.getoption("docstyle")
        and is_python_executable(path)
        # https://github.com/PyCQA/pydocstyle/blob/2.1.1/src/pydocstyle/config.py#L163
        and re.match(config.getini("docstyle_match"), path.basename)
    ):
        if not any(
            path.fnmatch(pattern) for pattern in config.getini("docstyle_exclude")
        ):
            return Item(path, parent)
