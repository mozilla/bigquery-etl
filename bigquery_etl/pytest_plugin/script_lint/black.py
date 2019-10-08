"""PyTest plugin for collecting black tests on python scripts."""

from pytest_black import BlackItem

from . import is_python_executable


# adapted from
# https://github.com/shopkeep/pytest-black/blob/0.3.7/pytest_black.py#L23-L26
def pytest_collect_file(parent, path):
    """Collect black tests."""
    config = parent.config
    if config.option.black and is_python_executable(path):
        return BlackItem(path, parent)
