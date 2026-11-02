"""PyTest plugin for collecting black tests on python scripts."""

from pytest_black import BlackFile

from . import is_python_executable


# adapted from
# https://github.com/coherent-oss/pytest-black/blob/v0.6.0/pytest_black.py#L23-L26
def pytest_collect_file(file_path, path, parent):
    """Collect black tests."""
    config = parent.config
    if config.option.black and is_python_executable(path):
        return BlackFile.from_parent(parent, path=file_path)
