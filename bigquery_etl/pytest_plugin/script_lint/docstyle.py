"""PyTest plugin for collecting docstyle tests on python scripts."""

import pydocstyle
from pytest_pydocstyle import File, _patch_sys_argv

from . import is_python_executable


# adapted from
# https://github.com/henry0312/pytest-docstyle/blob/v2.2.0/src/pytest_pydocstyle.py#L37-L46
def pytest_collect_file(parent, path):
    """Collect docstyle tests."""
    config = parent.config
    if config.getoption("pydocstyle") and is_python_executable(path):
        parser = pydocstyle.config.ConfigurationParser()
        args = [str(path.basename), "--match", ".*"]
        with _patch_sys_argv(args):
            parser.parse()
        for filename, _, _ in parser.get_files_to_check():
            return File.from_parent(parent=parent, fspath=path, config_parser=parser)
