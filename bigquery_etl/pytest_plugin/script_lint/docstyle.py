"""PyTest plugin for collecting docstyle tests on python scripts."""

import pydocstyle
from pytest_pydocstyle import File, _patch_sys_argv

from . import is_python_executable


# adapted from
# https://github.com/henry0312/pytest-pydocstyle/blob/v2.4.0/src/pytest_pydocstyle.py#L38-L51
def pytest_collect_file(file_path, parent):
    """Collect docstyle tests."""
    config = parent.config
    if config.getoption("pydocstyle") and is_python_executable(file_path):
        parser = pydocstyle.config.ConfigurationParser()
        args = [file_path.name, "--match", ".*"]
        with _patch_sys_argv(args):
            parser.parse()
        for filename, *_ in parser.get_files_to_check():
            return File.from_parent(parent=parent, path=file_path, config_parser=parser)
