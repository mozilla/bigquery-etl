"""PyTest plugins for running linters on python scripts without a .py extension."""

import re

python_hashbang = re.compile(r"#!(/usr)?/bin(/env |/)python(3(.7)?)?$")


def is_python_executable(file_path):
    """Check if the given path is a python script."""
    # pathlib gives dotfiles like .DS_Store an empty suffix, so check the name
    if "." not in file_path.name:
        with open(file_path) as fp:
            return python_hashbang.match(fp.readline().strip())
    return False
