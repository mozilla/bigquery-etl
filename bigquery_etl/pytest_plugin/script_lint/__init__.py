"""PyTest plugins for running linters on python scripts without a .py extension."""

import re

python_hashbang = re.compile(r"#!(/usr)?/bin(/env |/)python(3(.7)?)?$")


def is_python_executable(file_path):
    """Check if the given path is a python script."""
    if file_path.suffix == "":
        # ignore decode errors from binary files like .DS_Store
        with open(file_path, errors="ignore") as fp:
            return python_hashbang.match(fp.readline().strip())
    return False
