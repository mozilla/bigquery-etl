"""Generic functions for filtering tables in a script."""

from argparse import ArgumentParser, Namespace
from functools import partial
from typing import Callable, List
import fnmatch
import re
import logging


def add_table_filter_arguments(
    parser: ArgumentParser, example: str = "telemetry_stable.main_v*"
):
    """Add arguments for filtering tables."""
    format_ = f"pass names or globs like {example!r}"
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-o",
        "--only",
        nargs="+",
        dest="only_tables",
        help=f"Process only the given tables; {format_}",
    )
    group.add_argument(
        "-x",
        "--except",
        nargs="+",
        dest="except_tables",
        help=f"Process all tables except for the given tables; {format_}",
    )


def compile_glob_patterns(patterns: List[str]) -> re.Pattern:
    """Compile a list of glob patterns into a single regex."""
    return re.compile("|".join(fnmatch.translate(pattern) for pattern in patterns))


def glob_predicate(table: str, pattern: re.Pattern, arg: str) -> bool:
    """Log tables skipped due to table filter arguments."""
    matched = pattern.match(table) is not None
    if arg == "except":
        matched = not matched
    if not matched:
        logging.debug(f"Skipping {table} due to --{arg} argument")
    return matched


def get_table_filter(args: Namespace) -> Callable[[str], bool]:
    """Get a function that evaluates whether a given table that should be included."""
    for patterns, arg in [(args.only_tables, "only"), (args.except_tables, "except")]:
        if patterns:
            pattern = compile_glob_patterns(patterns)
            return partial(glob_predicate, pattern=pattern, arg=arg)
    return lambda table: True
