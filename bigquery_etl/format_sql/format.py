"""Format SQL."""

import glob
import os
import os.path
import sys

from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat  # noqa E402


def skip_format():
    """Return a list of configured queries for which formatting should be skipped."""
    return [
        file
        for skip in ConfigLoader.get("format", "skip", fallback=[])
        for file in glob.glob(skip, recursive=True)
    ]


def format(paths, check=False):
    """Format SQL files."""
    if not paths:
        query = sys.stdin.read()
        formatted = reformat(query, trailing_newline=True)
        if not check:
            print(formatted, end="")
        if check and query != formatted:
            sys.exit(1)
    else:
        sql_files = []

        for path in paths:
            if os.path.isdir(path):
                sql_files.extend(
                    filepath
                    for dirpath, _, filenames in os.walk(path)
                    for filename in filenames
                    if filename.endswith(".sql")
                    # skip tests/**/input.sql
                    and not (path.startswith("tests") and filename == "input.sql")
                    for filepath in [os.path.join(dirpath, filename)]
                    if filepath not in skip_format()
                )
            elif path:
                sql_files.append(path)
        if not sql_files:
            print("Error: no files were found to format")
            sys.exit(255)
        sql_files.sort()
        reformatted = unchanged = 0
        for path in sql_files:
            with open(path) as fp:
                query = fp.read()
            formatted = reformat(query, trailing_newline=True)
            if query != formatted:
                if check:
                    print(f"Needs reformatting: bqetl format {path}")
                else:
                    with open(path, "w") as fp:
                        fp.write(formatted)
                    print(f"Reformatted: {path}")
                reformatted += 1
            else:
                unchanged += 1
        print(
            ", ".join(
                f"{number} file{'s' if number > 1 else ''}"
                f"{' would be' if check else ''} {msg}"
                for number, msg in [
                    (reformatted, "reformatted"),
                    (unchanged, "left unchanged"),
                ]
                if number > 0
            )
            + "."
        )
        if check and reformatted:
            sys.exit(1)
