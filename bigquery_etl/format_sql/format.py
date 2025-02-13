"""Format SQL."""

import glob
import os
import os.path
import sys
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path

from sqlglot.errors import ParseError

from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat  # noqa E402
from bigquery_etl.util.common import qualify_table_references_in_file


def skip_format():
    """Return a list of configured queries for which formatting should be skipped."""
    return [
        file
        for skip in ConfigLoader.get("format", "skip", fallback=[])
        for file in glob.glob(skip, recursive=True)
    ]


def skip_qualifying_references():
    """Return a list of configured queries where fully qualifying references should be skipped."""
    return [
        file
        for skip in ConfigLoader.get(
            "format", "skip_qualifying_references", fallback=[]
        )
        for file in glob.glob(skip, recursive=True)
    ]


def _format_path(check, path):
    query = Path(path).read_text()

    try:
        if not any([path.endswith(s) for s in skip_qualifying_references()]):
            fully_referenced_query = qualify_table_references_in_file(Path(path))
        else:
            fully_referenced_query = query
    except NotImplementedError:
        fully_referenced_query = query  # not implemented for scripts
    except ParseError:
        print(f"Invalid syntax found for: {path}")
        return 0, 1

    formatted = reformat(fully_referenced_query, trailing_newline=True)

    if query != formatted:
        if check:
            print(f"Needs reformatting: bqetl format {path}")
        else:
            with open(path, "w") as fp:
                fp.write(formatted)
            print(f"Reformatted: {path}")
        return 1, 0
    else:
        return 0, 0


def format(paths, check=False, parallelism=8):
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
                    for dirpath, _, filenames in os.walk(path, followlinks=True)
                    for filename in filenames
                    if filename.endswith(".sql")
                    # skip tests/**/input.sql
                    and not (path.startswith("tests") and filename == "input.sql")
                    for filepath in [os.path.join(dirpath, filename)]
                    if not any([filepath.endswith(s) for s in skip_format()])
                )
            elif path:
                sql_files.append(path)
        if not sql_files:
            print("Error: no files were found to format")
            sys.exit(255)

        with Pool(parallelism) as pool:
            results = pool.map(partial(_format_path, check), sql_files)

        reformatted = sum([x[0] for x in results])
        unchanged = len(sql_files) - reformatted
        invalid = sum([x[1] for x in results])

        print(
            ", ".join(
                f"{number} file{'s' if number > 1 else ''}"
                f"{' would be' if check else ''} {msg}"
                for number, msg in [
                    (reformatted, "reformatted"),
                    (unchanged, "left unchanged"),
                    (invalid, "invalid"),
                ]
                if number > 0
            )
            + "."
        )
        if check and (reformatted or invalid):
            sys.exit(1)
