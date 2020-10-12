"""Validates SQL examples in documentations."""

from argparse import ArgumentParser
import os
from pathlib import Path
import tempfile
import sys

from bigquery_etl.dryrun import DryRun
from bigquery_etl.routine.parse_routine import read_routine_dir, sub_local_routines
from bigquery_etl.util import standard_args

DEFAULT_PROJECTS_DIRS = ["sql/mozfun"]
EXAMPLE_DIR = "examples"
UDF_FILE = "udf.sql"
UDF_CHAR = "[a-zA-z0-9_]"
MOZFUN_UDF_RE = fr"mozfun.({UDF_CHAR}+\.{UDF_CHAR}+)"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_dirs",
    "--project-dirs",
    help="Directories of projects documentation is validated for.",
    nargs="+",
    default=DEFAULT_PROJECTS_DIRS,
)
standard_args.add_log_level(parser)


def sql_for_dry_run(file, parsed_routines, project_dir):
    """
    Return the example SQL used for the dry run.

    Injects all UDFs the example depends on as temporary functions.
    """
    dry_run_sql = ""

    with open(file) as sql:
        example_sql = sql.read()

        # add UDFs that example depends on as temporary functions
        for udf, raw_routine in parsed_routines.items():
            if udf in example_sql:
                query = "".join(raw_routine.definitions)
                dry_run_sql += sub_local_routines(query, project_dir, parsed_routines)

        dry_run_sql += example_sql

        for udf, _ in parsed_routines.items():
            # temporary UDFs cannot contain dots, rename UDFS
            dry_run_sql = dry_run_sql.replace(udf, udf.replace(".", "_"))

        # remove explicit project references
        dry_run_sql = dry_run_sql.replace(os.path.basename(project_dir) + ".", "")

    return dry_run_sql


def validate(project_dirs):
    """Validate UDF docs."""
    is_valid = True

    for project_dir in project_dirs:
        if os.path.isdir(project_dir):
            parsed_routines = read_routine_dir(project_dir)

            for root, dirs, files in os.walk(project_dir):
                if os.path.basename(root) == EXAMPLE_DIR:
                    for file in files:
                        dry_run_sql = sql_for_dry_run(
                            os.path.join(root, file), parsed_routines, project_dir
                        )

                        # store sql in temporary file for dry_run
                        tmp_dir = Path(tempfile.mkdtemp()) / Path(root)
                        tmp_dir.mkdir(parents=True, exist_ok=True)
                        tmp_example_file = tmp_dir / file
                        tmp_example_file.write_text(dry_run_sql)

                        if not DryRun(str(tmp_example_file)).is_valid():
                            is_valid = False

    if not is_valid:
        print("Invalid examples.")
        sys.exit(1)


def main():
    """Validate SQL examples."""
    args = parser.parse_args()
    validate(args.project_dirs)


if __name__ == "__main__":
    main()
