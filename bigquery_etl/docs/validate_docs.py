"""Validates SQL examples in documentations."""

from argparse import ArgumentParser
import os
import re

from bigquery_etl.parse_udf import read_udf_dirs, persistent_udf_as_temp
from bigquery_etl.util import standard_args

DEFAULT_PROJECTS = ["mozfun"]
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
    default=DEFAULT_PROJECTS,
)
standard_args.add_log_level(parser)


def udfs_as_temp_functions(dir):
    """Return a single SQL string with all UDFs as temporary functions."""
    if os.path.isdir(dir):
        for root, dirs, files in os.walk(dir):
            if UDF_FILE in files:
                pass


def main():
    """Validate SQL examples."""
    args = parser.parse_args()

    # parse UDFs
    parsed_udfs = read_udf_dirs(*args.project_dirs)
   
    for project_dir in args.project_dirs:
        if os.path.isdir(project_dir):
            for root, dirs, files in os.walk(project_dir):
                if os.path.basename(root) == EXAMPLE_DIR:
                    for file in files:
                        with open(os.path.join(root, file)) as sql:
                            dry_run_sql = ""
                            example_sql = sql.read()

                            for udf, raw_udf in parsed_udfs.items():
                                if udf in example_sql:
                                    query = "".join(raw_udf.definitions)
                                    dry_run_sql += persistent_udf_as_temp(query, parsed_udfs)

                            dry_run_sql += example_sql

                            for udf, _ in parsed_udfs.items():
                                # temporary UDFs cannot contain dots
                                dry_run_sql = dry_run_sql.replace(udf, udf.replace(".", "_"))

                            # remove explicit project references
                            dry_run_sql = dry_run_sql.replace(project_dir + ".", "")


                            print(dry_run_sql)


if __name__ == "__main__":
    main()
