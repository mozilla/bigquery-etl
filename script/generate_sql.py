#!/usr/bin/python

"""
This script generates SQL files for each SQL file in `sql/` and 
includes all the temporary UDF definitions used in the queries.
"""

from argparse import ArgumentParser
from textwrap import dedent
import shutil
import re
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests.parse_udf import parse_udf_dir, udf_usages_in_file


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--destination",
    default="target/sql/",
    help="The path where generated SQL files will be stored.",
)
parser.add_argument(
    "--udf-dir",
    default="udf/",
    help="The directory where declarations of temporary UDFs are stored.",
)
parser.add_argument(
    "--sql-dir",
    default="sql/",
    help="The path where files with SQL queries are stored.",
)

def main():
    args = parser.parse_args()

    parsed_udfs = {x.name: x for x in parse_udf_dir(args.udf_dir)}

    # create output directory if not exists or remove existing files
    if os.path.exists(args.destination):
        shutil.rmtree(args.destination)
    os.makedirs(args.destination, exist_ok=True)

    for root, dirs, sql_files in os.walk(args.sql_dir):
        for filename in sql_files:
            # make sure only SQL files are considered
            if not filename.endswith('.sql'):
                continue

            # get names of the UDFs used in the SQL queries
            udf_usages = udf_usages_in_file(os.path.join(root, filename))

            with open(os.path.join(args.destination, filename), 'a+') as output_file:
                # write UDF declarations to file first
                for udf_usage in udf_usages:
                    output_file.write(parsed_udfs[udf_usage].full_sql)
                    output_file.write('\n--\n')

                # write SQL of query to file
                with open(os.path.join(root, filename)) as f:
                    sql_text = f.read()

                output_file.write(sql_text)


if __name__ == "__main__":
    main()