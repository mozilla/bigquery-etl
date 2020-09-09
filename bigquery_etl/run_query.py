"""
Runs SQL queries and writes results to destination tables.

When executing a query associated metadata is parsed to determine whether
results should be written to a corresponding public dataset.
"""

from argparse import ArgumentParser
import re
import subprocess
import sys
import yaml


from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.metadata.validate_metadata import validate_public_data

DESTINATION_TABLE_RE = re.compile(r"^[a-zA-Z0-9_$]{0,1024}$")
PUBLIC_PROJECT_ID = "mozilla-public-data"


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--public_project_id",
    default=PUBLIC_PROJECT_ID,
    help="Project with publicly accessible data",
)
parser.add_argument(
    "--destination_table", help="Destination table name results are written to"
)
parser.add_argument("--dataset_id", help="Destination dataset")
parser.add_argument("--query_file", help="File path to query to be executed")


def run(
    query_file,
    dataset_id,
    destination_table,
    query_arguments,
    public_project_id=PUBLIC_PROJECT_ID,
):
    """Execute bq to run a query."""
    if dataset_id is not None:
        # dataset ID was parsed by argparse but needs to be passed as parameter
        # when running the query
        query_arguments.append("--dataset_id={}".format(dataset_id))

    use_public_table = False

    try:
        metadata = Metadata.of_sql_file(query_file)
        if metadata.is_public_bigquery():
            if not validate_public_data(metadata, query_file):
                sys.exit(1)

            # change the destination table to write results to the public dataset;
            # a view to the public table in the internal dataset is created
            # when CI runs
            if (
                dataset_id is not None
                and destination_table is not None
                and re.match(DESTINATION_TABLE_RE, destination_table)
            ):
                destination_table = "{}:{}.{}".format(
                    public_project_id, dataset_id, destination_table
                )
                query_arguments.append(
                    "--destination_table={}".format(destination_table)
                )
                use_public_table = True
            else:
                print(
                    "ERROR: Cannot run public dataset query. Parameters"
                    " --destination_table=<table without dataset ID> and"
                    " --dataset_id=<dataset> required"
                )
                sys.exit(1)
    except yaml.YAMLError as e:
        print(e)
        sys.exit(1)
    except FileNotFoundError:
        print("INFO: No metadata.yaml found for {}", query_file)

    if not use_public_table and destination_table is not None:
        # destination table was parsed by argparse, however if it wasn't modified to
        # point to a public table it needs to be passed as parameter for the query
        query_arguments.append("--destination_table={}".format(destination_table))

    with open(query_file) as query_stream:
        # run the query as shell command so that passed parameters can be used as is
        subprocess.check_call(["bq"] + query_arguments, stdin=query_stream)


def main():
    """Run query."""
    args, query_arguments = parser.parse_known_args()
    run(
        args.query_file,
        args.dataset_id,
        args.destination_table,
        query_arguments,
        args.public_project_id,
    )


if __name__ == "__main__":
    main()
