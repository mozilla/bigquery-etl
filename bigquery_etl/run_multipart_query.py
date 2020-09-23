#!/usr/bin/env python3

"""
Writes multiple queries to temporary tables and then joins the results.

This is useful for queries that BigQuery deems too complex to run, usually due
to using a large number of subqueries; this pattern allows you to materialize
subsets of columns in multiple different queries so that they stay under the
complexity limit, and then join those results to generate a final wide result.

The query files must be in the same directory and all be prefixed with `part`.
"""

from argparse import ArgumentParser
from multiprocessing.pool import ThreadPool
from uuid import uuid4
import os.path

from google.cloud import bigquery


def dirpath(string):
    """Path to a dir that must exist."""
    if not os.path.isdir(string):
        raise ValueError()
    return string


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--using",
    default="document_id",
    help="comma separated list of join columns to use when combining results",
)
parser.add_argument(
    "--parallelism",
    default=4,
    type=int,
    help="Maximum number of queries to execute concurrently",
)
parser.add_argument(
    "--dataset_id",
    help="Default dataset, if not specified all tables must be qualified with dataset",
)
parser.add_argument(
    "--project_id", help="Default project, if not specified the sdk will determine one"
)
parser.add_argument(
    "--destination_table", help="table where combined results will be written"
)
parser.add_argument(
    "--time_partitioning_field",
    type=lambda f: bigquery.TimePartitioning(field=f),
    help="time partition field on the destination table",
)
parser.add_argument(
    "--clustering_fields",
    type=lambda f: f.split(","),
    help="comma separated list of clustering fields on the destination table",
)
parser.add_argument(
    "--dry_run",
    action="store_true",
    help="Print bytes that would be processed for each part and don't run queries",
)
parser.add_argument(
    "--parameter",
    action="append",
    default=[],
    dest="parameters",
    type=lambda p: bigquery.ScalarQueryParameter(*p.split(":", 2)),
    metavar="NAME:TYPE:VALUE",
    help="query parameter(s) to pass when running parts",
)
parser.add_argument(
    "--priority",
    choices=["BATCH", "INTERACTIVE"],
    default="INTERACTIVE",
    type=str.upper,
    help=(
        "Priority for BigQuery query jobs; BATCH priority will significantly slow "
        "down queries if reserved slots are not enabled for the billing project; "
        "defaults to INTERACTIVE"
    ),
)
parser.add_argument(
    "query_dir",
    type=dirpath,
    help="Path to the query directory that contains part*.sql files",
)

temporary_dataset = None


def get_temporary_dataset(client):
    """Get a cached reference to the dataset used for server-assigned destinations."""
    global temporary_dataset
    if temporary_dataset is None:
        # look up the dataset used for query results without a destination
        dry_run = bigquery.QueryJobConfig(dry_run=True)
        destination = client.query("SELECT 1", dry_run).destination
        temporary_dataset = client.dataset(destination.dataset_id, destination.project)
    return temporary_dataset


def _sql_table_id(table):
    return f"{table.project}.{table.dataset_id}.{table.table_id}"


def _run_part(client, part, args):
    with open(os.path.join(args.query_dir, part)) as sql_file:
        query = sql_file.read()
    job_config = bigquery.QueryJobConfig(
        destination=get_temporary_dataset(client).table(f"anon{uuid4().hex}"),
        default_dataset=args.dataset_id,
        use_legacy_sql=False,
        dry_run=args.dry_run,
        query_parameters=args.parameters,
        priority=args.priority,
        allow_large_results=True,
    )
    job = client.query(query=query, job_config=job_config)
    if job.dry_run:
        print(f"Would process {job.total_bytes_processed:,d} bytes for {part}")
    else:
        job.result()
        print(f"Processed {job.total_bytes_processed:,d} bytes for {part}")
    return part, job


def main():
    args = parser.parse_args()
    if "." not in args.dataset_id and args.project_id is not None:
        args.dataset_id = f"{args.project_id}.{args.dataset_id}"
    if "." not in args.destination_table and args.dataset_id is not None:
        args.destination_table = f"{args.dataset_id}.{args.destination_table}"
    client = bigquery.Client(args.project_id)
    with ThreadPool(args.parallelism) as pool:
        parts = pool.starmap(
            _run_part,
            [
                (client, part, args)
                for part in sorted(next(os.walk(args.query_dir))[2])
                if part.startswith("part") and part.endswith(".sql")
            ],
            chunksize=1,
        )
    if not args.dry_run:
        total_bytes = sum(job.total_bytes_processed for _, job in parts)
        query = (
            f"SELECT\n  *\nFROM\n  `{_sql_table_id(parts[0][1].destination)}`"
            + "".join(
                f"\nFULL JOIN\n  `{_sql_table_id(job.destination)}`"
                f"\nUSING\n  ({args.using})"
                for _, job in parts[1:]
            )
        )
        try:
            job = client.query(
                query=query,
                job_config=bigquery.QueryJobConfig(
                    destination=args.destination_table,
                    time_partitioning=args.time_partitioning_field,
                    clustering_fields=args.clustering_fields,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    use_legacy_sql=False,
                    priority=args.priority,
                ),
            )
            job.result()
            print(f"Processed {job.total_bytes_processed:,d} bytes to combine results")
            total_bytes += job.total_bytes_processed
            print(f"Processed {total_bytes:,d} bytes in total")
        finally:
            for _, job in parts:
                client.delete_table(_sql_table_id(job.destination).split("$")[0])
            print(f"Deleted {len(parts)} temporary tables")


if __name__ == "__main__":
    main()
