#!/usr/bin/env python3

"""Determine cost of previously scheduled bigquery-etl queries."""

from argparse import ArgumentParser
from fnmatch import fnmatchcase
from glob import glob
from pathlib import Path

from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument(
    "--destination_table", default="bigquery_etl_scheduled_queries_cost_v1"
)
parser.add_argument("--sql_dir", default="sql/")


def create_query(query_paths, date):
    datasets = ['"' + path.parent.parent.name + '"' for path in query_paths]

    tables = [path.parent.name + "*" for path in query_paths]

    return f"""
      SELECT
        DATE('{date}') AS submission_date,
        creation_time,
        destination_table.dataset_id AS dataset,
        -- remove partition from table name
        REGEXP_REPLACE(destination_table.table_id, r"\$.+", "") AS table,
        total_bytes_processed,
        total_bytes_processed / 1024 / 1024 / 1024 / 1024 * 5 AS cost_usd
      FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE DATE(creation_time) = '{date}'
        AND destination_table.dataset_id IN ({",".join(datasets)})
        AND REGEXP_CONTAINS(destination_table.table_id, "({"|".join(tables)})")
    """


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    sql_queries = list(map(Path, glob(f"{args.sql_dir}/**/query.sql", recursive=True)))
    python_queries = list(
        map(Path, glob(f"{args.sql_dir}/**/query.py", recursive=True))
    )
    multipart_queries = list(
        map(Path, glob(f"{args.sql_dir}/**/part1.sql", recursive=True))
    )
    query_paths = sql_queries + python_queries + multipart_queries

    query = create_query(query_paths, args.date)

    partition = args.date.replace("-", "")
    destination_table = f"{args.project}.{args.destination_dataset}.{args.destination_table}${partition}"
    job_config = bigquery.QueryJobConfig(
        destination=destination_table, write_disposition="WRITE_TRUNCATE"
    )
    client.query(query, job_config=job_config).result()


if __name__ == "__main__":
    main()
