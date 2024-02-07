#!/usr/bin/env python3

"""Determine cost of previously scheduled bigquery-etl queries."""

from argparse import ArgumentParser
from glob import glob
from pathlib import Path

from google.cloud import bigquery

DEFAULT_PROJECTS = [
    "moz-fx-data-shared-prod",
    "moz-fx-data-experiments",
    "mozdata"
]

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
# projects queries were run from that access table
parser.add_argument("--source_projects", nargs="+", default=DEFAULT_PROJECTS)
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument(
    "--destination_table", default="bigquery_etl_scheduled_query_usage_v1"
)
parser.add_argument("--sql_dir", default="sql/")


def create_query(query_paths, date, project):
    datasets = ['"' + path.parent.parent.name + '"' for path in query_paths]
    tables = [path.parent.name + "*" for path in query_paths]

    return f"""
      WITH logs AS (
        SELECT
            DATE('{date}') AS submission_date,
            project_id,
            referenced_tables,
          FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
          WHERE
            statement_type = "SELECT" AND
            DATE(creation_time) = '{date}'
        )
        SELECT
          submission_date,
          logs.project_id,
          CONCAT(referenced_table.project_id, ".", referenced_table.dataset_id, ".", referenced_table.table_id) as table,
          COUNT(*) AS number_of_accesses
        FROM logs
        CROSS JOIN UNNEST(logs.referenced_tables) AS referenced_table
        WHERE
          referenced_table.dataset_id IN ({",".join(datasets)})
          AND REGEXP_CONTAINS(referenced_table.table_id, "({"|".join(tables)})")
        GROUP BY submission_date, project_id, table
    """


def main():
    args = parser.parse_args()

    sql_queries = list(map(Path, glob(f"{args.sql_dir}/**/query.sql", recursive=True)))
    python_queries = list(
        map(Path, glob(f"{args.sql_dir}/**/query.py", recursive=True))
    )
    multipart_queries = list(
        map(Path, glob(f"{args.sql_dir}/**/part1.sql", recursive=True))
    )
    query_paths = sql_queries + python_queries + multipart_queries
    partition = args.date.replace("-", "")
    destination_table = f"{args.project}.{args.destination_dataset}.{args.destination_table}${partition}"

    # remove old partition in case of re-run
    client = bigquery.Client(args.project)
    client.delete_table(destination_table, not_found_ok=True)

    for project in args.source_projects:
        client = bigquery.Client(project)
        query = create_query(query_paths, args.date, project)
        job_config = bigquery.QueryJobConfig(
            destination=destination_table, write_disposition="WRITE_APPEND"
        )
        client.query(query, job_config=job_config).result()


if __name__ == "__main__":
    main()
