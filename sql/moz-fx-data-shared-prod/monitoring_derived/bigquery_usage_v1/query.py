#!/usr/bin/env python3

"""
Determine big query usage.

To read more on the source table, please visit:
https://cloud.google.com/bigquery/docs/information-schema-jobs
"""

from argparse import ArgumentParser

from google.cloud import bigquery

DEFAULT_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
]

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
# projects queries were run from that access table
parser.add_argument("--source_projects", nargs="+", default=DEFAULT_PROJECTS)
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="bigquery_usage_v1")


def create_query(date, source_project):
    """Create query for a source project."""
    return f"""
        SELECT
         "{source_project}" AS source_project,
          DATE('{date}') AS creation_date,
          job_id,
          job_type,
          state,
          referenced_tables.project_id AS reference_project_id,
          dataset_id AS reference_dataset_id,
          table_id AS reference_table_id,
          destination_table.project_id AS  destination_project_id,
          destination_table.dataset_id AS  destination_dataset_id,
          destination_table.table_id AS  destination_table_id,
          user_email,
          REGEXP_EXTRACT(query, r"Username: (.*?),") AS username,
          REGEXP_EXTRACT(query, r"Query ID: (\\w+), ") AS query_id,
          end_time-start_time as task_duration,
          ROUND(total_bytes_processed / 1024 / 1024 / 1024 / 1024, 4)
          AS total_terabytes_processed,
          ROUND(total_bytes_processed / 1024 / 1024 / 1024 / 1024 * 5, 2) AS cost_usd,
          error_result.location AS error_location,
          error_result.reason AS error_reason,
          error_result.message AS error_message,
        FROM
          `{source_project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        LEFT JOIN
          UNNEST(referenced_tables) AS referenced_tables
        WHERE
          DATE(creation_time) = '{date}'
    """


def main():
    """Run query for each source project."""
    args = parser.parse_args()

    partition = args.date.replace("-", "")
    destination_table = f"""
            {args.project}.{args.destination_dataset}.{args.destination_table}${partition}
    """

    # remove old partition in case of re-run
    client = bigquery.Client(args.project)
    client.delete_table(destination_table, not_found_ok=True)

    for project in args.source_projects:
        client = bigquery.Client(project)
        query = create_query(args.date, project)
        job_config = bigquery.QueryJobConfig(
            destination=destination_table, write_disposition="WRITE_APPEND"
        )
        client.query(query, job_config=job_config).result()


if __name__ == "__main__":
    main()
