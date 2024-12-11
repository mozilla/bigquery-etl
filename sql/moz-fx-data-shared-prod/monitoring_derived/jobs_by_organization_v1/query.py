#!/usr/bin/env python3

"""Put data from region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION into a table."""

from argparse import ArgumentParser
from datetime import date, timedelta

from google.cloud import bigquery

DEFAULT_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
    "moz-fx-data-bq-data-science",
]

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--date", required=True, type=date.fromisoformat
)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="jobs_by_organization_v1")


def create_query(job_date: date, project: str):
    """Create query with filter for source projects."""
    return f"""
        SELECT
          cache_hit,
          creation_time,
          destination_table,
          end_time,
          error_result,
          job_id,
          job_type,
          parent_job_id,
          priority,
          project_id,
          project_number,
          referenced_tables,
          reservation_id,
          start_time,
          state,
          statement_type,
          timeline,
          total_bytes_billed,
          total_bytes_processed,
          total_modified_partitions,
          total_slot_ms,
          user_email,
          query_info.resource_warning as query_info_resource_warning,
          query_info.query_hashes.normalized_literals as query_info_query_hashes_normalized_literals,
          transferred_bytes,
          DATE(creation_time) as creation_date
        FROM
          `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
        WHERE
          DATE(creation_time) = '{job_date.isoformat()}'
          AND (
                (
                  project_id IN UNNEST({DEFAULT_PROJECTS})
                  )
                OR EXISTS
                (
                  SELECT * FROM UNNEST(referenced_tables) AS ref_tables
                    WHERE ref_tables.project_id IN UNNEST({DEFAULT_PROJECTS})
                )
                OR
                (
                    destination_table.project_id IN UNNEST({DEFAULT_PROJECTS})
                )
            )
    """


def main():
    """Run query."""
    args = parser.parse_args()
    project = args.project

    # reprocess previous date to update jobs completed after the last run
    for partition_date in (args.date - timedelta(days=1), args.date):
        partition = partition_date.strftime("%Y%m%d")
        destination_table = (
            f"{args.destination_project}.{args.destination_dataset}"
            f".{args.destination_table}${partition}"
        )

        client = bigquery.Client(project)

        query = create_query(partition_date, project)
        job_config = bigquery.QueryJobConfig(
            destination=destination_table,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        result = client.query(query, job_config=job_config).result()

        print(f"Wrote {result.total_rows} rows to {destination_table}")


if __name__ == "__main__":
    main()
