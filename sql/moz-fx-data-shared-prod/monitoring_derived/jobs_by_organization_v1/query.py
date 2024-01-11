#!/usr/bin/env python3

"""Put data from region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION into a table."""

from argparse import ArgumentParser

from google.cloud import bigquery

DEFAULT_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
    "moz-fx-data-bq-data-science",
]

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="jobs_by_organization_v1")


def create_query(date, project):
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
          ARRAY_CONCAT(referenced_tables, (SELECT ARRAY_AGG(
            STRUCT(
              materialized_view.table_reference.project_id AS project_id,
              materialized_view.table_reference.dataset_id AS dataset_id,
              materialized_view.table_reference.table_id AS table_id
            )
          ) FROM UNNEST(materialized_view_statistics.materialized_view) AS materialized_view)) AS referenced_tables,
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
          DATE(creation_time) = '{date}'
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

    partition = args.date.replace("-", "")
    destination_table = f"{args.destination_project}.{args.destination_dataset}.{args.destination_table}${partition}"

    # remove old partition in case of re-run
    client = bigquery.Client(project)
    client.delete_table(destination_table, not_found_ok=True)

    query = create_query(args.date, project)
    job_config = bigquery.QueryJobConfig(
        destination=destination_table, write_disposition="WRITE_APPEND"
    )
    client.query(query, job_config=job_config).result()


if __name__ == "__main__":
    main()
