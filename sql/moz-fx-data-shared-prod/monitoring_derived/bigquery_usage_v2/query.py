#!/usr/bin/env python3

"""
Determine BigQuery usage.

To read more on the source table, please visit:
https://cloud.google.com/bigquery/docs/information-schema-jobs-by-organization
"""

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
parser.add_argument("--destination_table", default="bigquery_usage_v2")


def create_query(date, project):
    """Create query with filter for source projects."""
    return f"""
        WITH jobs_by_org AS (
          SELECT
            t1.project_id AS source_project,
            DATE('{date}') AS creation_date,
            job_id,
            job_type,
            reservation_id,
            cache_hit,
            state,
            statement_type,
            referenced_tables,
            destination_table.project_id AS  destination_project_id,
            destination_table.dataset_id AS  destination_dataset_id,
            destination_table.table_id AS  destination_table_id,
            user_email,
            end_time-start_time as task_duration,
            ROUND(total_bytes_processed / 1024 / 1024 / 1024 / 1024, 4)
            AS total_terabytes_processed,
            ROUND(total_bytes_billed / 1024 / 1024 / 1024 / 1024, 4)
            AS total_terabytes_billed,
            total_slot_ms,
            error_result.location AS error_location,
            error_result.reason AS error_reason,
            error_result.message AS error_message,
            query_info.resource_warning AS resource_warning,
          FROM
            `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION` AS t1
          WHERE
            DATE(creation_time) = '{date}'
            AND (t1.project_id IN UNNEST({DEFAULT_PROJECTS})
              OR referenced_tables.project_id IN UNNEST({DEFAULT_PROJECTS})
              OR destination_table.project_id IN UNNEST({DEFAULT_PROJECTS}))
          ),
        jobs_by_project AS (
          SELECT
            "{project}" AS source_project,
            DATE('{date}') AS creation_date,
            job_id,
            user_email,
            REGEXP_EXTRACT(query, r"Username: (.*?),") AS username,
            REGEXP_EXTRACT(query, r"Query ID: (\\w+), ") AS query_id,
          FROM
            `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
          WHERE
            DATE(creation_time) = '{date}'
          )
          SELECT
            jo.source_project,
            jo.creation_date,
            jo.job_id,
            jo.job_type,
            jo.reservation_id,
            jo.cache_hit,
            jo.state,
            jo.statement_type,
            jp.query_id,
            referenced_tables.project_id AS reference_project_id,
            referenced_tables.dataset_id AS reference_dataset_id,
            referenced_tables.table_id AS reference_table_id,
            jo.destination_project_id,
            jo.destination_dataset_id,
            jo.destination_table_id,
            jo.user_email,
            jp.username,
            jo.task_duration,
            jo.total_terabytes_processed,
            jo.total_terabytes_billed,
            jo.total_slot_ms,
            jo.error_location,
            jo.error_reason,
            jo.error_message,
            jo.resource_warning
          FROM jobs_by_org jo
          LEFT JOIN jobs_by_project jp
            USING(source_project,creation_date, job_id)
          LEFT JOIN
            UNNEST(referenced_tables) AS referenced_tables
          WHERE
            DATE(creation_time) = '{date}'
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
