#!/usr/bin/env python3

"""
Determine BigQuery usage.

To read more on the source tables, please visit:
https://cloud.google.com/bigquery/docs/information-schema-jobs-by-organization
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
parser.add_argument("--destination_project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="bigquery_usage_v2")
parser.add_argument("--tmp_table", default="jobs_by_project_tmp")


def create_jobs_by_project_tmp_table(date, client, tmp_table_name):
    """Create temp table to capture jobs by project tables."""
    # remove old table in case of re-run
    client.delete_table(tmp_table_name, not_found_ok=True)

    tmp_table = bigquery.Table(tmp_table_name)
    tmp_table.schema = (
        bigquery.SchemaField("source_project", "STRING"),
        bigquery.SchemaField("creation_date", "DATE"),
        bigquery.SchemaField("job_id", "STRING"),
        bigquery.SchemaField("user_email", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("query_id", "STRING"),
    )

    client.create_table(tmp_table)

    args = parser.parse_args()
    source_projects = args.source_projects

    for project in source_projects:
        query = f"""
              SELECT
                project_id AS source_project,
                date(creation_time) as creation_date,
                job_id,
                user_email,
                REGEXP_EXTRACT(query, r'Username: (.*?),') AS username,
                REGEXP_EXTRACT(query, r'Query ID: (\\w+), ') AS query_id,
              FROM
                `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
              WHERE
                DATE(creation_time) = '{date}'
        """

        try:
            job_config = bigquery.QueryJobConfig(
                destination=tmp_table_name, write_disposition="WRITE_APPEND"
            )
            client.query(query, job_config=job_config).result()

        except Exception as e:
            print(f"Error querying project {project}: {e}")


def create_query(date, tmp_table_name):
    """Create query to join tables."""
    return f"""
      SELECT DISTINCT
        jo.project_id AS source_project,
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
        jo.destination_table.project_id AS  destination_project_id,
        jo.destination_table.dataset_id AS  destination_dataset_id,
        jo.destination_table.table_id AS  destination_table_id,
        jo.user_email,
        jp.username,
        jo.end_time-jo.start_time AS task_duration,
        ROUND(jo.total_bytes_processed / 1024 / 1024 / 1024 / 1024, 4)
        AS total_terabytes_processed,
        ROUND(jo.total_bytes_billed / 1024 / 1024 / 1024 / 1024, 4)
        AS total_terabytes_billed,
        jo.total_slot_ms,
        jo.error_result.location AS error_location,
        jo.error_result.reason AS error_reason,
        jo.error_result.message AS error_message,
        jo.query_info_resource_warning AS resource_warning,
        DATE('{date}') AS submission_date,
      FROM `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1` jo
      LEFT JOIN UNNEST(referenced_tables) AS referenced_tables
      LEFT JOIN {tmp_table_name} jp
      ON jo.project_id = jp.source_project
      AND jo.creation_date = jp.creation_date
      AND jo.job_id = jp.job_id
      WHERE jo.creation_date = DATE('{date}')
    """


def main():
    """Run query."""
    args = parser.parse_args()
    project = args.project

    partition = args.date.replace("-", "")
    tmp_table_name = f"{args.project}.{args.destination_dataset}.{args.tmp_table}"
    destination_table = f"{args.destination_project}.{args.destination_dataset}.{args.destination_table}${partition}"

    # remove old partition in case of re-run
    client = bigquery.Client(project)
    client.delete_table(destination_table, not_found_ok=True)

    query = create_query(args.date, tmp_table_name)

    create_jobs_by_project_tmp_table(args.date, client, tmp_table_name)

    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="submission_date"
        ),
    )
    client.query(query, job_config=job_config).result()

    client.delete_table(tmp_table_name, not_found_ok=True)


if __name__ == "__main__":
    main()
