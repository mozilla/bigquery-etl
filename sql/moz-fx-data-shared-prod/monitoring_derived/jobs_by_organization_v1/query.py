#!/usr/bin/env python3

"""

Put data from the projects below into a table.

  mozdata.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION,
  moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION,
  moz-fx-data-marketing-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION,
  moz-fx-data-bq-data-science.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
into a table so that can be queried
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
parser.add_argument("--destination_table", default="jobs_by_organization")


def create_query(date, project):
    """Create query with filter for source projects."""
    return f"""
        SELECT
          bi_engine_statistics,
          cache_hit,
          creation_time,
          destination_table,
          end_time,
          error_result,
          job_id,
          job_stages,
          job_type,
          labels,
          parent_job_id,
          principal_subject,
          priority,
          project_id,
          project_number,
          query,
          referenced_tables,
          reservation_id,
          session_info,
          start_time,
          state,
          statement_type,
          timeline,
          total_bytes_billed,
          total_bytes_processed,
          total_modified_partitions,
          total_slot_ms ,
          transaction_id,
          user_email,
          query_info.resource_warning ,
          query_info.query_hashes.normalized_literals,
          query_info.performance_insights,
          transferred_bytes,
          materialized_view_statistics,
          DATE(creation_time) as creation_date
        FROM
          `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
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
