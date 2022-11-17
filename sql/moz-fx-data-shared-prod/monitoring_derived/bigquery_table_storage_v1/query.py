"""Determine big query table storage."""

from argparse import ArgumentParser

from google.cloud import bigquery

DEFAULT_PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-derived-datasets",
    "moz-fx-data-marketing-prod",
]

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
# projects queries were run from that access table
parser.add_argument("--source_projects", nargs="+", default=DEFAULT_PROJECTS)
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="bigquery_table_storage_v1")


def create_query(date, source_project):
    """Create query for a source project."""
    return f"""
        SELECT
          DATE('{date}') AS creation_date,
          project_id,
          table_schema AS dataset_id,
          table_name AS table_id,
          total_rows,
          total_partitions,
          total_logical_bytes,
          active_logical_bytes,
          long_term_logical_bytes,
          total_physical_bytes,
          active_physical_bytes,
          long_term_physical_bytes,
          time_travel_physical_bytes,
          (active_logical_bytes/POW(1024, 3) * 0.02)
          + ((long_term_logical_bytes/ POW(1024, 3)) * 0.01)
          AS logical_billing_cost_usd,
          (active_physical_bytes/POW(1024, 3) * 0.04)
          + ((long_term_physical_bytes/ POW(1024, 3)) * 0.02)
          AS physical_billing_cost_usd
        FROM `{source_project}.region-us.INFORMATION_SCHEMA.TABLE_STORAGE`
        WHERE
          DATE(creation_time) = '{date}'
        ORDER BY creation_date, project_id, dataset_id, table_id
    """


def main():
    """Run query for each source project."""
    args = parser.parse_args()

    partition = args.date.replace("-", "")
    destination_table = f"""
    {args.project}.{args.destination_dataset}.{args.destination_table}${partition}
    """

    for project in args.source_projects:
        client = bigquery.Client(project)
        query = create_query(args.date, project)
        job_config = bigquery.QueryJobConfig(
            destination=destination_table, write_disposition="WRITE_TRUNCATE"
        )
        client.query(query, job_config=job_config).result()


if __name__ == "__main__":
    main()
