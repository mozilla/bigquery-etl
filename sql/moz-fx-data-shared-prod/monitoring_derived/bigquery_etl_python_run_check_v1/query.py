#!/usr/bin/env python3

"""Run a query script to check if bigquery-etl is working properly."""

from argparse import ArgumentParser

from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--run_id", required=True)
parser.add_argument("--task_instance", required=True)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="bigquery_etl_python_run_check_v1")


def main():
    args = parser.parse_args()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    client = bigquery.Client(args.project)
    job_config = bigquery.QueryJobConfig(
        destination=destination_table, write_disposition="WRITE_TRUNCATE"
    )

    query = f"""
      SELECT
        CURRENT_TIMESTAMP() AS run_timestamp,
        '{args.task_instance}' AS task_instance,
        '{args.run_id}' AS run_id
    """

    client.query(query, job_config=job_config).result()


if __name__ == "__main__":
    main()
