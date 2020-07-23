#!/usr/bin/env python3

"""Determine stable_table size partitions by performing dry runs."""

from argparse import ArgumentParser
from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="telemetry_stable")
parser.add_argument("--destination_dataset", default="monitoring")
parser.add_argument("--destination_table", default="telemetry_stable_sizes_v1")


def get_tables(client, project, dataset):
    """Returns list of all available tables."""
    sql = f"SELECT dataset_id, table_id FROM `{project}.{dataset}.__TABLES__`"
    result = client.query(sql).result()

    return [(row.dataset_id, row.table_id) for row in result]


def get_partition_size(client, table, date):
    """Returns the size of a specific date parition of the specified table."""
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    sql = f"SELECT * FROM {table} WHERE DATE(submission_timestamp) = '{date}'"

    job = client.query(sql, job_config=job_config)

    return job.total_bytes_processed


def save_table_sizes(client, table_sizes, date, destination_dataset, destination_table):
    """Writes paritions sizes for tables for a specific day to BigQuery."""

    job_config = bigquery.LoadJobConfig()
    job_config.schema = (
        bigquery.SchemaField("submission_date", "DATE"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("table_id", "STRING"),
        bigquery.SchemaField("byte_size", "INT64"),
    )
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

    partition_date = date.replace("-", "")
    client.load_table_from_json(
        table_sizes,
        f"{destination_dataset}.{destination_table}${partition_date}",
        job_config=job_config,
    ).result()


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    stable_tables = get_tables(client, args.project, args.dataset)

    partition_sizes = [
        {
            "submission_date": args.date,
            "dataset_id": dataset,
            "table_id": table,
            "byte_size": get_partition_size(client, f"{dataset}.{table}", args.date),
        }
        for dataset, table in stable_tables
    ]

    save_table_sizes(
        client,
        partition_sizes,
        args.date,
        args.destination_dataset,
        args.destination_table,
    )


if __name__ == "__main__":
    main()
