#!/usr/bin/env python3

"""Determine stable table size partitions by performing dry runs."""

from argparse import ArgumentParser
from fnmatch import fnmatchcase
from functools import partial
from multiprocessing.pool import ThreadPool

from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="*_stable")  # pattern
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="stable_table_sizes_v1")


def get_tables(client, project, dataset):
    """Returns list of all available tables."""
    sql = f"SELECT dataset_id, table_id FROM `{project}.{dataset}.__TABLES__`"

    try:
        result = client.query(sql).result()
        return [(row.dataset_id, row.table_id) for row in result]
    except Exception as e:
        print(f"Error querying dataset {dataset}: {e}")

    return []


def get_partition_size_json(client, date, table):
    """Returns the size of a specific date parition of the specified table."""
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dataset_id = table[0]
    table_id = table[1]

    sql = f"""
        SELECT * FROM {dataset_id}.{table_id}
        WHERE DATE(submission_timestamp) = '{date}'
    """

    job = client.query(sql, job_config=job_config)

    size = job.total_bytes_processed

    return {
        "submission_date": date,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "byte_size": size,
    }


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

    stable_datasets = [
        dataset.dataset_id
        for dataset in list(client.list_datasets())
        if fnmatchcase(dataset.dataset_id, args.dataset)
    ]

    with ThreadPool(20) as p:
        stable_tables = p.map(
            partial(get_tables, client, args.project), stable_datasets, chunksize=1,
        )

        stable_tables = [table for tables in stable_tables for table in tables]

        partition_sizes = p.map(
            partial(get_partition_size_json, client, args.date),
            stable_tables,
            chunksize=1,
        )

        save_table_sizes(
            client,
            partition_sizes,
            args.date,
            args.destination_dataset,
            args.destination_table,
        )


if __name__ == "__main__":
    main()
