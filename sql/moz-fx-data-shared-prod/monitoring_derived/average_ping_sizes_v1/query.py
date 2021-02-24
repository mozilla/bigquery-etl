#!/usr/bin/env python3

"""Determine stable table average ping sizes."""

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
parser.add_argument("--destination_table", default="average_ping_sizes_v1")


def get_tables(client, project, dataset):
    """Returns list of all available tables."""
    sql = f"SELECT dataset_id, table_id FROM `{project}.{dataset}.__TABLES__`"

    try:
        result = client.query(sql).result()
        return [(row.dataset_id, row.table_id) for row in result]
    except Exception as e:
        print(f"Error querying dataset {dataset}: {e}")

    return []


def get_average_ping_size_json(client, date, table):
    """Returns the average ping sizes for a specific date."""
    print(table)
    try:
        dataset_id = table[0]
        table_id = table[1]

        sql = f"""
            WITH total_pings AS (
                SELECT COUNT(*) as total
                FROM {dataset_id}.{table_id}
                WHERE DATE(submission_timestamp) = '{date}'
            ) SELECT
                SAFE_DIVIDE(byte_size, total) AS average_ping_size,
                byte_size as total_byte_size,
                total as row_count
            FROM total_pings, `moz-fx-data-shared-prod.monitoring.stable_table_sizes`
            WHERE submission_date = '{date}' AND dataset_id = '{dataset_id}' AND table_id = '{table_id}'
        """

        result = list(client.query(sql).result())[0]

        return {
            "submission_date": date,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "average_byte_size": result.average_ping_size,
            "total_byte_size": result.total_byte_size,
            "row_count": result.row_count,
        }
    except Exception as e:
        print(e)
        return None


def save_average_ping_sizes(
    client, average_ping_sizes, date, destination_dataset, destination_table
):
    """Writes average pings sizes for tables for a specific day to BigQuery."""
    job_config = bigquery.LoadJobConfig()
    job_config.schema = (
        bigquery.SchemaField("submission_date", "DATE"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("table_id", "STRING"),
        bigquery.SchemaField("average_byte_size", "FLOAT64"),
        bigquery.SchemaField("total_byte_size", "INT64"),
        bigquery.SchemaField("row_count", "INT64"),
    )
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

    partition_date = date.replace("-", "")
    client.load_table_from_json(
        average_ping_sizes,
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
            partial(get_tables, client, args.project),
            stable_datasets,
            chunksize=1,
        )

        stable_tables = [table for tables in stable_tables for table in tables]

        average_ping_sizes = p.map(
            partial(get_average_ping_size_json, client, args.date),
            stable_tables,
            chunksize=1,
        )

        average_ping_sizes = [s for s in average_ping_sizes if s is not None]

        save_average_ping_sizes(
            client,
            average_ping_sizes,
            args.date,
            args.destination_dataset,
            args.destination_table,
        )


if __name__ == "__main__":
    main()
