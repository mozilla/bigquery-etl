#!/usr/bin/env python3

"""Determine stable and derived table size partitions by performing dry runs."""
import datetime
from argparse import ArgumentParser
from fnmatch import fnmatchcase
from functools import partial, cache
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery, exceptions

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--date", required=True, type=datetime.date.fromisoformat, help="Date in yyyy-mm-dd"
)
parser.add_argument(
    "--project",
    default="moz-fx-data-shared-prod",
    help="Project of tables to get sizes for",
)
parser.add_argument("--dataset", default=("*_derived", "*_stable"))  # pattern
parser.add_argument("--destination_project")  # default to --project
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="stable_and_derived_table_sizes_v1")


def get_tables(
    client: bigquery.Client, dataset: str
) -> List[Tuple[str, str]]:
    """Returns list of all available tables."""
    return [(table.dataset_id, table.table_id) for table in client.list_tables(dataset)]


@cache
def get_partition_columns_for_dataset(
    client: bigquery.Client, dataset_id: str
) -> Dict[str, str]:
    sql = f"""
    SELECT table_schema, table_name, column_name
    FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS
    WHERE is_partitioning_column = 'YES'
    """

    results = {
        f"{result.table_schema}.{result.table_name}": result.column_name
        for result in client.query_and_wait(sql)
    }

    return results


def get_partition_size_json(
    client: bigquery.Client, date: datetime.date, table: Tuple[str, str]
) -> Optional[Dict[str, Any]]:
    """Returns the size of a specific date parition of the specified table."""
    dataset_id = table[0]
    table_id = table[1]

    partitioning_column = get_partition_columns_for_dataset(client, dataset_id).get(
        f"{dataset_id}.{table_id}"
    )

    if partitioning_column in ("submission_date", "submission_timestamp"):
        try:
            table_info = client.get_table(
                f"{dataset_id}.{table_id}${date.strftime('%Y%m%d')}"
            )
            byte_size = table_info.num_bytes
            row_count = table_info.num_rows
        except exceptions.BadRequest as e:  # non-daily partitions will cause error
            print(f"Error getting partition {dataset_id}.{table_id}: {e}")

            # dry run partition if we can't get the daily partition
            try:
                sql = f"""
                SELECT * FROM `{dataset_id}.{table_id}`
                WHERE DATE({partitioning_column}) = '{date}'
                """
                job = client.query(
                    sql, job_config=bigquery.QueryJobConfig(dry_run=True)
                )
                byte_size = (
                    job.total_bytes_processed
                    if job.total_bytes_processed is not None
                    else 0
                )
                row_count = None
            except exceptions.Forbidden as e2:
                err_msg = e2.message.split("\n")[0]
                print(f"Error running query {dataset_id}.{table_id}: {err_msg}")
                return

        return {
            "submission_date": date.isoformat(),
            "dataset_id": dataset_id,
            "table_id": table_id,
            "byte_size": byte_size,
            "row_count": row_count,
        }


def save_table_sizes(
    client: bigquery.Client,
    table_sizes: List[Dict[str, Any]],
    date: datetime.date,
    destination_project: str,
    destination_dataset: str,
    destination_table: str,
):
    """Writes partitions sizes for tables for a specific day to BigQuery."""

    job_config = bigquery.LoadJobConfig()
    job_config.schema = (
        bigquery.SchemaField("submission_date", "DATE"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("table_id", "STRING"),
        bigquery.SchemaField("byte_size", "INT64"),
        bigquery.SchemaField("row_count", "INT64"),
    )
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema_update_options = bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION

    partition_date = date.strftime("%Y%m%d")
    client.load_table_from_json(
        table_sizes,
        f"{destination_project}.{destination_dataset}.{destination_table}${partition_date}",
        job_config=job_config,
    ).result()


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    for datediff in range(2):
        stable_derived_partition_sizes = []
        current_date = args.date - datetime.timedelta(days=datediff)
        print(f"Getting {current_date}")

        for arg_dataset in args.dataset:
            datasets = [
                dataset.dataset_id
                for dataset in list(client.list_datasets())
                if fnmatchcase(dataset.dataset_id, arg_dataset)
                and dataset.dataset_id not in ("monitoring_derived", "backfills_staging_derived")
            ]
            with ThreadPool(20) as p:
                tables = p.map(
                    partial(get_tables, client),
                    datasets,
                    chunksize=1,
                )
                tables = [table for tables in tables for table in tables]

                partition_sizes = p.map(
                    partial(get_partition_size_json, client, current_date),
                    tables,
                    chunksize=1,
                )
                partition_sizes = filter(lambda x: x is not None, partition_sizes)
                stable_derived_partition_sizes.extend(partition_sizes)

        save_table_sizes(
            client,
            stable_derived_partition_sizes,
            current_date,
            args.destination_project,
            args.destination_dataset,
            args.destination_table,
        )


if __name__ == "__main__":
    main()
