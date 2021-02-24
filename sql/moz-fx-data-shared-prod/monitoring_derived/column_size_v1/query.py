#!/usr/bin/env python3

"""Determine column sizes by performing dry runs."""

from argparse import ArgumentParser
from functools import partial
from multiprocessing.pool import ThreadPool

from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument(
    "--tables", nargs="*", default=["telemetry_stable.main_v4"]
)
parser.add_argument("--destination_dataset", default="monitoring_derived")
parser.add_argument("--destination_table", default="column_size_v1")


def get_columns(client, project, dataset, table):
    """Returns list of all columns in the table."""
    sql = (
        f"SELECT field_path AS column_name "
        f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` "
        f"WHERE table_name='{table}'"
    )

    try:
        result = client.query(sql).result()
        return [(dataset, table, row.column_name) for row in result]
    except Exception as e:
        print(f"Error querying dataset {dataset}: {e}")

    return []


def get_column_size_json(client, date, column):
    """Returns the size of a specific date parition of the specified table."""
    print(column)
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dataset_id = column[0]
        table_id = column[1]
        column = column[2]

        sql = f"""
            SELECT {column} FROM {dataset_id}.{table_id}
            WHERE DATE(submission_timestamp) = '{date}'
        """

        job = client.query(sql, job_config=job_config)

        size = job.total_bytes_processed

        return {
            "submission_date": date,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "column_name": column,
            "byte_size": size,
        }
    except Exception as e:
        print(e)
        return None


def save_column_sizes(
    client, column_sizes, date, destination_dataset, destination_table
):
    """Writes column sizes for tables for a specific day to BigQuery."""

    job_config = bigquery.LoadJobConfig()
    job_config.schema = (
        bigquery.SchemaField("submission_date", "DATE"),
        bigquery.SchemaField("dataset_id", "STRING"),
        bigquery.SchemaField("table_id", "STRING"),
        bigquery.SchemaField("column_name", "STRING"),
        bigquery.SchemaField("byte_size", "INT64"),
    )
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

    partition_date = date.replace("-", "")
    client.load_table_from_json(
        column_sizes,
        f"{destination_dataset}.{destination_table}${partition_date}",
        job_config=job_config,
    ).result()


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    with ThreadPool(20) as p:
        table_columns = [
            column
            for t in args.tables
            for column in get_columns(
                client, args.project, t.split(".")[0], t.split(".")[1]
            )
        ]

        column_sizes = p.map(
            partial(get_column_size_json, client, args.date),
            table_columns,
            chunksize=1,
        )

        column_sizes = [cs for cs in column_sizes if cs is not None]

        save_column_sizes(
            client,
            column_sizes,
            args.date,
            args.destination_dataset,
            args.destination_table,
        )


if __name__ == "__main__":
    main()
