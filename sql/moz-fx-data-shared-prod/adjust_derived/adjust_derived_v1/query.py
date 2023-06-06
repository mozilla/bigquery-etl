#!/usr/bin/env python3

"""
Import Adjust CSV data from  storage bucket.

The CSV files are updated daily and are retained for the last 30 days of data. This script
will import only CSV data of the file that was uploaded on the specified date into BigQuery.
The destination table will contain the data of all files, with submission_date
being used for partitioning and specifying when the CSV file was uploaded.

Historic data can change in more recent CSV files. The raw data will be used to calculate

"""

from argparse import ArgumentParser

import pandas as pd
from google.cloud import bigquery, storage

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod ")
parser.add_argument("--bucket", default="moz-fx-data-prod-external-adjust-data")
parser.add_argument("--prefix", default="")
parser.add_argument("--dataset", default="analysis")
parser.add_argument("--table", default="mhirose_adjust_derived_raw")


def main():
    """Load CSV data to temporary table."""
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    storage_client = storage.Client()
    blobs = list(
        storage_client.list_blobs(args.bucket, prefix=f"{args.prefix}_{args.date}")
    )

    if len(blobs) == 0:
        raise Exception(
            f"No Adjust data available for {args.date} in {args.bucket}/{args.prefix}"
        )

    uri = f"gs://{args.bucket}/{blobs[0].name}"
    df = pd.read_csv(uri)
    df["submission_date"] = pd.to_datetime(args.date)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="submission_date",
        ),
        schema=[
            bigquery.SchemaField("app_name_dashboard", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("install_begin_time", "DATETIME"),
            bigquery.SchemaField("installed_at", "DATETIME"),
            bigquery.SchemaField("conversion_duration", "INTEGER"),
            bigquery.SchemaField("app_name", "STRING"),
            bigquery.SchemaField("app_token", "STRING"),
            bigquery.SchemaField("app_version", "STRING"),
            bigquery.SchemaField("network_name", "STRING"),
            bigquery.SchemaField("network_type", "STRING"),
            bigquery.SchemaField("ad_revenue_network", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("adgroup_name", "STRING"),
            bigquery.SchemaField("creative_name", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("os_name", "STRING"),
            bigquery.SchemaField("adid", "STRING"),
            bigquery.SchemaField("device_manufacturer", "STRING"),
            bigquery.SchemaField("device_type", "STRING"),
            bigquery.SchemaField("store", "STRING"),
            bigquery.SchemaField("activity_kin", "STRING"),
            bigquery.SchemaField("submission_date", "DATE"),
        ],
    )

    partition = args.date.replace("-", "")
    destination = f"{args.project}.{args.dataset}.{args.table}${partition}"
    job = client.load_table_from_dataframe(df, destination, job_config=job_config)

    print(f"Running job {job.job_id}")
    job.result()
    print(f"Loaded {uri} for {args.date}")


if __name__ == "__main__":
    main()
