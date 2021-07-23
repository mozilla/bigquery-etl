#!/usr/bin/env python3

"""
Import iProspect CSV data from moz-fx-data-marketing-prod-iprospect storage bucket.

The CSV files are updated daily and contain the last 30 days of data. This script
will import only CSV data of the file that was uploaded on the specified date into BigQuery.
The destination table will contain the data of all files, with submission_date
being used for partitioning and specifying when the CSV file was uploaded.

Historic data can change in more recent CSV files. The raw data will be used for
debugging and for reporting changes of specific values in
moz-fx-data-marketing-prod.iprospect.detail_export_diff_v1
"""

from argparse import ArgumentParser
from google.cloud import bigquery
from google.cloud import storage

import pandas as pd

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-marketing-prod")
parser.add_argument("--bucket", default="moz-fx-data-marketing-prod-iprospect")
parser.add_argument("--prefix", default="mozilla_detail_export")
parser.add_argument("--dataset", default="iprospect")
parser.add_argument("--table", default="adspend_raw_v1")


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
            f"No iProspect data available for {args.date} in {args.bucket}/{args.prefix}"
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
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("fetch_ad_name", "STRING"),
            bigquery.SchemaField("vendor", "STRING"),
            bigquery.SchemaField("campaign", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("creative", "STRING"),
            bigquery.SchemaField("creative_concept", "STRING"),
            bigquery.SchemaField("creative_language", "STRING"),
            bigquery.SchemaField("creative_size", "STRING"),
            bigquery.SchemaField("creative_type", "STRING"),
            bigquery.SchemaField("device", "STRING"),
            bigquery.SchemaField("goal", "STRING"),
            bigquery.SchemaField("media_type", "STRING"),
            bigquery.SchemaField("operating_system", "STRING"),
            bigquery.SchemaField("placement", "STRING"),
            bigquery.SchemaField("product", "STRING"),
            bigquery.SchemaField("social_string", "STRING"),
            bigquery.SchemaField("targeting", "STRING"),
            bigquery.SchemaField("traffic_type", "STRING"),
            bigquery.SchemaField("imps_vendor", "INT64"),
            bigquery.SchemaField("clicks_vendor", "INT64"),
            bigquery.SchemaField("spend_vendor", "FLOAT64"),
            bigquery.SchemaField("client_fee", "FLOAT64"),
            bigquery.SchemaField("client_cost", "FLOAT64"),
            bigquery.SchemaField("video_3sec_vendor", "INT64"),
            bigquery.SchemaField("video_completions_vendor", "INT64"),
            bigquery.SchemaField("video_firstquartile_vendor", "INT64"),
            bigquery.SchemaField("video_midpoint_vendor", "INT64"),
            bigquery.SchemaField("video_thirdquartile_vendor", "INT64"),
            bigquery.SchemaField("video_views_vendor", "INT64"),
            bigquery.SchemaField("conversions", "FLOAT64"),
            bigquery.SchemaField("post_click_conversions", "FLOAT64"),
            bigquery.SchemaField("post_view_conversions", "FLOAT64"),
            bigquery.SchemaField("tweet_engagements", "FLOAT64"),
            bigquery.SchemaField("bounces_ga", "INT64"),
            bigquery.SchemaField("new_users_ga", "INT64"),
            bigquery.SchemaField("firefox_downloads_ga", "INT64"),
            bigquery.SchemaField("sessions_ga", "INT64"),
            bigquery.SchemaField("firefox_downloads_goal_2_sa360", "INT64"),
            bigquery.SchemaField("firefox_downloads_sa360", "INT64"),
            bigquery.SchemaField("desktop_downloads_sa360", "INT64"),
            bigquery.SchemaField("adjust_clicks", "INT64"),
            bigquery.SchemaField("adjust_installs", "INT64"),
            bigquery.SchemaField("adjust_sessions", "INT64"),
            bigquery.SchemaField("adjust_organic_clicks", "INT64"),
            bigquery.SchemaField("adjust_organic_installs", "INT64"),
            bigquery.SchemaField("adjust_organic_sessions", "INT64"),
            bigquery.SchemaField("dcm_active_view_eligible_impressions", "INT64"),
            bigquery.SchemaField("dcm_active_view_measurable_impressions", "INT64"),
            bigquery.SchemaField("dcm_active_view_viewable_impressions", "INT64"),
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
