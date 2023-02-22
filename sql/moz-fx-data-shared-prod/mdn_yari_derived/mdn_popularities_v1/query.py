#!/usr/bin/env python3

"""Monthly data exports of MDN 'Popularities'. This aggregates and counts total page visits and normalizes them agains the max."""
import logging
from argparse import ArgumentParser
from datetime import datetime

from google.cloud import bigquery, storage

QUERY_TEMPLATE = """\
WITH host_stripped AS (
  SELECT
    SPLIT(pge.metrics.url2.page_path, 'https://developer.mozilla.org')[OFFSET(1)] AS `path`,
    COUNT(*) AS page_hits,
  FROM
    `moz-fx-data-shared-prod.mdn_yari.page` AS pge
  WHERE
    DATE({date})
    BETWEEN DATE_TRUNC({date}, MONTH)
    AND LAST_DAY(@submission_date)
    AND CONTAINS_SUBSTR(pge.metrics.url2.page_path, 'https://developer.mozilla.org/')
    AND client_info.app_channel = 'prod'
  GROUP BY
    `path`
)
SELECT
  host_stripped.`path`,
  host_stripped.page_hits / FIRST_VALUE(host_stripped.page_hits) OVER (
    ORDER BY
      page_hits DESC
  ) AS popularity
FROM
  host_stripped
WHERE
  ARRAY_LENGTH(SPLIT(host_stripped.`path`, '/')) > 3
ORDER BY
    popularity DESC
"""

APP = "mdn_yari"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--date", type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(), required=True
)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--temp_dataset", default="tmp")
parser.add_argument("--temp_bucket", default="moz-fx-data-prod-external-data")
parser.add_argument("--temp_table", default="mdn_popularities_v1")
parser.add_argument("--destination_project", default="moz-fx-dev-gsleigh-migration")
parser.add_argument("--destination_bucket", default="mdn-dev")
parser.add_argument("--destination_path", default="popularities/current")


def main():
    """Generate a tmp table and extracts to GCS."""
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    # create temp table
    temp_table = f"{args.project}.{args.temp_dataset}.{args.temp_table}"
    logging.info("Creating a temp table: %s" % temp_table)

    client.query(
        QUERY_TEMPLATE.format(date=args.date),
        job_config=bigquery.QueryJobConfig(
            destination=temp_table,
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()

    # table to GCS object
    dataset_ref = bigquery.DatasetReference(args.project, args.temp_dataset)
    table_ref = dataset_ref.table(args.temp_table)

    target_file_name = f"{args.date.strftime('%m-%Y')}.json"
    temp_uri = (
        f"gs://{args.temp_bucket}/{APP}/{args.destination_path}/{target_file_name}.json"
    )

    extract_job = client.extract_table(
        table_ref,
        temp_uri,
        location="US",
    )
    extract_job.result()  # Waits for job to complete.
    logging.info("Exporting %s to GCS: %s" % (temp_table, temp_uri))

    client.delete_table(temp_table)
    logging.info("Deleting temp table: %s" % temp_table)

    # download the GCS object
    storage_client = storage.Client(args.project)
    bucket = storage_client.bucket(args.temp_bucket)
    local_path = f"/tmp/{target_file_name}.json"
    temp_blob_uri = f"{APP}/{args.destination_path}/{target_file_name}.json"

    logging.info("Downloading: %s to %s" % (temp_uri, local_path))
    temp_blob = bucket.blob(temp_blob_uri)
    temp_blob.download_to_filename(local_path)

    # upload to the target bucket
    mdn_storage_client = storage.Client(args.destination_project)
    mdn_bucket = mdn_storage_client.get_bucket(args.destination_bucket)
    mdn_uri = f"{args.destination_path}/{target_file_name}.json"

    logging.info("Uploading: %s to %s" % (local_path, mdn_uri))
    blob = mdn_bucket.blob(mdn_uri)
    blob.upload_from_filename(local_path)

    # delete temp GCS object
    # temp_blob.delete()
    # logging.info("Deleting temp blob: %s" % temp_blob_uri)


if __name__ == "__main__":
    main()
