#!/usr/bin/env python3

"""Monthly data exports of MDN 'Popularities'. This aggregates and counts total page visits."""
import logging
from argparse import ArgumentParser
from datetime import datetime
from os.path import join
from uuid import uuid4

from google.cloud import bigquery, storage

QUERY_TEMPLATE = """\
SELECT REGEXP_EXTRACT(pge.metrics.url2.page_path, r'^https://developer.mozilla.org(/.+?/docs/[^?#]+)') AS Page,
       COUNT(*) AS Pageviews,
FROM `moz-fx-data-shared-prod.mdn_yari.page` AS pge
WHERE DATE(submission_timestamp) BETWEEN DATE_TRUNC(@submission_date, MONTH) AND LAST_DAY(@submission_date)
  AND client_info.app_channel = 'prod'
  AND REGEXP_CONTAINS(pge.metrics.url2.page_path, r'^https://developer.mozilla.org(/.+?/docs/[^?#]+)')
  AND pge.metrics.string.page_http_status = '200'
  AND pge.metrics.string.navigator_user_agent LIKE 'Mozilla%'
GROUP BY Page
ORDER BY Pageviews DESC
"""

APP = "mdn_yari"

CURRENT_FILE_NAME = "current.csv"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--date", type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(), required=True
)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--temp_dataset", default="tmp")
parser.add_argument("--temp_table", default="mdn_popularities_v1")
parser.add_argument("--destination_project", default="moz-fx-mdn-prod")
parser.add_argument("--destination_bucket", default="popularities-prod-mdn")
parser.add_argument("--destination_path", default="")


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
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "submission_date", "DATE", str(args.date)
                ),
            ],
        ),
    ).result()

    # table to GCS object
    dataset_ref = bigquery.DatasetReference(args.project, args.temp_dataset)
    table_ref = dataset_ref.table(args.temp_table)

    target_file_name = f"{uuid4()}.csv"
    target_file_path = join(args.destination_path, args.date.strftime('%Y/%m'), target_file_name)
    mdn_uri = (
        f"gs://{args.destination_bucket}/{target_file_path}"
    )

    logging.info(
        "Exporting %s to GCS: %s:%s" % (temp_table, args.destination_project, mdn_uri)
    )
    extract_job = client.extract_table(
        table_ref,
        mdn_uri,
        location="US",
    )
    extract_job.result()  # Waits for job to complete.

    logging.info("Deleting temp table: %s" % temp_table)
    client.delete_table(temp_table)

    # Make it available as current.
    current_file_path = join(args.destination_path, CURRENT_FILE_NAME)
    
    storage_client = storage.Client(args.project)
    bucket = storage_client.get_bucket(args.destination_bucket)
    blob = bucket.get_blob(target_file_path)
    bucket.copy_blob(blob, bucket, current_file_path)

if __name__ == "__main__":
    main()
