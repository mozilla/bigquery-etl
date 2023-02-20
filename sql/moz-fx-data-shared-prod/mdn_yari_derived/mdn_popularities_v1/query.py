#!/usr/bin/env python3

"""Monthly data exports of MDN 'Popularities'. This aggregates and counts total page visits and normalizes them agains the max."""
from argparse import ArgumentParser
from datetime import datetime

from google.cloud import bigquery

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

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--date", type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(), required=True
)
parser.add_argument("--project", default="mozdata")
parser.add_argument("--destination_dataset", default="tmp")
parser.add_argument("--destination_table", default="mdn_popularities_v1")
parser.add_argument("--destination_bucket", default="mozdata-tmp")
parser.add_argument("--destination_gcs_path", default="mdn-dev/popularities/current")


def main():
    """Generate a tmp table and extracts to GCS."""
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    temp_table = f"{args.destination_project}.{args.destination_dataset}.{args.destination_table}"

    client.query(
        QUERY_TEMPLATE.format(date=args.date),
        job_config=bigquery.QueryJobConfig(
            destination=temp_table,
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()

    dataset_ref = bigquery.DatasetReference(args.project, args.destination_dataset)
    table_ref = dataset_ref.table(args.destination_table)

    target_file_name = f"{args.date.strftime('%m-%Y')}.json"
    destination_uri = f"gs://{args.destination_bucket}/{args.destination_gcs_path}/{target_file_name}.json"

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
    )
    extract_job.result()  # Waits for job to complete.

    client.delete_table(temp_table)


if __name__ == "__main__":
    main()
