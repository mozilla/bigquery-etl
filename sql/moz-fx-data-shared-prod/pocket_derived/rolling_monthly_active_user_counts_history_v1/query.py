#!/usr/bin/env python3

"""Load Pocket MAU data from GCS."""

from argparse import ArgumentParser

import uuid
from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True)  # expect string with format yyyy-mm-dd
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--source-bucket", default="moz-fx-data-prod-external-pocket-data")
parser.add_argument("--source-prefix", default="rolling_monthly_active_user_counts")
parser.add_argument("--destination_dataset", default="pocket_derived")
parser.add_argument(
    "--destination_table", default="rolling_monthly_active_user_counts_history_v1"
)


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    # First, we load the data from GCS to a temp table.
    uri = f"gs://{args.source_bucket}/{args.source_prefix}/{args.date}/*"
    tmp_suffix = uuid.uuid4().hex[0:6]
    tmp_table = f"tmp.pocket_mau_load_{tmp_suffix}"
    client.load_table_from_uri(
        uri,
        tmp_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.job.SourceFormat.PARQUET,
        ),
    ).result()

    # Next, we run a query that populates the destination table partition.
    client.query(
        f"""
        SELECT DATE("{args.date}") AS submission_date,
        measured_at,
        CAST(user_count AS INT64) AS user_count
        FROM {tmp_table}
        """,
        job_config=bigquery.QueryJobConfig(
            destination=".".join(
                args.project,
                args.destination_dataset,
                f"{args.destination_table}${args.date.replace('-', '')}",
            ),
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()

    # Finally, we clean up after ourselves.
    client.delete_table(tmp_table)


if __name__ == "__main__":
    main()
