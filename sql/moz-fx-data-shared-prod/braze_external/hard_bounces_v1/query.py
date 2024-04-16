#!/usr/bin/env python3

"""Load Braze current data from GCS."""

from argparse import ArgumentParser

from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument(
    "--source-bucket", default="moz-fx-data-marketing-prod-braze-firefox"
)
parser.add_argument(
    "--source-prefix",
    default="currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071/event_type=users.messages.email.Bounce",
)
parser.add_argument("--destination_dataset", default="braze_external")
parser.add_argument("--destination_table", default="hard_bounces_v1")


def main():
    """Load data from GCS into table."""
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    uri = f"gs://{args.source_bucket}/{args.source_prefix}/*"
    client.load_table_from_uri(
        uri,
        destination=f"{args.project}.{args.destination_dataset}.{args.destination_table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.job.SourceFormat.AVRO,
        ),
    ).result()


if __name__ == "__main__":
    main()
