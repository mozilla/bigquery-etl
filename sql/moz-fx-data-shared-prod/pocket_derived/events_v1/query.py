#!/usr/bin/env python3

"""Load Pocket Frontend event data from GCS."""
import uuid
from argparse import ArgumentParser
from datetime import datetime, timedelta

from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--date", type=lambda x: datetime.strptime(x, "%Y-%m-%d").date(), required=True
)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--source-bucket", default="moz-fx-data-prod-external-pocket-data")
parser.add_argument("--source-prefix", default="backend_events_for_mozilla")
parser.add_argument("--destination_dataset", default="pocket_derived")
parser.add_argument("--destination_table", default="events_v1")


def main():
    """Load data to temp table and populate destination table via query."""
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    print(f"Running for {args.date}")

    # First, we load the data from GCS to a temp table.
    # The folder naming is done by the Pocket side, so we stick with it.
    uri = f"gs://{args.source_bucket}/{args.source_prefix}/date={args.date}/time=00-00-00-000/*"
    tmp_suffix = uuid.uuid4().hex[0:6]
    tmp_table = f"tmp.pocket_events_{tmp_suffix}"
    result = client.load_table_from_uri(
        uri,
        tmp_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.job.SourceFormat.PARQUET,
        ),
    ).result()
    print(f"Loaded {result.output_rows} rows into {tmp_table}")

    # Next, we run a query that populates the destination table partition.
    result = client.query(
        f"""
WITH tmp AS (
SELECT
  * EXCEPT (METADATA, CLIENT_INFO, EVENT_EXTRAS),
  PARSE_JSON(METADATA).metadata AS metadata,
  PARSE_JSON(CLIENT_INFO).client_info AS client_info,
  PARSE_JSON(EVENT_EXTRAS) AS event_extra
FROM {tmp_table} AS t
WHERE
  DATE(SUBMISSION_TIMESTAMP) <= DATE("{args.date.isoformat()}")
)

SELECT
  SUBMISSION_TIMESTAMP as submission_timestamp,
  APP_ID as normalized_app_id,
  STRUCT(
    STRING(client_info.client_id) AS client_id,
    STRING(client_info.user_id) AS user_id,
    STRING(client_info.app_build) AS app_build,
    STRING(client_info.locale) AS locale,
    STRING(client_info.os) AS os,
    STRING(client_info.os_version) AS os_version
  ) AS client_info,
  STRUCT(
    STRUCT(
      STRING(metadata.geo.city) AS city,
      STRING(metadata.geo.country) AS country
    ) AS geo,
    STRUCT(
      STRING(metadata.user_agent.browser) AS browser,
      STRING(metadata.user_agent.os) AS os,
      STRING(metadata.user_agent.version) AS version
    ) AS user_agent
  ) AS metadata,
  EVENT_NAME as event,
  EVENT_TIMESTAMP as event_timestamp,
  event_extra
FROM tmp
        """,
        job_config=bigquery.QueryJobConfig(
            destination=".".join(
                [
                    args.project,
                    args.destination_dataset,
                    f"{args.destination_table}${args.date.strftime('%Y%m%d')}",
                ]
            ),
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()
    print(f"Ingested {result.total_rows} rows into {args.destination_table}")

    # Finally, we clean up after ourselves.
    client.delete_table(tmp_table)


if __name__ == "__main__":
    main()
