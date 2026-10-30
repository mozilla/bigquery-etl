"""Import Deepnote audit log CSV exports from GCS into BigQuery.

Until Deepnote provides an audit log export endpoint, the CSV export is downloaded
manually from Settings & members > Security > Audit log and uploaded to GCS.
Each export covers a rolling window (e.g. the last 30 days), so exports overlap;
rows are deduplicated on a hash of their content and merged into the table, which
makes re-running over all files idempotent.

The `ip` column of the export is never written to BigQuery.
"""

import logging
from argparse import ArgumentParser

from google.cloud import bigquery

DEFAULT_SOURCE_URI = "gs://moz-fx-data-shared-prod-analysis/deepnote/audit_logs/*.csv"
# Mozilla's default Deepnote workspace
DEFAULT_WORKSPACE_ID = "5730346c-9297-42b0-80b0-35611206a1d7"

# column order of the Deepnote audit log CSV export
CSV_COLUMNS = [
    "actor_type",
    "user_id",
    "email",
    "team_id",
    "resource_id",
    "ip",
    "user_agent",
    "type",
    "metadata",
    "created_at",
]

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--dataset", default="deepnote_derived")
parser.add_argument("--table", default="audit_log_v1")
parser.add_argument("--source-uri", "--source_uri", default=DEFAULT_SOURCE_URI)
parser.add_argument("--workspace-id", "--workspace_id", default=DEFAULT_WORKSPACE_ID)
parser.add_argument("--billing-project", "--billing_project", default=None)


MERGE_QUERY = """
MERGE
  `{destination}` AS target
USING
  (
    WITH parsed AS (
      SELECT
        TO_HEX(
          SHA256(
            TO_JSON_STRING(
              [created_at, actor_type, user_id, email, team_id, resource_id, user_agent, type, metadata]
            )
          )
        ) AS event_id,
        TIMESTAMP(created_at) AS created_at,
        NULLIF(actor_type, '') AS actor_type,
        NULLIF(user_id, '') AS user_id,
        NULLIF(email, '') AS email,
        team_id AS workspace_id,
        NULLIF(resource_id, '') AS resource_id,
        NULLIF(user_agent, '') AS user_agent,
        type AS event_type,
        SAFE.PARSE_JSON(NULLIF(metadata, '')) AS metadata,
        _FILE_NAME AS source_file,
      FROM
        audit_log_files
      WHERE
        team_id = @workspace_id
    )
    SELECT
      *,
      CURRENT_TIMESTAMP() AS loaded_at,
    FROM
      parsed
    WHERE
      TRUE
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY source_file) = 1
  ) AS source
ON
  target.event_id = source.event_id
WHEN NOT MATCHED
THEN
  INSERT
    ROW
"""


def main():
    """Merge all audit log exports under the source URI into the destination table."""
    args = parser.parse_args()
    client = bigquery.Client(args.billing_project or args.project)
    destination = f"{args.project}.{args.dataset}.{args.table}"

    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [args.source_uri]
    external_config.schema = [
        bigquery.SchemaField(column, "STRING") for column in CSV_COLUMNS
    ]
    external_config.options.skip_leading_rows = 1
    external_config.options.allow_quoted_newlines = True

    job_config = bigquery.QueryJobConfig(
        table_definitions={"audit_log_files": external_config},
        query_parameters=[
            bigquery.ScalarQueryParameter("workspace_id", "STRING", args.workspace_id)
        ],
    )
    job = client.query(
        MERGE_QUERY.format(destination=destination), job_config=job_config
    )
    job.result()
    logging.info(
        f"Merged {job.num_dml_affected_rows} new audit log rows into {destination}"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
