#!/usr/bin/env python

"""Import outerbounds flow descriptions from Google Sheets."""

import google.auth
from google.cloud import bigquery

# Use job config with external table definition
external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
external_config.source_uris = [
    "https://docs.google.com/spreadsheets/d/1E0kDpHwwtDnkMxAXbeEIzMTajckSHWJ2swF39JdRR1g"
]
external_config.schema = [
    bigquery.SchemaField("flow_name", "STRING"),
    bigquery.SchemaField("flow_description", "STRING"),
]
external_config.options.skip_leading_rows = 1
job_config = bigquery.QueryJobConfig(
    table_definitions={"description_sheet": external_config}
)

# Use credentials that include a google drive scope
credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
)
client = bigquery.Client(credentials=credentials, project=project)

query = client.query(
    """CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod`.monitoring_derived.outerbounds_flow_description_v1
AS
SELECT
  *
FROM
  description_sheet""",
    job_config=job_config,
)
query.result()
