#!/usr/bin/env python

"""Import VAT rates from Google Sheets."""

import google.auth
from google.cloud import bigquery

# Use job config with external table definition
external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
external_config.source_uris = [
    "https://docs.google.com/spreadsheets/d/1-eUbaYIuppfwoCyawKEfNVHRx_HJfn_VBOc7c2YYS48"
]
external_config.schema = [
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("vat", "STRING"),
    bigquery.SchemaField("effective_date", "DATE"),
]
external_config.options.skip_leading_rows = 2
job_config = bigquery.QueryJobConfig(
    table_definitions={"vat_rates_sheet": external_config}
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
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.vat_rates_v1
AS
SELECT
  * REPLACE (
    COALESCE(
      CAST(REGEXP_EXTRACT(vat, r"(.*)%") AS NUMERIC) / 100,
      CAST(vat AS NUMERIC)
    ) AS vat
  )
FROM
  vat_rates_sheet""",
    job_config=job_config,
)
query.result()
