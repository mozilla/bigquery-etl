#!/usr/bin/env python

"""Import Firefox Data Cta Uid Map from Google Sheets."""

from pathlib import Path

from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.util.google_sheets import import_google_sheet

query_path = Path(__file__)

import_google_sheet(
    destination_table=".".join(extract_from_query_path(query_path)),
    destination_schema=Schema.from_schema_file(query_path.parent / SCHEMA_FILE),
    sheet_url="https://docs.google.com/spreadsheets/d/1Pthv8UZbLtr4tYA-mzXn1GpZtdnki9LsjroHeYV8Rdw",
    sheet_range="A:G",
    skip_leading_rows=1,
)
