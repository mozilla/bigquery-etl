#!/usr/bin/env python

"""Import outerbounds flow descriptions from Google Sheets."""

from pathlib import Path

from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.util.google_sheets import import_google_sheet

query_path = Path(__file__)
table_id = ".".join(extract_from_query_path(query_path))
table_schema = Schema.from_schema_file(query_path.parent / SCHEMA_FILE)

import_google_sheet(
    table_id,
    table_schema,
    "https://docs.google.com/spreadsheets/d/1E0kDpHwwtDnkMxAXbeEIzMTajckSHWJ2swF39JdRR1g",
    skip_leading_rows=1,
)
