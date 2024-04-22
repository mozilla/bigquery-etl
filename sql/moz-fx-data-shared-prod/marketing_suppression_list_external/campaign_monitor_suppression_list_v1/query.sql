import json
import requests
import logging
from requests.auth import HTTPBasicAuth

import os
import yaml
from google.cloud import bigquery
import rich_click as click
from pathlib import Path

DEFAULT_PROJECT_ID = Path(__file__).parent.parent.parent.name
DEFAULT_DATASET_ID = Path(__file__).parent.parent.name
DEFAULT_TABLE_NAME = Path(__file__).parent.name
BASE_URL = "https://api.createsend.com/api/v3.3/clients"
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = bigquery.SchemaField.from_api_repr(
    {"name": "root", "type": "RECORD", **yaml.safe_load(SCHEMA_FILE.read_text())}
).fields

@click.command
@click.option(
  "--api_key",
  required=true,
  help="Campaign Monitor API key to use for authentication."
)
@click.option(
    "--bq_project_id",
    default=DEFAULT_PROJECT_ID,
    show_default=True,
    help="BigQuery project the data is written to.",
)
@click.option(
    "--bq_dataset_id",
    default=DEFAULT_DATASET_ID,
    show_default=True,
    help="BigQuery dataset the data is written to.",
)
@click.option(
    "--bq_table_name",
    default=DEFAULT_TABLE_NAME,
    show_default=True,
    help="Bigquery table the data is written to.",
)

