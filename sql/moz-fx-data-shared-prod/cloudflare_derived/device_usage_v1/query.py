#Load libraries
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
from google.cloud import storage

#Configs
device_usg_configs = {
    "timeout_limit": 2200,
    "locations": [
        "ALL",
        "BE",
        "BG",
        "CA",
        "CZ",
        "DE",
        "DK",
        "EE",
        "ES",
        "FI",
        "FR",
        "GB",
        "HR",
        "IE",
        "IT",
        "CY",
        "LV",
        "LT",
        "LU",
        "HU",
        "MT",
        "MX",
        "NL",
        "AT",
        "PL",
        "PT",
        "RO",
        "SI",
        "SK",
        "US",
        "SE",
        "GR",
    ],
    "bucket": "gs://moz-fx-data-prod-external-data/",
    "results_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/RESULTS_STAGING/%s_results.csv",
    "results_archive_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/RESULTS_ARCHIVE/%s_results.csv",
    "errors_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/ERRORS_STAGING/%s_errors.csv",
    "errors_archive_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/ERRORS_ARCHIVE/%s_errors.csv",
    "gcp_project_id": "moz-fx-data-shared-prod",
    "gcp_conn_id": "google_cloud_shared_prod",
}


def main():
    """ Call the API, save data to GCS, load to BQ staging, delete & load to BQ gold"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cloudflare_api_token", required=True)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="cloudflare_derived")

    args = parser.parse_args()