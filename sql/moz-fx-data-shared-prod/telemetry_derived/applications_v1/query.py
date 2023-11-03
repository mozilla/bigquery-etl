#!/usr/bin/env python3

"""Import application information from the probe info API."""

import json
import sys
from argparse import ArgumentParser

import requests
from google.cloud import bigquery

APP_LISTINGS_URL = "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="telemetry_derived")
parser.add_argument("--destination_table", default="applications_v1")
parser.add_argument("--sql_dir", default="sql/")
parser.add_argument("--dry_run", action="store_true")


def get_app_info():
    """Return a list of applications from the probeinfo API."""
    resp = requests.get(APP_LISTINGS_URL)
    resp.raise_for_status()
    return resp.json()


def main():
    """Run."""
    args = parser.parse_args()

    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    bq_schema = (
        bigquery.SchemaField("app_channel", "STRING"),
        bigquery.SchemaField("app_description", "STRING"),
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("app_name", "STRING"),
        bigquery.SchemaField("bq_dataset_family", "STRING"),
        bigquery.SchemaField("canonical_app_name", "STRING"),
        bigquery.SchemaField("document_namespace", "STRING"),
        bigquery.SchemaField("notification_emails", "STRING", mode="REPEATED"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("v1_name", "STRING"),
        bigquery.SchemaField("is_glean", "BOOL"),
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
    )
    job_config.schema = bq_schema

    app_info = get_app_info()
    app_data = []

    for data in app_info:
        app_data.append(
            {
                "app_channel": data.get("app_channel", "release"),
                "app_description": data.get("app_description"),
                "app_id": data.get("app_id"),
                "app_name": data.get("app_name"),
                "bq_dataset_family": data.get("bq_dataset_family"),
                "canonical_app_name": data.get("canonical_app_name"),
                "document_namespace": data.get("document_namespace"),
                "notification_emails": data.get("notification_emails"),
                "url": data.get("url"),
                "v1_name": data.get("v1_name"),
                "is_glean": True,
            }
        )

    # add legacy information
    legacy_desktop = {
        "app_channel": "release",
        "app_description": "Desktop version of Firefox using legacy telemetry",
        "app_id": None,
        "app_name": None,
        "bq_dataset_family": "telemetry",
        "canonical_app_name": "Firefox for Desktop",
        "document_namespace": "telemetry",
        "notification_emails": None,
        "url": "https://github.com/mozilla/gecko-dev",
        "v1_name": None,
        "is_glean": False,
    }

    for channel in ["nightly", "beta", "release"]:
        desktop_json = legacy_desktop.copy()
        desktop_json["app_channel"] = channel
        app_data.append(desktop_json)

    if args.dry_run:
        print(json.dumps(app_data))
        sys.exit(0)

    client = bigquery.Client(args.project)
    client.load_table_from_json(
        app_data, destination_table, job_config=job_config
    ).result()
    print(f"Loaded {len(app_data)} applications")


if __name__ == "__main__":
    main()
