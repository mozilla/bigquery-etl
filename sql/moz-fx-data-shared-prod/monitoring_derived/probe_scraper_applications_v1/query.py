"""Import Glean app listings from the probe-info service into BigQuery.

Source: https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings

The endpoint returns one entry per Glean app + channel (e.g. fenix release,
fenix beta, ...). Each entry is a flat record; the only nested shape is
``moz_pipeline_metadata``, which is a map keyed by ping name in the source
and is flattened here into a repeated record with ``ping_name`` set.
"""

import io
import json
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import requests
import yaml
from google.cloud import bigquery

URL = "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"


def _flatten_pipeline_metadata(metadata: dict | None) -> list[dict]:
    """Convert the moz_pipeline_metadata map (keyed by ping name) into a list.

    BigQuery doesn't have a native map type, so the source form
    ``{ping_name: {...}}`` becomes a repeated record with ``ping_name`` as a
    field on each row.
    """
    if not metadata:
        return []
    return [{"ping_name": ping_name, **fields} for ping_name, fields in metadata.items()]


def _row_for_listing(listing: dict[str, Any]) -> dict[str, Any]:
    return {
        "app_name": listing["app_name"],
        "canonical_app_name": listing.get("canonical_app_name"),
        "app_description": listing.get("app_description"),
        "app_id": listing.get("app_id"),
        "v1_name": listing.get("v1_name"),
        "app_channel": listing.get("app_channel"),
        "description": listing.get("description"),
        "bq_dataset_family": listing.get("bq_dataset_family"),
        "document_namespace": listing.get("document_namespace"),
        "url": listing.get("url"),
        "branch": listing.get("branch"),
        "deprecated": listing.get("deprecated") or False,
        "skip_documentation": listing.get("skip_documentation") or False,
        "prototype": listing.get("prototype") or False,
        "notification_emails": listing.get("notification_emails") or [],
        "metrics_files": listing.get("metrics_files") or [],
        "ping_files": listing.get("ping_files") or [],
        "tag_files": listing.get("tag_files") or [],
        "dependencies": listing.get("dependencies") or [],
        "moz_pipeline_metadata_defaults": listing.get("moz_pipeline_metadata_defaults"),
        "moz_pipeline_metadata": _flatten_pipeline_metadata(
            listing.get("moz_pipeline_metadata")
        ),
    }


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination_dataset", default="monitoring_derived")
    parser.add_argument("--destination_table", default="probe_scraper_applications_v1")
    args = parser.parse_args()

    response = requests.get(URL, timeout=60)
    response.raise_for_status()
    listings = response.json()

    rows = [_row_for_listing(listing) for listing in listings]

    client = bigquery.Client(args.project)
    schema_dict = yaml.safe_load(SCHEMA_FILE.read_text())["fields"]
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField.from_api_repr(f) for f in schema_dict],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_json(
        rows,
        f"{args.destination_dataset}.{args.destination_table}",
        job_config=job_config,
    )
    job.result()
    print(
        f"Loaded {len(rows)} app listings into "
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )


if __name__ == "__main__":
    main()
