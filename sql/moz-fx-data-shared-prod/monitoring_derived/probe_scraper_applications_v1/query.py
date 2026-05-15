"""Import applications from probe-scraper's repositories.yaml into BigQuery."""

import io
import json
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import requests
import yaml
from google.cloud import bigquery

URL = "https://raw.githubusercontent.com/mozilla/probe-scraper/main/repositories.yaml"
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"


def _flatten_pipeline_metadata(metadata: dict | None) -> list[dict]:
    """Convert the moz_pipeline_metadata map (keyed by ping name) into a list.

    BigQuery doesn't have a native map type, so the YAML form
    ``{ping_name: {...}}`` becomes a repeated record with ``ping_name`` as a
    field on each row.
    """
    if not metadata:
        return []
    return [{"ping_name": ping_name, **fields} for ping_name, fields in metadata.items()]


def _row_for_app(app: dict[str, Any]) -> dict[str, Any]:
    return {
        "app_name": app["app_name"],
        "canonical_app_name": app.get("canonical_app_name"),
        "app_description": app.get("app_description"),
        "url": app.get("url"),
        "branch": app.get("branch"),
        "deprecated": app.get("deprecated") or False,
        "skip_documentation": app.get("skip_documentation") or False,
        "prototype": app.get("prototype") or False,
        "notification_emails": app.get("notification_emails") or [],
        "metrics_files": app.get("metrics_files") or [],
        "ping_files": app.get("ping_files") or [],
        "tag_files": app.get("tag_files") or [],
        "dependencies": app.get("dependencies") or [],
        "channels": app.get("channels") or [],
        "moz_pipeline_metadata_defaults": app.get("moz_pipeline_metadata_defaults"),
        "moz_pipeline_metadata": _flatten_pipeline_metadata(
            app.get("moz_pipeline_metadata")
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
    data = yaml.safe_load(response.text)

    rows = [_row_for_app(app) for app in data.get("applications", [])]

    client = bigquery.Client(args.project)
    schema_dict = yaml.safe_load(SCHEMA_FILE.read_text())["fields"]
    job_config = bigquery.LoadJobConfig(
        schema=client.schema_from_json(io.StringIO(json.dumps(schema_dict))),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_json(
        rows,
        f"{args.destination_dataset}.{args.destination_table}",
        job_config=job_config,
    )
    job.result()
    print(
        f"Loaded {len(rows)} applications into "
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )


if __name__ == "__main__":
    main()
