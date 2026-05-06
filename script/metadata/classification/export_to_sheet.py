"""Export field classifications as CSV for Legal review.

Pulls rows from `mozdata-nonprod.analysis.akomar_field_classifications_v1`
for a hardcoded list of source tables and a single model, and writes a CSV
file. Paste the CSV contents into a Google Sheet manually.

`category_simple` is the assigned `primary_label` rolled up to the closest
ancestor that is a "Data type" in `Taxonomy overview - Data Types.csv`
(taxonomy.json entries with `level == "data_type"`). Most data types are
2-segment (`user.unique_id`) but some are 3-segment (`user.behavior.search`),
so this is a longest-ancestor lookup, not a fixed truncation.

Usage:
  python script/metadata/classification/export_to_sheet.py
  # or pipe to clipboard on macOS:
  python script/metadata/classification/export_to_sheet.py --stdout | pbcopy
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

DEST_TABLE = "mozdata-nonprod.analysis.akomar_field_classifications_v1"
DEST_PROJECT = "mozdata-nonprod"
TAXONOMY_PATH = Path(__file__).parent / "taxonomy.json"
DEFAULT_OUTPUT_PATH = Path(__file__).parent / "classifications.csv"

MODEL = "gemini-3.1-flash-lite-preview"
TABLES = [
    "ads_backend_stable.interaction_v1",
    "ads_derived.ad_metrics_v1",
    "firefox_desktop_stable.newtab_v1",
    "firefox_desktop_derived.newtab_clients_daily_v2",
    "firefox_desktop_stable.quick_suggest_v1",
    "search_terms_derived.suggest_impression_sanitized_v3",
    "contextual_services_derived.event_aggregates_suggest_v1",
    "contextual_services_derived.request_payload_suggest_v2",
    "search_terms_derived.adm_daily_aggregates_v1",
]

HEADER_ROW = [
    "dataset",
    "table",
    "column_name",
    "category",
    "category_simple",
    "data_collection_category",
    "confidence",
    "reasoning",
    "needs_review",
]


def load_data_types():
    """Return the set of taxonomy labels that are CSV-level 'Data type' entries."""
    taxonomy = json.loads(TAXONOMY_PATH.read_text())
    return {e["label"] for e in taxonomy if e.get("level") == "data_type"}


def category_simple(label, data_types):
    """Walk dot-ancestors of `label` to find the closest Data type match."""
    if not label:
        return ""
    parts = label.split(".")
    for i in range(len(parts), 0, -1):
        candidate = ".".join(parts[:i])
        if candidate in data_types:
            return candidate
    return label  # bare subject (e.g. "user") — keep as-is


def fetch_rows(bq_client):
    """Query the classifications table for the hardcoded scope."""
    query = f"""
        SELECT
          source_dataset, source_table, column_name,
          primary_label, data_collection_category,
          confidence, reasoning, needs_review
        FROM `{DEST_TABLE}`
        WHERE model = @model
          AND CONCAT(source_dataset, '.', source_table) IN UNNEST(@tables)
        ORDER BY source_dataset, source_table, column_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("model", "STRING", MODEL),
            bigquery.ArrayQueryParameter("tables", "STRING", TABLES),
        ]
    )
    return list(bq_client.query(query, job_config=job_config).result())


def build_rows(bq_rows, data_types):
    """Map BQ rows into the output CSV layout."""
    out = []
    for r in bq_rows:
        out.append([
            r.source_dataset or "",
            r.source_table or "",
            r.column_name or "",
            r.primary_label or "",
            category_simple(r.primary_label, data_types),
            r.data_collection_category or "",
            r.confidence or "",
            r.reasoning or "",
            "TRUE" if r.needs_review else "FALSE" if r.needs_review is False else "",
        ])
    return out


def write_csv(rows, fh):
    """Write header + data rows to a file handle as CSV."""
    writer = csv.writer(fh)
    writer.writerow(HEADER_ROW)
    writer.writerows(rows)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=f"Write CSV to stdout instead of {DEFAULT_OUTPUT_PATH.name}.",
    )
    return parser.parse_args()


def main():
    """Pull classifications from BQ and write a CSV for manual paste into Sheets."""
    args = parse_args()

    data_types = load_data_types()
    logging.info(f"Loaded {len(data_types)} 'Data type' entries from taxonomy")

    bq_client = bigquery.Client(project=DEST_PROJECT)
    bq_rows = fetch_rows(bq_client)
    logging.info(f"Fetched {len(bq_rows)} rows from BQ")

    rows = build_rows(bq_rows, data_types)

    if args.stdout:
        write_csv(rows, sys.stdout)
    else:
        with open(DEFAULT_OUTPUT_PATH, "w", newline="") as fh:
            write_csv(rows, fh)
        logging.info(f"Wrote {len(rows)} rows to {DEFAULT_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
