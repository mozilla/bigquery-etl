"""Load browser release-note records from GCS into BigQuery.

Reads developer- and user-facing release-note JSON records written by
docker-etl's release_scraping job (scheduled weekly by telemetry-airflow's
web_scraping DAG) under
gs://moz-fx-data-prod-external-data/MARKET_RESEARCH/STRUCTURED/, and
incrementally appends only blobs not already present in the destination
table.

The upstream corpus is write-once per identity: release_scraping's own
`gcs_path_for`/`gcs_user_release_path_for` key the blob name on
`browser`+`version`+`release_date` (not `scraped_date`), and the scraper
fetches existing GCS paths up front and skips scraping anything already
present -- so a given release note is scraped and uploaded at most once,
ever, and its blob is never touched again. That makes `blob_name` (already
stored per row for traceability) a safe load cursor: every run lists the
full GCS prefix, diffs it against the `blob_name`s already loaded, and
downloads+appends only what's new. The one edge case this doesn't cover:
if release_scraping's own dedup logic ever changed such that a blob got
rewritten in place under the same name, that update would be silently
missed here, since a previously-seen blob name is never re-fetched.
"""

import json
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from bigquery_etl.schema import Schema

GCS_BUCKET = "moz-fx-data-prod-external-data"
GCS_STRUCTURED_PREFIX = "MARKET_RESEARCH/STRUCTURED"
TABLE_ID = "moz-fx-data-shared-prod.external_derived.market_research_releases_v1"

# Filename prefixes release_scraping uses for developer and user release notes.
BLOB_PREFIXES = ("release_", "user_release_")

# Most runs only have a handful of new blobs, but keep concurrency for the
# first-ever backfill run (or a run after a long gap) where the new set can
# be as large as the full historical corpus.
DOWNLOAD_WORKERS = 16

# schema.yaml is the single source of truth for the destination schema.
# Passed explicitly (with autodetect=False) so an appended batch can never
# silently drift the destination schema from what schema.yaml declares.
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()
COLUMNS = [field.name for field in SCHEMA]


def get_existing_blob_names(bq_client):
    """Return the set of `blob_name`s already loaded into the destination table.

    Returns an empty set if the table doesn't exist yet (schema not deployed,
    or first-ever run), so that case is treated the same as "nothing loaded
    yet" rather than an error.
    """
    try:
        return {
            row.blob_name
            for row in bq_client.query(
                f"SELECT DISTINCT blob_name FROM `{TABLE_ID}`"
            ).result()
        }
    except NotFound:
        return set()


# Malformed dates encountered while normalizing, so that one bad value in one
# blob degrades to NULL instead of failing the whole weekly reload. Appended to
# from worker threads, so the order of entries isn't meaningful.
unparseable_dates = []


def list_release_blobs(client):
    """List release-note JSON blobs under the structured GCS prefix.

    Returns `(matched, skipped)`, where `skipped` holds the names of JSON
    files that don't match the expected filename prefixes. Those would
    otherwise be dropped silently if the upstream scraper changed its naming.
    """
    matched = []
    skipped = []
    for blob in client.list_blobs(GCS_BUCKET, prefix=GCS_STRUCTURED_PREFIX + "/"):
        if not blob.name.endswith(".json"):
            continue
        if blob.name.split("/")[-1].startswith(BLOB_PREFIXES):
            matched.append(blob)
        else:
            skipped.append(blob.name)
    return matched, skipped


def parse_date(value, date_format, blob_name, field):
    """Parse a date string into a `date`.

    Returns None for empty values, and for values that don't match
    `date_format` -- scraped vendor dates vary in format, and a single bad
    value shouldn't block the reload of the entire corpus.
    """
    if not value:
        return None
    try:
        return datetime.strptime(value, date_format).date()
    except ValueError:
        unparseable_dates.append(f"{blob_name}: {field}={value!r}")
        return None


def normalize_release_record(record, blob_name):
    """Flatten a scraped release-note record into a row matching schema.yaml.

    Developer release-note records don't have a `source_type` key at all
    (only user releases and blog posts do), so it's defaulted to
    "developer_release_notes" here to keep the column non-null across both
    release kinds.

    TODO: `features` is always an empty list in every record today, so it's
    dropped rather than modeled as a column. Re-add it if the scraper starts
    populating it.
    """
    return {
        "browser": record.get("browser"),
        "version": record.get("version"),
        "release_date": parse_date(
            record.get("release_date"), "%Y-%m-%d", blob_name, "release_date"
        ),
        "scraped_date": parse_date(
            record.get("scraped_date"), "%Y%m%d", blob_name, "scraped_date"
        ),
        "source_url": record.get("source_url"),
        "source_type": record.get("source_type") or "developer_release_notes",
        "raw_text": record.get("raw_text"),
        "blob_name": blob_name,
    }


def fetch_and_normalize(blob):
    """Download a single blob and normalize it into a row."""
    return normalize_release_record(json.loads(blob.download_as_text()), blob.name)


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    # This corpus is a cumulative, re-listable snapshot rather than a daily
    # time series, so --date isn't used to filter source data -- it's only
    # accepted to keep the CLI shape consistent with other bqetl query.py jobs.
    datetime.strptime(args.date, "%Y-%m-%d")

    storage_client = storage.Client()
    blobs, skipped_blobs = list_release_blobs(storage_client)
    print(
        f"Found {len(blobs)} release blobs under "
        f"gs://{GCS_BUCKET}/{GCS_STRUCTURED_PREFIX}/"
    )
    if skipped_blobs:
        print(
            f"WARNING: skipped {len(skipped_blobs)} JSON blob(s) that don't "
            f"start with any of {BLOB_PREFIXES}; release_scraping may have "
            f"changed its filename convention. Examples: {skipped_blobs[:5]}"
        )

    # The corpus only ever grows, so an empty full listing always means
    # something upstream broke (failed scrape, renamed prefix, lost bucket
    # access) -- fail loudly rather than silently loading nothing.
    if not blobs:
        raise ValueError(
            f"No release-note records found under gs://{GCS_BUCKET}/"
            f"{GCS_STRUCTURED_PREFIX}/ -- refusing to load {TABLE_ID}"
        )

    bq_client = bigquery.Client()
    existing_blob_names = get_existing_blob_names(bq_client)
    new_blobs = [blob for blob in blobs if blob.name not in existing_blob_names]
    print(
        f"{len(new_blobs)} of {len(blobs)} blobs are new "
        f"({len(blobs) - len(new_blobs)} already loaded)"
    )
    if not new_blobs:
        print("No new blobs since the last run -- nothing to load.")
        raise SystemExit(0)

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        rows = list(pool.map(fetch_and_normalize, new_blobs))

    if unparseable_dates:
        print(
            f"WARNING: {len(unparseable_dates)} date value(s) could not be "
            "parsed and were loaded as NULL:"
        )
        for detail in unparseable_dates[:20]:
            print(f"  {detail}")

    results_df = pd.DataFrame(rows, columns=COLUMNS)
    print(f"Parsed {len(results_df)} new rows")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
        schema=SCHEMA,
    )

    load_job = bq_client.load_table_from_dataframe(
        results_df,
        TABLE_ID,
        job_config=job_config,
    )
    load_job.result()  # waits for completion

    print(f"Appended {len(results_df)} new rows to {TABLE_ID} (incremental load)")
