"""Load competitor job posting records from GCS into BigQuery.

Reads every job posting JSON record written by docker-etl's release_scraping
job (scheduled weekly by telemetry-airflow's web_scraping DAG) under
gs://moz-fx-data-prod-external-data/MARKET_RESEARCH/JOBS/, and
truncate-and-reloads the destination table from the full corpus on each run.

Records come from two different scrapers (Greenhouse and Teamtailor)
with slightly different construction but the same final key set, so one
normalization function covers both.
"""

import json
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage

from bigquery_etl.schema import Schema

GCS_BUCKET = "moz-fx-data-prod-external-data"
GCS_JOBS_PREFIX = "MARKET_RESEARCH/JOBS"
TABLE_ID = "moz-fx-data-shared-prod.external_derived.market_research_jobs_v1"

# Filename prefix release_scraping uses for job posting records.
BLOB_PREFIX = "job_"

# Each record is its own small blob, so reloading the corpus is bound by request
# latency rather than bandwidth. Downloading concurrently keeps the runtime flat
# as the corpus grows instead of scaling with the number of blobs -- this matters
# most here, since every run adds a full snapshot under a new scraped_date.
DOWNLOAD_WORKERS = 16

# schema.yaml is the single source of truth for the destination schema. This
# load truncates the table, which replaces its schema, so passing a separately
# maintained schema here would silently overwrite the field descriptions that
# `bqetl deploy` applies from schema.yaml.
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"
SCHEMA = Schema.from_schema_file(SCHEMA_FILE).to_bigquery_schema()
COLUMNS = [field.name for field in SCHEMA]

# Malformed dates encountered while normalizing, so that one bad value in one
# blob degrades to NULL instead of failing the whole weekly reload. Appended to
# from worker threads, so the order of entries isn't meaningful.
unparseable_dates = []


def list_job_blobs(client):
    """List job posting JSON blobs under the jobs GCS prefix.

    Returns `(matched, skipped)`, where `skipped` holds the names of JSON
    files that don't match the expected filename prefix. Those would
    otherwise be dropped silently if the upstream scraper changed its naming.
    """
    matched = []
    skipped = []
    for blob in client.list_blobs(GCS_BUCKET, prefix=GCS_JOBS_PREFIX + "/"):
        if not blob.name.endswith(".json"):
            continue
        if blob.name.split("/")[-1].startswith(BLOB_PREFIX):
            matched.append(blob)
        else:
            skipped.append(blob.name)
    return matched, skipped


def parse_date(value, date_format, blob_name, field):
    """Parse a date string into a `date`.

    Returns None for empty values, and for values that don't match
    `date_format` -- a single bad value shouldn't block the reload of the
    entire corpus.
    """
    if not value:
        return None
    try:
        return datetime.strptime(value, date_format).date()
    except ValueError:
        unparseable_dates.append(f"{blob_name}: {field}={value!r}")
        return None


def normalize_job_record(record, blob_name):
    """Flatten a scraped job posting record into a row matching schema.yaml.

    `first_published` and `updated_at` are kept as STRING rather than
    TIMESTAMP/DATE: Greenhouse returns full ISO datetimes while Opera's
    JSON-LD only has date-only strings, and typing the column would break
    the load on whichever source doesn't match.

    `offices` is REPEATED, which can't hold SQL NULL, so a missing/empty
    list of offices is normalized to an empty array rather than None.
    """
    return {
        "company": record.get("company"),
        "source": record.get("source"),
        "scraped_date": parse_date(
            record.get("scraped_date"), "%Y%m%d", blob_name, "scraped_date"
        ),
        "job_id": record.get("job_id"),
        "title": record.get("title"),
        "department": record.get("department"),
        "location": record.get("location"),
        "offices": record.get("offices") or [],
        "url": record.get("url"),
        "first_published": record.get("first_published"),
        "updated_at": record.get("updated_at"),
        "description_html": record.get("description_html"),
        "description_text": record.get("description_text"),
        "blob_name": blob_name,
    }


def fetch_and_normalize(blob):
    """Download a single blob and normalize it into a row."""
    return normalize_job_record(json.loads(blob.download_as_text()), blob.name)


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    # This corpus is a cumulative, re-listable snapshot rather than a daily
    # time series, so --date isn't used to filter source data -- it's only
    # accepted to keep the CLI shape consistent with other bqetl query.py jobs.
    datetime.strptime(args.date, "%Y-%m-%d")

    storage_client = storage.Client()
    blobs, skipped_blobs = list_job_blobs(storage_client)
    print(f"Found {len(blobs)} job blobs under gs://{GCS_BUCKET}/{GCS_JOBS_PREFIX}/")
    if skipped_blobs:
        print(
            f"WARNING: skipped {len(skipped_blobs)} JSON blob(s) that don't "
            f"start with {BLOB_PREFIX!r}; release_scraping may have changed its "
            f"filename convention. Examples: {skipped_blobs[:5]}"
        )

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        rows = list(pool.map(fetch_and_normalize, blobs))

    # The corpus only ever grows, so an empty listing always means something
    # upstream broke (failed scrape, renamed prefix, lost bucket access).
    # Loading it would truncate the table, and because --date is ignored a
    # rerun can't restore the previous contents, so fail loudly instead.
    if not rows:
        raise ValueError(
            f"No job posting records found under gs://{GCS_BUCKET}/"
            f"{GCS_JOBS_PREFIX}/ -- refusing to truncate {TABLE_ID}"
        )

    if unparseable_dates:
        print(
            f"WARNING: {len(unparseable_dates)} date value(s) could not be "
            "parsed and were loaded as NULL:"
        )
        for detail in unparseable_dates[:20]:
            print(f"  {detail}")

    results_df = pd.DataFrame(rows, columns=COLUMNS)
    print(f"Parsed {len(results_df)} rows")

    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=SCHEMA,
    )

    load_job = bq_client.load_table_from_dataframe(
        results_df,
        TABLE_ID,
        job_config=job_config,
    )
    load_job.result()  # waits for completion

    print(f"Loaded {len(results_df)} rows into {TABLE_ID} (full reload)")
