from pathlib import Path

import click

from .config import *
from .crawler import (
    fetch_dataset_listing,
    fetch_table_listing,
    resolve_view_references,
    resolve_bigquery_etl_references,
)
from .utils import ensure_folder, ndjson_load, print_json, run

ROOT = Path(__file__).parent.parent


@click.group()
def cli():
    pass


@cli.command()
def crawl():
    """Crawl bigquery projects."""
    run(f"gsutil ls gs://{BUCKET}")
    run(f"bq ls {PROJECT}:{DATASET}")

    data_root = ensure_folder(ROOT / "data")
    project = "moz-fx-data-shared-prod"
    dataset_listing = fetch_dataset_listing(project, data_root)
    tables_listing = fetch_table_listing(dataset_listing, data_root / project)
    # tables_listing = ndjson_load(data_root / project / "tables_listing.ndjson")

    views_listing = [row for row in tables_listing if row["table_type"] == "VIEW"]
    resolve_view_references(views_listing, data_root / project)


@cli.command()
def etl():
    """Crawl bigquery-etl."""
    # this is basically dryrun, but with some data collection baked in.
    resolve_bigquery_etl_references(
        ROOT / "bigquery-etl", ensure_folder(ROOT / "data" / "bigquery_etl")
    )


cli()
