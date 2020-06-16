from pathlib import Path

from .config import *
from .crawler import fetch_dataset_listing, fetch_table_listing, resolve_view_references
from .utils import ensure_folder, ndjson_load, print_json, run

run(f"gsutil ls gs://{BUCKET}")
run(f"bq ls {PROJECT}:{DATASET}")

data_root = ensure_folder(Path(__file__).parent.parent / "data")
project = "moz-fx-data-shared-prod"

dataset_listing = fetch_dataset_listing(project, data_root)
tables_listing = fetch_table_listing(dataset_listing, data_root / project)
# tables_listing = ndjson_load(data_root / project / "tables_listing.ndjson")

views_listing = [row for row in tables_listing if row["table_type"] == "VIEW"]
resolve_view_references(views_listing, data_root / project)
