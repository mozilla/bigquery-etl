import json
from pathlib import Path

from .utils import ensure_folder, print_json, run, run_query, ndjson_load
from .config import *


def fetch_dataset_listing(project: str, data_root: Path):
    return run_query(
        f"select * from `{project}`.INFORMATION_SCHEMA.SCHEMATA",
        "dataset_listing",
        ensure_folder(data_root / project),
    )


class TableType:
    VIEW = "VIEWS"
    TABLE = "TABLES"


def generate_table_listing_sql(listing: list, table_type: TableType) -> str:
    datasets = [
        f'`{entry["catalog_name"]}`.{entry["schema_name"]}'
        for entry in listing
        # TODO: this is brittle, replace with a real test of auth
        if entry["schema_name"] != "payload_bytes_raw"
    ]
    queries = [
        f"SELECT * FROM {dataset}.INFORMATION_SCHEMA.{table_type}"
        for dataset in datasets
    ]
    return "\nUNION ALL\n".join(queries)


# NOTE: it would be nice if this definition were consistent with
# dataset_listing, but this avoids duplicate work.
def fetch_table_listing(
    dataset_listing: list, table_type: TableType, project_root: Path
):
    sql = generate_table_listing_sql(dataset_listing, table_type)
    name = f"{table_type.lower()}_listing"
    with (project_root / f"{name}.sql").open("w") as fp:
        fp.write(sql)
    return run_query(sql, name, project_root)


def resolve_view_references(view_listing, project_root):
    view_root = ensure_folder(project_root / "views")
    for view in view_listing:
        result = run(
            [
                "bq",
                "query",
                "--format=json",
                "--use_legacy_sql=false",
                "--dry_run",
                view["view_definition"],
            ]
        )
        # see NOTES.md for examples of the full response
        # NOTE: this could be done with a format string too...
        filename = ".".join([view["table_schema"], view["table_name"], "json"])
        # TODO: maybe an ndjson file instead?
        with (view_root / filename).open("w") as fp:
            json.dumps(view_root, indent=2)


run(f"gsutil ls gs://{BUCKET}")
run(f"bq ls {PROJECT}:{DATASET}")

data_root = ensure_folder(Path(__file__).parent.parent / "data")
project = "moz-fx-data-shared-prod"
# dataset_listing = fetch_dataset_listing(project, data_root)
# table_listing = fetch_table_listing(
#     dataset_listing, TableType.TABLE, data_root / project
# )
# view_listing = fetch_table_listing(dataset_listing, TableType.VIEW, data_root / project)

# unnecessary complexity :(, the view listing wasnt needed
view_listing = ndjson_load(data_root / project / "views_listing.ndjson")

resolve_view_references(view_listing, data_root / project)

# fetch tables
# fetch views
# resolve views
# resolve globs
