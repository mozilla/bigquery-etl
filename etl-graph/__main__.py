import json
from pathlib import Path

from .utils import ensure_folder, print_json, run, run_query


def fetch_dataset_listing(project: str, data_root: Path):
    return run_query(
        f"select * from `{project}`.INFORMATION_SCHEMA.SCHEMATA",
        ensure_folder(data_root / project) / "dataset_listing.json",
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
    return run_query(sql, project_root / f"{name}.json")


data_root = ensure_folder(Path(__file__).parent.parent / "data")
project = "moz-fx-data-shared-prod"
dataset_listing = fetch_dataset_listing(project, data_root)
table_listing = fetch_table_listing(
    dataset_listing, TableType.TABLE, data_root / project
)
view_listing = fetch_table_listing(dataset_listing, TableType.VIEW, data_root / project)

print_json(table_listing[:5])
print_json(view_listing[:5])

# fetch tables
# fetch views
# resolve views
# resolve globs
