import json
import multiprocessing
from functools import partial
from pathlib import Path
import shutil

import tqdm

from .config import *
from .utils import ensure_folder, ndjson_load, print_json, run, run_query


class TableType:
    # this contains only the views and the view defintions, but otherwise
    # consistent with tables
    VIEW = "VIEWS"
    TABLE = "TABLES"


def fetch_dataset_listing(project: str, data_root: Path):
    return run_query(
        f"select * from `{project}`.INFORMATION_SCHEMA.SCHEMATA",
        "dataset_listing",
        ensure_folder(data_root / project),
    )


def _generate_table_listing_sql(listing: list, table_type: TableType) -> str:
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


def fetch_table_listing(
    dataset_listing: list, project_root: Path, table_type: TableType = TableType.TABLE
):
    sql = _generate_table_listing_sql(dataset_listing, table_type)
    name = f"{table_type.lower()}_listing"
    with (project_root / f"{name}.sql").open("w") as fp:
        fp.write(sql)
    return run_query(sql, name, project_root)


def _view_dryrun(view_root, view):
    project_id = view["table_catalog"]
    dataset_id = view["table_schema"]
    table_id = view["table_name"]

    qualified_name = f"{dataset_id}.{table_id}"
    base_query = f"SELECT * from `{project_id}`.{qualified_name}"
    where_clauses = [
        "where date(submission_timestamp) = date_sub(current_date, interval 1 day)",
        "where submission_date = date_sub(current_date, interval 1 day)",
    ]
    queries = [f"{base_query} {clause}" for clause in where_clauses] + [base_query]
    data = None
    for query in queries:
        try:
            # see NOTES.md for examples of the full response
            result = run(
                [
                    "bq",
                    "query",
                    "--format=json",
                    "--use_legacy_sql=false",
                    "--dry_run",
                    query,
                ]
            )
            data = json.loads(result)
            break
        except:
            # Error in query string: ...
            continue
    if not data:
        print(f"unable to resolve {project_id}:{qualified_name}")
        return
    with (view_root / f"{qualified_name}.json").open("w") as fp:
        subset = data["statistics"]
        # not needed at the moment
        del subset["query"]["schema"]
        # add some extra metadata for our sanity, in the convention already laid
        # out by the query dryrun
        subset = {
            **dict(projectId=project_id, tableId=table_id, datasetId=dataset_id),
            **subset,
        }
        json.dump(subset, fp, indent=2)


def resolve_view_references(view_listing, project_root):
    view_root = ensure_folder(project_root / "views")

    pool = multiprocessing.Pool(MAX_CONCURRENCY)
    for _ in tqdm.tqdm(
        pool.imap_unordered(partial(_view_dryrun, view_root), view_listing),
        total=len(view_listing),
    ):
        pass

    # merge into ndjson
    with (project_root / "views_references.ndjson").open("w") as fp:
        for view in view_root.glob("*.json"):
            data = json.load(view.open())
            fp.write(json.dumps(data))
            fp.write("\n")
