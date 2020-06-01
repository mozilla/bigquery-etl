"""Tool for detecting and deleting broken views."""

import logging
from argparse import ArgumentParser
from functools import partial
from multiprocessing.pool import ThreadPool

from google.api_core.exceptions import NotFound, Forbidden
from google.cloud import bigquery

from bigquery_etl.util import standard_args  # noqa E402
from bigquery_etl.util.bigquery_id import sql_table_id

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_id",
    "--project-id",
    default="moz-fx-data-shar-nonprod-efed",
    help="ID of the project in which to find views",
)
standard_args.add_parallelism(parser)
standard_args.add_dry_run(parser, debug_log_queries=False)
standard_args.add_log_level(parser)
standard_args.add_table_filter(parser)


def main():
    """Dry run all views in the configured project and report on broken ones."""
    args = parser.parse_args()

    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        parser.error(f"argument --log-level: {e}")

    client = bigquery.Client(args.project_id)

    with ThreadPool(args.parallelism) as pool:
        views = list_views(
            client=client,
            pool=pool,
            project_id=args.project_id,
            only_tables=getattr(args, "only_tables", None),
            table_filter=args.table_filter,
        )
        results = pool.map(partial(dry_run_view, client), views)
    print("Failed with Forbidden error:")
    for result in results:
        status, view = result
        if status == "Forbidden":
            print(f"  {view}")
    print("\nFailed with NotFound error; run the following commands to remove:")
    for result in results:
        status, view = result
        if status == "NotFound":
            print(f"  bq rm -f {view.replace('.', ':', 1)}")


def list_views(client, pool, project_id, only_tables, table_filter):
    """Make parallel BQ API calls to grab all views.

    See `util.standard_args` for more context on table filtering.

    :param client:       A BigQuery client object
    :param pool:         A process pool for handling concurrent calls
    :param project_id:   Target project
    :param only_tables:  An iterable of globs in `<dataset>.<view>` format
    :param table_filter: A function for determining whether to include a view
    :return:             A list of matching views
    """
    if only_tables and not _contains_glob(only_tables):
        # skip list calls when only_tables exists and contains no globs
        return [f"{project_id}.{t}" for t in only_tables if table_filter(t)]
    if only_tables and not _contains_glob(
        _extract_dataset_from_glob(t) for t in only_tables
    ):
        # skip list_datasets call when only_tables exists and datasets contain no globs
        datasets = {
            f"{project_id}.{_extract_dataset_from_glob(t)}" for t in only_tables
        }
    else:
        datasets = [d.reference.dataset_id for d in client.list_datasets(project_id)]
    return [
        sql_table_id(t)
        for tables in pool.map(client.list_tables, datasets)
        for t in tables
        if table_filter(f"{t.dataset_id}.{t.table_id}") and t.table_type == "VIEW"
    ]


def dry_run_view(client, view):
    """Dry run a view, returning a tuple of (status, view_name)."""
    sql = (
        "CREATE OR REPLACE VIEW telemetry.dry_run_view AS "
        + client.get_table(view).view_query
    )
    job_config = bigquery.QueryJobConfig(dry_run=True, use_legacy_sql=False)
    try:
        client.query(sql, job_config)
    except NotFound:
        return ("NotFound", view)
    except Forbidden:
        return ("Forbidden", view)
    else:
        return ("Success", view)


def _contains_glob(patterns):
    return any({"*", "?", "["}.intersection(pattern) for pattern in patterns)


def _extract_dataset_from_glob(pattern):
    # Assumes globs are in <dataset>.<table> form without a project specified.
    return pattern.split(".", 1)[0]


if __name__ == "__main__":
    main()
