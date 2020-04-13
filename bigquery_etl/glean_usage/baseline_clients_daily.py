"""Generating and run baseline_clients_daily queries for Glean apps."""

from argparse import ArgumentParser
from datetime import datetime
from functools import partial
import logging
from multiprocessing.pool import ThreadPool

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition, ScalarQueryParameter

from bigquery_etl.util import standard_args  # noqa E402
from bigquery_etl.glean_usage.common import (
    list_baseline_tables,
    render,
    table_names_from_baseline,
    write_sql,
)

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_id",
    "--project-id",
    default="moz-fx-data-shar-nonprod-efed",
    help="ID of the project in which to find tables",
)
parser.add_argument(
    "--date",
    required=True,
    type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
    help="Date partition to process, in format 2019-01-01",
)
parser.add_argument(
    "--output_dir",
    "--output-dir",
    help="Also write the query text underneath the given sql dir",
)
standard_args.add_parallelism(parser)
standard_args.add_dry_run(parser, debug_log_queries=False)
standard_args.add_log_level(parser)
standard_args.add_priority(parser)
standard_args.add_billing_projects(parser)
standard_args.add_table_filter(parser)


BASELINE_DAILY_TABLE_ID = "baseline_clients_daily_v1"
QUERY_FILENAME = f"{BASELINE_DAILY_TABLE_ID}.sql"
VIEW_FILENAME = f"{BASELINE_DAILY_TABLE_ID[:-3]}.view.sql"


def main():
    """Generate and run queries based on CLI args."""
    args = parser.parse_args()

    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        parser.error(f"argument --log-level: {e}")

    client = bigquery.Client(args.project_id)

    with ThreadPool(args.parallelism) as pool:
        baseline_tables = list_baseline_tables(
            client=client,
            pool=pool,
            project_id=args.project_id,
            only_tables=getattr(args, "only_tables", None),
            table_filter=args.table_filter,
        )
        # Do a first pass with dry_run=True so we don't end up with a partial success;
        # we also write out queries in this pass if so configured.
        pool.map(
            partial(
                run_query,
                client,
                date=args.date,
                dry_run=True,
                output_dir=args.output_dir,
            ),
            baseline_tables,
        )
        logging.info(
            f"Dry runs successful for {len(baseline_tables)}"
            " baseline_clients_daily table(s)"
        )
        # Now, actually run the queries.
        if not args.dry_run:
            pool.map(
                partial(run_query, client, date=args.date, dry_run=False),
                baseline_tables,
            )


def run_query(client, baseline_table, date, dry_run, output_dir=None):
    """Process a single table, potentially also writing out the generated queries."""
    tables = table_names_from_baseline(baseline_table)

    daily_table = tables["daily_table"]
    daily_view = tables["daily_view"]
    render_kwargs = dict(header="-- Generated via bigquery_etl.glean_usage\n")
    render_kwargs.update(tables)
    job_kwargs = dict(use_legacy_sql=False, dry_run=dry_run)

    sql = render(QUERY_FILENAME, **render_kwargs)
    init_sql = render(QUERY_FILENAME, init=True, **render_kwargs)
    view_sql = render(VIEW_FILENAME, **render_kwargs)
    if output_dir:
        write_sql(output_dir, daily_table, "query.sql", sql)
        write_sql(output_dir, daily_table, "init.sql", init_sql)
        write_sql(output_dir, daily_view, "view.sql", view_sql)

    try:
        client.get_table(daily_table)
    except NotFound:
        if dry_run:
            logging.info(f"Table does not yet exist: {daily_table}")
        else:
            logging.info(f"Creating table: {daily_table}")
        sql = init_sql
    else:
        # Table exists, so we will run the incremental query.
        job_kwargs.update(
            destination=f"{daily_table}${date.strftime('%Y%m%d')}",
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            query_parameters=[ScalarQueryParameter("submission_date", "DATE", date)],
        )
        if not dry_run:
            logging.info(f"Running query for: {daily_table}")
    job_config = bigquery.QueryJobConfig(**job_kwargs)
    job = client.query(sql, job_config)
    if not dry_run:
        job.result()
        logging.info(f"Recreating view {daily_view}")
        client.query(view_sql, bigquery.QueryJobConfig(use_legacy_sql=False)).result()


if __name__ == "__main__":
    main()
