"""Generating and run baseline_clients_daily queries for Glean apps."""
import logging

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter, WriteDisposition

from bigquery_etl.glean_usage.common import (
    generate_and_run_query,
    referenced_table_exists,
    render,
    table_names_from_baseline,
    write_sql,
)

BASELINE_DAILY_TABLE_ID = "baseline_clients_daily_v1"
QUERY_FILENAME = f"{BASELINE_DAILY_TABLE_ID}.sql"
VIEW_FILENAME = f"{BASELINE_DAILY_TABLE_ID[:-3]}.view.sql"
VIEW_METADATA_FILENAME = f"{BASELINE_DAILY_TABLE_ID[:-3]}.metadata.yaml"


def main():
    """Generate and run queries based on CLI args."""
    generate_and_run_query(run_query, __doc__)


def run_query(
    project_id, baseline_table, date, dry_run, output_dir=None, output_only=False
):
    """Process a single table, potentially also writing out the generated queries."""
    tables = table_names_from_baseline(baseline_table, include_project_id=False)

    daily_table = tables["daily_table"]
    daily_view = tables["daily_view"]
    render_kwargs = dict(
        header="-- Generated via bigquery_etl.glean_usage\n", project_id=project_id
    )
    render_kwargs.update(tables)
    job_kwargs = dict(use_legacy_sql=False, dry_run=dry_run)

    query_sql = render(QUERY_FILENAME, **render_kwargs)
    init_sql = render(QUERY_FILENAME, init=True, **render_kwargs)
    view_sql = render(VIEW_FILENAME, **render_kwargs)
    view_metadata = render(VIEW_METADATA_FILENAME, format=False, **render_kwargs)
    sql = query_sql

    if not (referenced_table_exists(view_sql)):
        if output_only:
            logging.info(f"Skipping view for table which doesn't exist: {daily_table}")
            return
        elif dry_run:
            logging.info(f"Table does not yet exist: {daily_table}")
        else:
            logging.info(f"Creating table: {daily_table}")
        sql = init_sql
    elif output_only:
        pass
    else:
        # Table exists, so we will run the incremental query.
        job_kwargs.update(
            destination=f"{project_id}.{daily_table}${date.strftime('%Y%m%d')}",
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            query_parameters=[ScalarQueryParameter("submission_date", "DATE", date)],
        )
        if not dry_run:
            logging.info(f"Running query for: {daily_table}")

    if output_dir:
        write_sql(output_dir, daily_view, "metadata.yaml", view_metadata)
        write_sql(output_dir, daily_view, "view.sql", view_sql)
        write_sql(output_dir, daily_table, "query.sql", query_sql)
        write_sql(output_dir, daily_table, "init.sql", init_sql)
    if output_only:
        # Return before we initialize the BQ client so that we can generate SQL
        # without having BQ credentials.
        return

    client = bigquery.Client(project_id)
    job_config = bigquery.QueryJobConfig(**job_kwargs)
    job = client.query(sql, job_config)
    if not dry_run:
        job.result()
        logging.info(f"Recreating view {daily_view}")
        client.query(view_sql, bigquery.QueryJobConfig(use_legacy_sql=False)).result()


if __name__ == "__main__":
    main()
