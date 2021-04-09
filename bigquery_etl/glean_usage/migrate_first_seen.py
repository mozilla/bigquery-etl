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


def main():
    """Generate and run queries based on CLI args."""
    generate_and_run_query(run_query, __doc__)


def run_query(
    project_id, baseline_table, date, dry_run, output_dir=None, output_only=False
):
    """Process a single table, potentially also writing out the generated queries."""
    tables = table_names_from_baseline(baseline_table, include_project_id=False)

    daily_table = tables["daily_table"]
    last_seen_table = tables["daily_view"]
    render_kwargs = dict(
        header="-- Generated via bigquery_etl.glean_usage\n", project_id=project_id
    )
    render_kwargs.update(tables)
    job_kwargs = dict(use_legacy_sql=False, dry_run=dry_run)

    bcd_sql = render("migrate_baseline_clients_daily_v1.sql", **render_kwargs)
    cls_sql = render("migrate_baseline_clients_last_seen_v1.sql", **render_kwargs)

    if not dry_run:
        logging.info(f"Running query for: {daily_table}")
        logging.info(f"Running query for: {last_seen_table}")

    if output_only:
        # Return before we initialize the BQ client so that we can generate SQL
        # without having BQ credentials.
        print(bcd_sql)
        print()
        print(cls_sql)
        return

    client = bigquery.Client(project_id)
    job_config = bigquery.QueryJobConfig(**job_kwargs)
    job = client.query(bcd_sql, job_config)
    if not dry_run:
        job.result()
        print(f"job id: {job.job_id}")
    job = client.query(cls_sql, job_config)
    if not dry_run:
        job.result()
        print(f"job id: {job.job_id}")


if __name__ == "__main__":
    main()
