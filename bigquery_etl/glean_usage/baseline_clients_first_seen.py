"""Generate and run baseline_clients_first_seen queries for Glean apps."""

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

TARGET_TABLE_ID = "baseline_clients_first_seen_v1"
INIT_FILENAME = f"{TARGET_TABLE_ID}.init.sql"
QUERY_FILENAME = f"{TARGET_TABLE_ID}.query.sql"
VIEW_FILENAME = f"{TARGET_TABLE_ID[:-3]}.view.sql"
VIEW_METADATA_FILENAME = f"{TARGET_TABLE_ID[:-3]}.metadata.yaml"


def main():
    """Generate and run queries based on CLI args."""
    generate_and_run_query(run_query, __doc__)


def run_query(
    project_id, baseline_table, date, dry_run, output_dir=None, output_only=False
):
    """Process a single table, potentially also writing out the generated queries."""
    tables = table_names_from_baseline(baseline_table, include_project_id=False)

    table_id = tables["first_seen_table"]
    view_id = tables["first_seen_view"]
    render_kwargs = dict(
        header="-- Generated via bigquery_etl.glean_usage\n",
        project_id=project_id,
        # do not match on org_mozilla_firefoxreality
        fennec_id=any(
            (f"{app_id}_stable" in baseline_table)
            for app_id in [
                "org_mozilla_firefox",
                "org_mozilla_fenix_nightly",
                "org_mozilla_fennec_aurora",
                "org_mozilla_firefox_beta",
                "org_mozilla_fenix",
            ]
        ),
    )
    render_kwargs.update(tables)
    job_kwargs = dict(use_legacy_sql=False, dry_run=dry_run)

    query_sql = render(QUERY_FILENAME, **render_kwargs)
    init_sql = render(INIT_FILENAME, **render_kwargs)
    view_sql = render(VIEW_FILENAME, **render_kwargs)
    view_metadata = render(VIEW_METADATA_FILENAME, format=False, **render_kwargs)
    sql = query_sql

    if not (referenced_table_exists(view_sql)):
        if output_only:
            logging.info("Skipping view for table which doesn't exist:" f" {table_id}")
            return
        elif dry_run:
            logging.info(f"Table does not yet exist: {table_id}")
        else:
            logging.info(f"Creating table: {table_id}")
        sql = init_sql
    elif output_only:
        pass
    else:
        # Table exists, so just overwrite the entire table with the day's results
        job_kwargs.update(
            destination=f"{project_id}.{table_id}",
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            query_parameters=[ScalarQueryParameter("submission_date", "DATE", date)],
        )
        if not dry_run:
            logging.info(f"Running query for: {table_id}")

    if output_dir:
        write_sql(output_dir, view_id, "metadata.yaml", view_metadata)
        write_sql(output_dir, view_id, "view.sql", view_sql)
        write_sql(output_dir, table_id, "query.sql", query_sql)
        write_sql(output_dir, table_id, "init.sql", init_sql)
    if output_only:
        # Return before we initialize the BQ client so that we can generate SQL
        # without having BQ credentials.
        return

    client = bigquery.Client(project_id)
    job_config = bigquery.QueryJobConfig(**job_kwargs)
    job = client.query(sql, job_config)
    if not dry_run:
        job.result()


if __name__ == "__main__":
    main()
