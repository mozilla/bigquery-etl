#!/usr/bin/env python3
"""Copy a range of ga_sessions_* tables from one project to another."""
import datetime

import click
from google.cloud import bigquery, exceptions


def get_qualified_table_name(project, ga_id, table_date):
    """Get qualified name for ga_sessions table."""
    return (
        f"{project}.{ga_id}.ga_sessions_"
        f"{datetime.datetime.strftime(table_date, '%Y%m%d')}"
    )


def copy_single_table(bq_client, src_table, dst_table, overwrite):
    """Copy a single day of ga_sessions."""
    job_config = bigquery.CopyJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        if overwrite
        else bigquery.WriteDisposition.WRITE_EMPTY
    )
    try:
        copy_job = bq_client.copy_table(
            sources=src_table,
            destination=dst_table,
            job_config=job_config,
        )
    except exceptions.NotFound:
        print(f"{src_table} not found, copy skipped")
        return

    try:
        copy_job.result()
    except exceptions.Conflict:
        print(f"{dst_table} already exists, copy skipped")


@click.command()
@click.option("--start-date", required=True, type=datetime.datetime.fromisoformat)
@click.option("--end-date", type=datetime.datetime.fromisoformat)
@click.option("--src-project", required=True)
@click.option("--dst-project", required=True)
@click.option("--overwrite/--no-overwrite", default=False)
@click.argument("ga-ids", nargs=-1)
def copy_ga_sessions(ga_ids, start_date, end_date, src_project, dst_project, overwrite):
    """Copy ga_sessions tables for one or multiple properties over a date range."""
    bq_client = bigquery.Client(project=dst_project)

    if end_date is None:
        end_date = start_date

    for ga_id in ga_ids:
        bq_client.create_dataset(f"{dst_project}.{ga_id}", exists_ok=True)

        for table_date in [
            end_date - datetime.timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]:
            src_table = get_qualified_table_name(src_project, ga_id, table_date)
            dst_table = get_qualified_table_name(dst_project, ga_id, table_date)

            print(f"copying {src_table} to {dst_table}")
            copy_single_table(bq_client, src_table, dst_table, overwrite)


if __name__ == "__main__":
    copy_ga_sessions()
