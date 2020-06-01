"""Get the string id in various formats for BigQuery resources."""

import re

FULL_JOB_ID_RE = re.compile(r"(?P<project>[^:]+):(?P<location>[^.]+).(?P<job_id>.+)")


def full_job_id(job):
    """Get the bq cli format fully qualified id for a job."""
    return f"{job.project}:{job.location}.{job.job_id}"


def sql_table_id(table):
    """Get the standard sql format fully qualified id for a table."""
    return f"{table.project}.{table.dataset_id}.{table.table_id}"


def qualified_table_id(table):
    """Get the partially qualified id for a table."""
    return f"{table.dataset_id}.{table.table_id}"
