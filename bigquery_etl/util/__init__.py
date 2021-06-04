"""BigQuery ETL Utilities."""

from pathlib import Path


def extract_from_query_path(path):
    """Extract table, dataset and project ID from query file."""
    query_path = Path(path)

    if query_path.is_file():
        # path might be to query.sql, schema.yaml, ...
        query_path = query_path.parent

    table = query_path.name
    dataset = query_path.parent.name
    project = query_path.parent.parent.name

    return project, dataset, table
