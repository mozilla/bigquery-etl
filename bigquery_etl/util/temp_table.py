"""Generate temporary tables."""

from uuid import uuid4

from google.cloud import bigquery

temporary_dataset = None


def get_temporary_dataset(client: bigquery.Client):
    """Get a cached reference to the dataset used for server-assigned destinations."""
    global temporary_dataset
    if temporary_dataset is None:
        # look up the dataset used for query results without a destination
        dry_run = bigquery.QueryJobConfig(dry_run=True)
        destination = client.query("SELECT NULL", dry_run).destination
        temporary_dataset = client.dataset(destination.dataset_id, destination.project)
    return temporary_dataset


def get_temporary_table(client: bigquery.Client):
    """Generate a temporary table and return the specified date partition.

    Generates a table name that looks similar to, but won't collide with, a
    server assigned table and that the web console will consider temporary.

    In order for query results to use time partitioning, clustering, or an
    expiration other than 24 hours, destination table must be explicitly set.
    Destination must be generated locally and never collide with server
    assigned table names, because server-assigned tables cannot be modified.
    Server assigned tables for a dry_run query cannot be reused as that
    constitutes a modification. Table expiration can't be set in the query job
    config, but it can be set via a CREATE TABLE statement.

    Server assigned tables have names that start with "anon" and follow with
    either 40 hex characters or a uuid replacing "-" with "_", and cannot be
    modified (i.e. reused).

    The web console considers a table temporary if the dataset name starts with
    "_" and table_id starts with "anon" and is followed by at least one
    character.
    """
    return get_temporary_dataset(client).table(f"anon{uuid4().hex}")
