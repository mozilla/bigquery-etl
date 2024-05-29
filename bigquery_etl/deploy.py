"""Utilities for deploying BigQuery tables from bigquery-etl queries."""

import logging
from pathlib import Path
from typing import Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .config import ConfigLoader
from .dryrun import DryRun
from .metadata.parse_metadata import Metadata
from .metadata.publish_metadata import attach_metadata
from .schema import SCHEMA_FILE, Schema

log = logging.getLogger(__name__)


class SkippedDeployException(Exception):
    """Raised when a deployment is skipped."""


class FailedDeployException(Exception):
    """Raised for failed deployments."""


def deploy_table(
    query_file: Path,
    destination_table: Optional[str] = None,
    force: bool = False,
    use_cloud_function: bool = False,
    skip_existing: bool = False,
    update_metadata: bool = True,
    respect_dryrun_skip: bool = True,
    sql_dir=ConfigLoader.get("default", "sql_dir"),
) -> None:
    """Deploy a query to a destination."""
    if respect_dryrun_skip and str(query_file) in DryRun.skipped_files():
        raise SkippedDeployException(f"Dry run skipped for {query_file}.")

    try:
        metadata = Metadata.of_query_file(query_file)
        if (
            metadata.scheduling
            and "destination_table" in metadata.scheduling
            and metadata.scheduling["destination_table"] is None
        ):
            raise FailedDeployException(
                f"Destination table configured for {query_file} in metadata but is empty."
            )
    except FileNotFoundError:
        log.warning(f"No metadata found for {query_file}.")

    table_name = query_file.parent.name
    dataset_name = query_file.parent.parent.name
    project_name = query_file.parent.parent.parent.name

    if destination_table is None:
        destination_table = f"{project_name}.{dataset_name}.{table_name}"

    existing_schema_path = query_file.parent / SCHEMA_FILE
    try:
        existing_schema = Schema.from_schema_file(existing_schema_path)
    except Exception as e:  # TODO: Raise/catch more specific exception
        raise SkippedDeployException(f"Schema missing for {query_file}.") from e

    if not force and str(query_file).endswith("query.sql"):
        query_schema = Schema.from_query_file(
            query_file,
            use_cloud_function=use_cloud_function,
            respect_skip=respect_dryrun_skip,
            sql_dir=sql_dir,
        )
        if not existing_schema.equal(query_schema):
            raise FailedDeployException(
                f"Query {query_file} does not match "
                f"schema in {existing_schema_path}. "
                f"To update the local schema file, "
                f"run `./bqetl query schema update "
                f"{dataset_name}.{table_name}`",
            )

    client = bigquery.Client()
    try:
        table = client.get_table(destination_table)
    except NotFound:
        table = bigquery.Table(destination_table)

    bigquery_schema = existing_schema.to_bigquery_schema()
    table.schema = bigquery_schema
    if update_metadata:
        attach_metadata(query_file, table)

    _create_or_update(client, table, destination_table, skip_existing)


def _create_or_update(
    client: bigquery.Client,
    table: bigquery.Table,
    destination_table: str,
    skip_existing: bool = False,
) -> None:
    if table.created:
        if skip_existing:
            raise SkippedDeployException(f"{destination_table} already exists.")
        log.info(f"{destination_table} already exists, updating.")
        client.update_table(
            table,
            [
                "schema",
                "friendly_name",
                "description",
                "time_partitioning",
                "clustering_fields",
                "labels",
            ],
        )
        log.info(f"{destination_table} updated.")
    else:
        client.create_table(table)
        log.info(f"{destination_table} created.")
