"""Utilities for deploying BigQuery tables from bigquery-etl queries."""

import logging
from pathlib import Path
from typing import Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .config import ConfigLoader
from .dryrun import DryRun, get_id_token
from .metadata.parse_metadata import METADATA_FILE, Metadata
from .metadata.publish_metadata import attach_external_data_config, attach_metadata
from .schema import SCHEMA_FILE, Schema

log = logging.getLogger(__name__)


class SkippedDeployException(Exception):
    """Raised when a deployment is skipped."""


class FailedDeployException(Exception):
    """Raised for failed deployments."""


class SkippedExternalDataException(Exception):
    """Raised for external data tables if skip_external_data is True."""


def deploy_table(
    artifact_file: Path,
    destination_table: Optional[str] = None,
    force: bool = False,
    use_cloud_function: bool = False,
    skip_existing: bool = False,
    skip_external_data: bool = False,
    update_metadata: bool = True,
    respect_dryrun_skip: bool = True,
    sql_dir=ConfigLoader.get("default", "sql_dir"),
    credentials=None,
    id_token=None,
) -> None:
    """Deploy a query to a destination."""
    if respect_dryrun_skip and str(artifact_file) in DryRun.skipped_files():
        raise SkippedDeployException(f"Dry run skipped for {artifact_file}.")

    metadata = None
    try:
        if artifact_file.name == METADATA_FILE:
            metadata = Metadata.from_file(artifact_file)
        else:
            metadata = Metadata.of_query_file(artifact_file)

        if metadata.external_data:
            # We generate stub query.py sripts for external tables in stage, so this needs to be the first check
            if skip_external_data:
                raise SkippedExternalDataException(
                    f"Skipping deploy of external data table {artifact_file}"
                )
            if artifact_file.suffix == ".sql" or artifact_file.name == "query.py":
                raise FailedDeployException(
                    f"Invalid metadata: {artifact_file} has both a SQL file and "
                    f"external data config"
                )

        if (
            metadata.scheduling
            and "destination_table" in metadata.scheduling
            and metadata.scheduling["destination_table"] is None
        ):
            raise SkippedDeployException(
                f"Skipping deploy for {artifact_file}, null destination_table configured."
            )
    except FileNotFoundError:
        log.warning(f"No metadata found for {artifact_file}.")

    table_name = artifact_file.parent.name
    dataset_name = artifact_file.parent.parent.name
    project_name = artifact_file.parent.parent.parent.name

    if destination_table is None:
        destination_table = f"{project_name}.{dataset_name}.{table_name}"

    existing_schema_path = artifact_file.parent / SCHEMA_FILE
    try:
        existing_schema = Schema.from_schema_file(existing_schema_path)
    except Exception as e:  # TODO: Raise/catch more specific exception
        raise SkippedDeployException(f"Schema missing for {artifact_file}.") from e

    client = bigquery.Client(credentials=credentials)
    if not force and str(artifact_file).endswith("query.sql"):
        query_schema = Schema.from_query_file(
            artifact_file,
            use_cloud_function=use_cloud_function,
            respect_skip=respect_dryrun_skip,
            sql_dir=sql_dir,
            client=client,
            id_token=id_token if not use_cloud_function or id_token else get_id_token(),
        )
        if not existing_schema.equal(query_schema):
            raise FailedDeployException(
                f"Query {artifact_file} does not match "
                f"schema in {existing_schema_path}. "
                f"To update the local schema file, "
                f"run `./bqetl query schema update "
                f"{dataset_name}.{table_name}`",
            )

    try:
        table = client.get_table(destination_table)
    except NotFound:
        table = bigquery.Table(destination_table)
    table.schema = existing_schema.to_bigquery_schema()

    if metadata and metadata.external_data:
        attach_external_data_config(artifact_file, table)

    if update_metadata:
        attach_metadata(artifact_file, table)

    _create_or_update(client, table, skip_existing)


def _create_or_update(
    client: bigquery.Client,
    table: bigquery.Table,
    skip_existing: bool = False,
) -> None:
    if table.created:
        if skip_existing:
            raise SkippedDeployException(f"{table} already exists.")
        log.info(f"{table} already exists, updating.")
        try:
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
        except Exception as e:
            raise FailedDeployException(f"Unable to update table {table}: {e}") from e
        log.info(f"{table} updated.")
    else:
        try:
            client.create_table(table)
        except Exception as e:
            raise FailedDeployException(f"Unable to create table {table}: {e}") from e
        log.info(f"{table} created.")
