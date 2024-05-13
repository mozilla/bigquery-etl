"""Utility functions used by backfills."""

import re
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from bigquery_etl.backfill.parse import BACKFILL_FILE, Backfill, BackfillStatus
from bigquery_etl.metadata.parse_metadata import (
    DATASET_METADATA_FILE,
    METADATA_FILE,
    DatasetMetadata,
    Metadata,
)
from bigquery_etl.util import extract_from_query_path

QUALIFIED_TABLE_NAME_RE = re.compile(
    r"(?P<project_id>[a-zA-z0-9_-]+)\.(?P<dataset_id>[a-zA-z0-9_-]+)\.(?P<table_id>[a-zA-Z0-9-_$]+)"
)

BACKFILL_DESTINATION_PROJECT = "moz-fx-data-shared-prod"
BACKFILL_DESTINATION_DATASET = "backfills_staging_derived"

# currently only supporting backfilling tables with workgroup access: mozilla-confidential.
VALID_WORKGROUP_MEMBER = ["workgroup:mozilla-confidential"]


def get_entries_from_qualified_table_name(
    sql_dir, qualified_table_name, status=None
) -> List[Backfill]:
    """Return backfill entries from qualified table name."""
    backfills = []

    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_path = Path(sql_dir) / project / dataset / table

    if not table_path.exists():
        click.echo(f"{project}.{dataset}.{table}" + " does not exist")
        sys.exit(1)

    backfill_file = get_backfill_file_from_qualified_table_name(
        sql_dir, qualified_table_name
    )

    if backfill_file.exists():
        backfills = Backfill.entries_from_file(backfill_file, status)

    return backfills


def get_qualified_table_name_to_entries_map_by_project(
    sql_dir, project_id: str, status: Optional[str] = None
) -> Dict[str, List[Backfill]]:
    """Return backfill entries from project."""
    backfills_dict: dict = {}

    backfill_files = Path(sql_dir).glob(f"{project_id}/*/*/{BACKFILL_FILE}")
    for backfill_file in backfill_files:
        project, dataset, table = extract_from_query_path(backfill_file)
        qualified_table_name = f"{project}.{dataset}.{table}"

        entries = get_entries_from_qualified_table_name(
            sql_dir, qualified_table_name, status
        )

        if entries:
            backfills_dict[qualified_table_name] = entries

    return backfills_dict


def get_backfill_file_from_qualified_table_name(sql_dir, qualified_table_name) -> Path:
    """Return backfill file from qualified table name."""
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    path = Path(sql_dir)
    query_path = path / project / dataset / table
    backfill_file = query_path / BACKFILL_FILE

    return backfill_file


# TODO: It would be better to take in a backfill object.
def get_backfill_staging_qualified_table_name(qualified_table_name, entry_date) -> str:
    """Return full table name where processed backfills are stored."""
    _, _, table = qualified_table_name_matching(qualified_table_name)
    backfill_table_id = f"{table}_{entry_date}".replace("-", "_")

    return f"{BACKFILL_DESTINATION_PROJECT}.{BACKFILL_DESTINATION_DATASET}.{backfill_table_id}"


def get_backfill_backup_table_name(qualified_table_name: str, entry_date: date) -> str:
    """Return full table name where backup of production table is stored."""
    _, _, table = qualified_table_name_matching(qualified_table_name)
    cloned_table_id = f"{table}_backup_{entry_date}".replace("-", "_")

    return f"{BACKFILL_DESTINATION_PROJECT}.{BACKFILL_DESTINATION_DATASET}.{cloned_table_id}"


def validate_depends_on_past(sql_dir, qualified_table_name) -> bool:
    """
    Check if the table depends on past.

    Managed backfills currently do not support tables that depends on past.
    """
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_metadata_path = Path(sql_dir) / project / dataset / table / METADATA_FILE

    table_metadata = Metadata.from_file(table_metadata_path)

    if "depends_on_past" in table_metadata.scheduling:
        return not table_metadata.scheduling[
            "depends_on_past"
        ]  # skip if depends_on_past

    return True


def validate_metadata_workgroups(sql_dir, qualified_table_name) -> bool:
    """
    Check if either table or dataset metadata workgroup is valid.

    The backfill staging dataset currently only support backfilling datasets and tables for workgroup:mozilla-confidential.
    """
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    query_file = Path(sql_dir) / project / dataset / table / "query.sql"
    dataset_path = Path(sql_dir) / project / dataset
    dataset_metadata_path = dataset_path / DATASET_METADATA_FILE
    table_metadata_path = dataset_path / table / METADATA_FILE

    if not query_file.exists():
        click.echo("No query.sql file found for {}", qualified_table_name)
        sys.exit(1)

    # check dataset level metadata
    try:
        dataset_metadata = DatasetMetadata.from_file(dataset_metadata_path)
        dataset_workgroup_access = dataset_metadata.workgroup_access
        dataset_default_table_workgroup_access = (
            dataset_metadata.default_table_workgroup_access
        )

    except FileNotFoundError as e:
        raise ValueError(
            f"Unable to validate workgroups for {qualified_table_name}"
        ) from e

    if _validate_workgroup_members(dataset_workgroup_access, DATASET_METADATA_FILE):
        return True

    # check table level metadata
    try:
        table_metadata = Metadata.from_file(table_metadata_path)
        table_workgroup_access = table_metadata.workgroup_access

        if _validate_workgroup_members(table_workgroup_access, METADATA_FILE):
            return True

    except FileNotFoundError:
        # default table workgroup access is applied to table if metadata.yaml file is missing
        if _validate_workgroup_members(
            dataset_default_table_workgroup_access, DATASET_METADATA_FILE
        ):
            return True

    return False


def _validate_workgroup_members(workgroup_access, metadata_filename):
    """Return True if workgroup members is valid (workgroup:mozilla-confidential)."""
    if workgroup_access:
        for workgroup in workgroup_access:
            if metadata_filename == METADATA_FILE:
                members = workgroup.members
            elif metadata_filename == DATASET_METADATA_FILE:
                members = workgroup["members"]

            if members == VALID_WORKGROUP_MEMBER:
                return True

    return False


def qualified_table_name_matching(qualified_table_name) -> Tuple[str, str, str]:
    """Match qualified table name pattern."""
    if match := QUALIFIED_TABLE_NAME_RE.match(qualified_table_name):
        project_id = match.group("project_id")
        dataset_id = match.group("dataset_id")
        table_id = match.group("table_id")
    else:
        raise AttributeError(
            "Qualified table name must be named like:" + " <project>.<dataset>.<table>"
        )

    return project_id, dataset_id, table_id


def get_scheduled_backfills(
    sql_dir,
    project: str,
    qualified_table_name: Optional[str] = None,
    status: Optional[str] = None,
) -> Dict[str, Backfill]:
    """Return backfill entries to initiate or complete."""
    client = bigquery.Client(project=project)

    if qualified_table_name:
        backfills_dict = {
            qualified_table_name: get_entries_from_qualified_table_name(
                sql_dir, qualified_table_name, status
            )
        }
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project, status
        )

    backfills_to_process_dict = {}

    for qualified_table_name, entries in backfills_dict.items():
        # do not return backfill if depends on past
        if not validate_depends_on_past(sql_dir, qualified_table_name):
            continue

        # do not return backfill if not mozilla-confidential
        if not validate_metadata_workgroups(sql_dir, qualified_table_name):
            continue

        if not entries:
            continue

        if BackfillStatus.INITIATE.value == status and (len(entries)) > 1:
            raise ValueError(
                f"More than one backfill entry for {qualified_table_name} found with status: {status}."
            )

        entry_to_process = entries[0]

        if (
            BackfillStatus.INITIATE.value == status
            and _should_initiate(client, entry_to_process, qualified_table_name)
        ) or (
            BackfillStatus.COMPLETE.value == status
            and _should_complete(client, entry_to_process, qualified_table_name)
        ):
            backfills_to_process_dict[qualified_table_name] = entry_to_process

    return backfills_to_process_dict


def _should_initiate(
    client: bigquery.Client, backfill: Backfill, qualified_table_name: str
) -> bool:
    """Determine whether a backfill should be initiated.

    Return true if the backfill is in Initiate status and the staging data does not yet exist.
    """
    if backfill.status != BackfillStatus.INITIATE:
        return False

    staging_table = get_backfill_staging_qualified_table_name(
        qualified_table_name, backfill.entry_date
    )

    if _table_exists(client, staging_table):
        return False

    return True


def _should_complete(
    client: bigquery.Client, backfill: Backfill, qualified_table_name: str
) -> bool:
    """Determine whether a backfill should be completed.

    Return true if the backfill is in Complete status, the staging data exists, and the backup does not yet exist.
    """
    if backfill.status != BackfillStatus.COMPLETE:
        return False

    staging_table = get_backfill_staging_qualified_table_name(
        qualified_table_name, backfill.entry_date
    )
    if not _table_exists(client, staging_table):
        click.echo(f"Backfill staging table does not exist: {staging_table}")
        return False

    backup_table_name = get_backfill_backup_table_name(
        qualified_table_name, backfill.entry_date
    )
    if _table_exists(client, backup_table_name):
        click.echo(f"Backfill backup table already exists: {backup_table_name}")
        return False

    return True


def _table_exists(client: bigquery.Client, qualified_table_name: str) -> bool:
    try:
        client.get_table(qualified_table_name)
        return True
    except NotFound:
        return False
