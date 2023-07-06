"""Utility functions used by backfills."""

import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import click
from google.auth.exceptions import DefaultCredentialsError
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


def get_qualified_table_name_to_entries_dict(
    sql_dir, qualified_table_name, status=None
) -> defaultdict[str, List[Backfill]]:
    """Return backfill entries from qualified table name."""
    backfills_dict = defaultdict(list)

    project_id, dataset_id, table_id = qualified_table_name_matching(
        qualified_table_name
    )

    table_path = Path(sql_dir) / project_id / dataset_id / table_id

    if not table_path.exists():
        click.echo(f"{project_id}.{dataset_id}.{table_id}" + " does not exist")
        sys.exit(1)

    backfill_file = get_backfill_file_from_qualified_table_name(
        sql_dir, qualified_table_name
    )

    if backfill_file.exists():
        backfills = Backfill.entries_from_file(backfill_file, status)
        if backfills:
            backfills_dict[qualified_table_name] = backfills

    return backfills_dict


def get_qualified_table_name_to_entries_map_by_project(
    sql_dir, project_id, status=None
) -> defaultdict[str, List[Backfill]]:
    """Return backfill entries from project."""
    backfills_dict_all: defaultdict = defaultdict(list)

    search_path = Path(sql_dir) / project_id
    backfill_files = Path(search_path).rglob(BACKFILL_FILE)

    for backfill_file in backfill_files:
        project, dataset, table = extract_from_query_path(backfill_file)
        qualified_table_name = f"{project}.{dataset}.{table}"

        backfills_dict = get_qualified_table_name_to_entries_dict(
            sql_dir, qualified_table_name, status
        )

        backfills_dict_all.update(backfills_dict)

    return backfills_dict_all


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
    project, dataset, table = qualified_table_name_matching(qualified_table_name)

    backfill_table_id = f"{table}_{entry_date}".replace("-", "_")

    return f"{BACKFILL_DESTINATION_PROJECT}.{BACKFILL_DESTINATION_DATASET}.{backfill_table_id}"


def validate_metadata_workgroups(sql_dir, qualified_table_name) -> bool:
    """
    Return True if metadata workgroup is mozilla-confidential or None.

    The backfill staging dataset currently only support backfilling tables for workgroup:mozilla-confidential.
    When workgroup is None, the default (workgroup:mozilla-confidential) will be applied.
    """
    valid_workgroup = ["workgroup:mozilla-confidential"]

    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    dataset_path = Path(sql_dir) / project / dataset

    query_files = Path(dataset_path).rglob("*.sql")

    for query_file in query_files:
        try:
            # check table level metadata
            table_metadata_path = query_file.parent / METADATA_FILE
            table_metadata = Metadata.from_file(table_metadata_path)
            table_workgroup_access = table_metadata.workgroup_access

            if table_workgroup_access is not None:
                if not table_workgroup_access:
                    return False

                for table_workgroup in table_workgroup_access:
                    if table_workgroup.members != valid_workgroup:
                        return False

            # check dataset level metadata
            dataset_metadata_path = dataset_path / DATASET_METADATA_FILE
            dataset_metadata = DatasetMetadata.from_file(dataset_metadata_path)
            dataset_workgroup_access = dataset_metadata.workgroup_access

            if dataset_workgroup_access is not None:
                if not dataset_workgroup_access:
                    return False

                for dataset_workgroup in dataset_workgroup_access:
                    if dataset_workgroup["members"] != valid_workgroup:
                        return False

        except FileNotFoundError:
            click.echo("No metadata.yaml found for {}", qualified_table_name)

    return True


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


def get_backfill_entries_to_process_dict(
    sql_dir, project, qualified_table_name=None
) -> Dict[str, Backfill]:
    """Return backfill entries that require processing."""
    try:
        bigquery.Client(project="")
    except DefaultCredentialsError:
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    client = bigquery.Client(project=project)

    if qualified_table_name:
        backfills_dict = get_qualified_table_name_to_entries_dict(
            sql_dir, qualified_table_name, BackfillStatus.DRAFTING.value
        )
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project, BackfillStatus.DRAFTING.value
        )

    backfills_to_process_dict = {}

    for qualified_table_name, entries in backfills_dict.items():
        # do not return backfill if not mozilla-confidential
        if not validate_metadata_workgroups(sql_dir, qualified_table_name):
            click.echo(
                f"Only mozilla-confidential workgroups are supported.  {qualified_table_name} contain workgroup access that is not supported"
            )
            sys.exit(1)

        if (len(entries)) > 1:
            click.echo(
                f"There should not be more than one entry with drafting status: for {qualified_table_name} "
            )
            sys.exit(1)

        entry_to_process = entries[0]

        backfill_staging_qualified_table_name = (
            get_backfill_staging_qualified_table_name(
                qualified_table_name, entry_to_process.entry_date
            )
        )

        try:
            client.get_table(backfill_staging_qualified_table_name)
            click.echo(
                f"""
                Backfill staging table already exists for {qualified_table_name}: {backfill_staging_qualified_table_name}.
                Backfills will not be processed for this table.
                """
            )
        except NotFound:
            backfills_to_process_dict[qualified_table_name] = entry_to_process

    return backfills_to_process_dict
