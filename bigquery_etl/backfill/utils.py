"""Utility functions used by backfills."""

import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import List, Tuple

import click

from bigquery_etl.backfill.parse import BACKFILL_FILE, Backfill
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


def get_qualifed_table_name_to_entries_map(
    sql_dir, project_id, qualified_table_name, status=None
) -> defaultdict[str, List[Backfill]]:
    """Return backfill entries from qualified table name."""
    backfills_dict = defaultdict(list)

    search_path = Path(sql_dir) / project_id

    if qualified_table_name:
        try:
            project_id, dataset_id, table_id = qualified_table_name_matching(
                qualified_table_name
            )

            search_path = search_path / dataset_id / table_id

            if not search_path.exists():
                click.echo(f"{project_id}.{dataset_id}.{table_id}" + " does not exist")
                sys.exit(1)

        except AttributeError as e:
            click.echo(e)
            sys.exit(1)

    backfill_files = Path(search_path).rglob(BACKFILL_FILE)

    for backfill_file in backfill_files:
        project, dataset, table = extract_from_query_path(backfill_file)
        qualified_table_name = f"{project}.{dataset}.{table}"
        backfills = Backfill.entries_from_file(backfill_file, status)

        if backfills:
            backfills_dict[qualified_table_name].extend(backfills)

    return backfills_dict


def get_backfill_file_from_qualified_table_name(sql_dir, qualified_table_name) -> Path:
    """Return backfill file from qualified table name."""
    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    path = Path(sql_dir)
    query_path = path / project / dataset / table
    backfill_file = query_path / BACKFILL_FILE

    return backfill_file


def get_backfill_staging_qualified_table_name(qualified_table_name, entry_date) -> str:
    """Return full table name where processed backfills are stored."""
    project, dataset, table = qualified_table_name_matching(qualified_table_name)

    backfill_table_id = f"{table}_{entry_date}".replace("-", "_")

    return f"{BACKFILL_DESTINATION_PROJECT}.{BACKFILL_DESTINATION_DATASET}.{backfill_table_id}"


def validate_metadata_workgroups(qualified_table_name, sql_dir) -> bool:
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
