"""Utility functions used by backfills."""

import sys
from collections import defaultdict
from pathlib import Path
from typing import List

import click

from bigquery_etl.backfill.parse import BACKFILL_FILE, Backfill
from bigquery_etl.cli.utils import (
    paths_matching_name_pattern,
    qualified_table_name_matching,
)
from bigquery_etl.docs.derived_datasets.generate_derived_dataset_docs import (
    _get_metadata,
)
from bigquery_etl.metadata.parse_metadata import DATASET_METADATA_FILE
from bigquery_etl.util import extract_from_query_path

BACKFILL_DESTINATION_PROJECT = "moz-fx-data-shared-prod"
BACKFILL_DESTINATION_DATASET = "backfills_staging_derived"


def get_qualifed_table_name_to_entries_map(
    sql_dir, project_id, qualified_table_name, status=None
) -> defaultdict[str, List[Backfill]]:
    """Return backfill entries from qualified table name."""
    backfills_dict = defaultdict(list)

    table_id = None

    if qualified_table_name:
        try:
            project_id, dataset_id, table_id = qualified_table_name_matching(
                qualified_table_name
            )

            path = Path(sql_dir)
            query_path = path / project_id / dataset_id / table_id

            if not query_path.exists():
                click.echo(f"{project_id}.{dataset_id}.{table_id}" + " does not exist")
                sys.exit(1)

        except AttributeError as e:
            click.echo(e)
            sys.exit(1)

    backfill_files = paths_matching_name_pattern(
        table_id, sql_dir, project_id, [BACKFILL_FILE]
    )

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
    """Check if metadata workgroup is mozilla-confidential or empty."""
    valid_workgroup = ["workgroup:mozilla-confidential"]

    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    query_files = paths_matching_name_pattern(table, sql_dir, project)

    for query_file in query_files:
        try:
            # check table level metadata
            table_path = query_file.parent
            table_metadata = _get_metadata(table_path)
            table_workgroup_access = table_metadata.workgroup_access
            for table_workgroup in table_workgroup_access:
                if table_workgroup.members != valid_workgroup:
                    return False

            # check dataset level metadata
            dataset_path = query_file.parent.parent
            dataset_metadata = _get_metadata(dataset_path, DATASET_METADATA_FILE)
            dataset_workgroup_access = dataset_metadata.workgroup_access
            for dataset_workgroup in dataset_workgroup_access:
                if dataset_workgroup["members"] != valid_workgroup:
                    return False

        except FileNotFoundError:
            click.echo("No metadata.yaml found for {}", qualified_table_name)

    return True
