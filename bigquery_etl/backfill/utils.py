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


def get_backfill_file_to_entries_map(
    sql_dir, project_id, qualified_table_name
) -> defaultdict[Path, List[Backfill]]:
    """Return backfills with name pattern."""
    backfills_dict = defaultdict(list)

    table_id = None

    if qualified_table_name:
        try:
            (project_id, dataset_id, table_id) = qualified_table_name_matching(
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

    for file in backfill_files:
        backfills = Backfill.entries_from_file(file)
        if backfills:
            backfills_dict[file].extend(backfills)

    return backfills_dict
