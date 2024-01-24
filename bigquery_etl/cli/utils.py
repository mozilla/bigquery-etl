"""Utility functions used by the CLI."""

import fnmatch
import os
import re
from fnmatch import fnmatchcase
from glob import glob
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import click
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader
from bigquery_etl.util.common import TempDatasetReference, project_dirs

QUERY_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+(_v[0-9]+)?)/"
    r"(?:query\.sql|part1\.sql|script\.sql|query\.py|view\.sql|metadata\.yaml|backfill\.yaml)$"
)
CHECKS_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+(_v[0-9]+)?)/"
    r"(?:checks\.sql)$"
)


def is_valid_dir(ctx, param, value):
    """Check if the parameter provided via click is an existing directory."""
    if not os.path.isdir(value) or not os.path.exists(value):
        raise click.BadParameter(f"Invalid directory path to {value}")
    return value


def is_valid_file(ctx, param, value):
    """Check if the parameter provided via click is an existing file."""
    if not os.path.isfile(value) or not os.path.exists(value):
        raise click.BadParameter(f"Invalid file path to {value}")
    return value


def is_authenticated():
    """Check if the user is authenticated to GCP."""
    try:
        bigquery.Client(project="")
    except DefaultCredentialsError:
        return False
    return True


def is_valid_project(ctx, param, value):
    """Check if the provided project_id corresponds to an existing project."""
    if (
        value is None
        or value
        in [Path(p).name for p in project_dirs()]
        + [
            ConfigLoader.get("default", "test_project"),
            ConfigLoader.get("default", "user_facing_project", fallback="mozdata"),
        ]
        + ConfigLoader.get("default", "additional_projects", fallback=[])
        or value.startswith(ConfigLoader.get("default", "backfill_project"))
    ):
        return value
    raise click.BadParameter(f"Invalid project {value}")


def table_matches_patterns(pattern, invert, table):
    """Check if tables match pattern."""
    if not isinstance(pattern, list):
        pattern = [pattern]

    matching = False
    for p in pattern:
        compiled_pattern = re.compile(fnmatch.translate(p))
        if compiled_pattern.match(table) is not None:
            matching = True
            break

    return matching != invert


def paths_matching_checks_pattern(
    pattern: str, sql_path: Optional[str], project_id: Optional[str]
) -> Iterator[Tuple[Path, str, str, str]]:
    """Return single path to checks.sql matching the name pattern."""
    checks_files = paths_matching_name_pattern(
        pattern, sql_path, project_id, ["checks.sql"], CHECKS_FILE_RE
    )

    if checks_files:
        for checks_file in checks_files:
            match = CHECKS_FILE_RE.match(str(checks_file))
            if match:
                project = match.group(1)
                dataset = match.group(2)
                table = match.group(3)
            yield checks_file, project, dataset, table
    else:
        print(f"No checks.sql file found in {sql_path}/{project_id}/{pattern}")


def paths_matching_name_pattern(
    pattern, sql_path, project_id, files=["*.sql"], file_regex=QUERY_FILE_RE
) -> List[Path]:
    """Return paths to queries matching the name pattern."""
    matching_files: List[Path] = []

    if pattern is None:
        pattern = "*.*"

    if os.path.isdir(pattern):
        for root, _, _ in os.walk(pattern):
            for file in files:
                matching_files.extend(
                    map(Path, glob(f"{root}/**/{file}", recursive=True))
                )
    elif os.path.isfile(pattern):
        matching_files.append(Path(pattern))
    else:
        sql_path = Path(sql_path)
        if project_id is not None:
            sql_path = sql_path / project_id

        all_matching_files: List[Path] = []

        for file in files:
            all_matching_files.extend(
                map(Path, glob(f"{sql_path}/**/{file}", recursive=True))
            )

        for query_file in all_matching_files:
            match = file_regex.match(str(query_file))
            if match:
                project = match.group(1)
                dataset = match.group(2)
                table = match.group(3)
                query_name = f"{project}.{dataset}.{table}"
                if fnmatchcase(query_name, f"*{pattern}"):
                    matching_files.append(query_file)
                elif project_id and fnmatchcase(query_name, f"{project_id}.{pattern}"):
                    matching_files.append(query_file)

    if len(matching_files) == 0:
        print(f"No files matching: {pattern}")

    return matching_files


sql_dir_option = click.option(
    "--sql_dir",
    "--sql-dir",
    help="Path to directory which contains queries.",
    type=click.Path(file_okay=False),
    default=ConfigLoader.get("default", "sql_dir", fallback="sql"),
    callback=is_valid_dir,
)


use_cloud_function_option = click.option(
    "--use_cloud_function",
    "--use-cloud-function",
    help=(
        "Use the Cloud Function for dry running SQL, if set to `True`. "
        "The Cloud Function can only access tables in shared-prod. "
        "If set to `False`, use active GCP credentials for the dry run."
    ),
    type=bool,
    default=True,
)


def parallelism_option(default=8):
    """Generate a parallelism option, with optional default."""
    return click.option(
        "--parallelism",
        "-p",
        default=default,
        type=int,
        help="Number of threads for parallel processing",
    )


def project_id_option(default=None, required=False):
    """Generate a project-id option, with optional default."""
    return click.option(
        "--project-id",
        "--project_id",
        help="GCP project ID",
        default=default,
        callback=is_valid_project,
        required=required,
    )


def respect_dryrun_skip_option(default=True):
    """Generate a respect_dryrun_skip option."""
    flags = {True: "--respect-dryrun-skip", False: "--ignore-dryrun-skip"}
    return click.option(
        f"{flags[True]}/{flags[False]}",
        help="Respect or ignore dry run skip configuration. "
        f"Default is {flags[default]}.",
        default=default,
    )


def no_dryrun_option(default=False):
    """Generate a skip_dryrun option."""
    return click.option(
        "--no-dryrun",
        "--no_dryrun",
        help="Skip running dryrun. " f"Default is {default}.",
        default=default,
        is_flag=True,
    )


def temp_dataset_option(
    default=f"{ConfigLoader.get('default', 'project', fallback='moz-fx-data-shared-prod')}.tmp",
):
    """Generate a temp-dataset option."""
    return click.option(
        "--temp-dataset",
        "--temp_dataset",
        "--temporary-dataset",
        "--temporary_dataset",
        default=default,
        type=TempDatasetReference.from_string,
        help="Dataset where intermediate query results will be temporarily stored, "
        "formatted as PROJECT_ID.DATASET_ID",
    )
