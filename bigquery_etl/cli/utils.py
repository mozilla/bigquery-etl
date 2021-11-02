"""Utility functions used by the CLI."""

import os
import fnmatch
from fnmatch import fnmatchcase
from pathlib import Path

import click
import re
from google.cloud import bigquery

from bigquery_etl.util.common import project_dirs

QUERY_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+(_v[0-9]+)?)/"
    r"(?:query\.sql|part1\.sql|script\.sql|query\.py|view\.sql)$"
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


def is_authenticated(project_id=None):
    """Check if the user is authenticated to GCP and can access the project."""
    client = bigquery.Client()

    if project_id:
        return client.project == project_id

    return True


def is_valid_project(ctx, param, value):
    """Check if the provided project_id corresponds to an existing project."""
    if value is None or value in [Path(p).name for p in project_dirs()]:
        return value
    raise click.BadParameter(f"Invalid project {value}")


def table_matches_patterns(pattern, invert, table):
    """Check if tables match pattern."""
    pattern = re.compile(fnmatch.translate(pattern))
    return (pattern.match(table) is not None) != invert


def paths_matching_name_pattern(pattern, sql_path, project_id, files=("*.sql")):
    """Return paths to queries matching the name pattern."""
    matching_files = []

    if pattern is None:
        pattern = "*.*"

    if os.path.isdir(pattern):
        for root, _, _ in os.walk(pattern):
            for file in files:
                matching_files.extend(Path(root).rglob(file))
    elif os.path.isfile(pattern):
        for file in files:
            matching_files.extend(Path(sql_path).rglob(file))
    else:
        sql_path = Path(sql_path)
        if project_id is not None:
            sql_path = sql_path / project_id

        all_matching_files = []

        for file in files:
            all_matching_files.extend(Path(sql_path).rglob(file))

        for query_file in all_matching_files:
            match = QUERY_FILE_RE.match(str(query_file))
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
    help="Path to directory which contains queries.",
    type=click.Path(file_okay=False),
    default="sql",
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

parallelism_option = click.option(
    "--parallelism",
    "-p",
    default=8,
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
        help="Respect or ignore dry run SKIP configuration. "
        f"Default is {flags[default]}.",
        default=default,
    )
