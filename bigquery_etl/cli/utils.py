"""Utility functions used by the CLI."""

import fnmatch
import os
import re
from fnmatch import fnmatchcase
from functools import cache
from glob import glob
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import click
import requests
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader
from bigquery_etl.util.common import TempDatasetReference, project_dirs

QUERY_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+(_v[0-9]+)?)/"
    r"(?:query\.sql|shredder_mitigation_query\.sql|part1\.sql|script\.sql|query\.py|view\.sql|metadata\.yaml|backfill\.yaml)$"
)
CHECKS_FILE_RE = re.compile(
    r"^.*/([a-zA-Z0-9-]+)/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+(_v[0-9]+)?)/"
    r"(?:checks\.sql)$"
)
GLEAN_APP_LISTINGS_URL = "https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings"


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
    pattern,
    sql_path,
    project_id,
    files=["*.sql"],
    file_regex=QUERY_FILE_RE,
    silent=False,
) -> List[Path]:
    """Return paths to queries matching the name pattern."""
    matching_files: List[Path] = []

    if pattern is None:
        pattern = "*.*"

    # click nargs are passed in as a tuple
    if isinstance(pattern, tuple) or isinstance(pattern, list):
        for p in pattern:
            matching_files += paths_matching_name_pattern(
                str(p), sql_path, project_id, files, file_regex, silent
            )
    elif os.path.isdir(pattern):
        for root, _, _ in os.walk(pattern, followlinks=True):
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

    if len(matching_files) == 0 and not silent:
        print(f"No files matching: {pattern}, {files}")

    return list(set(matching_files))


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
    """Generate a project-id option, with optional default.

    Used for file discovery in sql/ directory structure.
    """

    def callback(ctx, param, value):
        if value is not None:
            return is_valid_project(ctx, param, value)

        if default is not None:
            return is_valid_project(ctx, param, default)

        return None

    return click.option(
        "--project-id",
        "--project_id",
        help="GCP project ID for locating query files",
        default=None,
        callback=callback,
        required=required,
    )


def multi_project_id_option(default=None, required=False):
    """Generate a multi project-id option.

    Used for file discovery in sql/ directory structure.
    """
    fallback_default = default if default is not None else []

    def callback(ctx, param, value):
        if value:
            return [is_valid_project(ctx, param, v) for v in value]

        return fallback_default

    return click.option(
        "--project-ids",
        "--project_ids",
        "--project-id",
        "--project_id",
        help="GCP project IDs for locating query files",
        default=None,
        multiple=True,
        callback=callback,
        required=required,
    )


def destination_project_id_option(default=None, required=False):
    """Generate a destination-project-id option for BigQuery operations.

    If --target is specified, uses the target's project_id as default.
    Precedence: explicit CLI value > target > default parameter.
    """

    def callback(ctx, param, value):
        if value is not None:
            return is_valid_project(ctx, param, value)

        if ctx.obj and "target" in ctx.obj and ctx.obj["target"]:
            target = ctx.obj["target"]
            if target.project_id:
                return target.project_id

        if default is not None:
            return is_valid_project(ctx, param, default)

        return None

    return click.option(
        "--destination-project-id",
        "--destination-project_id",
        help="GCP project ID for BigQuery operations (defaults to target project if --target is specified)",
        default=None,
        callback=callback,
        required=required,
    )


def dataset_prefix_option(required=False):
    """Generate a dataset-prefix option, with optional default.

    If --target is specified, uses the target's dataset_prefix as default.
    Precedence: explicit CLI value > target > default parameter.
    """

    def callback(ctx, _param, value):
        if value is not None:
            return value

        if ctx.obj and "target" in ctx.obj and ctx.obj["target"]:
            target = ctx.obj["target"]
            if target.dataset_prefix:
                return target.dataset_prefix

        return None

    return click.option(
        "--dataset-prefix",
        "--dataset_prefix",
        help="Dataset prefix to prepend to dataset names (defaults to target prefix if --target is specified)",
        callback=callback,
        required=required,
    )


def defer_option():
    """Generate a --defer option.

    Smart reference rewriting: only rewrites references to artifacts that exist in the
    target directory. Other references remain pointing to prod (similar to dbt's --defer).
    """
    return click.option(
        "--defer",
        is_flag=True,
        default=False,
        help=(
            "Rewrite references to artifacts that exist in the target directory. "
            "Other references remain pointing to prod. "
            "Used for development workflows."
        ),
    )


def isolated_option():
    """Generate a --isolated option.

    Complete reference rewriting: rewrites ALL references to point to the target
    environment and creates stubs for any missing artifacts. Used for stage deployments
    and complete environment isolation.
    """
    return click.option(
        "--isolated",
        is_flag=True,
        default=False,
        help=(
            "Rewrite ALL references to point to target environment. "
            "Creates stubs for all referenced artifacts. "
            "Used for stage deployments and complete environment isolation."
        ),
    )


def billing_project_option(default=None, required=False):
    """Generate a billing-project option, with optional default."""
    return click.option(
        "--billing-project",
        "--billing_project",
        help=(
            "GCP project ID to run the query in. "
            "This can be used to run a query using a different slot reservation "
            "than the one used by the query's default project."
        ),
        type=str,
        default=default,
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


@cache
def get_glean_app_id_to_app_name_mapping() -> Dict[str, str]:
    """Return a dict where key is the channel app id and the value is the shared app name.

    e.g. {
        "org_mozilla_firefox": "fenix",
        "org_mozilla_firefox_beta": "fenix",
        "org_mozilla_ios_firefox": "firefox_ios",
        "org_mozilla_ios_firefoxbeta": "firefox_ios",
    }
    """
    response = requests.get(GLEAN_APP_LISTINGS_URL)
    response.raise_for_status()

    app_listings = response.json()

    return {
        app["bq_dataset_family"]: app["app_name"]
        for app in app_listings
        if "bq_dataset_family" in app and "app_name" in app
    }


def resolve_effective_project(
    destination_project_id: Optional[str], project_id: str
) -> str:
    """Resolve the effective project for BigQuery operations."""
    return destination_project_id if destination_project_id else project_id


def apply_dataset_prefix(dataset: str, dataset_prefix: Optional[str]) -> str:
    """Apply dataset prefix if provided."""
    return f"{dataset_prefix}{dataset}" if dataset_prefix else dataset


def resolve_destination_table(
    project: str,
    dataset: str,
    table: str,
    destination_project_id: Optional[str] = None,
    dataset_prefix: Optional[str] = None,
) -> str:
    """Resolve the fully qualified destination table ID."""
    effective_project = resolve_effective_project(destination_project_id, project)
    effective_dataset = apply_dataset_prefix(dataset, dataset_prefix)
    return f"{effective_project}.{effective_dataset}.{table}"
