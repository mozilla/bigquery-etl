"""bigquery-etl CLI dryrun command."""

import fnmatch
import glob
import os
import re
import sys
from functools import partial
from multiprocessing.pool import Pool
from typing import List, Set

import rich_click as click
from google.cloud import bigquery

from ..cli.utils import is_authenticated
from ..config import ConfigLoader
from ..dryrun import DryRun


@click.command(
    help="""Dry run SQL.
        Uses the dryrun Cloud Function by default which only has access to shared-prod.
        To dryrun queries accessing tables in another project use set
        `--use-cloud-function=false` and ensure that the command line has access to a
        GCP service account.

    Examples:
    ./bqetl dryrun sql/moz-fx-data-shared-prod/telemetry_derived/

    # Dry run SQL with tables that are not in shared prod
    ./bqetl dryrun --use-cloud-function=false sql/moz-fx-data-marketing-prod/
    """,
)
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(file_okay=True),
)
@click.option(
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
@click.option(
    "--validate_schemas",
    "--validate-schemas",
    help="Require dry run schema to match destination table and file if present.",
    is_flag=True,
    default=False,
)
@click.option(
    "--respect-skip/--ignore-skip",
    help="Respect or ignore query skip configuration. Default is --respect-skip.",
    default=True,
)
@click.option(
    "--project",
    help="GCP project to perform dry run in when --use_cloud_function=False",
    default=ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod"),
)
def dryrun(
    paths: List[str],
    use_cloud_function: bool,
    validate_schemas: bool,
    respect_skip: bool,
    project: str,
):
    """Perform a dry run."""
    file_names = ("query.sql", "view.sql", "part*.sql", "init.sql")
    file_re = re.compile("|".join(map(fnmatch.translate, file_names)))

    sql_files: Set[str] = set()
    for path in paths:
        if os.path.isdir(path):
            sql_files |= {
                sql_file
                for pattern in file_names
                for sql_file in glob.glob(f"{path}/**/{pattern}", recursive=True)
            }
        elif os.path.isfile(path):
            if file_re.fullmatch(os.path.basename(path)):
                sql_files.add(path)
        else:
            click.echo(f"Invalid path {path}", err=True)
            sys.exit(1)
    if respect_skip:
        sql_files -= DryRun.skipped_files()

    if not sql_files:
        print("Skipping dry run because no queries matched")
        sys.exit(0)

    if not use_cloud_function and not is_authenticated():
        click.echo("Not authenticated to GCP. Run `gcloud auth login` to login.")
        sys.exit(1)

    sql_file_valid = partial(
        _sql_file_valid, use_cloud_function, project, respect_skip, validate_schemas
    )

    with Pool(8) as p:
        result = p.map(sql_file_valid, sql_files, chunksize=1)
    if not all(result):
        sys.exit(1)


def _sql_file_valid(
    use_cloud_function, project, respect_skip, validate_schemas, sqlfile
):
    if not use_cloud_function:
        client = bigquery.Client(project=project)
    else:
        client = None

    """Dry run the SQL file."""
    result = DryRun(
        sqlfile,
        use_cloud_function=use_cloud_function,
        client=client,
        respect_skip=respect_skip,
    )
    if validate_schemas:
        return result.validate_schema()
    return result.is_valid()
