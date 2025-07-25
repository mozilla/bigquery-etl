"""bigquery-etl CLI dryrun command."""

import fnmatch
import glob
import os
import re
import sys
import time
from functools import partial
from multiprocessing.pool import Pool
from typing import List, Set, Tuple

import rich_click as click

from ..cli.utils import billing_project_option, is_authenticated
from ..config import ConfigLoader
from ..dryrun import DryRun, get_credentials, get_id_token


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
@billing_project_option()
def dryrun(
    paths: List[str],
    use_cloud_function: bool,
    validate_schemas: bool,
    respect_skip: bool,
    project: str,
    billing_project: str,
):
    """Perform a dry run."""
    file_names = (
        "query.sql",
        "view.sql",
        "part*.sql",
        "init.sql",
        "materialized_view.sql",
    )
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
        click.echo("Skipping dry run because no queries matched")
        return

    if not use_cloud_function and not is_authenticated():
        click.echo(
            "Not authenticated to GCP. Run `gcloud auth login  --update-adc` to login."
        )
        sys.exit(1)

    credentials = get_credentials()
    id_token = get_id_token(credentials=credentials)

    sql_file_valid = partial(
        _sql_file_valid,
        use_cloud_function,
        respect_skip,
        validate_schemas,
        credentials=credentials,
        id_token=id_token,
        billing_project=billing_project,
    )
    start_time = time.time()
    with Pool(8) as p:
        result = p.map(sql_file_valid, sql_files, chunksize=1)
    print(f"Total dryrun time: {time.time() - start_time:.2f}s")

    failures = sorted([r[1] for r in result if not r[0]])
    if len(failures) > 0:
        click.echo(
            f"Failed to validate {len(failures)} queries (see above for error messages):",
            err=True,
        )
        click.echo("\n".join(failures), err=True)
        sys.exit(1)


def _sql_file_valid(
    use_cloud_function,
    respect_skip,
    validate_schemas,
    sqlfile,
    credentials,
    id_token,
    billing_project=None,
) -> Tuple[bool, str]:
    """Dry run the SQL file."""
    result = DryRun(
        sqlfile,
        use_cloud_function=use_cloud_function,
        credentials=credentials,
        respect_skip=respect_skip,
        id_token=id_token,
        billing_project=billing_project,
    )
    if validate_schemas:
        try:
            success = result.validate_schema()
        except Exception as e:  # validate_schema raises base exception
            click.echo(e, err=True)
            success = False
        return success, sqlfile

    return result.is_valid(), sqlfile
