"""bigquery-etl CLI dryrun command."""

import click
from google.cloud import bigquery
import glob
import os
from multiprocessing.pool import ThreadPool
from pathlib import Path
import sys

from ..dryrun import DryRun, SKIP
from ..cli.utils import is_authenticated


@click.command(help="Dry run SQL.",)
@click.argument(
    "path", default="sql/", type=click.Path(file_okay=True),
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
    "--project",
    help="GCP project to perform dry run in when --use_cloud_function=False",
    default="moz-fx-data-shared-prod",
)
def dryrun(path, use_cloud_function, project):
    """Perform a dry run."""
    if os.path.isdir(path) and os.path.exists(path):
        sql_files = [
            f for f in glob.glob(path + "/*.sql", recursive=True) if f not in SKIP
        ]
    elif os.path.isfile(path) and os.path.exists(path):
        sql_files = [path]
    else:
        click.echo(f"Invalid path {path}", err=True)

    if use_cloud_function:

        def cloud_function_dryrun(sqlfile):
            """Dry run SQL files."""
            return DryRun(sqlfile).is_valid()

        sql_file_valid = cloud_function_dryrun
    else:
        if not is_authenticated():
            click.echo("Not authenticated to GCP. Run `gcloud auth login` to login.")
            sys.exit(1)

        client = bigquery.Client()

        def gcp_dryrun(sqlfile):
            """Dry run the SQL file."""
            dataset = Path(sqlfile).parent.parent.name
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
                default_dataset=f"{project}.{dataset}",
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "submission_date", "DATE", "2019-01-01"
                    )
                ],
            )

            with open(sqlfile) as query_stream:
                query = query_stream.read()

                try:
                    client.query(query, job_config=job_config)
                    click.echo(f"{sqlfile:59} OK")
                    return True
                except Exception as e:
                    click.echo(f"{sqlfile:59} ERROR: {e}")
                    return False

        sql_file_valid = gcp_dryrun

    with ThreadPool(8) as p:
        result = p.map(sql_file_valid, sql_files, chunksize=1)
    if not all(result):
        sys.exit(1)
