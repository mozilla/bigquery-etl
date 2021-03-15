"""Find view definition files and execute them."""

import logging
import os
import sys
import time
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path
from types import SimpleNamespace

import click
import sqlparse
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

VIEWS_TO_SKIP = (
    # Access Denied
    "activity_stream/tile_id_types/view.sql",
    "pocket/pocket_reach_mau/view.sql",
    "telemetry/buildhub2/view.sql",
    # Dataset glam-fenix-dev:glam_etl was not found
    # TODO: this should be removed if views are to be automatically deployed
    *[str(path) for path in Path("sql/glam-fenix-dev").glob("glam_etl/**/view.sql")],
)

# suffixes of datasets with non-user-facing views
NON_USER_FACING_DATASET_SUFFIXES = (
    "_derived",
    "_external",
    "_bi",
    "_restricted",
)


def _process_file(client, args, filepath):
    if any(filepath.endswith(p) for p in VIEWS_TO_SKIP):
        print(f"Skipping authorized view definition {filepath}")
        return True
    with open(filepath) as f:
        sql = f.read()
    parsed = sqlparse.parse(sql)[0]
    tokens = [
        t
        for t in parsed.tokens
        if not (t.is_whitespace or isinstance(t, sqlparse.sql.Comment))
    ]
    is_view_statement = (
        " ".join(tokens[0].normalized.split()) == "CREATE OR REPLACE"
        and tokens[1].normalized == "VIEW"
    )
    if is_view_statement:
        target_view_orig = str(tokens[2]).strip().split()[0]
        target_view = target_view_orig
        if args.target_project:
            # target_view must be a fully-qualified BigQuery Standard SQL table
            # identifier, which is of the form f"{project_id}.{dataset_id}.{table_id}".
            # dataset_id and table_id may not contain "." or "`". Each component may be
            # a backtick (`) quoted identifier, or the whole thing may be a backtick
            # quoted identifier, but not both.
            # Project IDs must contain 6-63 lowercase letters, digits, or dashes. Some
            # project IDs also include domain name separated by a colon. IDs must start
            # with a letter and may not end with a dash. For more information see also
            # https://github.com/mozilla/bigquery-etl/pull/1427#issuecomment-707376291
            project_id = target_view_orig.replace("`", "").rsplit(".", 2)[0]
            # Only views for moz-fx-data-shared-prod will get published to other
            # projects, for example to moz-fx-data-derived-datasets
            if project_id != "moz-fx-data-shared-prod":
                print(f"Skipping {filepath} because --target-project is set")
                return True
            target_view = target_view_orig.replace(project_id, args.target_project, 1)
            # We only change the first occurrence, which is in the target view name.
            sql = sql.replace(project_id, args.target_project, 1)
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False, dry_run=args.dry_run)
        query_job = client.query(sql, job_config)
        if args.dry_run:
            print(f"Validated definition of {target_view} in {filepath}")
        else:
            try:
                query_job.result()
            except BadRequest as e:
                if "Invalid snapshot time" in e.message:
                    # This occasionally happens due to dependent views being
                    # published concurrently; we wait briefly and give it one
                    # extra try in this situation.
                    time.sleep(1)
                    client.query(sql, job_config).result()
                else:
                    raise
            print(f"Published view {target_view}")
    else:
        print(
            f"ERROR: {filepath} does not appear to be "
            "a CREATE OR REPLACE VIEW statement! Quitting..."
        )
        return False
    return True


@click.command()
@click.argument(
    "target",
    nargs=-1,
    required=True,
)
@click.option(
    "--target-project",
    help=(
        "If specified, create views in the target project rather than"
        " the project specified in the file. Only views for "
        " moz-fx-data-shared-prod will be published if this is set."
    ),
)
@click.option("--log-level", default="INFO", help="Defaults to INFO")
@click.option(
    "-p",
    "--parallelism",
    default=8,
    type=int,
    help="Number of views to process in parallel",
)
@click.option(
    "--dry_run",
    "--dry-run",
    is_flag=True,
    help="Validate view definitions, but do not publish them.",
)
@click.option(
    "--user-facing-only",
    "--user_facing_only",
    is_flag=True,
    help=(
        "Publish user-facing views only. User-facing views are views"
        " part of datasets without suffixes (such as telemetry,"
        " but not telemetry_derived)."
    ),
)
def main(**kwargs):
    """Find view definition files in TARGET and execute them."""
    args = SimpleNamespace(**kwargs)
    client = bigquery.Client()

    # set log level
    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        click.error(f"argument --log-level: {e}")

    worker_entrypoint = partial(_process_file, client, args)
    sql_files = []

    for target in args.target:
        if os.path.isdir(target):
            for root, dirs, files in os.walk(target):
                dataset_dir = os.path.dirname(root)
                if not args.user_facing_only or not dataset_dir.endswith(
                    NON_USER_FACING_DATASET_SUFFIXES
                ):
                    if "view.sql" in files:
                        sql_files.append(os.path.join(root, "view.sql"))
        else:
            sql_files.append(target)

    with ThreadPool(args.parallelism) as p:
        result = p.map(worker_entrypoint, sql_files, chunksize=1)
    if all(result):
        exitcode = 0
    else:
        exitcode = 1
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
