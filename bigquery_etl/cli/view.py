"""bigquery-etl CLI view command."""
import functools
import logging
import re
import string
import sys
from multiprocessing.pool import Pool, ThreadPool

import click

from ..cli.utils import (
    parallelism_option,
    paths_matching_name_pattern,
    project_id_option,
    respect_dryrun_skip_option,
    sql_dir_option,
    use_cloud_function_option,
)
from ..metadata.parse_metadata import METADATA_FILE, Metadata
from ..view import View, broken_views
from .dryrun import dryrun

VIEW_NAME_RE = re.compile(r"(?P<dataset>[a-zA-z0-9_]+)\.(?P<name>[a-zA-z0-9_]+)")


@click.group(help="Commands for managing views.")
def view():
    """Create the CLI group for the view command."""
    pass


@view.command(
    help="""Create a new view with name
    <dataset>.<view_name>, for example: telemetry_derived.active_profiles.
    Use the `--project_id` option to change the project the view is added to;
    default is `moz-fx-data-shared-prod`.

    Examples:

    \b
    ./bqetl view create telemetry_derived.deviations \\
      --owner=example@mozilla.com
    """,
)
@click.argument("name")
@sql_dir_option
@project_id_option("moz-fx-data-shared-prod")
@click.option(
    "--owner",
    "-o",
    help="Owner of the query (email address)",
    default="example@mozilla.com",
)
def create(name, sql_dir, project_id, owner):
    """Create a new view."""
    # with dataset metadata
    try:
        match = VIEW_NAME_RE.match(name)
        name = match.group("name")
        dataset = match.group("dataset")
    except AttributeError:
        click.echo("New views must be named like: <dataset>.<view>")
        sys.exit(1)

    view = View.create(project_id, dataset, name, sql_dir)
    metadata = Metadata(
        friendly_name=string.capwords(name.replace("_", " ")),
        description="Please provide a description for the view",
        owners=[owner],
    )
    metadata.write(view.path.parent / METADATA_FILE)
    click.echo(f"Created new view {view.path}")


@view.command(
    help="""Validate a view.
    Checks formatting, naming, references and dry runs the view.

    Examples:

    ./bqetl view validate telemetry.clients_daily
    """
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option(default=None)
@use_cloud_function_option
@click.option(
    "--validate_schemas",
    "--validate-schemas",
    help="Require dry run schema to match destination table and file if present.",
    is_flag=True,
    default=False,
)
@parallelism_option
@respect_dryrun_skip_option()
@click.pass_context
def validate(
    ctx,
    name,
    sql_dir,
    project_id,
    use_cloud_function,
    validate_schemas,
    parallelism,
    respect_dryrun_skip,
):
    """Validate the view definition."""
    view_files = paths_matching_name_pattern(
        name, sql_dir, project_id, files=("view.sql",)
    )
    views = [View.from_file(f) for f in view_files]

    with Pool(parallelism) as p:
        result = p.map(_view_is_valid, views)
    if not all(result):
        sys.exit(1)

    # dryrun views
    ctx.invoke(
        dryrun,
        paths=[str(f) for f in view_files],
        use_cloud_function=use_cloud_function,
        project=project_id,
        validate_schemas=validate_schemas,
        respect_skip=respect_dryrun_skip,
    )

    click.echo("All views are valid.")


def _view_is_valid(view):
    return view.is_valid()


@view.command(
    help="""Publish views.
    Examples:

    # Publish all views
    ./bqetl view publish

    # Publish a specific view
    ./bqetl view publish telemetry.clients_daily
    """
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option(default=None)
@click.option(
    "--target-project",
    help=(
        "If specified, create views in the target project rather than"
        " the project specified in the file. Only views for "
        " moz-fx-data-shared-prod will be published if this is set."
    ),
)
@click.option("--log-level", default="INFO", help="Defaults to INFO")
@parallelism_option
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
def publish(
    name,
    sql_dir,
    project_id,
    target_project,
    log_level,
    parallelism,
    dry_run,
    user_facing_only,
):
    """Publish views."""
    # set log level
    try:
        logging.basicConfig(level=log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        click.error(f"argument --log-level: {e}")

    view_files = paths_matching_name_pattern(
        name, sql_dir, project_id, files=("view.sql",)
    )

    views = [View.from_file(f) for f in view_files]
    views = [v for v in views if not user_facing_only or v.is_user_facing]

    with ThreadPool(parallelism) as p:
        publish_view = functools.partial(_publish_view, target_project, dry_run)
        result = p.map(publish_view, views, chunksize=1)

    if not all(result):
        sys.exit(1)

    click.echo("All have been published.")


def _publish_view(target_project, dry_run, view):
    return view.publish(target_project, dry_run)


@view.command(
    help="""List broken views.
    Examples:

    # Publish all views
    ./bqetl view list-broken

    # Publish a specific view
    ./bqetl view list-broken --only telemetry
    """
)
@project_id_option()
@parallelism_option
@click.option(
    "--only",
    "-o",
    help="Process only the given tables",
)
@click.option(
    "--log-level",
    "--log_level",
    help="Log level.",
    default=logging.getLevelName(logging.INFO),
    type=str.upper,
)
def list_broken(project_id, parallelism, only, log_level):
    """List broken views."""
    broken_views.list_broken_views(project_id, parallelism, only, log_level)
