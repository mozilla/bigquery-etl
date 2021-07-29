"""bigquery-etl CLI view command."""
import click
import sys

from multiprocessing.pool import Pool

from ..view import View
from .dryrun import dryrun
from ..cli.common import (
    sql_dir_option,
    use_cloud_function_option,
    parallelism_option,
    paths_matching_name_pattern,
    project_id_option,
    respect_dryrun_skip_option,
)


@click.group(help="Commands for managing views.")
def view():
    """Create the CLI group for the view command."""
    pass



@view.command(
    help="""Validate a view.
    Checks formatting, naming, references and dry runs the view.

    Examples:

    ./bqetl view validate telemetry.clients_daily
    """
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option()
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
        name, sql_dir, project_id, files=("*view.sql")
    )
    views = [View.from_file(f) for f in view_files]

    with Pool(parallelism) as p:
        result = p.map(_view_is_valid, views, chunksize=1)
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


@view.command(help="""Publish views.
    Examples:

    # Publish all views
    ./bqetl view publish

    # Publish a specific view
    ./bqetl view validate telemetry.clients_daily
    """)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option()
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
def publish(name, sql_dir, project_id, target_project, log_level, parallelism, dry_run, user_facing_only):
    """Publish views."""
    view_files = paths_matching_name_pattern(
        name, sql_dir, project_id, files=("view.sql",)
    )
    
    views = [View.from_file(f) for f in view_files]

    with Pool(parallelism) as p:
        result = p.map(_publish_view, views, chunksize=1)
    if not all(result):
        sys.exit(1)

    click.echo("All have been published.")

def _publish_view(view):
    view.publish()
