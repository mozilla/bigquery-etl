"""bigquery-etl CLI view command."""
import logging
import re
import string
import sys
from fnmatch import fnmatchcase
from graphlib import TopologicalSorter
from multiprocessing.pool import Pool, ThreadPool
from traceback import print_exc

import click

from ..cli.utils import (
    no_dryrun_option,
    parallelism_option,
    paths_matching_name_pattern,
    project_id_option,
    respect_dryrun_skip_option,
    sql_dir_option,
    use_cloud_function_option,
)
from ..config import ConfigLoader
from ..dryrun import DryRun
from ..metadata.parse_metadata import METADATA_FILE, Metadata
from ..util.bigquery_id import sql_table_id
from ..util.client_queue import ClientQueue
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
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
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
@parallelism_option()
@respect_dryrun_skip_option()
@no_dryrun_option(default=False)
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
    no_dryrun,
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
    if not no_dryrun:
        ctx.invoke(
            dryrun,
            paths=[str(f) for f in view_files],
            use_cloud_function=use_cloud_function,
            project=project_id,
            validate_schemas=validate_schemas,
            respect_skip=respect_dryrun_skip,
        )
    else:
        click.echo("Dry run skipped for view files.")

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
@parallelism_option()
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
@click.option(
    "--skip-authorized",
    "--skip_authorized",
    is_flag=True,
    help="Don't publish views with labels: {authorized: true} in metadata.yaml",
)
@click.option(
    "--force",
    is_flag=True,
    help="Publish views even if there are no changes to the view query",
)
@click.option(
    "--add-managed-label",
    "--add_managed_label",
    is_flag=True,
    help=(
        'Add a label "managed" to views, that can be used to remove views from BigQuery'
        " when they are removed from --sql-dir."
    ),
)
@respect_dryrun_skip_option(default=False)
def publish(
    name,
    sql_dir,
    project_id,
    target_project,
    log_level,
    parallelism,
    dry_run,
    user_facing_only,
    skip_authorized,
    force,
    add_managed_label,
    respect_dryrun_skip,
):
    """Publish views."""
    # set log level
    try:
        logging.basicConfig(level=log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        raise click.ClickException(f"argument --log-level: {e}")

    views = _collect_views(name, sql_dir, project_id, user_facing_only, skip_authorized)
    if respect_dryrun_skip:
        views = [view for view in views if view.path not in DryRun.skipped_files()]
    if add_managed_label:
        for view in views:
            view.labels["managed"] = ""
    if not force:
        # only views with changes
        with ThreadPool(parallelism) as p:
            changes = p.map(lambda v: v.has_changes(target_project), views, chunksize=1)
        views = [v for v, has_changes in zip(views, changes) if has_changes]
    views_by_id = {v.view_identifier: v for v in views}

    view_id_graph = {
        view.view_identifier: {
            ref for ref in view.table_references if ref in views_by_id
        }
        for view in views
    }

    view_id_order = TopologicalSorter(view_id_graph).static_order()

    result = []
    for view_id in view_id_order:
        try:
            result.append(views_by_id[view_id].publish(target_project, dry_run))
        except Exception:
            print(f"Failed to publish view: {view_id}")
            print_exc()
            result.append(False)

    if not all(result):
        sys.exit(1)

    click.echo("All have been published.")


def _collect_views(name, sql_dir, project_id, user_facing_only, skip_authorized):
    view_files = paths_matching_name_pattern(
        name, sql_dir, project_id, files=("view.sql",)
    )

    views = [View.from_file(f) for f in view_files]
    if user_facing_only:
        views = [v for v in views if v.is_user_facing]
    if skip_authorized:
        views = [
            v
            for v in views
            if not (
                v.metadata
                and v.metadata.labels
                # labels with boolean true are translated to ""
                and v.metadata.labels.get("authorized") == ""
            )
        ]
    return views


@view.command(
    help="""Remove managed views that are not present in the sql dir.
    Examples:

    # Clean managed views in shared prod
    ./bqetl view clean --target-project=moz-fx-data-shared-prod --skip-authorized

    # Clean managed user facing views in mozdata
    ./bqetl view clean --target-project=mozdata --user-facing-only --skip-authorized
    """
)
@click.argument("name", required=False)
@sql_dir_option
@project_id_option(default=None)
@click.option(
    "--target-project",
    help=(
        "If specified, clean views in the target project rather than"
        " the project specified in the file. Only views for "
        " moz-fx-data-shared-prod will be included if this is set."
    ),
)
@click.option("--log-level", default="INFO", help="Defaults to INFO")
@parallelism_option()
@click.option(
    "--dry_run",
    "--dry-run",
    is_flag=True,
    help="Identify views to delete, but do not delete them.",
)
@click.option(
    "--user-facing-only",
    "--user_facing_only",
    is_flag=True,
    help=(
        "Remove user-facing views only. User-facing views are views"
        " part of datasets without suffixes (such as telemetry,"
        " but not telemetry_derived)."
    ),
)
@click.option(
    "--skip-authorized",
    "--skip_authorized",
    is_flag=True,
    help="Don't publish views with labels: {authorized: true} in metadata.yaml",
)
def clean(
    name,
    sql_dir,
    project_id,
    target_project,
    log_level,
    parallelism,
    dry_run,
    user_facing_only,
    skip_authorized,
):
    """Clean managed views."""
    # set log level
    try:
        logging.basicConfig(level=log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        raise click.ClickException(f"argument --log-level: {e}")

    if project_id is None and target_project is None:
        raise click.ClickException("command requires --project-id or --target-project")

    expected_view_ids = {
        view.target_view_identifier(target_project)
        for view in _collect_views(
            name, sql_dir, project_id, user_facing_only, skip_authorized
        )
    }

    client_q = ClientQueue([project_id], parallelism)
    with client_q.client() as client:
        datasets = [
            dataset
            for dataset in client.list_datasets(target_project)
            if not user_facing_only
            or not dataset.dataset_id.endswith(
                tuple(
                    ConfigLoader.get(
                        "default", "non_user_facing_dataset_suffixes", fallback=[]
                    )
                )
            )
        ]
    with ThreadPool(parallelism) as p:
        managed_view_ids = {
            sql_table_id(view)
            for views in p.starmap(
                client_q.with_client,
                ((_list_managed_views, dataset, name) for dataset in datasets),
                chunksize=1,
            )
            for view in views
            if not skip_authorized or "authorized" not in view.labels
        }

        remove_view_ids = sorted(managed_view_ids - expected_view_ids)
        p.starmap(
            client_q.with_client,
            ((_remove_view, view_id, dry_run) for view_id in remove_view_ids),
            chunksize=1,
        )


def _list_managed_views(client, dataset, pattern):
    return [
        table
        for table in client.list_tables(dataset)
        if table.table_type == "VIEW"
        and "managed" in table.labels
        and (pattern is None or fnmatchcase(sql_table_id(table), f"*{pattern}"))
    ]


def _remove_view(client, view_id, dry_run):
    if dry_run:
        click.echo(f"Would delete {view_id}")
    else:
        click.echo(f"Deleting {view_id}")
        client.delete_table(view_id)


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
@parallelism_option()
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
