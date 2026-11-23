"""bigquery-etl CLI metadata command."""

import csv
import io
import json
import os
import re
import sys
from datetime import date, datetime
from fnmatch import fnmatchcase
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path
from typing import cast

import click
import requests
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

from bigquery_etl.datahub.auth import LoginError
from bigquery_etl.datahub.lineage import (
    DATAHUB_LINEAGE_DEGREES,
    DEFAULT_LOOKER_TMP_DAYS,
    ENTITY_TYPES,
    AuthError,
    DataHubClient,
    LineageTooLargeError,
    format_text,
)
from bigquery_etl.datahub.lineage import (
    get_downstream_lineage as get_downstream_lineage_from_datahub,
)
from bigquery_etl.deprecation import (
    DeprecatedArtifact,
    IncludeDownstream,
    delete_artifact,
    downstream_candidates,
    find_references,
    pick,
    preselect,
    selection_summary,
    tracked_files,
)
from bigquery_etl.metadata.parse_metadata import (
    DATASET_METADATA_FILE,
    DatasetMetadata,
    Metadata,
    WorkgroupAccessMetadata,
)
from bigquery_etl.metadata.publish_metadata import publish_metadata
from bigquery_etl.metadata.validate_metadata import (
    MetadataValidationError,
    validate_default_table_workgroup_access,
)
from bigquery_etl.metadata.validate_metadata import (
    validate_deprecation as validate_deprecation_metadata,
)
from bigquery_etl.metadata.validate_metadata import (
    validate_directories,
    validate_workgroup_access,
)

from ..cli.utils import (
    flush_stdin,
    parallelism_option,
    paths_matching_name_pattern,
    project_id_option,
    sql_dir_option,
)
from ..config import ConfigLoader
from ..dryrun import get_credentials
from ..util import extract_from_query_path
from ..util.common import block_coding_agents


@click.group(help="""
    Commands for managing bqetl metadata.
    """)
@click.pass_context
def metadata(ctx):
    """Create the CLI group for the metadata command."""


@metadata.command(
    help="""
    Update metadata yaml files.
    Updates workgroup access metadata based on the dataset_metadata.yaml and
    deprecation metadata.

    Example:
     ./bqetl metadata update ga_derived.downloads_with_attribution_v2
    """,
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    },
)
@click.argument("name")
@project_id_option()
@sql_dir_option
@parallelism_option()
def update(
    name: str, sql_dir: str | None, project_id: str | None, parallelism: int
) -> None:
    """Update metadata yaml file."""
    table_metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    retained_dataset_roles = ConfigLoader.get(
        "deprecation", "retain_dataset_roles", fallback=[]
    )

    # group table metadata files by dataset
    datasets: dict[Path, list[Path]] = {}
    for table_metadata_file in table_metadata_files:
        dataset_path = Path(table_metadata_file).parent.parent
        if dataset_path not in datasets:
            datasets[dataset_path] = []
        datasets[dataset_path].append(table_metadata_file)

    # process each dataset in parallel
    if parallelism > 1:
        with Pool(parallelism) as pool:
            pool.map(
                partial(_update_dataset_metadata, retained_dataset_roles),
                datasets.items(),
            )
    else:
        for dataset_path, dataset_table_files in datasets.items():
            _update_dataset_metadata(
                retained_dataset_roles, (dataset_path, dataset_table_files)
            )


def _update_dataset_metadata(retained_dataset_roles, dataset_info):
    """Update all metadata files for a single dataset."""
    dataset_path, table_metadata_files = dataset_info

    # process all table metadata files in this dataset
    for table_metadata_file in table_metadata_files:
        try:
            dataset_metadata_path = (
                Path(table_metadata_file).parent.parent / "dataset_metadata.yaml"
            )
            if not dataset_metadata_path.exists():
                continue

            dataset_metadata = DatasetMetadata.from_file(dataset_metadata_path)
            table_metadata = Metadata.from_file(table_metadata_file)

            dataset_metadata_updated = False
            table_metadata_updated = False

            # set dataset metadata default_table_workgroup_access to table_workgroup_access if not set
            if not dataset_metadata.default_table_workgroup_access:
                dataset_metadata.default_table_workgroup_access = (
                    dataset_metadata.workgroup_access
                )
                dataset_metadata_updated = True

            if table_metadata.deprecated:
                # filter table workgroup_access to only retained roles
                if table_metadata.workgroup_access is not None:
                    table_metadata.workgroup_access = [
                        workgroup
                        for workgroup in table_metadata.workgroup_access
                        if workgroup.role in retained_dataset_roles
                    ]
                else:
                    table_metadata.workgroup_access = []
                table_metadata_updated = True

                # filter dataset workgroup_access to only retained roles
                dataset_metadata.workgroup_access = [
                    workgroup
                    for workgroup in dataset_metadata.workgroup_access
                    if workgroup.get("role") in retained_dataset_roles
                ]
                dataset_metadata_updated = True

                # if dataViewer role exists in default_table_workgroup_access, ensure metadataViewer is present in workgroup_access
                # https://mozilla-hub.atlassian.net/browse/DENG-8843
                data_viewer = next(
                    (
                        workgroup
                        for workgroup in dataset_metadata.default_table_workgroup_access
                        if workgroup.get("role") == "roles/bigquery.dataViewer"
                    ),
                    None,
                )
                if data_viewer:
                    has_metadata_viewer = any(
                        workgroup.get("role") == "roles/bigquery.metadataViewer"
                        for workgroup in dataset_metadata.workgroup_access
                    )
                    if not has_metadata_viewer:
                        dataset_metadata.workgroup_access.append(
                            {
                                "role": "roles/bigquery.metadataViewer",
                                "members": sorted(data_viewer.get("members", [])),
                            }
                        )
            else:
                if table_metadata.workgroup_access is None:
                    table_metadata.workgroup_access = []

                for (
                    default_workgroup_access
                ) in dataset_metadata.default_table_workgroup_access:
                    role_exists = False
                    for i, table_workgroup_access in enumerate(
                        table_metadata.workgroup_access
                    ):
                        if table_workgroup_access.role == default_workgroup_access.get(
                            "role"
                        ):
                            role_exists = True
                            table_metadata.workgroup_access[i].members = sorted(
                                set(table_workgroup_access.members)
                                | set(default_workgroup_access.get("members", []))
                            )
                            table_metadata_updated = True

                    if not role_exists:
                        table_metadata.workgroup_access.append(
                            WorkgroupAccessMetadata(
                                role=default_workgroup_access["role"],
                                members=sorted(
                                    default_workgroup_access.get("members", [])
                                ),
                            )
                        )
                        table_metadata_updated = True

            if dataset_metadata_updated:
                dataset_metadata.write(dataset_metadata_path)
                click.echo(f"Updated {dataset_metadata_path}")
            if table_metadata_updated:
                table_metadata.write(table_metadata_file)
                click.echo(f"Updated {table_metadata_file}")
        except Exception as e:
            click.echo(f"Error processing {table_metadata_file}: {e}", err=True)
            raise e


@metadata.command(
    help="""
    Publish all metadata based on metadata.yaml file.

    Coding agents may only run this against an allow-listed dev `--target` while impersonating a sandbox service account.

    Example:
     ./bqetl metadata publish ga_derived.downloads_with_attribution_v2
    """,
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    },
)
@block_coding_agents
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
@parallelism_option()
@click.option(
    "--skip-stable-datasets",
    default=True,
    type=bool,
    help="Skip metadata publishing for tables in *_stable datasets which are managed by infra deploys.",
)
@click.option(
    "--add-managed-label",
    "--add_managed_label",
    is_flag=True,
    default=False,
    help='Add a label "managed" to published tables for lifecycle management',
)
def publish(
    name: str,
    sql_dir: str | None,
    project_id: str | None,
    parallelism: int,
    skip_stable_datasets: bool,
    add_managed_label: bool,
) -> None:
    """Publish Bigquery metadata."""
    table_metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    # https://mozilla-hub.atlassian.net/browse/SVCSE-4108
    if skip_stable_datasets:
        table_metadata_files = [
            metadata_file
            for metadata_file in table_metadata_files
            if re.fullmatch(
                r".+/moz-fx-data-shared-prod/[a-z0-9_]+_stable/[a-z0-9_]+/metadata.yaml$",
                str(metadata_file),
                flags=re.IGNORECASE,
            )
            is None
        ]

    if parallelism > 1:
        credentials = get_credentials()

        with Pool(parallelism) as pool:
            pool.map(
                partial(
                    _publish_metadata,
                    project_id,
                    credentials,
                    add_managed_label=add_managed_label,
                ),
                table_metadata_files,
            )
    else:
        for metadata_file in table_metadata_files:
            _publish_metadata(
                project_id,
                credentials=None,
                metadata_file=metadata_file,
                add_managed_label=add_managed_label,
            )


def _publish_metadata(project_id, credentials, metadata_file, add_managed_label=False):
    project, dataset, table = extract_from_query_path(metadata_file)
    try:
        metadata = Metadata.from_file(metadata_file)
        publish_metadata(
            bigquery.Client(project=project_id, credentials=credentials),
            project,
            dataset,
            table,
            metadata,
            add_managed_label=add_managed_label,
        )
    except FileNotFoundError:
        print(f"No metadata file for: {project}.{dataset}.{table}")


@metadata.command(help="""
    Deprecate BigQuery table by updating metadata.yaml file.
    Deletion date defaults to 3 months from today; TUI asks to confirm.

    TUI offers to show downstream lineage from DataHub and pick
    downstream tables and views to deprecate. --include-downstream fetches
    the lineage without asking and pre-selects all/unused/none downstream assets.

    Examples:

    \b
     ./bqetl metadata deprecate firefox_desktop_derived.desktop_retention_aggregates_v1 --deletion_date=2027-03-02
     ./bqetl metadata deprecate firefox_desktop_derived.desktop_retention_aggregates_v1 --include-downstream=unused
    """)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
@click.option(
    "--deletion_date",
    "--deletion-date",
    help="Date when table is scheduled for deletion. Date format: yyyy-mm-dd",
    type=click.DateTime(formats=["%Y-%m-%d"]),
)
@click.option(
    "--include_downstream",
    "--include-downstream",
    type=click.Choice([mode.value for mode in IncludeDownstream]),
    help="Fetch downstream lineage from DataHub without asking and pre-select "
    "downstream assets to deprecate: all, unused (older than 30 days, no queries "
    "in the last 30) or none.",
)
@click.option(
    "--since_days",
    "--since-days",
    type=click.IntRange(min=1),
    help="Only follow lineage DataHub has seen in the last N days. Default: all time.",
)
def deprecate(
    name: str,
    sql_dir: str,
    project_id: str,
    deletion_date: datetime | None,
    include_downstream: str | None,
    since_days: int | None,
) -> None:
    """Deprecate Bigquery table by updating metadata yaml file(s)."""
    table_metadata_files = _matching_metadata_files(name, sql_dir, project_id)
    if not table_metadata_files:
        raise click.ClickException(_no_match_message(name))

    interactive = _is_interactive()
    prompted = False
    if (
        include_downstream is None
        and interactive
        and click.confirm("Show downstream lineage?", default=True)
    ):
        include_downstream = IncludeDownstream.NONE
        prompted = True
    if since_days and not include_downstream:
        click.echo(
            "Warning: --since-days is ignored without downstream lineage.", err=True
        )

    downstream: list[Path] = []
    assets = None
    if include_downstream:
        tables = [".".join(extract_from_query_path(f)) for f in table_metadata_files]
        targets = tables[0] if len(tables) == 1 else f"{len(tables)} tables"
        try:
            assets = _fetch_downstream(
                tables, max_degree_hint=False, since_days=since_days, include_usage=True
            )
        except click.ClickException as e:
            # Lineage wasn't requested by flag, so offer to continue without it.
            if not prompted:
                raise
            click.echo(f"Warning: {e.message}", err=True)
            # Drop keys pressed while waiting so they don't answer the prompt.
            flush_stdin()
            if not click.confirm(f"Deprecate {targets} without downstream?"):
                click.echo("Cancelled, nothing was changed.")
                return
        if assets is not None:
            candidates, external = downstream_candidates(
                assets, sql_dir, exclude={Path(f).parent for f in table_metadata_files}
            )
            mode = IncludeDownstream(include_downstream)
            preselected = preselect(candidates, mode)
            options = []
            if since_days:
                options.append(f"lineage seen in the last {since_days} days")
            if not prompted:
                options.append(f"pre-selected: {mode}")
            note = f" ({'; '.join(options)})" if options else ""
            if interactive:
                flush_stdin()
                picked = pick(targets, candidates, external, preselected, note)
                if picked is None:
                    click.echo("Cancelled, nothing was changed.")
                    return
                downstream = picked
            else:
                click.echo(format_text(targets + note, assets, show_usage=True))
                click.echo(selection_summary(candidates, preselected, mode))
                downstream = sorted(preselected)

    if deletion_date is None:
        deletion_date = datetime.today() + relativedelta(months=+3)
        if interactive:
            deletion_date = cast(
                datetime,
                click.prompt(
                    "Deletion date (default is 3 months from today)",
                    default=f"{deletion_date:%Y-%m-%d}",
                    type=click.DateTime(formats=["%Y-%m-%d"]),
                ),
            )

    for metadata_file in table_metadata_files + [
        p / "metadata.yaml" for p in downstream
    ]:
        metadata = Metadata.from_file(metadata_file)

        metadata.deprecated = True
        metadata.deletion_date = deletion_date.date()

        metadata.write(metadata_file)
        click.echo(f"Updated {metadata_file} with deprecation.")


@metadata.command(help="""
    Delete the directories of deprecated tables or views, and their tests/sql
    directories. BigQuery objects are not deleted.

    NAME must match the whole dataset.table, and may use wildcards. Tables whose
    deletion_date hasn't passed need --force. If any match isn't deprecated, isn't
    tracked by git, or has invalid metadata, nothing is deleted, even with --force.

    Examples:

    \b
     ./bqetl metadata delete firefox_desktop_derived.desktop_retention_aggregates_v1
     ./bqetl metadata delete firefox_desktop_derived.desktop_retention_aggregates_v1 --force
    """)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Also delete deprecated tables whose deletion_date hasn't passed.",
)
def delete(name: str, sql_dir: str, project_id: str, force: bool) -> None:
    """Delete the files of deprecated tables or views."""
    metadata_files = _matching_metadata_files(name, sql_dir, project_id)
    if not metadata_files:
        raise click.ClickException(_no_match_message(name))

    today = date.today()
    artifacts, problems = [], []
    tracked: dict[Path, set[Path] | None] = {}  # tracked metadata files by sql dir
    for metadata_file in metadata_files:
        try:
            metadata = Metadata.from_file(metadata_file)
            artifact = DeprecatedArtifact.from_metadata(metadata_file, metadata)
        except Exception as e:
            problems.append(f"{metadata_file} is invalid: {e}")
            continue
        sql_root = artifact.path.parents[2]
        if sql_root not in tracked:
            tracked[sql_root] = tracked_files(str(sql_root))
        tracked_in_root = tracked[sql_root]
        if not metadata.deprecated:
            problems.append(f"{artifact.id} is not deprecated")
        elif tracked_in_root is None:
            problems.append(f"{artifact.id} could not be checked with git")
        elif Path(metadata_file).resolve() not in tracked_in_root:
            # git can't restore it, and generated SQL comes back on the next generate
            problems.append(f"{artifact.id} isn't tracked by git (generated SQL?)")
        elif not force and not artifact.is_past_due(today):
            when = (
                f"has deletion_date {artifact.deletion_date}"
                if artifact.deletion_date
                else "has no deletion_date"
            )
            problems.append(f"{artifact.id} {when}, use --force to delete it anyway")
        else:
            artifacts.append(artifact)
    if problems:
        message = ["Nothing was deleted.", "Can't delete:"]
        message += [f"  {problem}" for problem in problems]
        if artifacts:
            message += ["Can delete:"] + [f"  {a.id}" for a in artifacts]
        raise click.ClickException("\n".join(message))

    for i, artifact in enumerate(artifacts):
        try:
            deleted = delete_artifact(artifact)
        except OSError as e:
            message = [
                f"Failed while deleting {artifact.id}, so some of its files may be "
                f"gone: {e}"
            ]
            if remaining := artifacts[i + 1 :]:
                message += ["Not attempted:"] + [f"  {a.id}" for a in remaining]
            message.append(
                "Run `git status` to see what was deleted, "
                "and `git restore <path>` to bring files back."
            )
            raise click.ClickException("\n".join(message))
        for path in deleted:
            click.echo(f"Deleted {path}")

    references = find_references(artifacts)
    if references is None:
        click.secho(
            "Warning: could not search git for references.", fg="yellow", err=True
        )
        return
    for artifact_id, files in references.items():
        if files:
            click.secho(
                f"Warning: files still reference {artifact_id}:",
                fg="yellow",
                bold=True,
                err=True,
            )
            for f in files:
                click.echo(f"  {f}", err=True)


def _matching_metadata_files(name: str, sql_dir: str, project_id: str) -> list[Path]:
    """Return the metadata.yaml files of tables whose whole name matches name.

    name can also be a table directory, or a file in one such as query.sql.
    """
    if os.path.isfile(name):
        name = str(Path(name).parent)
    files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )
    if not os.path.exists(name):
        # paths_matching_name_pattern matches suffixes, so d.t_v1 would match xd.t_v1
        files = [
            f
            for f in files
            if fnmatchcase(".".join(extract_from_query_path(f)), name)
            or fnmatchcase(".".join(extract_from_query_path(f)), f"{project_id}.{name}")
        ]
    return sorted(set(files))


def _no_match_message(name: str) -> str:
    """Return the error for a name that matched no tables."""
    message = f"No metadata file(s) were found for: {name}"
    if "." not in name and not os.path.exists(name):
        message += f". Try including the dataset, e.g. <dataset>.{name}"
    return message


def _is_interactive() -> bool:
    """Return True when prompts can be shown."""
    return sys.stdin.isatty() and sys.stdout.isatty()


def _fetch_downstream(
    tables: list[str], max_degree_hint: bool = True, **kwargs
) -> list[dict]:
    """Return the downstream lineage of tables; failures become click errors."""
    try:
        return get_downstream_lineage_from_datahub(DataHubClient(), tables, **kwargs)
    except LoginError as e:
        raise click.ClickException(str(e))
    except AuthError as e:
        raise click.ClickException(
            f"DataHub rejected the token ({e}). If DATAHUB_GMS_TOKEN is set, check "
            "it is valid and not expired."
        )
    except LineageTooLargeError:
        raise click.ClickException(
            "Lineage is too large for one DataHub query (maxRelations limit). "
            "Re-run with --since-days (e.g. --since-days=90) to only follow recent "
            "lineage"
            + (
                ", or --max-degree=1 or 2 to only list the nearest assets."
                if max_degree_hint
                else "."
            )
        )
    except (requests.RequestException, RuntimeError) as e:
        raise click.ClickException(f"Could not get lineage from DataHub: {e}")


@metadata.command(help="""
    Validate table and dataset metadata files without dry running or formatting queries.

    With no arguments, validates the metadata of every table in all projects under
    the sql directory, plus every dataset_metadata.yaml (including datasets that
    have no tables). Pass a name pattern and/or --project-id to narrow the scope.
    Files listed under metadata.validation.skip in bqetl_project.yaml are skipped.

    Example:

    \b
     ./bqetl metadata validate
     ./bqetl metadata validate telemetry_derived.clients_daily_v6
     ./bqetl metadata validate "telemetry_derived.*"
     ./bqetl metadata validate --project-id moz-fx-data-shared-prod
    """)
@click.argument("name", required=False)
@project_id_option()
@sql_dir_option
def validate(name: str | None, sql_dir: str, project_id: str | None):
    """Validate table and dataset metadata files."""
    # relative paths are needed to match `metadata.validation.skip` entries
    sql_dir = os.path.relpath(sql_dir)
    metadata_files = paths_matching_name_pattern(
        name or "*.*", sql_dir, project_id=project_id, files=["metadata.yaml"]
    )
    if name and not metadata_files:
        raise click.ClickException(f"No metadata files found matching: {name}")

    table_dirs = sorted({Path(f).parent for f in metadata_files})
    dataset_dirs = {d.parent for d in table_dirs}

    # without a name, also cover datasets that have no tables
    if name is None:
        project_dirs = (
            [Path(sql_dir) / project_id]
            if project_id
            else [p for p in Path(sql_dir).iterdir() if p.is_dir()]
        )
        dataset_dirs.update(
            d
            for project_dir in project_dirs
            if project_dir.is_dir()
            for d in project_dir.iterdir()
            if d.is_dir() and (d / DATASET_METADATA_FILE).exists()
        )

    errors = validate_directories(table_dirs, sorted(dataset_dirs))
    if errors:
        click.echo(
            click.style(f"Metadata validation failed ({len(errors)}):", fg="red")
        )
        click.echo("\n".join(errors))
        raise MetadataValidationError(f"Metadata validation failed for: {errors}")

    click.echo(f"Validated metadata for {len(table_dirs)} tables.")


@metadata.command(help="""
    Validate that deprecation metadata is valid.
    Used by CI to ensure that manual metadata edits have all required info.

    Example:
     ./bqetl metadata validate-deprecation path/to/metadata.yaml path/to/other/metadata.yaml
    """)
@click.argument("metadata_files", nargs=-1)
def validate_deprecation(metadata_files: list[str]):
    """Validate that deprecation metadata is valid."""
    failed_files = set()
    if not metadata_files:
        skip_validation = ConfigLoader.get(
            "metadata", "validation", "skip", fallback=[]
        )
        metadata_files = [
            str(file)
            for file in paths_matching_name_pattern(
                None, "sql", project_id=None, files=["metadata.yaml"]
            )
            if str(file) not in skip_validation
        ]

    for file in metadata_files:
        if Metadata.is_metadata_file(file):
            metadata = Metadata.from_file(file)
            if not validate_deprecation_metadata(metadata, file):
                failed_files.add(file)

    if len(failed_files) > 0:
        click.echo(click.style("Deprecation metadata invalid for:", fg="red"))
        for file in sorted(failed_files):
            click.echo(click.style(str(file), fg="red"))
        raise MetadataValidationError(
            f"Deprecation metadata invalid for: {failed_files}."
        )


@metadata.command(help="""
    Validate workgroup_access and default_table_workgroup_access configurations.

    Example:
     ./bqetl metadata validate-workgroups ga_derived.downloads_with_attribution_v2
    """)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
def validate_workgroups(
    name: str,
    sql_dir: str,
    project_id: str,
):
    """Validate workgroup_access and default_table_workgroup_access configuration."""
    failed_files = set()

    table_metadata_files = list(
        set(
            paths_matching_name_pattern(
                name, sql_dir, project_id=project_id, files=["metadata.yaml"]
            )
        )
    )
    skip_validation = ConfigLoader.get("metadata", "validation", "skip", fallback=[])

    for file in table_metadata_files:
        if str(file) not in skip_validation:
            if Metadata.is_metadata_file(file):
                metadata = Metadata.from_file(file)
                if not validate_workgroup_access(metadata, file):
                    failed_files.add(file)

                if not validate_default_table_workgroup_access(file):
                    failed_files.add(file)

    if len(failed_files) > 0:
        click.echo(click.style("Metadata workgroup validation failed for:", fg="red"))
        for file in failed_files:
            click.echo(click.style(str(file), fg="red"))
        raise MetadataValidationError(
            f"Metadata workgroup validation failed for: {failed_files}"
        )


@metadata.command(help="""
    Get the downstream lineage of a BigQuery table from DataHub.

    Example:
     ./bqetl metadata get-downstream-lineage telemetry_derived.clients_last_seen_v2 --since-days=90
    """)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@click.option(
    "--since_days",
    "--since-days",
    type=click.IntRange(min=1),
    help="Only follow lineage DataHub has seen in the last N days. Default: all time.",
)
@click.option(
    "--max_degree",
    "--max-degree",
    type=click.Choice(DATAHUB_LINEAGE_DEGREES),
    default=DATAHUB_LINEAGE_DEGREES[-1],
    show_default=True,
    help="Only list assets up to this many hops away. 3+ lists all of them.",
)
@click.option(
    "--type",
    "types",
    multiple=True,
    type=click.Choice(ENTITY_TYPES, case_sensitive=False),
    help="Only list this entity type. Repeatable.",
)
@click.option(
    "--include_usage",
    "--include-usage",
    is_flag=True,
    help="Add query count, user count and last query date (last 30 days) for "
    "BigQuery tables and views.",
)
@click.option(
    "--include_deleted",
    "--include-deleted",
    is_flag=True,
    help="Also list soft-deleted assets.",
)
@click.option(
    "--looker_tmp_days",
    "--looker-tmp-days",
    type=click.IntRange(min=0),
    default=DEFAULT_LOOKER_TMP_DAYS,
    show_default=True,
    help="Drop Looker aggregate table copies (looker_tmp.LR_*) built more than "
    "this many days ago. 0 keeps all.",
)
@click.option(
    "--output_format",
    "--output-format",
    type=click.Choice(["text", "json", "csv"]),
    default="text",
    show_default=True,
    help="Output format.",
)
def get_downstream_lineage(
    name: str,
    project_id: str,
    since_days: int | None,
    max_degree: str,
    types: tuple[str, ...],
    include_deleted: bool,
    include_usage: bool,
    looker_tmp_days: int,
    output_format: str,
) -> None:
    """Get the downstream lineage of a BigQuery table from DataHub."""
    parts = name.split(".")
    if len(parts) == 2:
        table = f"{project_id}.{name}"
    elif len(parts) == 3:
        table = name
    else:
        raise click.BadParameter(
            "Expected dataset.table or project.dataset.table", param_hint="NAME"
        )

    assets = _fetch_downstream(
        [table],
        since_days=since_days,
        include_deleted=include_deleted,
        include_usage=include_usage,
        types=set(types),
        looker_tmp_days=looker_tmp_days,
        max_degree=DATAHUB_LINEAGE_DEGREES.index(max_degree) + 1,
    )

    fields = [
        "degree",
        "type",
        "subtype",
        "platform",
        "name",
        "qualified_name",
        "urn",
    ]
    if include_usage:
        fields += ["queries_30d", "users_30d", "last_query"]
    if include_deleted:
        fields.append("deleted")
    assets = [{f: a[f] for f in fields} for a in assets]

    if output_format == "json":
        click.echo(
            json.dumps(
                {
                    "table": table,
                    "since_days": since_days,
                    "count": len(assets),
                    "downstream": assets,
                },
                indent=2,
            )
        )
    elif output_format == "csv":
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader()
        writer.writerows(assets)
        click.echo(out.getvalue(), nl=False)
    else:
        click.echo(
            format_text(
                table, assets, show_deleted=include_deleted, show_usage=include_usage
            )
        )

    if since_days:
        click.echo(f"Note: only lineage seen in the last {since_days} days.", err=True)
