"""bigquery-etl CLI sharing command."""

import os
from fnmatch import fnmatchcase
from glob import glob
from pathlib import Path
from typing import List, Optional

import click

from bigquery_etl.config import ConfigLoader
from bigquery_etl.metadata.parse_metadata import DatasetMetadata
from bigquery_etl.sharing import (
    _sanitize_id,
    analytics_hub_client,
    bigquery_client,
    clean_sharing,
    publish_dataset_sharing,
)

from ..cli.utils import project_id_option, sql_dir_option
from ..util.common import block_coding_agents

DATASET_METADATA_FILE = "dataset_metadata.yaml"


def _dataset_metadata_files(name, sql_dir, project_id) -> List[Path]:
    """Return dataset_metadata.yaml paths matching `name`.

    `name` may be a directory, a file, or a dataset name glob (matched against
    `<dataset>` or `<project>.<dataset>`; `*` matches all).
    """
    if os.path.isfile(name):
        return [Path(name)]
    if os.path.isdir(name):
        return sorted(
            Path(p) for p in glob(f"{name}/**/{DATASET_METADATA_FILE}", recursive=True)
        )

    base = Path(sql_dir)
    if project_id:
        base = base / project_id

    matches = []
    for p in glob(f"{base}/**/{DATASET_METADATA_FILE}", recursive=True):
        path = Path(p)
        dataset = path.parent.name
        project = path.parent.parent.name
        if (
            name in ("*", "*.*")
            or fnmatchcase(dataset, name)
            or fnmatchcase(f"{project}.{dataset}", name)
        ):
            matches.append(path)
    return sorted(set(matches))


@click.group(help="""Commands for managing external data sharing.""")
@click.pass_context
def sharing(ctx):
    """Create the CLI group for the sharing command."""
    pass


@sharing.command(help="""
    Deploy BigQuery Sharing data exchanges and subscriber grants for the
    `_shared` datasets that declare an `external_sharing` block in their
    `dataset_metadata.yaml`.

    Sharing is set up on the user-facing project (mozdata) copies of these
    datasets, so the command must run after the `_shared` views have been
    published there.

    Examples:

    \b
    # Deploy sharing for all configured datasets
    ./bqetl sharing deploy '*'

    \b
    # Deploy sharing for a single dataset
    ./bqetl sharing deploy telemetry_shared
    """)
@block_coding_agents
@click.argument("name")
@project_id_option("moz-fx-data-shared-prod")
@sql_dir_option
@click.option(
    "--user-facing-project",
    "--user_facing_project",
    default=None,
    help="Project hosting the shared datasets and BigQuery Sharing exchanges. "
    "Defaults to `default.user_facing_project` in bqetl_project.yaml (mozdata). "
    "Override to deploy against a sandbox project for testing.",
)
@click.option(
    "--dry-run",
    "--dry_run",
    "--dryrun",
    is_flag=True,
    default=False,
    help="Show the changes that would be made without applying them.",
)
@click.pass_context
def deploy(
    ctx,
    name: str,
    sql_dir: Optional[str],
    project_id: Optional[str],
    user_facing_project: Optional[str],
    dry_run: bool,
) -> None:
    """Deploy external sharing configuration to BigQuery Sharing."""
    metadata_files = _dataset_metadata_files(name, sql_dir, project_id)

    if len(metadata_files) == 0:
        click.echo(f"No dataset metadata files matching {name}")
        return

    # `_shared` datasets are published to the user-facing project (mozdata); the
    # sharing exchange, listing and location all reference that copy, not the
    # shared-prod source the metadata lives under.
    if user_facing_project is None:
        user_facing_project = ConfigLoader.get(
            "default", "user_facing_project", fallback="mozdata"
        )

    client = analytics_hub_client()
    bq_client = bigquery_client(user_facing_project)
    deployed = 0
    for metadata_file in metadata_files:
        metadata = DatasetMetadata.from_file(metadata_file)

        if not metadata.external_sharing:
            continue

        dataset = metadata_file.parent.name

        publish_dataset_sharing(
            client,
            bq_client,
            metadata,
            project=user_facing_project,
            dataset=dataset,
            dry_run=dry_run,
        )
        deployed += 1

    click.echo(f"Deployed external sharing for {deployed} dataset(s).")


def _desired_listings(metadata_files):
    """Build `exchange_id -> set(listing_id)` for configured `_shared` datasets."""
    desired: dict = {}
    for metadata_file in metadata_files:
        metadata = DatasetMetadata.from_file(metadata_file)
        if not metadata.external_sharing:
            continue
        exchange_id = _sanitize_id(metadata.external_sharing.exchange)
        listing_id = _sanitize_id(metadata_file.parent.name)
        desired.setdefault(exchange_id, set()).add(listing_id)
    return desired


@sharing.command(help="""
    Delete BigQuery Sharing exchanges and listings that are no longer backed by
    an `external_sharing` block in a `_shared` dataset's `dataset_metadata.yaml`.

    Only resources created by `bqetl sharing deploy` are removed (identified by
    a managed marker in their description); manually-created exchanges and
    listings are left untouched.

    Reconciles a single `--location` against the whole repo config (exchanges
    are per-location); pass the location where the orphaned exchanges live.

    Examples:

    \b
    # Remove orphaned managed sharing resources in the US
    ./bqetl sharing clean
    """)
@block_coding_agents
@project_id_option("moz-fx-data-shared-prod")
@sql_dir_option
@click.option(
    "--user-facing-project",
    "--user_facing_project",
    default=None,
    help="Project hosting the shared datasets and BigQuery Sharing exchanges. "
    "Defaults to `default.user_facing_project` in bqetl_project.yaml (mozdata).",
)
@click.option(
    "--location",
    default="US",
    help="BigQuery Sharing location to reconcile (exchanges are per-location).",
)
@click.option(
    "--dry-run",
    "--dry_run",
    "--dryrun",
    is_flag=True,
    default=False,
    help="Show the resources that would be deleted without deleting them.",
)
@click.pass_context
def clean(
    ctx,
    sql_dir: Optional[str],
    project_id: Optional[str],
    user_facing_project: Optional[str],
    location: str,
    dry_run: bool,
) -> None:
    """Delete managed sharing resources no longer backed by config."""
    # Desired set is built from ALL configs so still-configured datasets are
    # never treated as orphaned.
    desired = _desired_listings(_dataset_metadata_files("*", sql_dir, project_id))

    if user_facing_project is None:
        user_facing_project = ConfigLoader.get(
            "default", "user_facing_project", fallback="mozdata"
        )

    client = analytics_hub_client()
    clean_sharing(
        client,
        project=user_facing_project,
        location=location,
        desired=desired,
        dry_run=dry_run,
    )
