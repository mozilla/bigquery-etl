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
    analytics_hub_client,
    bigquery_client,
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
