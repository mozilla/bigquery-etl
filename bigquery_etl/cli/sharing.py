"""bigquery-etl CLI sharing command."""

from typing import Optional

import click

from bigquery_etl.config import ConfigLoader
from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.sharing import (
    analytics_hub_client,
    bigquery_client,
    publish_table_sharing,
)

from ..cli.utils import paths_matching_name_pattern, project_id_option, sql_dir_option
from ..util import extract_from_query_path
from ..util.common import block_coding_agents


@click.group(help="""Commands for managing external data sharing.""")
@click.pass_context
def sharing(ctx):
    """Create the CLI group for the sharing command."""
    pass


@sharing.command(help="""
    Deploy BigQuery Sharing data exchanges and subscriber grants for the
    `_shared` views that declare an `external_sharing` block in their metadata.

    Sharing is set up on the user-facing project (mozdata) copies of these
    views, so the command must run after the `_shared` views have been
    published there.

    The command is idempotent: exchanges, listings and subscriber grants are
    reconciled, so it is safe to run on a schedule (e.g. from Airflow).

    Examples:

    \b
    # Deploy sharing for all configured views
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
    dry_run: bool,
) -> None:
    """Deploy external sharing configuration to BigQuery Sharing."""
    metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    if len(metadata_files) == 0:
        click.echo(f"No metadata files matching {name}")
        return

    # `_shared` views are published to the user-facing project (mozdata); the
    # sharing exchange, listing and location all reference that copy, not the
    # shared-prod source the metadata lives under.
    user_facing_project = ConfigLoader.get(
        "default", "user_facing_project", fallback="mozdata"
    )

    client = analytics_hub_client()
    bq_client = bigquery_client(user_facing_project)
    deployed = 0
    for metadata_file in sorted(set(metadata_files)):
        try:
            metadata = Metadata.from_file(metadata_file)
        except FileNotFoundError:
            continue

        if not metadata.external_sharing:
            continue

        _, dataset, table = extract_from_query_path(metadata_file)

        publish_table_sharing(
            client,
            bq_client,
            metadata,
            project=user_facing_project,
            dataset=dataset,
            table=table,
            dry_run=dry_run,
        )
        deployed += 1

    click.echo(f"Deployed external sharing for {deployed} table(s).")
