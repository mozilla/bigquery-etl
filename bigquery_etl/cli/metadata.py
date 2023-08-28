"""bigquery-etl CLI metadata command."""
import os
from pathlib import Path
from typing import Optional

import click

from bigquery_etl.metadata.parse_metadata import DatasetMetadata, Metadata

from ..cli.utils import paths_matching_name_pattern, project_id_option, sql_dir_option


@click.group(
    help="""
        Commands for managing bqetl metadata.
        \b

        UNDER ACTIVE DEVELOPMENT See https://mozilla-hub.atlassian.net/browse/DENG-1381
        """
)
@click.pass_context
def metadata(ctx):
    """Create the CLI group for the metadata command."""
    pass


@metadata.command(
    help="""
    Update table level metadata yaml files.
   \b

    Example:
     ./bqetl metadata update ga_derived.downloads_with_attribution_v2
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@project_id_option()
@sql_dir_option
def update(name: str, sql_dir: Optional[str], project_id: Optional[str]) -> None:
    """Update metadata yaml file."""
    table_metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )
    for table_metadata_file in table_metadata_files:
        dataset_metadata_path = (
            Path(table_metadata_file).parent.parent / "dataset_metadata.yaml"
        )
        if os.path.exists(dataset_metadata_path):
            dataset_metadata = DatasetMetadata.from_file(dataset_metadata_path)
            table_metadata = Metadata.from_file(table_metadata_file)
            if table_metadata.deprecated:
                table_metadata.workgroup_access = [
                    dict(
                        role="roles/bigquery.metadataViewer",
                        members=["workgroup:deprecated"],
                    )
                ]
            else:
                if dataset_metadata.default_table_workgroup_access and (
                    table_metadata.workgroup_access == []
                    or table_metadata.workgroup_access is None
                ):
                    table_metadata.workgroup_access = (
                        dataset_metadata.default_table_workgroup_access
                    )
            table_metadata.write(table_metadata_file)

            click.echo(f"Updated {table_metadata_file}")
        else:
            click.echo("Dataset metadata yaml does not exist")
    return None
