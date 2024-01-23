"""bigquery-etl CLI metadata command."""
from pathlib import Path
from typing import Optional

import click

from bigquery_etl.metadata.parse_metadata import DatasetMetadata, Metadata

from ..cli.utils import paths_matching_name_pattern, project_id_option, sql_dir_option


@click.group(
    help="""
        Commands for managing bqetl metadata.
        """
)
@click.pass_context
def metadata(ctx):
    """Create the CLI group for the metadata command."""
    pass


@metadata.command(
    help="""
    Update metadata yaml files.
    Updates workgroup access metadata based on the dataset_metadata.yaml and
    deprecation metadata.

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
    dataset_metadata_path = None
    # create and populate the dataset metadata yaml file if it does not exist
    for table_metadata_file in table_metadata_files:
        dataset_metadata_path = (
            Path(table_metadata_file).parent.parent / "dataset_metadata.yaml"
        )
        if not dataset_metadata_path.exists():
            continue
        dataset_metadata = DatasetMetadata.from_file(dataset_metadata_path)
        table_metadata = Metadata.from_file(table_metadata_file)

        # set dataset metadata default_table_workgroup_access to table_workgroup_access if not set
        if not dataset_metadata.default_table_workgroup_access:
            dataset_metadata.default_table_workgroup_access = (
                dataset_metadata.workgroup_access
            )

        if table_metadata.deprecated:
            # set workgroup: [] if table has been tagged as deprecated
            # this overwrites existing workgroups
            table_metadata.workgroup_access = []
            dataset_metadata.workgroup_access = []
        else:
            if table_metadata.workgroup_access is None:
                table_metadata.workgroup_access = (
                    dataset_metadata.default_table_workgroup_access
                )

        dataset_metadata.write(dataset_metadata_path)
        table_metadata.write(table_metadata_file)
        click.echo(f"Updated {table_metadata_file}")
    return None
