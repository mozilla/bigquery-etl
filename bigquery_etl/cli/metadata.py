"""bigquery-etl CLI metadata command."""

from datetime import datetime
from pathlib import Path
from typing import Optional

import click
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

from bigquery_etl.metadata.parse_metadata import DatasetMetadata, Metadata
from bigquery_etl.metadata.publish_metadata import publish_metadata

from ..cli.utils import paths_matching_name_pattern, project_id_option, sql_dir_option
from ..config import ConfigLoader
from ..util import extract_from_query_path


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

    # create and populate the dataset metadata yaml file if it does not exist
    for table_metadata_file in table_metadata_files:
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
            # set workgroup: [] if table has been tagged as deprecated
            # this overwrites existing workgroups
            table_metadata.workgroup_access = []
            table_metadata_updated = True
            dataset_metadata.workgroup_access = []
            dataset_metadata_updated = True
        else:
            if table_metadata.workgroup_access is None:
                table_metadata.workgroup_access = (
                    dataset_metadata.default_table_workgroup_access
                )
                table_metadata_updated = True

        if dataset_metadata_updated:
            dataset_metadata.write(dataset_metadata_path)
            click.echo(f"Updated {dataset_metadata_path}")
        if table_metadata_updated:
            table_metadata.write(table_metadata_file)
            click.echo(f"Updated {table_metadata_file}")

    return None


@metadata.command(
    help="""
    Publish all metadata based on metadata.yaml file.

    Example:
     ./bqetl metadata publish ga_derived.downloads_with_attribution_v2
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@sql_dir_option
def publish(name: str, sql_dir: Optional[str], project_id: Optional[str]) -> None:
    """Publish Bigquery metadata."""
    client = bigquery.Client(project_id)

    table_metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    for metadata_file in table_metadata_files:
        project, dataset, table = extract_from_query_path(metadata_file)
        try:
            metadata = Metadata.from_file(metadata_file)
            publish_metadata(client, project, dataset, table, metadata)
        except FileNotFoundError:
            print("No metadata file for: {}.{}.{}".format(project, dataset, table))

    return None


@metadata.command(
    help="""
    Deprecate BigQuery table by updating metadata.yaml file.
    Deletion date is by default 3 months from current date if not provided.

    Example:
     ./bqetl metadata deprecate ga_derived.downloads_with_attribution_v2 --deletion_date=2024-03-02
    """
)
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
    default=datetime.today() + relativedelta(months=+3),
)
def deprecate(
    name: str,
    sql_dir: str,
    project_id: str,
    deletion_date: datetime,
) -> None:
    """Deprecate Bigquery table by updating metadata yaml file(s)."""
    table_metadata_files = paths_matching_name_pattern(
        name, sql_dir, project_id=project_id, files=["metadata.yaml"]
    )

    for metadata_file in table_metadata_files:
        metadata = Metadata.from_file(metadata_file)

        metadata.deprecated = True
        metadata.deletion_date = deletion_date.date()

        metadata.write(metadata_file)
        click.echo(f"Updated {metadata_file} with deprecation.")

    if not table_metadata_files:
        raise FileNotFoundError(f"No metadata file(s) were found for: {name}")
