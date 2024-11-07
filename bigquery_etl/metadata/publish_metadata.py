"""Update metadata of BigQuery tables and views."""

from argparse import ArgumentParser
from pathlib import Path

from google.cloud import bigquery

from ..config import ConfigLoader
from ..util import standard_args
from .parse_metadata import Metadata

METADATA_FILE = "metadata.yaml"
DEFAULT_PATTERN = (
    f"{ConfigLoader.get('default', 'project', fallback='moz-fx-data-shared-prod')}:*.*"
)


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "patterns",
    metavar="[project:]dataset[.table]",
    default=[DEFAULT_PATTERN],
    nargs="*",
    help="Table that should have a latest-version view, may use shell-style wildcards,"
    f" defaults to: {DEFAULT_PATTERN}",
)
parser.add_argument("--target", help="File or directory containing metadata files")
standard_args.add_log_level(parser)


def publish_metadata(client, project, dataset, table, metadata):
    """Push metadata to BigQuery tables."""
    try:
        table_ref = client.dataset(dataset).table(table)
        table = client.get_table(table_ref)

        if metadata.friendly_name is not None:
            table.friendly_name = metadata.friendly_name

        if metadata.description is not None:
            table.description = metadata.description

        table.labels = {
            key: value
            for key, value in metadata.labels.items()
            if isinstance(value, str)
        }

        if metadata.deprecated is True:
            table.labels["deprecated"] = "true"
        if metadata.deletion_date:
            table.labels["deletion_date"] = metadata.deletion_date.strftime("%Y-%m-%d")
            # TODO: in the future we can consider updating the table expiration date based on deletion_date
        if metadata.monitoring and metadata.monitoring.enabled:
            table.labels["monitoring"] = "true"

        client.update_table(table, ["friendly_name", "description", "labels"])
        print("Published metadata for: {}.{}.{}".format(project, dataset, table))

    except Exception as e:
        print(e)


def attach_metadata(query_file_path: Path, table: bigquery.Table) -> None:
    """Add metadata from query file's metadata.yaml to table object."""
    try:
        metadata = Metadata.of_query_file(query_file_path)
    except FileNotFoundError:
        return

    table.description = metadata.description
    table.friendly_name = metadata.friendly_name

    if metadata.bigquery and metadata.bigquery.time_partitioning:
        table.time_partitioning = bigquery.TimePartitioning(
            metadata.bigquery.time_partitioning.type.bigquery_type,
            field=metadata.bigquery.time_partitioning.field,
            require_partition_filter=(
                metadata.bigquery.time_partitioning.require_partition_filter
            ),
            expiration_ms=metadata.bigquery.time_partitioning.expiration_ms,
        )
    elif metadata.bigquery and metadata.bigquery.range_partitioning:
        table.range_partitioning = bigquery.RangePartitioning(
            field=metadata.bigquery.range_partitioning.field,
            range_=bigquery.PartitionRange(
                start=metadata.bigquery.range_partitioning.range.start,
                end=metadata.bigquery.range_partitioning.range.end,
                interval=metadata.bigquery.range_partitioning.range.interval,
            ),
        )

    if metadata.bigquery and metadata.bigquery.clustering:
        table.clustering_fields = metadata.bigquery.clustering.fields

    # BigQuery only allows for string type labels with specific requirements to be published:
    # https://cloud.google.com/bigquery/docs/labels-intro#requirements
    if metadata.labels:
        table.labels = {
            key: value
            for key, value in metadata.labels.items()
            if isinstance(value, str)
        }
