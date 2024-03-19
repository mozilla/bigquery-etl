"""Update metadata of BigQuery tables and views."""

from argparse import ArgumentParser

from ..config import ConfigLoader
from ..util import standard_args

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

        client.update_table(table, ["friendly_name", "description", "labels"])
        print("Published metadata for: {}.{}.{}".format(project, dataset, table))

    except Exception as e:
        print(e)
