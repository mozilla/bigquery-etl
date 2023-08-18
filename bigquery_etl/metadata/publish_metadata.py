"""Update metadata of BigQuery tables and views."""

import logging
import os
from argparse import ArgumentParser

import yaml
from google.cloud import bigquery

from ..config import ConfigLoader
from ..util import standard_args
from ..util.bigquery_tables import get_tables_matching_patterns
from ..util.common import project_dirs
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


def publish_metadata(client, dataset, table, metadata):
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

        client.update_table(table, ["friendly_name", "description", "labels"])
    except yaml.YAMLError as e:
        print(e)


def main():
    """Update BigQuery table metadata."""
    args = parser.parse_args()

    # set log level
    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        parser.error(f"argument --log-level: {e}")

    projects = project_dirs(args.target)

    for target in projects:
        client = bigquery.Client(target)
        if os.path.isdir(target):
            for full_table_id in get_tables_matching_patterns(client, args.patterns):
                [project, dataset, table] = full_table_id.split(".")
                metadata_file = os.path.join(target, dataset, table, METADATA_FILE)

                try:
                    metadata = Metadata.from_file(metadata_file)
                    publish_metadata(client, dataset, table, metadata)
                except FileNotFoundError:
                    print("No metadata file for: {}.{}".format(dataset, table))
        else:
            print(
                """
                Invalid target: {}, target must be a directory with
                structure <project>/<dataset>/<table>/metadata.yaml.
                """.format(
                    target
                )
            )


if __name__ == "__main__":
    main()
