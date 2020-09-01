"""Validate metadata files."""

from argparse import ArgumentParser
import logging
import os
import sys

from .parse_metadata import Metadata
from ..util import standard_args

DEFAULT_DIR = "sql/"

parser = ArgumentParser(description=__doc__)

parser.add_argument(
    "--target", default=DEFAULT_DIR, help="File or directory containing metadata files"
)
standard_args.add_log_level(parser)


def validate_public_data(metadata, path):
    """Check if the metadata for public data queries is valid."""
    is_valid = True

    if metadata.is_public_bigquery() or metadata.is_public_json():
        if not metadata.review_bug():
            logging.error(f"Missing review bug for public data: {path}")
            is_valid = False

    return is_valid


def validate(target):
    """Validate metadata files."""
    failed = False

    if os.path.isdir(target):
        for root, dirs, files in os.walk(target):
            for file in files:
                if Metadata.is_metadata_file(file):
                    path = os.path.join(root, *dirs, file)
                    metadata = Metadata.from_file(path)

                    if not validate_public_data(metadata, path):
                        failed = True

                    # todo more validation
                    # e.g. https://github.com/mozilla/bigquery-etl/issues/924
    else:
        logging.error(f"Invalid target: {target}, target must be a directory.")
        sys.exit(1)

    if failed:
        sys.exit(1)


def main():
    """Validate all metadata.yaml files in the provided target directory."""
    args = parser.parse_args()

    # set log level
    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        parser.error(f"argument --log-level: {e}")

    validate(args.target)


if __name__ == "__main__":
    main()
