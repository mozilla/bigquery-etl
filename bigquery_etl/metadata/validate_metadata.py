"""Validate metadata files."""

import logging
import os
import sys
from argparse import ArgumentParser

import click

from ..util import standard_args
from ..util.common import project_dirs
from .parse_metadata import DatasetMetadata, Metadata

parser = ArgumentParser(description=__doc__)

parser.add_argument("--target", help="File or directory containing metadata files")
standard_args.add_log_level(parser)

CODEOWNERS_FILE = "CODEOWNERS"
CHANGE_CONTROL_LABEL = "change_controlled"


def validate_public_data(metadata, path):
    """Check if the metadata for public data queries is valid."""
    is_valid = True

    if metadata.is_public_bigquery() or metadata.is_public_json():
        if not metadata.review_bugs():
            logging.error(f"Missing review bug for public data: {path}")
            is_valid = False

    return is_valid


def validate_change_control(
    file_path,
    metadata,
    codeowners_file,
    project_id="moz-fx-data-shared-prod",
    sql_dir="sql",
):
    """Verify that a query is correctly setup for change control."""
    path_to_add = file_path.partition(f"{project_id}/")[2]
    path_in_codeowners = os.path.join(sql_dir, project_id, path_to_add)
    has_change_control = CHANGE_CONTROL_LABEL in metadata.labels

    if has_change_control:
        # This label requires to have at least one owner in the metadata file.
        # And for any of the owners, at least one entry in the CODEOWNERS file.
        # The owners can be emails or GitHub identities e.g. mozilla/team_name.
        if len(metadata.owners) == 0:
            click.echo(
                click.style(
                    f"The metadata for {file_path} has the label"
                    f" change_controlled but it's missing code owners."
                )
            )
            return

        with open(codeowners_file, "r") as owners_file:
            content = owners_file.readlines()
        occurrences = 0
        rows_expected = []
        for meta_owner in metadata.owners:
            if "@" not in meta_owner:
                meta_owner = f"@{meta_owner}"
            row_to_search_for = f"/{path_in_codeowners} {meta_owner}"
            rows_expected.append(row_to_search_for)
            for line in content:
                if row_to_search_for == line.rstrip("\n"):
                    occurrences += 1
        if occurrences == 0:
            click.echo(
                click.style(
                    f"The metadata includes label change_controlled, which "
                    f"requires CODEOWNERS to include at least one owner's "
                    f"record: {rows_expected}."
                )
            )
            return
    return True


def validate(target):
    """Validate metadata files."""
    failed = False

    if os.path.isdir(target):
        for root, dirs, files in os.walk(target):
            for file in files:
                if Metadata.is_metadata_file(file):
                    path = os.path.join(root, file)
                    metadata = Metadata.from_file(path)

                    if not validate_public_data(metadata, path):
                        failed = True

                    if not validate_change_control(
                        file_path=root,
                        metadata=metadata,
                        codeowners_file=CODEOWNERS_FILE,
                    ):
                        failed = True

                    # todo more validation
                    # e.g. https://github.com/mozilla/bigquery-etl/issues/924

    else:
        logging.error(f"Invalid target: {target}, target must be a directory.")
        sys.exit(1)

    if failed:
        sys.exit(1)


def validate_datasets(target):
    """Validate dataset metadata files."""
    failed = False

    if os.path.isdir(target):
        for root, dirs, files in os.walk(target):
            for file in files:
                if DatasetMetadata.is_dataset_metadata_file(file):
                    path = os.path.join(root, file)
                    _ = DatasetMetadata.from_file(path)
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

    if args.target:
        validate(args.target)
    else:
        for project in project_dirs():
            validate(project)


if __name__ == "__main__":
    main()
