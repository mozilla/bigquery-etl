"""Validate metadata files."""

import logging
import os
import sys
from argparse import ArgumentParser

import click

from bigquery_etl.config import ConfigLoader

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
    project_id=ConfigLoader.get(
        "default", "project", fallback="moz-fx-data-shared-prod"
    ),
    sql_dir=ConfigLoader.get("default", "sql_dir", fallback="sql"),
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
                    f" change_controlled but it's missing owners."
                )
            )
            return False

        with open(codeowners_file, "r") as owners_file:
            file_content = owners_file.readlines()
            content = [line for line in file_content if not line.startswith("#")]

        owners_list = []
        for owner in metadata.owners:
            if "@" not in owner:
                owner = f"@{owner}"
            owners_list.append(owner)
        sample_row_all_owners = f"/{path_in_codeowners} {(' '.join(owners_list))}"

        if not [line for line in content if path_in_codeowners in line]:
            click.echo(
                click.style(
                    f"ERROR: This query has label `change_controlled` which "
                    f"requires the query path and at least one of the owners in "
                    f"CODEOWNERS. Sample row expected: {sample_row_all_owners}"
                )
            )
            return False

        for line in content:
            if path_in_codeowners in line and not any(
                owner in line for owner in owners_list
            ):
                click.echo(
                    click.style(
                        f"ERROR: This query has label `change_controlled` which "
                        f"requires at least one of the owners to be registered "
                        f"in CODEOWNERS. Sample row expected: \n"
                        f"{sample_row_all_owners}"
                    )
                )
                return False
    return True


def validate(target):
    """Validate metadata files."""
    failed = False

    if os.path.isdir(target):
        for root, dirs, files in os.walk(target, followlinks=True):
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
        for root, dirs, files in os.walk(target, followlinks=True):
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
