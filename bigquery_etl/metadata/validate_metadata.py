"""Validate metadata files."""

import logging
import os
import sys
from argparse import ArgumentParser
from pathlib import Path

import click
import yaml
from gcloud.exceptions import NotFound, Forbidden
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader
from bigquery_etl.schema import SCHEMA_FILE, Schema

from ..util import extract_from_query_path, standard_args
from ..util.common import extract_last_group_by_from_query, project_dirs
from .parse_metadata import DatasetMetadata, Metadata

parser = ArgumentParser(description=__doc__)

parser.add_argument("--target", help="File or directory containing metadata files")
standard_args.add_log_level(parser)

CODEOWNERS_FILE = "CODEOWNERS"
CHANGE_CONTROL_LABEL = "change_controlled"
SHREDDER_MITIGATION_LABEL = "shredder_mitigation"
ID_LEVEL_COLUMNS_FILE_PATH = Path(__file__).parent / "id_level_columns.yaml"


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


def validate_shredder_mitigation(query_dir, metadata):
    """Check queries with shredder mitigation label comply with requirements."""
    has_shredder_mitigation = SHREDDER_MITIGATION_LABEL in metadata.labels
    table_not_empty = True

    if has_shredder_mitigation:
        schema_file = Path(query_dir) / SCHEMA_FILE
        schema = Schema.from_schema_file(schema_file).to_bigquery_schema()

        # This label requires that the query doesn't have id-level columns,
        # has a group by that is explicit & all schema columns have descriptions.
        query_file = Path(query_dir) / "query.sql"
        query_group_by = extract_last_group_by_from_query(sql_path=query_file)

        # Validate that this label is only applied to tables in version 1 if they're not empty
        # If the table is empty, it should be backfilled before applying the label.
        project, dataset, table = extract_from_query_path(Path(query_dir))
        client = bigquery.Client(project=project)
        error_message = (
            f"The shredder-mitigation label can only be applied to existing and "
            f"non-empty tables.\nEnsure that the table `{project}.{dataset}.{table}` is deployed"
            f" and run a managed backfill without mitigation before applying this label to"
            f" a new or empty table."
            f"\n\nSubsequent backfills then can use the [shredder mitigation process]"
            f"(https://mozilla.github.io/bigquery-etl/cookbooks/creating_a_derived_dataset/#initiating-the-backfill)."
        )

        # Check that the table exists and it's not empty.
        query_table_is_not_empty = (
            f"SELECT EXISTS (SELECT 1 "
            f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` "
            f"WHERE table_name = '{table}' LIMIT 1) AS not_empty;"
        )

        try:
            table_not_empty = client.query(query_table_is_not_empty).result()
        except Forbidden:
            click.echo(
                click.style(
                    f"Table {project}.{dataset}.{table} is not accessible. If the table "
                    f"is in a private repository, please ensure that it exists and has data "
                    f"before running a backfill with shredder mitigation.",
                    fg="yellow",
                )
            )
        except NotFound:
            click.echo(
                click.style(
                    f"Table {project}.{dataset}.{table} not found or not accessible."
                    f"Please ensure that the table exists and the name is correct.",
                    fg="yellow",
                )
            )
            return False

        if table_not_empty is None or table_not_empty is False:
            click.echo(click.style(error_message, fg="yellow"))
            return False

        # Validate that the query group by is explicit and as required.
        integers_in_group_by = False
        for e in query_group_by:
            try:
                int(e)
                integers_in_group_by = True
            except ValueError:
                continue
        if (
            "ALL" in query_group_by
            or not all(isinstance(e, str) for e in query_group_by)
            or not query_group_by
            or integers_in_group_by
        ):
            click.echo(
                "Shredder mitigation validation failed, GROUP BY must use an explicit list "
                "of columns. Avoid expressions like `GROUP BY ALL` or `GROUP BY 1, 2, 3`."
            )
            return False

        with open(ID_LEVEL_COLUMNS_FILE_PATH, "r") as columns_file:
            columns_from_file = yaml.safe_load(columns_file)
            id_level_columns = columns_from_file.get("id_level_columns", [])

        for field in schema:
            # Validate that the query columns have descriptions.
            if not field.description:
                click.echo(
                    f"Shredder mitigation validation failed, {field.name} does not have "
                    f"a description in the schema."
                )
                return False
            # Validate that id-level columns are not present in the query schema.
            if field.name in id_level_columns:
                click.echo(
                    f"Shredder mitigation validation failed, {field.name} is an id-level"
                    f" column that is not allowed for this type of backfill."
                )
                return False
    return True


def validate_deprecation(metadata, path):
    """Check that deprecated is True when deletion date exists."""
    if metadata.deletion_date and not metadata.deprecated:
        click.echo(
            f"Deletion date should only be added when table is deprecated in {path}"
        )
        return False

    return True


def validate_exclusion_list_expiration_days(metadata, path):
    """Check if any of the retention exclusion tables have expiration_days set."""
    is_valid = True
    retention_exclusion_list = set(
        ConfigLoader.get("retention_exclusion_list", fallback=[])
    )
    normalized_path = str(Path(path).parent)

    if normalized_path in retention_exclusion_list:
        # Access and check expiration_days metadata
        expiration_days = (
            metadata.bigquery.time_partitioning.expiration_days
            if metadata
            and getattr(metadata, "bigquery", None)
            and getattr(metadata.bigquery, "time_partitioning", None)
            else None
        )

        if expiration_days is not None:
            click.echo(
                click.style(
                    f"ERROR: Table at {path} is in the retention exclusion list but has expiration_days set to {expiration_days}.",
                    fg="red",
                )
            )
            is_valid = False

    return is_valid


def validate_retention_policy_based_on_table_type(metadata, path):
    """Check if any of the retention exclusion tables have expiration_days set."""
    is_valid = True
    table_type = (
        metadata.labels.get("table_type") if isinstance(metadata.labels, dict) else None
    )
    expiration_days = (
        metadata.bigquery.time_partitioning.expiration_days
        if metadata
        and getattr(metadata, "bigquery", None)
        and getattr(metadata.bigquery, "time_partitioning", None)
        else None
    )

    # retention_exclusion_list = set(
    #     ConfigLoader.get("retention_exclusion_list", fallback=[])
    # )

    # normalized_path = str(Path(path).parent)
    if expiration_days is not None and table_type == "aggregate":
        click.echo(
            click.style(
                f"ERROR: Table at {path} is an aggregate table but has expiration_days set to {expiration_days}.",
                fg="red",
            )
        )
        is_valid = False
    # The below line of code should be uncommented when the retention project is completed
    # if (
    #     expiration_days is None
    #     and table_type == "client_level"
    #     and normalized_path not in retention_exclusion_list
    # ):
    #     click.echo(
    #         click.style(
    #             f"ERROR: Table at {path} is an client level table and needs expiration_days to be set",
    #             fg="red",
    #         )
    #     )
    #     is_valid = False
    return is_valid


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

                    if not validate_shredder_mitigation(
                        query_dir=root,
                        metadata=metadata,
                    ):
                        failed = True

                    if not validate_deprecation(metadata, path):
                        failed = True

                    if not validate_exclusion_list_expiration_days(metadata, path):
                        failed = True

                    if not validate_retention_policy_based_on_table_type(
                        metadata, path
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
