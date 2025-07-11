"""Validate metadata files."""

import glob
import logging
import os
from argparse import ArgumentParser
from pathlib import Path

import click
import yaml
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
            owner_file_content = [
                line for line in file_content if not line.startswith("#")
            ]

        # generate a new codeowners list for each path that matches the pattern defined
        # inside CODEOWNERS
        expanded_owners_list = [
            f"{path} {' '.join(entry.split(' ')[1:])}".rstrip()
            for entry in owner_file_content
            for path in glob.glob(entry.split(" ")[0].removeprefix("/"), recursive=True)
        ]

        owners_list = []
        for owner in metadata.owners:
            if "@" not in owner:
                owner = f"@{owner}"
            owners_list.append(owner)
        sample_row_all_owners = f"/{path_in_codeowners} {(' '.join(owners_list))}"

        if not [line for line in expanded_owners_list if path_in_codeowners in line]:
            click.echo(
                click.style(
                    f"ERROR: This query has label `change_controlled` which "
                    f"requires the query path and at least one of the owners in "
                    f"CODEOWNERS. Sample row expected: {sample_row_all_owners}"
                )
            )
            return False

        for line in expanded_owners_list:
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
        if not schema_file.exists():
            click.echo(
                click.style(
                    f"Table {query_dir} does not have schema.yaml required for shredder mitigation.",
                    fg="yellow",
                )
            )
            return False
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
        except Exception:
            click.echo(
                click.style(
                    f"Table {project}.{dataset}.{table} not found or inaccessible."
                    f" for validation. Please check that the name is correct and if the table"
                    f" is in a private repository, ensure that it exists and has data before"
                    f" running a backfill with shredder mitigation.",
                    fg="yellow",
                )
            )

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


def validate_col_desc_enforced(query_dir, metadata):
    """Check schemas with require_column_descriptions = True comply with requirements."""
    if not metadata.require_column_descriptions:
        return True

    schema_file = Path(query_dir) / SCHEMA_FILE
    if not schema_file.exists():
        click.echo(
            click.style(
                f"Table {query_dir} does not have schema.yaml required for column descriptions.",
                fg="yellow",
            )
        )
        return False

    schema = Schema.from_schema_file(schema_file).to_bigquery_schema()

    def validate_fields(fields, parent_path=""):
        """Recursively validate field descriptions, including nested fields."""
        for field in fields:
            field_path = f"{parent_path}.{field.name}" if parent_path else field.name

            if not field.description:
                click.echo(
                    f"Column description validation failed for {query_dir}, "
                    f"{field_path} does not have a description in the schema."
                )
                return False

            # If the field has nested fields (e.g., RECORD type), recurse
            if hasattr(field, "fields") and field.fields:
                if not validate_fields(field.fields, field_path):
                    return False

        return True

    return validate_fields(schema)


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


def validate_workgroup_access(metadata, path):
    """Check if there are any specifications of table-level access that are redundant with dataset access."""
    is_valid = True
    dataset_metadata_path = Path(path).parent.parent / "dataset_metadata.yaml"
    if not dataset_metadata_path.exists():
        return is_valid

    dataset_metadata = DatasetMetadata.from_file(dataset_metadata_path)
    default_table_workgroup_access_dict = {
        workgroup_access.get("role"): workgroup_access.get("members", [])
        for workgroup_access in dataset_metadata.default_table_workgroup_access
        if dataset_metadata.default_table_workgroup_access
    }

    if metadata.workgroup_access:
        for table_workgroup_access in metadata.workgroup_access:
            if table_workgroup_access.role in default_table_workgroup_access_dict:
                for table_workgroup_member in table_workgroup_access.members:
                    if (
                        table_workgroup_member
                        in default_table_workgroup_access_dict[
                            table_workgroup_access.role
                        ]
                    ):
                        is_valid = False
                        click.echo(
                            click.style(
                                f"ERROR: Redundant table-level access specification in {path}. "
                                + f"Table-level access for {table_workgroup_access.role}: {table_workgroup_member} defined in dataset_metadata.yaml.",
                                fg="red",
                            )
                        )

    return is_valid


def validate_default_table_workgroup_access(path):
    """
    Check that default_table_workgroup_access does not exist in metadata.

    default_table_workgroup_access will be generated from workgroup_access and
    should not be overridden.
    """
    is_valid = True
    dataset_metadata_path = Path(path).parent.parent / "dataset_metadata.yaml"
    if not dataset_metadata_path.exists():
        return is_valid

    with open(dataset_metadata_path, "r") as yaml_stream:
        try:
            metadata = yaml.safe_load(yaml_stream)
        except yaml.YAMLError as e:
            raise e

    if "default_table_workgroup_access" in metadata:
        is_valid = False
        click.echo(
            click.style(
                f"ERROR: default_table_workgroup_access should not be explicity specified in {dataset_metadata_path}. "
                + "The default_table_workgroup_access configuration will be automatically generated.",
                fg="red",
            )
        )

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


class MetadataValidationError(Exception):
    """Metadata validation failed."""


def validate(target):
    """Validate metadata files."""
    failed = False
    skip_validation = ConfigLoader.get("metadata", "validation", "skip", fallback=[])

    if os.path.isdir(target):
        for root, dirs, files in os.walk(target, followlinks=True):
            for file in files:
                if Metadata.is_metadata_file(file):
                    path = os.path.join(root, file)

                    if path in skip_validation:
                        continue

                    metadata = Metadata.from_file(path)

                    if not validate_public_data(metadata, path):
                        failed = True

                    # root looks like .../sql/project/dataset/table
                    project_id = Path(root).parent.parent.name

                    if not validate_change_control(
                        file_path=root,
                        metadata=metadata,
                        codeowners_file=CODEOWNERS_FILE,
                        project_id=project_id,
                    ):
                        failed = True

                    # Shredder mitigation checks still WIP
                    # if not validate_shredder_mitigation(
                    #     query_dir=root,
                    #     metadata=metadata,
                    # ):
                    #     failed = True

                    if not validate_deprecation(metadata, path):
                        failed = True

                    if not validate_exclusion_list_expiration_days(metadata, path):
                        failed = True

                    if not validate_retention_policy_based_on_table_type(
                        metadata, path
                    ):
                        failed = True

                    if not validate_col_desc_enforced(root, metadata):
                        failed = True

                    # todo more validation
                    # e.g. https://github.com/mozilla/bigquery-etl/issues/924
    else:
        raise ValueError(f"Invalid target: {target}, target must be a directory.")

    if failed:
        # TODO: add failed checks to message
        raise MetadataValidationError(f"Metadata validation failed for {target}")


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
        raise ValueError(f"Invalid target: {target}, target must be a directory.")

    if failed:
        raise MetadataValidationError(
            f"Dataset metadata validation failed for {target}"
        )


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
