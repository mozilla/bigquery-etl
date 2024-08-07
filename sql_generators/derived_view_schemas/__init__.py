"""Generate and record schemas for user-facing derived dataset views."""

from functools import partial
from pathlib import Path

import click
from pathos.multiprocessing import ProcessingPool

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.dryrun import get_id_token

NON_USER_FACING_DATASET_SUBSTRINGS = (
    "_derived",
    "_external",
    "_bi",
    "_restricted",
    "udf",
)

VIEW_FILE = "view.sql"
METADATA_FILE = "metadata.yaml"
SCHEMA_FILE = "schema.yaml"


def _generate_view_schema(sql_dir, view_directory, id_token=None):
    import logging

    from bigquery_etl.dependency import extract_table_references
    from bigquery_etl.metadata.parse_metadata import Metadata
    from bigquery_etl.schema import Schema
    from bigquery_etl.util.common import render
    from bigquery_etl.view import View

    logging.basicConfig(format="%(levelname)s (%(filename)s:%(lineno)d) - %(message)s")

    # If the view references only one table, we can:
    # 1. Get the reference table partition column if it exists.
    #   (to dry run views to partitioned tables).
    # 2. Get the reference table schema and use it to enrich the
    #   view schema we get from dry-running.
    def _get_reference_dir_path(view_file):
        view_references = extract_table_references(
            render(view_file.name, view_file.parent)
        )
        if len(view_references) != 1:
            return

        target_project = view_file.parent.parent.parent.name
        target_dataset = view_file.parent.parent.name

        target_reference = view_references[0]
        parts = target_reference.split(".")
        if len(parts) == 3:
            reference_project_id, reference_dataset_id, reference_table_id = parts
        # Fully qualify the reference:
        elif len(parts) == 2:
            reference_project_id = target_project
            reference_dataset_id, reference_table_id = parts
        elif len(parts) == 1:
            reference_project_id = target_project
            reference_dataset_id = target_dataset
            reference_table_id = parts[0]
        else:
            return

        return (
            sql_dir / reference_project_id / reference_dataset_id / reference_table_id
        )

    def _get_reference_partition_column(ref_path):
        if ref_path is None:
            logging.debug("No table reference, skipping partition column.")
            return

        try:
            reference_metadata = Metadata.from_file(ref_path / METADATA_FILE)
        except Exception as metadata_exception:
            logging.warning(f"Unable to get reference metadata: {metadata_exception}")
            return

        bigquery_metadata = reference_metadata.bigquery
        if bigquery_metadata is None:
            logging.warning(
                f"No bigquery metadata at {ref_path}, unable to get partition column."
            )
            return

        partition_metadata = bigquery_metadata.time_partitioning
        if partition_metadata is None:
            logging.warning(
                f"No partition metadata at {ref_path}, unable to get partition column."
            )
            return

        return partition_metadata.field

    view_file = view_directory / VIEW_FILE
    if not view_file.exists():
        return

    reference_path = _get_reference_dir_path(view_file)

    # If this is a view to a stable table, don't try to write the schema:
    if reference_path is not None:
        reference_dataset = reference_path.parent.name
        if reference_dataset.endswith("_stable"):
            return

    # Optionally get the upstream partition column
    reference_partition_column = _get_reference_partition_column(reference_path)
    if reference_partition_column is None:
        logging.debug("No reference partition column, dry running without one.")

    view = View.from_file(
        view_file, partition_column=reference_partition_column, id_token=id_token
    )

    # `View.schema` prioritizes the configured schema over the dryrun schema, but here
    # we prioritize the dryrun schema because the `schema.yaml` file might be out of date.
    schema = view.dryrun_schema or view.configured_schema
    if view.dryrun_schema and view.configured_schema:
        try:
            schema.merge(
                view.configured_schema,
                attributes=["description"],
                add_missing_fields=False,
                ignore_missing_fields=True,
            )
        except Exception as e:
            logging.warning(
                f"Error enriching {view.view_identifier} view schema from {view.schema_path}: {e}"
            )
    if not schema:
        logging.warning(
            f"Couldn't get schema for {view.view_identifier} potentially "
            f"due to dry-run error. Won't write yaml."
        )
        return

    # Optionally enrich the view schema if we have a valid table reference
    if reference_path:
        reference_schema_file = reference_path / SCHEMA_FILE
        if reference_schema_file.exists():
            try:
                reference_schema = Schema.from_schema_file(reference_schema_file)
                schema.merge(
                    reference_schema,
                    attributes=["description"],
                    add_missing_fields=False,
                    ignore_missing_fields=True,
                )
            except Exception as e:
                logging.warning(
                    f"Error enriching {view.view_identifier} view schema from {reference_schema_file}: {e}"
                )

    schema.to_yaml_file(view_directory / SCHEMA_FILE)


@click.command("generate")
@click.option(
    "--target_project",
    "--target-project",
    help="Which project the views should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--output-dir",
    "--output_dir",
    "--sql-dir",
    "--sql_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@click.option(
    "--parallelism",
    "-P",
    help="Maximum number of tasks to execute concurrently",
    default=20,
    type=int,
)
@use_cloud_function_option
def generate(target_project, output_dir, parallelism, use_cloud_function):
    """
    Generate schema yaml files for views in output_dir/target_project.

    We dry-run to get the schema data and where possible we enrich the
    view schemas with underlying table descriptions.
    """
    project_path = Path(f"{output_dir}/{target_project}")

    dataset_paths = [
        dataset_path
        for dataset_path in project_path.iterdir()
        if dataset_path.is_dir()
        and all(
            substring not in str(dataset_path)
            for substring in NON_USER_FACING_DATASET_SUBSTRINGS
        )
    ]

    id_token = get_id_token()
    view_directories = []

    for dataset_path in dataset_paths:
        view_directories += [
            path
            for path in dataset_path.iterdir()
            if path.is_dir() and (path / VIEW_FILE).exists()
        ]

    with ProcessingPool(parallelism) as pool:
        pool.map(
            partial(_generate_view_schema, Path(output_dir), id_token=id_token),
            view_directories,
        )
