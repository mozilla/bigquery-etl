"""Generate and record schemas for user-facing derived dataset views."""

from functools import partial
from pathlib import Path

import click
from pathos.multiprocessing import ProcessingPool

from bigquery_etl.cli.utils import use_cloud_function_option

NON_USER_FACING_DATASET_SUBSTRINGS = (
    "_derived",
    "_external",
    "_bi",
    "_restricted",
    "udf",
)


def _generate_view_schema(sql_dir, view_directory):
    import logging

    from bigquery_etl.dependency import extract_table_references
    from bigquery_etl.metadata.parse_metadata import Metadata
    from bigquery_etl.schema import Schema
    from bigquery_etl.util.common import render

    logging.basicConfig(format="%(levelname)s (%(filename)s:%(lineno)d) - %(message)s")

    VIEW_FILE = "view.sql"
    METADATA_FILE = "metadata.yaml"
    SCHEMA_FILE = "schema.yaml"

    # If the view references only one table, we can:
    # 1. Get the reference table partition key if it exists.
    #   (to dry run views to partitioned tables).
    # 2. Get the reference table schema and use it to enrich the
    #   view schema we get from dry-running.
    def _get_reference_dir_path(view_dir):
        view_file = view_dir / VIEW_FILE
        if not view_file.exists():
            return

        view_references = extract_table_references(render(view_file.name, view_dir))
        if len(view_references) != 1:
            return

        target_project = view_dir.parent.parent.name
        target_dataset = view_dir.parent.name

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

    def _get_reference_partition_key(ref_path):
        if ref_path is None:
            logging.debug("No table reference, skipping partition key.")
            return

        try:
            reference_metadata = Metadata.from_file(ref_path / METADATA_FILE)
        except Exception as metadata_exception:
            logging.warning(f"Unable to get reference metadata: {metadata_exception}")
            return

        bigquery_metadata = reference_metadata.bigquery
        if bigquery_metadata is None:
            logging.warning(
                f"No bigquery metadata at {ref_path}, unable to get partition key."
            )
            return

        partition_metadata = bigquery_metadata.time_partitioning
        if partition_metadata is None:
            logging.warning(
                f"No partition metadata at {ref_path}, unable to get partition key."
            )
            return

        return partition_metadata.field

    reference_path = _get_reference_dir_path(view_directory)

    # If this is a view to a stable table, don't try to write the schema:
    if reference_path is not None:
        reference_dataset = reference_path.parent.name
        if reference_dataset.endswith("_stable"):
            return

    # Optionally get the upstream partition key
    reference_partition_key = _get_reference_partition_key(reference_path)
    if reference_partition_key is None:
        logging.debug("No reference partition key, dry running without one.")

    project_id = view_directory.parent.parent.name
    dataset_id = view_directory.parent.name
    view_id = view_directory.name

    schema = Schema.for_table(
        project_id, dataset_id, view_id, partitioned_by=reference_partition_key
    )
    if len(schema.schema.get("fields")) == 0:
        logging.warning(
            f"Got empty schema for {project_id}.{dataset_id}.{view_id} potentially "
            f"due to dry-run error. Won't write yaml."
        )
        return

    # Optionally enrich the view schema if we have a valid table reference
    if reference_path:
        try:
            reference_schema = Schema.from_schema_file(reference_path / SCHEMA_FILE)
            schema.merge(reference_schema, add_missing_fields=False)
        except Exception as e:
            logging.info(
                f"Unable to open reference schema; unable to enrich schema: {e}"
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

    for dataset_path in dataset_paths:
        view_directories = [path for path in dataset_path.iterdir() if path.is_dir()]

        with ProcessingPool(parallelism) as pool:
            pool.map(
                partial(
                    _generate_view_schema,
                    Path(output_dir),
                ),
                view_directories,
            )
