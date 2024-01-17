"""Generate and record schemas for user-facing derived dataset views."""

from functools import partial
from pathlib import Path
from typing import Optional

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


def _generate_view_schema(sql_dir: Path, view_directory: Path) -> None:
    import logging

    from view import View

    from bigquery_etl.metadata.parse_metadata import Metadata
    from bigquery_etl.schema import Schema

    logging.basicConfig(format="%(levelname)s (%(filename)s:%(lineno)d) - %(message)s")

    VIEW_FILE = "view.sql"
    METADATA_FILE = "metadata.yaml"
    SCHEMA_FILE = "schema.yaml"

    # If the view references only one table, we can:
    # 1. Get the reference table partition key if it exists.
    #   (to dry run views to partitioned tables).
    # 2. Get the reference table schema and use it to enrich the
    #   view schema we get from dry-running.

    view = View.from_file(view_directory / VIEW_FILE)
    if len(view.table_references) != 1:
        return None

    ref_project, ref_dataset, ref_table = view.table_references[0].split(".")
    ref_dir = sql_dir / ref_project / ref_dataset / ref_table

    non_user_facing = any(s in ref_dataset for s in NON_USER_FACING_DATASET_SUBSTRINGS)
    if not ref_dir.exists() or non_user_facing or ref_dataset.name.endswith("_stable"):
        return None

    def _get_reference_partition_key(ref_path: Path) -> Optional[str]:
        try:
            reference_metadata = Metadata.from_file(ref_path / METADATA_FILE)
        except Exception as metadata_exception:
            logging.warning(f"Unable to get reference metadata: {metadata_exception}")
            return None

        bigquery_metadata = reference_metadata.bigquery
        if bigquery_metadata is None or bigquery_metadata.time_partitioning is None:
            logging.warning(
                f"No partition metadata at {ref_path}, unable to get partition key."
            )
            return None

        return bigquery_metadata.time_partitioning.field

    # Optionally get the upstream partition key
    reference_partition_key = _get_reference_partition_key(ref_dir)
    if reference_partition_key is None:
        logging.debug("No reference partition key, dry running without one.")

    view_schema = Schema.for_table(
        view.project, view.dataset, view.name, partitioned_by=reference_partition_key
    )

    if len(view_schema.get("fields")) == 0:
        logging.warning(
            f"Got empty schema for {view.path} potentially "
            f"due to dry-run error. Won't write yaml."
        )
        return None

    # Optionally enrich the view schema if we have a valid table reference
    try:
        reference_schema = Schema.from_schema_file(ref_dir / SCHEMA_FILE)
        view_schema.merge(reference_schema, add_missing_fields=False)
    except Exception as e:
        logging.info(f"Unable to open reference schema; unable to enrich schema: {e}")

    view_schema.to_yaml_file(view_directory / SCHEMA_FILE)


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
    view_files = project_path.glob("*/*/view.sql")

    with ProcessingPool(parallelism) as pool:
        pool.map(
            partial(
                _generate_view_schema,
                Path(output_dir),
            ),
            [path.parent for path in view_files],
        )
