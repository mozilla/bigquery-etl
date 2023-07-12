"""Methods for publishing static CSV files to BigQuery."""

import functools
import glob
import json
import logging
import os

import click
from google.cloud import bigquery

from bigquery_etl.cli.utils import project_id_option
from bigquery_etl.config import ConfigLoader
from bigquery_etl.metadata.parse_metadata import DATASET_METADATA_FILE, DatasetMetadata
from bigquery_etl.util.common import project_dirs

DATA_FILENAME = "data.csv"
SCHEMA_FILENAME = "schema.json"
DESCRIPTION_FILENAME = "description.txt"


@click.group("static", help="Commands for working with static CSV files.")
def static_():
    """Create the CLI group for static commands."""
    pass


@static_.command("publish", help="Publish CSV files as BigQuery tables.")
@project_id_option()
def publish(project_id):
    """Publish CSV files as BigQuery tables."""
    source_project = project_id
    target_project = project_id
    if target_project == ConfigLoader.get(
        "default", "user_facing_project", fallback="mozdata"
    ):
        source_project = ConfigLoader.get(
            "default", "project", fallback="moz-fx-data-shared-prod"
        )

    for project_dir in project_dirs(source_project):
        # Assumes directory structure is project/dataset/table/files.
        for data_file_path in glob.iglob(
            os.path.join(project_dir, "*", "*", DATA_FILENAME)
        ):
            table_dir = os.path.dirname(data_file_path)
            dataset_dir = os.path.dirname(table_dir)
            if target_project == ConfigLoader.get(
                "default", "user_facing_project", fallback="mozdata"
            ) and not _is_user_facing_dataset(dataset_dir):
                logging.debug(
                    f"Skipping `{data_file_path}` for {target_project} because it isn't"
                    " in a user-facing dataset."
                )
                continue

            schema_file_path = os.path.join(table_dir, SCHEMA_FILENAME)
            if not os.path.exists(schema_file_path):
                schema_file_path = None

            description_file_path = os.path.join(table_dir, DESCRIPTION_FILENAME)
            if not os.path.exists(description_file_path):
                description_file_path = None

            _load_table(
                data_file_path,
                schema_file_path,
                description_file_path,
                target_project,
            )


def _load_table(
    data_file_path, schema_file_path=None, description_file_path=None, project=None
):
    # Assume path is ...project/dataset/table/data.csv
    path_split = os.path.normcase(data_file_path).split(os.path.sep)
    dataset_id = path_split[-3]
    table_id = path_split[-2]
    if not project:
        project = path_split[-4]
    logging.info(
        f"Loading `{project}.{dataset_id}.{table_id}` table from `{data_file_path}`."
    )

    client = bigquery.Client(project)
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(data_file_path, "rb") as data_file:
        if schema_file_path is None:
            fields = data_file.readline().decode().strip().split(",")
            # Assume all fields are strings and nullable
            job_config.schema = [
                bigquery.SchemaField(field, field_type="STRING") for field in fields
            ]
            data_file.seek(0)
        else:
            with open(schema_file_path) as schema_file:
                fields = json.load(schema_file)
            job_config.schema = [
                bigquery.SchemaField(
                    field["name"],
                    field_type=field.get("type", "STRING"),
                    mode=field.get("mode", "NULLABLE"),
                    description=field.get("description"),
                )
                for field in fields
            ]

        job = client.load_table_from_file(data_file, table_ref, job_config=job_config)

    job.result()

    if description_file_path is not None:
        with open(description_file_path) as description_file:
            description = description_file.read()
            table = client.get_table(table_ref)
            table.description = description
            client.update_table(table, ["description"])


@functools.lru_cache
def _is_user_facing_dataset(dataset_directory):
    dataset_metadata_file_path = os.path.join(dataset_directory, DATASET_METADATA_FILE)
    return (
        os.path.exists(dataset_metadata_file_path)
        and DatasetMetadata.from_file(dataset_metadata_file_path).user_facing
    )
