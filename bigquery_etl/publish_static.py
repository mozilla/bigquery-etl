"""Publish csv files as BigQuery tables."""

import json
import os
from argparse import ArgumentParser

from google.cloud import bigquery

from bigquery_etl.util.common import project_dirs

DATA_FILENAME = "data.csv"
SCHEMA_FILENAME = "schema.json"
DESCRIPTION_FILENAME = "description.txt"


def _parse_args():
    parser = ArgumentParser(__doc__)
    parser.add_argument(
        "--project-id", "--project_id", help="Project to publish tables to"
    )
    return parser.parse_args()


def _load_table(
    data_file_path, schema_file_path=None, description_file_path=None, project=None
):
    client = bigquery.Client(project)

    # Assume path is ...project/dataset/table/data.csv
    path_split = os.path.normcase(data_file_path).split("/")
    dataset_id = path_split[-3]
    table_id = path_split[-2]
    if not project:
        project = path_split[0]
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
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


def main():
    """Publish csv files as BigQuery tables."""
    args = _parse_args()

    # This machinery is only compatible with
    # the sql/moz-fx-data-shared-prod/static directory.
    projects = project_dirs("moz-fx-data-shared-prod")

    for data_dir in projects:
        for root, dirs, files in os.walk(data_dir):
            for filename in files:
                if filename == DATA_FILENAME:
                    schema_file_path = (
                        os.path.join(root, SCHEMA_FILENAME)
                        if SCHEMA_FILENAME in files
                        else None
                    )
                    description_file_path = (
                        os.path.join(root, DESCRIPTION_FILENAME)
                        if DESCRIPTION_FILENAME in files
                        else None
                    )
                    _load_table(
                        os.path.join(root, filename),
                        schema_file_path,
                        description_file_path,
                        args.project_id,
                    )


if __name__ == "__main__":
    main()
