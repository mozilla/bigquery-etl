"""Publish UDFs and resources to the public mozfun GCP project."""

from argparse import ArgumentParser
import json
import os
import re

from google.cloud import bigquery
from google.cloud import storage

from bigquery_etl.util import standard_args
from bigquery_etl.util.common import project_dirs
from bigquery_etl.routine.parse_routine import (
    read_routine_dir,
    accumulate_dependencies,
)

DEFAULT_UDF_DEPENDENCY_DIR = "udf_js_lib/"
DEFAULT_GCS_BUCKET = "moz-fx-data-prod-bigquery-etl"
DEFAULT_GCS_PATH = ""
DEFAULT_PROJECT = "sql/moz-fx-data-shared-prod"
SQL_DIR = "sql/"

OPTIONS_LIB_RE = re.compile(r'library = "gs://[^"]+/([^"]+)"')
OPTIONS_RE = re.compile(r"OPTIONS(\n|\s)*\(")

SKIP = ["sql/moz-fx-data-shared-prod/udf/main_summary_scalars/udf.sql"]

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project-id",
    "--project_id",
    required=False,
    help="Project to publish UDFs to. "
    "If not set, publish UDFs for all projects except mozfun.",
)
parser.add_argument(
    "--target",
    default=DEFAULT_PROJECT,
    required=False,
    help="Path to project directory.",
)
parser.add_argument(
    "--dependency-dir",
    "--dependency_dir",
    default=DEFAULT_UDF_DEPENDENCY_DIR,
    help="The directory JavaScript dependency files for UDFs are stored.",
)
parser.add_argument(
    "--gcs-bucket",
    "--gcs_bucket",
    default=DEFAULT_GCS_BUCKET,
    help="The GCS bucket where dependency files are uploaded to.",
)
parser.add_argument(
    "--gcs-path",
    "--gcs_path",
    default=DEFAULT_GCS_PATH,
    help="The GCS path in the bucket where dependency files are uploaded to.",
)
parser.add_argument(
    "--public",
    default=False,
    help="The published UDFs should be publicly accessible.",
)
standard_args.add_log_level(parser)


def main():
    """Publish routine."""
    args = parser.parse_args()

    if args.target is not None:
        projects = [args.target]
    else:
        projects = project_dirs()

    for project in projects:
        publish(
            args.target,
            args.project_id,
            os.path.join(SQL_DIR, project, args.dependency_dir),
            args.gcs_bucket,
            args.gcs_path,
            args.public,
        )


def publish(target, project_id, dependency_dir, gcs_bucket, gcs_path, public):
    """Publish routines in the provided directory."""
    client = bigquery.Client(project_id)

    if dependency_dir and os.path.exists(dependency_dir):
        push_dependencies_to_gcs(
            gcs_bucket, gcs_path, dependency_dir, os.path.basename(target)
        )

    raw_routines = read_routine_dir(target)

    published_routines = []

    for raw_routine in raw_routines:
        # get all dependencies for UDF and publish as persistent UDF
        udfs_to_publish = accumulate_dependencies([], raw_routines, raw_routine)
        udfs_to_publish.append(raw_routine)

        for dep in udfs_to_publish:
            if dep not in published_routines and raw_routines[dep].filepath not in SKIP:
                publish_routine(
                    raw_routines[dep],
                    client,
                    project_id,
                    gcs_bucket,
                    gcs_path,
                    raw_routines.keys(),
                    public,
                )
                published_routines.append(dep)


def publish_routine(
    raw_routine, client, project_id, gcs_bucket, gcs_path, known_udfs, is_public
):
    """Publish a specific routine to BigQuery."""
    if is_public:
        # create new dataset for routine if necessary
        dataset = client.create_dataset(raw_routine.dataset, exists_ok=True)

        # set permissions for dataset, public for everyone
        entry = bigquery.AccessEntry("READER", "specialGroup", "allAuthenticatedUsers")
        entries = list(dataset.access_entries)
        entries.append(entry)
        dataset.access_entries = entries
        dataset = client.update_dataset(dataset, ["access_entries"])

    # transforms temporary UDF to persistent UDFs and publishes them
    for definition in raw_routine.definitions:
        # Within a standard SQL function, references to other entities require
        # explicit project IDs
        for udf in set(known_udfs):
            # ensure UDF definitions are not replaced twice as would be the case for
            # `mozfun`.stats.mode_last and `mozfun`.stats.mode_last_retain_nulls
            # since one name is a substring of the other
            definition = definition.replace(f"`{project_id}`.{udf}", udf)
            definition = definition.replace(f"{project_id}.{udf}", udf)
            definition = definition.replace(udf, f"`{project_id}`.{udf}")

        # adjust paths for dependencies stored in GCS
        query = OPTIONS_LIB_RE.sub(
            fr'library = "gs://{gcs_bucket}/{gcs_path}\1"', definition
        )

        # add UDF descriptions
        if raw_routine.filepath not in SKIP and not raw_routine.is_stored_procedure:
            # descriptions need to be escaped since quotation marks and other
            # characters, such as \x01, will make the query invalid otherwise
            escaped_description = json.dumps(str(raw_routine.description))
            query = OPTIONS_RE.sub(f"OPTIONS(description={escaped_description},", query)

            if "OPTIONS(" not in query and query[-1] == ";":
                query = query[:-1] + f"OPTIONS(description={escaped_description});"

        print(f"Publish {raw_routine.name}")
        client.query(query).result()


def push_dependencies_to_gcs(bucket, path, dependency_dir, project_id):
    """Upload UDF dependencies to a GCS bucket."""
    client = storage.Client(project_id)
    bucket = client.get_bucket(bucket)

    for root, dirs, files in os.walk(dependency_dir):
        for filename in files:
            blob = bucket.blob(path + filename)
            blob.upload_from_filename(os.path.join(root, filename))


if __name__ == "__main__":
    main()
