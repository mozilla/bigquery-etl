"""Publish UDFs and resources to the public mozfun GCP project."""

from argparse import ArgumentParser
import os
import re

from google.cloud import bigquery
from google.cloud import storage

from bigquery_etl.util import standard_args
from bigquery_etl.udf.parse_udf import read_udf_dirs, accumulate_dependencies

DEFAULT_PROJECT_ID = "moz-fx-data-shared-prod"
DEFAULT_UDF_DIR = ["udf/", "udf_js/"]
DEFAULT_DEPENDENCY_DIR = "udf_js/lib/"
DEFAULT_GCS_BUCKET = "moz-fx-data-prod-bigquery-etl"
DEFAULT_GCS_PATH = ""

OPTIONS_LIB_RE = re.compile(r'library = "gs://[^"]+/([^"]+)"')

SKIP = {"udf/main_summary_scalars/udf.sql"}

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project-id",
    "--project_id",
    default=DEFAULT_PROJECT_ID,
    help="Project to publish UDFs to",
)
parser.add_argument(
    "--udf-dirs",
    "--udf_dirs",
    nargs="+",
    default=DEFAULT_UDF_DIR,
    help="Directory containing UDF definitions",
)
parser.add_argument(
    "--dependency-dir",
    "--dependency_dir",
    default=DEFAULT_DEPENDENCY_DIR,
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
    "--public", default=False, help="The published UDFs should be publicly accessible.",
)
standard_args.add_log_level(parser)


def main():
    """Publish UDFs."""
    args = parser.parse_args()

    client = bigquery.Client(args.project_id)

    if args.dependency_dir and os.path.exists(args.dependency_dir):
        push_dependencies_to_gcs(
            args.gcs_bucket, args.gcs_path, args.dependency_dir, args.project_id
        )

    raw_udfs = read_udf_dirs(*args.udf_dirs)

    published_udfs = []

    for raw_udf in raw_udfs:
        # get all dependencies for UDF and publish as persistent UDF
        udfs_to_publish = accumulate_dependencies([], raw_udfs, raw_udf)
        udfs_to_publish.append(raw_udf)

        for dep in udfs_to_publish:
            if dep not in published_udfs and raw_udfs[dep].filepath not in SKIP:
                publish_udf(
                    raw_udfs[dep],
                    client,
                    args.project_id,
                    args.gcs_bucket,
                    args.gcs_path,
                    raw_udfs.keys(),
                    args.public,
                )
                published_udfs.append(dep)


def publish_udf(
    raw_udf, client, project_id, gcs_bucket, gcs_path, known_udfs, is_public
):
    """Publish a specific UDF to BigQuery."""
    if is_public:
        # create new dataset for UDF if necessary
        dataset = client.create_dataset(raw_udf.dataset, exists_ok=True)

        # set permissions for dataset, public for everyone
        entry = bigquery.AccessEntry("READER", "specialGroup", "allAuthenticatedUsers")
        entries = list(dataset.access_entries)
        entries.append(entry)
        dataset.access_entries = entries
        dataset = client.update_dataset(dataset, ["access_entries"])

    # transforms temporary UDF to persistent UDFs and publishes them
    for definition in raw_udf.definitions:
        # Within a standard SQL function, references to other entities require
        # explicit project IDs

        for udf in known_udfs:
            definition.replace(udf, f"`{project_id}`.{udf}")

        # adjust paths for dependencies stored in GCS
        query = OPTIONS_LIB_RE.sub(
            fr'library = "gs://{gcs_bucket}/{gcs_path}\1"', definition
        )

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
