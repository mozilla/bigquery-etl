"""Generate and upload JSON metadata files for public datasets on GCS."""

from argparse import ArgumentParser
import json
import logging
import os
import re
import smart_open

from google.cloud import storage
from itertools import groupby

from bigquery_etl.metadata.parse_metadata import Metadata
from bigquery_etl.util import standard_args


DEFAULT_BUCKET = "mozilla-public-data-http"
DEFAULT_API_VERSION = "v1"
DEFAULT_ENDPOINT = "https://public-data.telemetry.mozilla.org/"
REVIEW_LINK = "https://bugzilla.mozilla.org/show_bug.cgi?id="
GCS_FILE_PATH_RE = re.compile(
    r"api/(?P<api_version>.+)/tables/(?P<dataset>.+)/(?P<table>.+)/(?P<version>.+)/"
    r"files/(?:(?P<date>.+)/)?(?P<filename>.+\.json(\.gz)?)"
)

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--project_id", "--project-id", default="mozilla-public-data", help="Target project"
)
parser.add_argument(
    "--target-bucket",
    "--target_bucket",
    default=DEFAULT_BUCKET,
    help="GCP bucket JSON data is exported to",
)
parser.add_argument(
    "--endpoint", default=DEFAULT_ENDPOINT, help="The URL to access the HTTP endpoint"
)
parser.add_argument(
    "--api_version",
    "--api-version",
    default=DEFAULT_API_VERSION,
    help="Endpoint API version",
)
parser.add_argument(
    "--target", help="File or directory containing metadata files", default="sql/"
)
standard_args.add_log_level(parser)


class GcsTableMetadata:
    """Metadata associated with table data stored on GCS."""

    def __init__(self, blobs, endpoint, target_dir):
        """Initialize container for metadata of a table published on GCS."""
        assert len(blobs) > 0
        self.blobs = blobs
        self.endpoint = endpoint
        self.files_path = self.blobs[0].name.split("files")[0] + "files"
        self.files_uri = endpoint + self.files_path

        (self.dataset, self.table, self.version) = dataset_table_version_from_gcs_blob(
            self.blobs[0]
        )
        self.metadata = Metadata.of_table(
            self.dataset, self.table, self.version, target_dir
        )

        self.last_updated_path = self.blobs[0].name.split("files")[0] + "last_updated"
        self.last_updated_uri = endpoint + self.last_updated_path

    def table_metadata_to_json(self):
        """Return a JSON object of the table metadata for GCS."""
        metadata_json = {}
        metadata_json["friendly_name"] = self.metadata.friendly_name
        metadata_json["description"] = self.metadata.description
        metadata_json["incremental"] = self.metadata.is_incremental()
        metadata_json["incremental_export"] = self.metadata.is_incremental_export()

        if self.metadata.review_bug() is not None:
            metadata_json["review_link"] = REVIEW_LINK + self.metadata.review_bug()

        metadata_json["files_uri"] = self.files_uri
        metadata_json["last_updated"] = self.last_updated_uri

        return metadata_json

    def files_metadata_to_json(self):
        """Return a JSON object containing metadata of the files on GCS."""
        if self.metadata.is_incremental_export():
            metadata_json = {}

            for blob in self.blobs:
                match = GCS_FILE_PATH_RE.match(blob.name)
                date = match.group("date")

                if date is not None:
                    if date in metadata_json:
                        metadata_json[date].append(self.endpoint + blob.name)
                    else:
                        metadata_json[date] = [self.endpoint + blob.name]

            return metadata_json
        else:
            return [self.endpoint + blob.name for blob in self.blobs]


def dataset_table_version_from_gcs_blob(gcs_blob):
    """Extract the dataset, table and version from the provided GCS blob path."""
    match = GCS_FILE_PATH_RE.match(gcs_blob.name)

    if match is not None:
        return (match.group("dataset"), match.group("table"), match.group("version"))
    else:
        return None


def get_public_gcs_table_metadata(
    storage_client, bucket, api_version, endpoint, target_dir
):
    """Return a list of metadata of public tables and their locations on GCS."""
    prefix = f"api/{api_version}"

    blobs = storage_client.list_blobs(bucket, prefix=prefix)

    return [
        GcsTableMetadata(list(blobs), endpoint, target_dir)
        for table, blobs in groupby(blobs, dataset_table_version_from_gcs_blob)
        if table is not None
    ]


def publish_all_datasets_metadata(table_metadata, output_file):
    """Write metadata about all available public datasets to GCS."""
    metadata_json = {}

    for metadata in table_metadata:
        if metadata.dataset not in metadata_json:
            metadata_json[metadata.dataset] = {}

        dataset = metadata_json[metadata.dataset]

        if metadata.table not in dataset:
            dataset[metadata.table] = {}

        table = dataset[metadata.table]

        if metadata.version not in table:
            table[metadata.version] = {}

        table[metadata.version] = metadata.table_metadata_to_json()

    logging.info(f"Write metadata to {output_file}")

    with smart_open.open(output_file, "w") as fout:
        fout.write(json.dumps(metadata_json, indent=4))


def publish_table_metadata(storage_client, table_metadata, bucket):
    """Write metadata for each public table to GCS."""
    for metadata in table_metadata:
        output_file = f"gs://{bucket}/{metadata.files_path}"

        logging.info(f"Write metadata to {output_file}")
        with smart_open.open(output_file, "w") as fout:
            fout.write(json.dumps(metadata.files_metadata_to_json(), indent=4))

        set_content_type(
            storage_client, bucket, f"{metadata.files_path}", "application/json"
        )


def set_content_type(storage_client, bucket, file, content_type):
    """Set the file content type."""
    blob = storage_client.get_bucket(bucket).get_blob(file)
    blob.content_type = content_type
    blob.patch()


def main():
    """Generate and upload GCS metadata."""
    args = parser.parse_args()
    storage_client = storage.Client(args.project_id)

    # set log level
    try:
        logging.basicConfig(level=args.log_level, format="%(levelname)s %(message)s")
    except ValueError as e:
        parser.error(f"argument --log-level: {e}")

    if os.path.isdir(args.target):
        gcs_table_metadata = get_public_gcs_table_metadata(
            storage_client,
            args.target_bucket,
            args.api_version,
            args.endpoint,
            args.target,
        )

        output_file = f"gs://{args.target_bucket}/all-datasets.json"
        publish_all_datasets_metadata(gcs_table_metadata, output_file)
        set_content_type(
            storage_client, args.target_bucket, "all-datasets.json", "application/json"
        )
        publish_table_metadata(storage_client, gcs_table_metadata, args.target_bucket)
    else:
        print(
            f"Invalid target: {args.target}, target must be a directory with"
            "structure /<dataset>/<table>/metadata.yaml."
        )


if __name__ == "__main__":
    main()
