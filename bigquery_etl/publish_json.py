"""Machinery for exporting query results as JSON to Cloud storage."""

from google.cloud import bigquery
import smart_open
import logging
import sys
import re

from bigquery_etl.parse_metadata import Metadata


METADATA_FILE = "metadata.yaml"
SUBMISSION_DATE_RE = re.compile(r"^submission_date:DATE:(\d\d\d\d-\d\d-\d\d)$")
QUERY_FILE_RE = re.compile(r"^.*/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)_(v[0-9]+)/query\.sql$")
MAX_JSON_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB as max. size of exported JSON files
# maximum number of JSON output files, output file names are only up to 12 characters
MAX_FILE_COUNT = 10_000
# exported file name format: 000000000000.json.gz, 000000000001.json.gz, ...
MAX_JSON_NAME_LENGTH = 12

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s: %(levelname)s: %(message)s"
)


class JsonPublisher:
    """Publishes query results as JSON."""

    def __init__(
        self,
        client,
        storage_client,
        project_id,
        query_file,
        api_version,
        target_bucket,
        parameter=None,
    ):
        """Init JsonPublisher."""
        self.project_id = project_id
        self.query_file = query_file
        self.api_version = api_version
        self.target_bucket = target_bucket
        self.parameter = parameter
        self.client = client
        self.storage_client = storage_client
        self.temp_table = None
        self.date = None
        self.stage_gcs_path = "stage/json/"

        self.metadata = Metadata.of_sql_file(self.query_file)

        # only for incremental exports files are written into separate directories
        # for each date, ignore date parameters for non-incremental exports
        if self.metadata.is_incremental_export() and self.parameter:
            for p in self.parameter:
                date_search = re.search(SUBMISSION_DATE_RE, p)

                if date_search:
                    self.date = date_search.group(1)

        query_file_re = re.search(QUERY_FILE_RE, self.query_file)
        if query_file_re:
            self.dataset = query_file_re.group(1)
            self.table = query_file_re.group(2)
            self.version = query_file_re.group(3)
        else:
            logging.error("Invalid file naming format: {}", self.query_file)
            sys.exit(1)

    def __del__(self):
        """Delete temporary artifacts."""
        if self.temp_table:
            self.client.delete_table(self.temp_table)

        self._clear_stage_directory()

    def _clear_stage_directory(self):
        """Delete files in stage directory."""
        tmp_blobs = self.storage_client.list_blobs(
            self.target_bucket, prefix=self.stage_gcs_path
        )

        for tmp_blob in tmp_blobs:
            tmp_blob.delete()

    def publish_json(self):
        """Publish query results as JSON to GCP Storage bucket."""
        if self.metadata.is_incremental_export():
            if self.date is None:
                logging.error(
                    "Cannot publish JSON. submission_date missing in parameter."
                )
                sys.exit(1)

            # if it is an incremental query, then the query result needs to be
            # written to a temporary table to get exported as JSON
            self._write_results_to_temp_table()
            self._publish_table_as_json(self.temp_table)
        else:
            # for non-incremental queries, the entire destination table is exported
            result_table = f"{self.dataset}.{self.table}_{self.version}"
            self._publish_table_as_json(result_table)

    def _publish_table_as_json(self, result_table):
        """Export the `result_table` data as JSON to Cloud Storage."""
        prefix = (
            f"api/{self.api_version}/tables/{self.dataset}/"
            f"{self.table}/{self.version}/files/"
        )

        if self.date is not None:
            # if date exists, then query is incremental and newest results are exported
            prefix += f"{self.date}/"

        logging.info(f"""Export JSON for {result_table} to {self.stage_gcs_path}""")

        table_ref = self.client.get_table(result_table)

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = "NEWLINE_DELIMITED_JSON"

        # "*" makes sure that files larger than 1GB get split up into JSON files
        # files are written to a stage directory first
        destination_uri = (
            f"gs://{self.target_bucket}/" + self.stage_gcs_path + "*.ndjson"
        )
        extract_job = self.client.extract_table(
            table_ref, destination_uri, location="US", job_config=job_config
        )
        extract_job.result()

        self._gcp_convert_ndjson_to_json(prefix)

    def _gcp_convert_ndjson_to_json(self, gcs_path):
        """Convert ndjson files on GCP to json files."""
        blobs = self.storage_client.list_blobs(
            self.target_bucket, prefix=self.stage_gcs_path
        )
        bucket = self.storage_client.bucket(self.target_bucket)

        # keeps track of the number of bytes written to the JSON output file
        output_size = 0
        # tracks the current output file name
        output_file_counter = 0
        # track if the first JSON object is currently processed
        first_line = True
        # output file handler
        output_file = None

        for blob in blobs:
            blob_path = f"gs://{self.target_bucket}/{blob.name}"

            # stream from GCS
            with smart_open.open(blob_path) as fin:
                for line in fin:
                    # check if JSON output file reached limit
                    if output_file is None or output_size >= MAX_JSON_SIZE:
                        if output_file is not None:
                            output_file.write("]")
                            output_file.close()

                        if output_file_counter >= MAX_FILE_COUNT:
                            logging.error(
                                "Maximum number of JSON output files reached."
                            )
                            sys.exit(1)

                        tmp_blob_name = "/".join(blob.name.split("/")[:-1])
                        tmp_blob_name += (
                            "/"
                            + str(output_file_counter).zfill(MAX_JSON_NAME_LENGTH)
                            + ".json.tmp.gz"
                        )
                        tmp_blob_path = f"gs://{self.target_bucket}/{tmp_blob_name}"

                        logging.info(f"""Write {blob_path} to {tmp_blob_path}""")

                        output_file = smart_open.open(tmp_blob_path, "w")
                        output_file.write("[")
                        first_line = True
                        output_file_counter += 1
                        output_size = 1

                    # skip the first line, it has no preceding json object
                    if not first_line:
                        output_file.write(",")

                    output_file.write(line.replace("\n", ""))
                    output_size += len(line)
                    first_line = False

        output_file.write("]")
        output_file.close()

        # move all files from stage directory to target directory
        tmp_blobs = self.storage_client.list_blobs(
            self.target_bucket, prefix=self.stage_gcs_path
        )

        for tmp_blob in tmp_blobs:
            # only copy gzipped files to target directory
            if "tmp.gz" in tmp_blob.name:
                # remove .tmp from the final file name
                file_name = tmp_blob.name.split("/")[-1].replace("tmp.", "")

                logging.info(f"""Move {tmp_blob.name} to {gcs_path + file_name}""")

                bucket.rename_blob(tmp_blob, gcs_path + file_name)

    def _write_results_to_temp_table(self):
        """Write the query results to a temporary table and return the table name."""
        table_date = self.date.replace("-", "")
        self.temp_table = (
            f"{self.project_id}.tmp.{self.table}_{self.version}_{table_date}_temp"
        )

        with open(self.query_file) as query_stream:
            sql = query_stream.read()

            query_parameters = []
            for p in self.parameter:
                [name, parameter_type, value] = p.split(":")
                query_parameters.append(
                    bigquery.ScalarQueryParameter(name, parameter_type, value)
                )

            job_config = bigquery.QueryJobConfig(
                destination=self.temp_table, query_parameters=query_parameters
            )
            query_job = self.client.query(sql, job_config=job_config)
            query_job.result()
