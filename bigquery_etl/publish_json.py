"""
Machinery for exporting query results as JSON to Cloud storage.
"""

from google.cloud import storage
from google.cloud import bigquery

import json
import os
import sys
import re

# sys.path needs to be modified to enable package imports from parent
# and sibling directories. Also see:
# https://stackoverflow.com/questions/6323860/sibling-package-imports/23542795#23542795
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bigquery_etl.parse_metadata import Metadata  # noqa E402


METADATA_FILE = "metadata.yaml"
SUBMISSION_DATE_RE = re.compile(r"^submission_date:DATE:(\d\d\d\d-\d\d-\d\d)$")
QUERY_FILE_RE = re.compile(r"^.*/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)_(v[0-9]+)/query\.sql$")


class JsonPublisher:
    def __init__(self, project_id, query_file, api_version, target_bucket, parameter=None):
        self.project_id = project_id
        self.query_file = query_file
        self.api_version = api_version
        self.target_bucket = target_bucket
        self.parameter = parameter
        self.temp_table = None
        self.date = None

        self.client = bigquery.Client(project_id)
        self.metadata = Metadata.of_sql_file(self.query_file)

        if self.parameter:
            date_search = re.search(SUBMISSION_DATE_RE, self.parameter)

            if date_search:
                self.date = date_search.group(1)

        query_file_re = re.search(QUERY_FILE_RE, self.query_file)
        if query_file_re:
            self.dataset = query_file_re.group(1)
            self.table = query_file_re.group(2)
            self.version = query_file_re.group(3)
        else:
            print("Invalid file naming format: {}", self.query_file)
            sys.exit(1)

    def __exit__(self):
        # if a temporary table has been created, it should be deleted
        if self.temp_table:
            self.client.delete_table(self.temp_table)

    def publish_json(self):
        if self.metadata.is_incremental():
            if self.date is None:
                print("Cannot publish JSON. submission_date missing in parameter.")
                sys.exit(1)

            # if it is an incremental query, then the query result needs to be
            # written to a temporary table to get exported as JSON
            self.write_results_to_temp_table()
            self.publish_table_as_json(self.temp_table)
        else:
            # for non-incremental queries, the entire destination table is exported
            result_table = f"{self.dataset}.{self.table}_{self.version}"
            self.publish_table_as_json(result_table)

    def publish_table_as_json(self, result_table):
        """Export the `result_table` data as JSON to Cloud Storage."""
        prefix = (
            f"api/{self.api_version}/tables/{self.dataset}/"
            f"{self.table}/{self.version}/files/"
        )

        if self.date is not None:
            # if date exists, then query is incremental and newest results are exported
            prefix += f"{self.date}/"

        table_ref = self.client.get_table(result_table)

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = "NEWLINE_DELIMITED_JSON"

        # "*" makes sure that files larger than 1GB get split up into multiple JSON files
        destination_uri = f"gs://{self.target_bucket}/" + prefix + "*.json"
        extract_job = self.client.extract_table(
            table_ref, destination_uri, location="US", job_config=job_config
        )
        extract_job.result()

        self.gcp_convert_ndjson_to_json(prefix)

    def gcp_convert_ndjson_to_json(self, gcp_path):
        """Converts ndjson files on GCP to json files."""
        storage_client = storage.Client()

        blobs = storage_client.list_blobs(self.target_bucket, prefix=gcp_path)

        for blob in blobs:
            content = blob.download_as_string().decode("utf-8").strip()
            json_list = [json.loads(line) for line in content.split("\n")]

            blob.upload_from_string(json.dumps(json_list, indent=2))

    def write_results_to_temp_table(self):
        """
        Write the results of the query to a temporary table and return the table
        name.
        """
        table_date = self.date.replace("-", "")
        self.temp_table = (
            f"{self.project_id}.tmp.{self.table}_{self.version}_{table_date}_temp"
        )

        with open(self.query_file) as query_stream:
            sql = query_stream.read()
            job_config = bigquery.QueryJobConfig(destination=self.temp_table)
            query_job = self.client.query(sql, job_config=job_config)
            query_job.result()
