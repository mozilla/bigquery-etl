"""PyTest configuration."""


import os
import random
import string
import subprocess
from pathlib import Path

import pytest
from google.cloud import bigquery, storage

TEST_BUCKET = "moz-fx-data-integration-tests-bigquery-etl"

pytest_plugins = [
    "bigquery_etl.pytest_plugin.sql",
    "bigquery_etl.pytest_plugin.routine",
    "bigquery_etl.pytest_plugin.script_lint.black",
    "bigquery_etl.pytest_plugin.script_lint.docstyle",
    "bigquery_etl.pytest_plugin.script_lint.flake8",
    "bigquery_etl.pytest_plugin.script_lint.mypy",
]

def pytest_collection_modifyitems(config, items):
    keywordexpr = config.option.keyword
    markexpr = config.option.markexpr
    if keywordexpr or markexpr:
        return

    skip_integration = pytest.mark.skip(reason="integration marker not selected")

    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)

@pytest.fixture
def project_id():
    return os.environ.get("GOOGLE_PROJECT_ID")

@pytest.fixture
def bigquery_client(project_id):
    return bigquery.Client(project_id)

@pytest.fixture
def storage_client():
    yield storage.Client()

@pytest.fixture
def test_bucket(storage_client):
    return storage_client.bucket(TEST_BUCKET)

@pytest.fixture
def temp_path(tmp_path):
    return tmp_path

@pytest.fixture
def temporary_dataset(bigquery_client):
    test_dataset = "test_" + "".join(random.choices(string.ascii_lowercase, k=12))
    bigquery_client.create_dataset(test_dataset)
    yield test_dataset
    bigquery_client.delete_dataset(test_dataset, delete_contents=True, not_found_ok=True)

@pytest.fixture
def temporary_gcs_folder(test_bucket):
    test_folder = "test_" + "".join(random.choices(string.ascii_lowercase, k=12)) + "/"
    yield test_folder
    blobs = test_bucket.list_blobs(prefix=test_folder)
    for blob in blobs:
        blob.delete()

@pytest.fixture
def uploaded_gcs_file(test_bucket, temporary_gcs_folder, temp_path):
    local_file = temp_path / "data.csv"
    local_file.write_text("id,name\n1,test_item")
    blob_name = f"{temporary_gcs_folder}data.csv"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_filename(str(local_file))
    gcs_uri = f"gs://{test_bucket.name}/{blob_name}"
    yield gcs_uri
    blob.delete()

@pytest.fixture
def uploaded_bq_table(bigquery_client, temporary_dataset, uploaded_gcs_file):
    table_name = "test_table_" + "".join(random.choices(string.ascii_lowercase, k=5))
    table_id = f"{temporary_dataset}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE", 
    )
    load_job = bigquery_client.load_table_from_uri(uploaded_gcs_file, table_id, job_config=job_config)
    load_job.result()  
    return table_id

