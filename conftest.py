"""PyTest configuration."""

import os
import random
import string
import subprocess
from pathlib import Path

import pytest
from google.cloud import bigquery, storage

TEST_BUCKET = "bigquery-etl-integration-test-bucket"


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
    """Provide a BigQuery project ID."""
    # GOOGLE_PROJECT_ID needs to be set for integration tests to run
    project_id = os.environ["GOOGLE_PROJECT_ID"]

    return project_id


@pytest.fixture
def bigquery_client():
    """Provide a BigQuery client."""
    project_id = os.environ["GOOGLE_PROJECT_ID"]
    return bigquery.Client(project_id)


@pytest.fixture
def temporary_dataset():
    """Fixture for creating a random temporary BigQuery dataset."""
    # generate a random test dataset to avoid conflicts when running tests in parallel
    test_dataset = "test_" + "".join(
        random.choice(string.ascii_lowercase) for i in range(12)
    )

    project_id = os.environ["GOOGLE_PROJECT_ID"]
    client = bigquery.Client(project_id)
    client.create_dataset(test_dataset)

    yield test_dataset

    # cleanup and remove temporary dataset
    client.delete_dataset(test_dataset, delete_contents=True, not_found_ok=True)


@pytest.fixture
def test_bucket():
    """Provide a test bucket instance."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(TEST_BUCKET)

    yield bucket


@pytest.fixture
def temporary_gcs_folder():
    """Provide a temporary folder in the GCS test bucket."""
    test_folder = (
        "test_"
        + "".join(random.choice(string.ascii_lowercase) for i in range(12))
        + "/"
    )

    yield test_folder

    storage_client = storage.Client()
    bucket = storage_client.bucket(TEST_BUCKET)

    # delete test folder
    blobs = bucket.list_blobs(prefix=test_folder)

    for blob in blobs:
        blob.delete()


@pytest.fixture
def storage_client():
    """Provide a client instance for cloud storage."""
    yield storage.Client()
