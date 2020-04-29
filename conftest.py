"""PyTest configuration."""

from google.cloud import bigquery
from google.cloud import storage
import os
import pytest
import random
import string


TEST_BUCKET = "bigquery-etl-integration-test-bucket"


pytest_plugins = [
    "bigquery_etl.pytest_plugin.sql",
    "bigquery_etl.pytest_plugin.udf",
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
    # GOOGLE_PROJECT_ID needs to be set for integration tests to run
    project_id = os.environ["GOOGLE_PROJECT_ID"]

    return project_id


@pytest.fixture
def bigquery_client():
    project_id = os.environ["GOOGLE_PROJECT_ID"]
    return bigquery.Client(project_id)


@pytest.fixture
def temporary_dataset():
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
    storage_client = storage.Client()
    bucket = storage_client.bucket(TEST_BUCKET)

    yield bucket

    # cleanup test bucket
    bucket.delete_blobs(bucket.list_blobs())


@pytest.fixture
def storage_client():
    yield storage.Client()
