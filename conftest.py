"""PyTest configuration."""

from google.cloud import bigquery
from google.cloud import storage
import os
import pytest
import random
import string

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

    skip_integration = pytest.mark.skip(
        reason='integration marker not selected'
    )

    for item in items:
        if 'integration' in item.keywords:
            item.add_marker(skip_integration)

@pytest.fixture
def bigquery_client():
    try:
        project_id = os.environ["GOOGLE_PROJECT_ID"]

    # generate a random test dataset to avoid conflicts when running tests in parallel
    test_dataset = "test_" + "".join(random.choice(string.ascii_lowercase) for i in range(10))

def storage_client():
    pass