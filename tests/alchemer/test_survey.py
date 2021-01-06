import pytest
from uuid import uuid4
from google.cloud import bigquery


@pytest.fixture()
def testing_client():
    bq = bigquery.Client()
    yield bq


@pytest.fixture()
def testing_dataset(testing_client):
    bq = testing_client
    dataset_id = f"test_survey_pytest_{str(uuid4())[:8]}"
    bq.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    dataset = bq.create_dataset(dataset_id)
    yield dataset
    bq.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.fixture()
def testing_table(testing_dataset):
    table_ref = testing_dataset.table(f"survey_testing_table_{str(uuid4())[:8]}")
    yield table


def test_utc_date_to_eastern_time():
    assert False


def test_date_plus_one():
    assert False


def test_format_response():
    assert False


def test_construct_data():
    assert False


def test_get_survey_data():
    assert False


def test_response_schema():
    assert False


def test_insert_to_bq():
    assert False


def test_cli():
    assert False
