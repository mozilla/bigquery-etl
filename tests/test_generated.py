from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from .util import coerce_result, generate_tests

import json
import os
import pytest


@pytest.fixture(scope="session")
def bq():
    return bigquery.Client(project=os.environ.get("GCP_PROJECT", None))


@pytest.fixture(params=list(generate_tests()))
def generated_test(request):
    return request.param


@pytest.fixture
def dataset(bq, generated_test):
    # create dataset
    dataset_id = generated_test.get_dataset_id()
    try:
        bq.get_dataset(dataset_id)
    except NotFound:
        bq.create_dataset(dataset_id)
    # wait for test
    yield bq.dataset(dataset_id)
    # clean up
    try:
        bq.delete_dataset(dataset_id).result()
    except BadRequest as e:
        if str(e).endswith("still in use."):
            pass  # ignore still in use exception on delete_dataset


@pytest.fixture(autouse=True)
def tables(bq, dataset, generated_test):
    # load tables into dataset
    for table in generated_test.tables:
        destination = f"{dataset.dataset_id}.{table.name}"
        job_config = bigquery.LoadJobConfig(
            default_dataset=dataset,
            source_format=table.source_format,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=table.get_schema(),
        )
        if job_config.schema is None:
            # autodetect schema if not provided
            job_config.autodetect = True
        else:
            # look for time_partitioning_field in provided schema
            for field in job_config.schema:
                if field.description == "time_partitioning_field":
                    job_config.time_partitioning = bigquery.TimePartitioning(
                        field=field.name
                    )
                    break  # stop because there can only be one time partitioning field
        with open(table.source_path, "rb") as file_obj:
            job = bq.load_table_from_file(file_obj, destination, job_config=job_config)
        job.result()
    # clean up handled by default_dataset fixture


def test_generated(bq, dataset, generated_test):
    # configure job
    job_config = bigquery.QueryJobConfig(
        default_dataset=dataset,
        destination=bigquery.TableReference(dataset, generated_test.query_name),
        query_parameters=generated_test.query_params,
        use_legacy_sql=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # run query
    job = bq.query(generated_test.get_modified_query(), job_config=job_config)
    result = list(coerce_result(*job.result()))
    result.sort(key=lambda row: json.dumps(row))

    assert result == generated_test.expect
