# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""Automatically generated tests."""

from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from .util import coerce_result, generate_tests

import json
import pytest


@pytest.fixture(scope="session")
def bq():
    return bigquery.Client()


@pytest.fixture(params=list(generate_tests()))
def generated_test(request):
    return request.param


@pytest.fixture
def dataset(bq, generated_test):
    # create dataset
    try:
        bq.get_dataset(generated_test.dataset_id)
    except NotFound:
        bq.create_dataset(generated_test.dataset_id)
    # wait for test
    yield bq.dataset(generated_test.dataset_id)
    # clean up
    bq.delete_dataset(generated_test.dataset_id, delete_contents=True)


@pytest.fixture(autouse=True)
def tables(bq, dataset, generated_test):
    # load tables into dataset
    for table in generated_test.tables.values():
        destination = f"{dataset.dataset_id}.{table.name}"
        job_config = bigquery.LoadJobConfig(
            default_dataset=dataset,
            source_format=table.source_format,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        if table.schema is None:
            # autodetect schema if not provided
            job_config.autodetect = True
        else:
            job_config.schema = table.schema
            # look for time_partitioning_field in provided schema
            for field in job_config.schema:
                if field.description == "time_partitioning_field":
                    job_config.time_partitioning = bigquery.TimePartitioning(
                        field=field.name
                    )
                    break  # stop because there can only be one time partitioning field
        with open(table.source_path, "rb") as file_obj:
            job = bq.load_table_from_file(file_obj, destination, job_config=job_config)
        try:
            job.result()
        except BadRequest:
            print(job.errors)
            raise
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
    job = bq.query(generated_test.modified_query, job_config=job_config)
    result = list(coerce_result(*job.result()))
    result.sort(key=lambda row: json.dumps(row, sort_keys=True))
    generated_test.expect.sort(key=lambda row: json.dumps(row, sort_keys=True))

    assert generated_test.expect == result
